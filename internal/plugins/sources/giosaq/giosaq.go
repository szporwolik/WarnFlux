// Package giosaq is a hazard source for the official GIOŚ (Chief
// Inspectorate of Environmental Protection) air-quality exceedance feed:
// active information about exceedances of the information, alarm,
// admissible and target levels (O3, PM10, PM2.5, SO2, NO2, ...). One
// record becomes one WarnFlux hazard event (category environment), with
// the official norm type mapped onto a canonical severity, the affected
// zone as the area, and — when the measurement station is found in the
// GIOŚ station directory — coordinates so the home map can draw it.
//
// The feed is a paginated archive (newest first); the plugin reads only
// the first page and applies its own recency filter, so historical
// records never re-surface as alerts.
package giosaq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/severity"
)

// Type is the plugin type name used in the YAML configuration.
const Type = "giosaq"

const (
	defaultPollInterval    = 5 * time.Minute // the levels endpoint allows 2 req/min
	defaultRequestTimeout  = 20 * time.Second
	defaultStationsRefresh = 24 * time.Hour
	defaultMaxAge          = 24 * time.Hour
	defaultBaseURL         = "https://api.gios.gov.pl/pjp-api"
	// defaultStationPollInterval publishes the current air-quality index
	// of the stations in the area (one request per station).
	defaultStationPollInterval = 15 * time.Minute

	minPollInterval   = 2 * time.Minute
	maxPollInterval   = 24 * time.Hour
	maxRequestTimeout = time.Minute
	// maxRecords bounds the parsed page (the provider pages by 20; more
	// than this is a transport/parsing risk).
	maxRecords = 500
	// maxStations bounds how many stations get one index request per
	// informational poll.
	maxStations = 64
)

// SeverityConfig maps the official norm-type keywords onto canonical
// WarnFlux severities.
type SeverityConfig struct {
	// Alarm matches "alarmowy" (alarm level).
	Alarm string
	// Info matches "informowania" (information level).
	Info string
	// Limit matches "dopuszczalnego"/"docelowego" (admissible/target).
	Limit string
}

// Config is the plugin-specific configuration.
type Config struct {
	PollInterval   time.Duration `yaml:"poll_interval"`
	RequestTimeout time.Duration `yaml:"request_timeout"`
	// BaseURL is the API root (default: the official GIOŚ PJP API);
	// tests override it with an httptest server.
	BaseURL string `yaml:"base_url"`
	// MaxAge bounds how old an exceedance record may be to become an
	// alert; 0 disables the cutoff (everything on the first page).
	MaxAge time.Duration `yaml:"max_age"`
	// Zones filters records by zone-name substrings (case-insensitive);
	// empty = every zone.
	Zones []string `yaml:"zones"`
	// AlarmSeverity/InfoSeverity/LimitSeverity map the norm type onto
	// canonical severities (defaults: severe/moderate/minor).
	AlarmSeverity string `yaml:"alarm_severity"`
	InfoSeverity  string `yaml:"info_severity"`
	LimitSeverity string `yaml:"limit_severity"`
	// StationsRefresh re-downloads the station directory (coordinates
	// for the map) at most this often.
	StationsRefresh time.Duration `yaml:"stations_refresh"`
	// AirIndex publishes the current air-quality index of every station
	// in the area as an informational map layer (kind air_quality).
	// Requires the APRS hub (the area center/radius come from it).
	AirIndex bool `yaml:"air_index"`
	// StationPollInterval bounds how often the station indexes refresh.
	StationPollInterval time.Duration `yaml:"station_poll_interval"`
	// StationRadiusKM overrides the area radius for the station layer;
	// 0 = the APRS hub area radius.
	StationRadiusKM float64 `yaml:"station_radius_km"`
	// CenterLat/CenterLon override the area center for the station layer;
	// unset = the APRS hub area center.
	CenterLat *float64 `yaml:"center_latitude"`
	CenterLon *float64 `yaml:"center_longitude"`
}

// Source polls the GIOŚ exceedance feed and ingests official air-quality
// exceedances. The station directory (coordinates) is cached and
// refreshed on the configured interval; with air_index enabled the
// current index of the stations in the area is published as an
// informational map layer.
type Source struct {
	cfg       Config
	sev       SeverityConfig
	client    *Client
	zones     []string
	stationMu sync.Mutex
	stations  map[string]stationRef
	refreshed time.Time
	// area scope for the station layer (air_index).
	centerLat, centerLon, radiusKM float64
	airIndex                       bool
	lastStationPoll                time.Time
}

// New decodes and validates the plugin-specific configuration. The APRS
// hub is optional: the exceedance alerts work without it (the feed
// carries the zone), but the station air-quality layer (air_index)
// needs its area center and radius.
func New(node *yaml.Node, hub *aprs.Hub) (plugin.SourcePlugin, error) {
	var cfg Config
	if err := plugin.DecodeConfig(node, &cfg); err != nil {
		return nil, err
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.PollInterval < minPollInterval || cfg.PollInterval > maxPollInterval {
		return nil, fmt.Errorf("giosaq: poll_interval must be between %s and %s, got %s", minPollInterval, maxPollInterval, cfg.PollInterval)
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}
	if cfg.RequestTimeout <= 0 || cfg.RequestTimeout > maxRequestTimeout {
		return nil, fmt.Errorf("giosaq: request_timeout must be >0 and at most %s, got %s", maxRequestTimeout, cfg.RequestTimeout)
	}
	if cfg.MaxAge == 0 {
		cfg.MaxAge = defaultMaxAge
	}
	if cfg.MaxAge < 0 {
		return nil, fmt.Errorf("giosaq: max_age must be 0 (disabled) or positive, got %s", cfg.MaxAge)
	}
	if cfg.StationsRefresh == 0 {
		cfg.StationsRefresh = defaultStationsRefresh
	}
	if cfg.StationsRefresh < 0 {
		return nil, fmt.Errorf("giosaq: stations_refresh must be 0 (disabled) or positive, got %s", cfg.StationsRefresh)
	}
	if cfg.StationPollInterval == 0 {
		cfg.StationPollInterval = defaultStationPollInterval
	}
	if cfg.StationPollInterval < minPollInterval || cfg.StationPollInterval > maxPollInterval {
		return nil, fmt.Errorf("giosaq: station_poll_interval must be between %s and %s, got %s", minPollInterval, maxPollInterval, cfg.StationPollInterval)
	}

	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("giosaq: base_url must be a valid http(s) URL with a host, got %q", cfg.BaseURL)
	}
	cfg.BaseURL = strings.TrimRight(baseURL, "/")

	sev := SeverityConfig{
		Alarm: strings.TrimSpace(cfg.AlarmSeverity),
		Info:  strings.TrimSpace(cfg.InfoSeverity),
		Limit: strings.TrimSpace(cfg.LimitSeverity),
	}
	if sev.Alarm == "" {
		sev.Alarm = severity.Severe
	}
	if sev.Info == "" {
		sev.Info = severity.Moderate
	}
	if sev.Limit == "" {
		sev.Limit = severity.Minor
	}
	for _, v := range []string{sev.Alarm, sev.Info, sev.Limit} {
		if _, ok := severity.Rank(v); !ok {
			return nil, fmt.Errorf("giosaq: unknown severity %q (want unknown/minor/moderate/severe/extreme)", v)
		}
	}

	zones := make([]string, 0, len(cfg.Zones))
	for _, z := range cfg.Zones {
		if v := strings.TrimSpace(z); v != "" {
			zones = append(zones, strings.ToLower(v))
		}
	}

	src := &Source{
		cfg:      cfg,
		sev:      sev,
		client:   NewClient(cfg.BaseURL, cfg.RequestTimeout),
		zones:    zones,
		stations: make(map[string]stationRef),
	}

	// The station air-quality layer is scoped to the APRS hub area (with
	// the usual per-plugin overrides), exactly like the geo-scoped feeds.
	if hub != nil {
		src.centerLat, src.centerLon = hub.AreaLat(), hub.AreaLon()
		if cfg.CenterLat != nil || cfg.CenterLon != nil {
			if cfg.CenterLat == nil || cfg.CenterLon == nil {
				return nil, fmt.Errorf("giosaq: center_latitude and center_longitude must be set together")
			}
			src.centerLat, src.centerLon = *cfg.CenterLat, *cfg.CenterLon
		}
		src.radiusKM = cfg.StationRadiusKM
		if src.radiusKM == 0 {
			src.radiusKM = hub.AreaRadius()
		}
		if src.radiusKM < 1 || src.radiusKM > 1000 {
			return nil, fmt.Errorf("giosaq: station_radius_km must be between 1 and 1000, got %v", src.radiusKM)
		}
		src.airIndex = cfg.AirIndex
	} else if cfg.AirIndex {
		slog.Warn("giosaq: air_index requires the APRS hub (its area defines the station scope); the station layer stays disabled")
	}

	return src, nil
}

// Name returns the plugin type name.
func (s *Source) Name() string { return Type }

// Run polls the exceedance feed on the configured interval and, with
// air_index enabled, refreshes the station air-quality layer on its own
// interval. Transient provider errors are logged and never terminate the
// run.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	reporter, _ := emit.(plugin.SourceHealthReporter)
	slog.Info("giosaq plugin started",
		"poll_interval", s.cfg.PollInterval, "max_age", s.cfg.MaxAge,
		"zones", s.cfg.Zones, "severities", s.sev,
		"air_index", s.airIndex, "station_poll_interval", s.cfg.StationPollInterval)

	for {
		if ctx.Err() != nil {
			break
		}
		err := s.pollOnce(ctx, emit)
		if err != nil && ctx.Err() == nil {
			slog.Warn("giosaq poll failed", "error", err)
			if reporter != nil {
				reporter.ReportSourceDegraded(err)
			}
		} else if err == nil && reporter != nil {
			reporter.ReportSourceHealthy()
		}

		if s.airIndex && time.Since(s.lastStationPoll) >= s.cfg.StationPollInterval {
			if err := s.pollStations(ctx, emit); err != nil && ctx.Err() == nil {
				slog.Warn("giosaq station poll failed", "error", err)
			}
		}

		timer := time.NewTimer(s.cfg.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	slog.Info("giosaq plugin stopping")
	return nil
}

// pollOnce fetches the newest page of exceedance records, filters it by
// zone and recency, geocodes the survivors through the station directory
// and emits them. The feed is a history, not a current-state snapshot, so
// disappearances are NOT reconciled: events retire through their own
// expiry.
func (s *Source) pollOnce(ctx context.Context, emit plugin.Emitter) error {
	records, _, err := s.client.FetchLevels(ctx)
	if err != nil {
		return err
	}
	if len(records) > maxRecords {
		return fmt.Errorf("feed page has %d records, bound is %d", len(records), maxRecords)
	}

	if err := s.refreshStations(ctx); err != nil {
		// Coordinates are a nice-to-have: the alert still routes by area
		// when the directory is unavailable.
		slog.Warn("giosaq station directory refresh failed", "error", err)
	}

	now := time.Now()
	emitted, filtered, stale := 0, 0, 0
	for i := range records {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rec := records[i]
		if !s.zoneAllowed(rec.Zone) {
			filtered++
			continue
		}
		ev, err := normalize(rec, s.sev, s.cfg.BaseURL)
		if err != nil {
			filtered++
			slog.Debug("giosaq record skipped", "record", i, "reason", err.Error())
			continue
		}
		if ev.EffectiveAt != nil {
			if s.cfg.MaxAge > 0 && now.Sub(*ev.EffectiveAt) > s.cfg.MaxAge {
				stale++
				continue
			}
			// Expire shortly after the feed would stop carrying the record
			// as fresh; the provider keeps hours-long durations.
			expiry := ev.EffectiveAt.Add(24 * time.Hour)
			ev.ExpiresAt = &expiry
		}
		s.geocode(&ev, rec.Station)
		if err := emit.Emit(ctx, ev); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Warn("giosaq emit failed; retrying on the next poll", "event_key", ev.Key(), "error", err)
			continue
		}
		emitted++
	}

	slog.Info("giosaq feed processed",
		"records", len(records), "emitted", emitted, "filtered", filtered, "stale", stale)
	if stats, ok := emit.(plugin.SourceStatsReporter); ok {
		stats.ReportSourceStats(fmt.Sprintf("%d alerts", emitted))
	}
	if fr, ok := emit.(plugin.SourceFilterReporter); ok {
		fr.ReportSourceFiltered(filtered + stale)
	}
	return nil
}

// zoneAllowed reports whether a record's zone matches the configured
// zone filter (empty filter = everything).
func (s *Source) zoneAllowed(zone string) bool {
	if len(s.zones) == 0 {
		return true
	}
	z := strings.ToLower(strings.TrimSpace(zone))
	for _, want := range s.zones {
		if strings.Contains(z, want) {
			return true
		}
	}
	return false
}

// geocode attaches station coordinates to the event when the station
// directory knows the measurement station. The record names the station
// with a sensor suffix (e.g. "MpSzarowSpok-O3-1g"); the directory key is
// the bare station code.
func (s *Source) geocode(ev *core.HazardEvent, stationName string) {
	code := strings.TrimSpace(stationName)
	if idx := strings.IndexAny(code, "-"); idx > 0 {
		code = code[:idx]
	}
	s.stationMu.Lock()
	ref, ok := s.stations[code]
	s.stationMu.Unlock()
	if !ok {
		return
	}
	ev.Latitude = &ref.Lat
	ev.Longitude = &ref.Lon
	if ref.Name != "" {
		if ev.Description != "" {
			ev.Description += "\n"
		}
		ev.Description += "Stacja pomiarowa: " + ref.Name
	}
}

// refreshStations reloads the station directory when the configured
// refresh interval has elapsed.
func (s *Source) refreshStations(ctx context.Context) error {
	if s.cfg.StationsRefresh <= 0 {
		return nil
	}
	s.stationMu.Lock()
	stale := time.Since(s.refreshed) >= s.cfg.StationsRefresh
	s.stationMu.Unlock()
	if !stale {
		return nil
	}
	stations, err := s.client.FetchStations(ctx)
	if err != nil {
		return err
	}
	s.stationMu.Lock()
	s.stations = stations
	s.refreshed = time.Now()
	s.stationMu.Unlock()
	slog.Info("giosaq station directory refreshed", "stations", len(stations))
	return nil
}

// pollStations publishes the current air-quality index of every station
// directory entry inside the configured area as one air_quality
// informational message each. One station = one request; the poll is
// bounded by maxStations.
func (s *Source) pollStations(ctx context.Context, emit plugin.Emitter) error {
	if err := s.refreshStations(ctx); err != nil {
		return err
	}
	s.stationMu.Lock()
	refs := make([]stationRef, 0, len(s.stations))
	for _, ref := range s.stations {
		refs = append(refs, ref)
	}
	s.stationMu.Unlock()

	published := 0
	for _, ref := range refs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if aprs.DistanceKM(s.centerLat, s.centerLon, ref.Lat, ref.Lon) > s.radiusKM {
			continue
		}
		idx, err := s.client.FetchAQIndex(ctx, ref.ID)
		if err != nil {
			slog.Warn("giosaq station index fetch failed", "station", ref.Name, "error", err)
			continue
		}
		msg, err := aqSnapshot(ref, idx)
		if err != nil {
			slog.Warn("giosaq station snapshot skipped", "station", ref.Name, "error", err)
			continue
		}
		if err := emit.EmitInformation(ctx, msg); err != nil && ctx.Err() == nil {
			slog.Warn("giosaq station snapshot emit failed", "station", ref.Name, "error", err)
			continue
		}
		published++
		if published >= maxStations {
			break
		}
	}
	s.lastStationPoll = time.Now()
	slog.Info("giosaq station layer published", "stations", published)
	return nil
}

// aqSnapshot builds one air_quality information message from a station
// reference and its official index.
func aqSnapshot(ref stationRef, idx aqIndex) (core.InformationMessage, error) {
	payload := aqPayload{
		SchemaVersion:  1,
		StationCode:    strings.ToLower(strings.TrimSpace(ref.Code)),
		StationName:    ref.Name,
		Latitude:       ref.Lat,
		Longitude:      ref.Lon,
		IndexLevelID:   idx.IndexLevelID,
		IndexLevelName: idx.IndexLevelName,
		GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
	}
	for _, p := range []aqPollutant{
		{Code: "PM10", LevelID: idx.PM10LevelID, LevelName: idx.PM10LevelName},
		{Code: "PM2.5", LevelID: idx.PM25LevelID, LevelName: idx.PM25LevelName},
		{Code: "NO2", LevelID: idx.NO2LevelID, LevelName: idx.NO2LevelName},
		{Code: "SO2", LevelID: idx.SO2LevelID, LevelName: idx.SO2LevelName},
		{Code: "O3", LevelID: idx.O3LevelID, LevelName: idx.O3LevelName},
	} {
		if p.LevelID != nil || p.LevelName != "" {
			payload.Pollutants = append(payload.Pollutants, p)
		}
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return core.InformationMessage{}, fmt.Errorf("giosaq: marshal station snapshot: %w", err)
	}
	key := payload.StationCode
	if key == "" {
		return core.InformationMessage{}, fmt.Errorf("giosaq: station %q has no usable code", ref.Name)
	}
	return core.InformationMessage{
		Source:      sourceName,
		ProducerID:  "", // stamped by the manager at the source boundary
		Key:         key,
		Kind:        "air_quality",
		GeneratedAt: time.Now().UTC(),
		Payload:     data,
	}, nil
}

// Register registers the source plugin type; hub optionally provides the
// area scope for the station air-quality layer.
func Register(reg *plugin.Registry, hub *aprs.Hub) error {
	return reg.RegisterSource(Type, func(node *yaml.Node) (plugin.SourcePlugin, error) {
		return New(node, hub)
	})
}
