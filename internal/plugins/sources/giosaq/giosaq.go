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
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

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

	minPollInterval   = 2 * time.Minute
	maxPollInterval   = 24 * time.Hour
	maxRequestTimeout = time.Minute
	// maxRecords bounds the parsed page (the provider pages by 20; more
	// than this is a transport/parsing risk).
	maxRecords = 500
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
}

// Source polls the GIOŚ exceedance feed and ingests official air-quality
// exceedances. The station directory (coordinates) is cached and
// refreshed on the configured interval.
type Source struct {
	cfg       Config
	sev       SeverityConfig
	client    *Client
	zones     []string
	stationMu sync.Mutex
	stations  map[string]stationRef
	refreshed time.Time
}

// New decodes and validates the plugin-specific configuration. The APRS
// hub is not required: the feed already carries the zone and the station
// directory provides coordinates.
func New(node *yaml.Node) (plugin.SourcePlugin, error) {
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

	return &Source{
		cfg:      cfg,
		sev:      sev,
		client:   NewClient(cfg.BaseURL, cfg.RequestTimeout),
		zones:    zones,
		stations: make(map[string]stationRef),
	}, nil
}

// Name returns the plugin type name.
func (s *Source) Name() string { return Type }

// Run polls the feed on the configured interval. Transient provider
// errors are logged and never terminate the run.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	reporter, _ := emit.(plugin.SourceHealthReporter)
	slog.Info("giosaq plugin started",
		"poll_interval", s.cfg.PollInterval, "max_age", s.cfg.MaxAge,
		"zones", s.cfg.Zones, "severities", s.sev)

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

// Register registers the source plugin type.
func Register(reg *plugin.Registry) error {
	return reg.RegisterSource(Type, func(node *yaml.Node) (plugin.SourcePlugin, error) {
		return New(node)
	})
}
