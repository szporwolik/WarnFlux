package gios

import (
	"context"
	"errors"
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
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/snapshotutil"
)

// Type is the plugin type name used in the YAML configuration.
const Type = "gios"

const (
	defaultPollInterval   = 6 * time.Hour
	defaultRequestTimeout = 10 * time.Second
	defaultBaseURL        = "https://dane.gios.gov.pl/api/powazne-awarie"

	minPollInterval = 30 * time.Minute
	maxPollInterval = 7 * 24 * time.Hour
	maxTimeout      = time.Minute
)

// Config is the plugin-specific configuration.
type Config struct {
	PollInterval   time.Duration `yaml:"poll_interval"`
	RequestTimeout time.Duration `yaml:"request_timeout"`
	// BaseURL is the GIOŚ PA API root (default: the official endpoint);
	// tests override it with an httptest server.
	BaseURL string `yaml:"base_url"`
	// GeocoderURL is the GUGiK ULDK service used for locality → point
	// lookups (default: the official service).
	GeocoderURL string `yaml:"geocoder_url"`
	// ReverseURL serves the one-shot home-position → voivodeship lookup
	// used only when wojewodztwo is not set (default: Nominatim).
	ReverseURL string `yaml:"reverse_url"`
	// Wojewodztwo is the administrative pre-filter (one of the 16
	// provider names). Empty = derive once from the home position.
	Wojewodztwo string `yaml:"wojewodztwo"`
	// Powiat optionally narrows the administrative filter further.
	Powiat string `yaml:"powiat"`
	// Rok overrides the register year; 0 = the current year.
	Rok int `yaml:"rok"`
	// RadiusKM overrides the geographic radius; 0 = the APRS hub radius.
	RadiusKM float64 `yaml:"radius_km"`
	// CenterLat/CenterLon override the area center; when unset the
	// configured APRS station position is used.
	CenterLat *float64 `yaml:"center_latitude"`
	CenterLon *float64 `yaml:"center_longitude"`
}

// Source polls the GIOŚ serious-accident register and ingests current-year
// accidents in the configured area. Each fetch is a COMPLETE provider
// snapshot (year + administrative scope), so disappearances are reconciled
// against the SQLite active state.
type Source struct {
	cfg       Config
	client    *Client
	geocoder  *Geocoder
	centerLat float64
	centerLon float64
	radiusKM  float64

	// adminScope caches the resolved administrative filter; adminResolved
	// guards the one-shot reverse geocode of the home position.
	mu            sync.Mutex
	adminWoj      string
	adminPowiat   string
	adminResolved bool
}

// New decodes and validates the plugin-specific configuration. hub is the
// shared APRS hub: its configured position and radius define the default
// geographic scope. A nil hub is rejected — without a position there is
// no area.
func New(node *yaml.Node, hub *aprs.Hub) (plugin.SourcePlugin, error) {
	if hub == nil {
		return nil, fmt.Errorf("gios: the APRS hub is disabled, but its position and radius define the geographic scope")
	}
	var cfg Config
	if err := plugin.DecodeConfig(node, &cfg); err != nil {
		return nil, err
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.PollInterval < minPollInterval || cfg.PollInterval > maxPollInterval {
		return nil, fmt.Errorf("poll_interval must be between %s and %s, got %s", minPollInterval, maxPollInterval, cfg.PollInterval)
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}
	if cfg.RequestTimeout <= 0 || cfg.RequestTimeout > maxTimeout {
		return nil, fmt.Errorf("request_timeout must be >0 and at most %s, got %s", maxTimeout, cfg.RequestTimeout)
	}

	baseURL, err := validateServiceURL(cfg.BaseURL, defaultBaseURL, "base_url")
	if err != nil {
		return nil, err
	}
	geocoderURL, err := validateServiceURL(cfg.GeocoderURL, defaultGeocoderURL, "geocoder_url")
	if err != nil {
		return nil, err
	}
	reverseURL, err := validateServiceURL(cfg.ReverseURL, defaultReverseURL, "reverse_url")
	if err != nil {
		return nil, err
	}
	cfg.BaseURL, cfg.GeocoderURL, cfg.ReverseURL = baseURL, geocoderURL, reverseURL

	woj := strings.ToLower(strings.TrimSpace(cfg.Wojewodztwo))
	if woj != "" && !wojewodztwa[woj] {
		return nil, fmt.Errorf("wojewodztwo %q is not one of the 16 provider names", cfg.Wojewodztwo)
	}
	if p := strings.TrimSpace(cfg.Powiat); len(p) > 80 {
		return nil, fmt.Errorf("powiat is %d characters, maximum 80", len(p))
	}
	if cfg.Rok < 0 || cfg.Rok >= 10000 || cfg.Rok != 0 && cfg.Rok < 2017 {
		return nil, fmt.Errorf("rok must be 0 (current year) or a 4-digit year >= 2017, got %d", cfg.Rok)
	}

	centerLat, centerLon := hub.CenterLat(), hub.CenterLon()
	if cfg.CenterLat != nil || cfg.CenterLon != nil {
		if cfg.CenterLat == nil || cfg.CenterLon == nil {
			return nil, fmt.Errorf("center_latitude and center_longitude must be set together")
		}
		centerLat, centerLon = *cfg.CenterLat, *cfg.CenterLon
	}
	radius := cfg.RadiusKM
	if radius == 0 {
		radius = hub.RadiusKM()
	}
	if radius < 1 || radius > 1000 {
		return nil, fmt.Errorf("radius_km must be between 1 and 1000, got %v", radius)
	}

	return &Source{
		cfg:       cfg,
		client:    NewClient(cfg.BaseURL, cfg.RequestTimeout),
		geocoder:  NewGeocoder(cfg.GeocoderURL, cfg.ReverseURL, cfg.RequestTimeout),
		centerLat: centerLat,
		centerLon: centerLon,
		radiusKM:  radius,
	}, nil
}

// validateServiceURL validates an http(s) service URL, applying the
// default when the configured value is empty.
func validateServiceURL(raw, fallback, field string) (string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		s = fallback
	}
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("%s must be a valid http(s) URL with a host, got %q", field, raw)
	}
	return strings.TrimRight(s, "/"), nil
}

// Name returns the plugin type name.
func (s *Source) Name() string { return Type }

// Run polls the register on the configured interval. Transient provider
// errors are logged and never terminate the run.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	reporter, _ := emit.(plugin.SourceHealthReporter)
	slog.Info("gios plugin started",
		"poll_interval", s.cfg.PollInterval, "radius_km", s.radiusKM,
		"center_lat", s.centerLat, "center_lon", s.centerLon)

	for {
		if ctx.Err() != nil {
			break
		}
		err := s.pollOnce(ctx, emit)
		if err != nil && ctx.Err() == nil {
			slog.Warn("gios poll failed", "error", err)
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
	slog.Info("gios plugin stopping")
	return nil
}

// resolveAdminScope returns the administrative filter. A configured
// voivodeship wins; otherwise the home position is reverse-geocoded once
// and the answer cached for the process lifetime.
func (s *Source) resolveAdminScope(ctx context.Context) (wojewodztwo, powiat string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.adminResolved {
		return s.adminWoj, s.adminPowiat, nil
	}
	if s.cfg.Wojewodztwo != "" {
		s.adminWoj, s.adminPowiat = strings.ToLower(strings.TrimSpace(s.cfg.Wojewodztwo)), strings.TrimSpace(s.cfg.Powiat)
		s.adminResolved = true
		return s.adminWoj, s.adminPowiat, nil
	}
	scope, err := s.geocoder.Reverse(ctx, s.centerLat, s.centerLon)
	if err != nil {
		return "", "", fmt.Errorf("derive voivodeship from the home position: %w", err)
	}
	s.adminWoj = scope.Wojewodztwo
	s.adminPowiat = s.cfg.Powiat // powiat stays config-only (provider names differ)
	s.adminResolved = true
	return s.adminWoj, s.adminPowiat, nil
}

// pollOnce fetches the complete snapshot, geocodes and filters it to the
// configured area, emits the surviving identities and reconciles
// disappearances.
func (s *Source) pollOnce(ctx context.Context, emit plugin.Emitter) error {
	now := time.Now()
	woj, powiat, err := s.resolveAdminScope(ctx)
	if err != nil {
		return err
	}
	rok := s.cfg.Rok
	if rok == 0 {
		rok = now.Year()
	}

	records, err := s.client.FetchAwarie(ctx, rok, woj, powiat)
	if err != nil {
		return err
	}

	type pending struct {
		ev    core.HazardEvent
		count int
	}
	seen := make(map[string]*pending, len(records))
	complete := true
	filtered := 0
	for i := range records {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r := records[i]
		point, err := s.geocoder.Geocode(ctx, r.Miejscowosc)
		if err != nil {
			// Geocoding failures are transient provider/network issues,
			// not data problems: skip the entry and refuse to reconcile
			// this round so nothing is cancelled on partial data.
			slog.Warn("gios geocode failed; snapshot marked incomplete", "miejscowosc", r.Miejscowosc, "error", err)
			complete = false
			filtered++
			continue
		}
		if aprs.DistanceKM(s.centerLat, s.centerLon, point.Lat, point.Lon) > s.radiusKM {
			filtered++
			continue
		}
		ev, err := normalize(r, point.Lat, point.Lon, s.cfg.BaseURL)
		if err != nil {
			slog.Debug("gios entry skipped", "entry", i, "reason", err.Error())
			filtered++
			continue
		}
		if p, ok := seen[ev.Key()]; ok {
			p.count++
			complete = false
			slog.Warn("gios feed contains a duplicate identity; snapshot marked incomplete", "event_key", ev.Key())
			continue
		}
		seen[ev.Key()] = &pending{ev: ev, count: 1}
	}

	keys := make(map[string]bool, len(seen))
	for key, p := range seen {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if p.count != 1 {
			continue
		}
		keys[key] = true
		if err := emit.Emit(ctx, p.ev); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Warn("emit failed; retrying on the next poll", "event_key", key, "error", err)
			continue
		}
	}

	if !complete {
		// An ambiguous snapshot must never cancel anything: reconciliation
		// requires the snapshot to be trusted in full.
		return errors.New("gios: snapshot incomplete (geocode failures or duplicate identities); reconciliation skipped")
	}
	if _, err := snapshotutil.Reconcile(ctx, emit, sourceName, keys, now); err != nil && ctx.Err() == nil {
		slog.Warn("gios reconciliation failed", "error", err)
	}
	slog.Info("gios feed processed",
		"rok", rok, "wojewodztwo", woj, "powiat", powiat,
		"entries", len(records), "in_area", len(keys), "filtered", filtered)
	if stats, ok := emit.(plugin.SourceStatsReporter); ok {
		stats.ReportSourceStats(fmt.Sprintf("%d / %d in area (rok %d)", len(keys), len(records), rok))
	}
	if fr, ok := emit.(plugin.SourceFilterReporter); ok {
		fr.ReportSourceFiltered(filtered)
	}
	return nil
}

// Register registers the source plugin type; hub provides the default
// geographic scope.
func Register(reg *plugin.Registry, hub *aprs.Hub) error {
	return reg.RegisterSource(Type, func(node *yaml.Node) (plugin.SourcePlugin, error) {
		return New(node, hub)
	})
}
