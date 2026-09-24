package metar

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// Type is the plugin type name used in the YAML configuration.
const Type = "metar"

const (
	defaultPollInterval   = 15 * time.Minute
	defaultRequestTimeout = 10 * time.Second
	defaultBaseURL        = "https://aviationweather.gov/api/data"

	minPollInterval = 5 * time.Minute
	maxPollInterval = 24 * time.Hour
	maxTimeout      = time.Minute
	maxStations     = 32
)

// Station is one configured airport.
type Station struct {
	// ID is the ICAO station identifier, e.g. "EPKK".
	ID string `yaml:"id"`
	// Name optionally overrides the provider's long airport name for
	// display (e.g. "Balice (EPKK)").
	Name string `yaml:"name"`
	// Timezone is the optional IANA zone for display. METAR reports are
	// UTC-native, so "UTC" is the default.
	Timezone string `yaml:"timezone"`
}

// Config is the plugin-specific configuration.
type Config struct {
	PollInterval   time.Duration `yaml:"poll_interval"`
	RequestTimeout time.Duration `yaml:"request_timeout"`
	// BaseURL is the NOAA AWC API root (default: the official endpoint);
	// tests override it with an httptest server.
	BaseURL string `yaml:"base_url"`
	// Stations lists the airports to observe. The API reports each
	// station's position itself, so no coordinates are configured.
	Stations []Station `yaml:"stations"`
}

// Source polls METARs for the configured airports and publishes each
// observation as a canonical weather information snapshot.
type Source struct {
	cfg    Config
	client *Client
}

// New decodes and validates the plugin-specific configuration. Unlike the
// geo-scoped hazard sources, METAR needs no APRS hub: the feed carries
// the station coordinates.
func New(node *yaml.Node) (plugin.SourcePlugin, error) {
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
	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("base_url must be a valid http(s) URL with a host, got %q", cfg.BaseURL)
	}
	cfg.BaseURL = strings.TrimRight(baseURL, "/")

	if len(cfg.Stations) == 0 {
		return nil, fmt.Errorf("at least one station is required")
	}
	if len(cfg.Stations) > maxStations {
		return nil, fmt.Errorf("at most %d stations are supported, got %d", maxStations, len(cfg.Stations))
	}
	seen := make(map[string]bool, len(cfg.Stations))
	for i := range cfg.Stations {
		id := strings.ToUpper(strings.TrimSpace(cfg.Stations[i].ID))
		if len(id) != 4 {
			return nil, fmt.Errorf("station %d: id must be a 4-letter ICAO code, got %q", i, cfg.Stations[i].ID)
		}
		if seen[id] {
			return nil, fmt.Errorf("duplicate station %q", id)
		}
		seen[id] = true
		cfg.Stations[i].ID = id
		if len(strings.TrimSpace(cfg.Stations[i].Name)) > 512 {
			return nil, fmt.Errorf("station %s: name is too long", id)
		}
		tz := strings.TrimSpace(cfg.Stations[i].Timezone)
		if tz == "" {
			tz = "UTC"
		}
		if _, err := time.LoadLocation(tz); err != nil {
			return nil, fmt.Errorf("station %s: timezone %q is not a valid IANA timezone", id, tz)
		}
		cfg.Stations[i].Timezone = tz
	}

	return &Source{
		cfg:    cfg,
		client: NewClient(cfg.BaseURL, cfg.RequestTimeout),
	}, nil
}

// Name returns the plugin type name.
func (s *Source) Name() string { return Type }

// Run polls the configured airports on the configured interval. Transient
// provider errors are logged and never terminate the run.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	reporter, _ := emit.(plugin.SourceHealthReporter)
	slog.Info("metar plugin started",
		"poll_interval", s.cfg.PollInterval, "stations", stationIDs(s.cfg.Stations))

	for {
		if ctx.Err() != nil {
			break
		}
		err := s.pollOnce(ctx, emit)
		if err != nil && ctx.Err() == nil {
			slog.Warn("metar poll failed", "error", err)
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
	slog.Info("metar plugin stopping")
	return nil
}

// pollOnce fetches one batched METAR response and publishes every
// station observation as a canonical weather snapshot.
func (s *Source) pollOnce(ctx context.Context, emit plugin.Emitter) error {
	ids := make([]string, len(s.cfg.Stations))
	names := make(map[string]string, len(s.cfg.Stations))
	tzs := make(map[string]string, len(s.cfg.Stations))
	for i, st := range s.cfg.Stations {
		ids[i] = st.ID
		names[st.ID] = strings.TrimSpace(st.Name)
		tzs[st.ID] = st.Timezone
	}

	records, err := s.client.Fetch(ctx, ids)
	if err != nil {
		return err
	}

	generated := time.Now().UTC()
	validUntil := generated.Add(2 * s.cfg.PollInterval)

	byID := make(map[string]metarRecord, len(records))
	for _, rec := range records {
		byID[strings.ToUpper(strings.TrimSpace(rec.IcaoID))] = rec
	}

	succeeded := 0
	var firstErr error
	for _, id := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rec, ok := byID[id]
		if !ok {
			slog.Warn("metar: no report for station", "station", id)
			continue
		}
		snapshot, err := normalize(rec, names[id], tzs[id], generated, validUntil)
		if err != nil {
			slog.Warn("metar: station skipped", "station", id, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		message, err := core.NewWeatherInformation("", snapshot)
		if err != nil {
			slog.Warn("metar: weather snapshot rejected", "station", id, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := emit.EmitInformation(ctx, message); err != nil {
			slog.Warn("weather publish failed", "station", id, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		succeeded++
		slog.Info("weather snapshot published", "station", id)
	}

	if stats, ok := emit.(plugin.SourceStatsReporter); ok {
		stats.ReportSourceStats(fmt.Sprintf("%d / %d stations published", succeeded, len(ids)))
	}
	if succeeded == 0 {
		if firstErr != nil {
			return firstErr
		}
		return errors.New("metar: no station observations published")
	}
	return nil
}

func stationIDs(stations []Station) []string {
	out := make([]string, len(stations))
	for i, st := range stations {
		out[i] = st.ID
	}
	return out
}

// Register registers the source plugin type. METAR needs no APRS hub:
// the feed carries the station coordinates.
func Register(reg *plugin.Registry) error {
	return reg.RegisterSource(Type, New)
}
