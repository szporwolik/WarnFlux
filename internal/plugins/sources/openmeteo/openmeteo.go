// Package openmeteo implements the Open-Meteo informational source plugin:
// it periodically fetches ordinary weather data for configured locations
// and publishes the latest weather snapshot to retained MQTT information
// topics. Weather is INFORMATION, not a HazardEvent: nothing here touches
// the hazard journal, lifecycle or cursors.
package openmeteo

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// Type is the plugin type name used in the YAML configuration.
const Type = "openmeteo"

const (
	defaultPollInterval   = 15 * time.Minute
	defaultRequestTimeout = 10 * time.Second
	defaultForecastHours  = 48
	defaultForecastDays   = 7

	minPollInterval   = time.Minute
	maxPollInterval   = 24 * time.Hour
	maxRequestTimeout = 5 * time.Minute
	minForecastHours  = 1
	maxForecastHours  = 168
	minForecastDays   = 1
	maxForecastDays   = 16
	maxLocations      = 64
	maxLocationName   = 256
	maxAPIKeyFileSize = 64 * 1024
)

// locationSlugRE is the accepted shape of location IDs. They become MQTT
// topic segments, so the rules match WarnFlux's plugin ID slug style.
var locationSlugRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// Location is one configured weather location.
type Location struct {
	ID        string  `yaml:"id"`
	Name      string  `yaml:"name"`
	Latitude  float64 `yaml:"latitude"`
	Longitude float64 `yaml:"longitude"`
}

// Config is the plugin-specific configuration.
type Config struct {
	PollInterval   time.Duration `yaml:"poll_interval"`
	RequestTimeout time.Duration `yaml:"request_timeout"`
	ForecastHours  int           `yaml:"forecast_hours"`
	ForecastDays   int           `yaml:"forecast_days"`

	// APIKey / APIKeyFile are mutually exclusive. The public API needs no
	// key; a key selects the commercial customer endpoint unless BaseURL
	// is set explicitly. Credentials are never logged.
	APIKey     string `yaml:"api_key"`
	APIKeyFile string `yaml:"api_key_file"`

	// BaseURL overrides the provider endpoint (testing/custom deployments).
	BaseURL string `yaml:"base_url"`

	Locations []Location `yaml:"locations"`
}

// Source is the Open-Meteo source plugin instance.
type Source struct {
	cfg    Config
	key    string // resolved API key; never logged
	client *Client
	now    func() time.Time
}

// New decodes and validates the plugin-specific configuration.
func New(node *yaml.Node) (plugin.SourcePlugin, error) {
	var cfg Config
	if err := plugin.DecodeConfig(node, &cfg); err != nil {
		return nil, err
	}
	return newSource(cfg)
}

func newSource(cfg Config) (*Source, error) {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}
	if cfg.ForecastHours == 0 {
		cfg.ForecastHours = defaultForecastHours
	}
	if cfg.ForecastDays == 0 {
		cfg.ForecastDays = defaultForecastDays
	}

	if cfg.PollInterval < minPollInterval || cfg.PollInterval > maxPollInterval {
		return nil, fmt.Errorf("poll_interval must be between %s and %s, got %s", minPollInterval, maxPollInterval, cfg.PollInterval)
	}
	if cfg.RequestTimeout <= 0 || cfg.RequestTimeout > maxRequestTimeout {
		return nil, fmt.Errorf("request_timeout must be positive and at most %s, got %s", maxRequestTimeout, cfg.RequestTimeout)
	}
	if cfg.ForecastHours < minForecastHours || cfg.ForecastHours > maxForecastHours {
		return nil, fmt.Errorf("forecast_hours must be %d..%d, got %d", minForecastHours, maxForecastHours, cfg.ForecastHours)
	}
	if cfg.ForecastDays < minForecastDays || cfg.ForecastDays > maxForecastDays {
		return nil, fmt.Errorf("forecast_days must be %d..%d, got %d", minForecastDays, maxForecastDays, cfg.ForecastDays)
	}
	if cfg.APIKey != "" && cfg.APIKeyFile != "" {
		return nil, fmt.Errorf("api_key and api_key_file are mutually exclusive")
	}
	if len(cfg.Locations) == 0 {
		return nil, fmt.Errorf("at least one location is required")
	}
	if len(cfg.Locations) > maxLocations {
		return nil, fmt.Errorf("at most %d locations are supported, got %d", maxLocations, len(cfg.Locations))
	}
	seen := make(map[string]bool, len(cfg.Locations))
	for _, loc := range cfg.Locations {
		if !locationSlugRE.MatchString(loc.ID) {
			return nil, fmt.Errorf("location id must be a lowercase slug matching %s, got %q", locationSlugRE, loc.ID)
		}
		if seen[loc.ID] {
			return nil, fmt.Errorf("duplicate location id %q", loc.ID)
		}
		seen[loc.ID] = true
		if len(loc.Name) > maxLocationName {
			return nil, fmt.Errorf("location %q: name is %d bytes, maximum %d", loc.ID, len(loc.Name), maxLocationName)
		}
		if math.IsNaN(loc.Latitude) || math.IsInf(loc.Latitude, 0) || loc.Latitude < -90 || loc.Latitude > 90 {
			return nil, fmt.Errorf("location %q: latitude must be finite and within -90..90, got %v", loc.ID, loc.Latitude)
		}
		if math.IsNaN(loc.Longitude) || math.IsInf(loc.Longitude, 0) || loc.Longitude < -180 || loc.Longitude > 180 {
			return nil, fmt.Errorf("location %q: longitude must be finite and within -180..180, got %v", loc.ID, loc.Longitude)
		}
	}

	key := cfg.APIKey
	if key == "" && cfg.APIKeyFile != "" {
		info, err := os.Stat(cfg.APIKeyFile)
		if err != nil {
			return nil, fmt.Errorf("stat api_key_file: %w", err)
		}
		if info.Size() > maxAPIKeyFileSize {
			return nil, fmt.Errorf("api_key_file is %d bytes, maximum %d", info.Size(), maxAPIKeyFileSize)
		}
		data, err := os.ReadFile(cfg.APIKeyFile)
		if err != nil {
			return nil, fmt.Errorf("read api_key_file: %w", err)
		}
		key = strings.TrimRight(string(data), "\r\n")
	}

	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		if key != "" {
			baseURL = customerBaseURL
		} else {
			baseURL = defaultBaseURL
		}
	}
	// base_url is permanent configuration: a malformed value must fail
	// startup instead of producing a warning every poll forever.
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("base_url must be a valid http(s) URL with a host, got %q", cfg.BaseURL)
	}

	return &Source{
		cfg:    cfg,
		key:    key,
		client: NewClient(baseURL, key, cfg.RequestTimeout),
		now:    time.Now,
	}, nil
}

// Name returns the plugin type name.
func (s *Source) Name() string { return Type }

// Run fetches weather immediately and then schedules every poll via a
// timer (not a fixed ticker), so a provider Retry-After can lengthen the
// next poll without blocking other locations. Transient provider errors
// are logged and never terminate the run.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	reporter, _ := emit.(plugin.SourceHealthReporter)
	slog.Info("openmeteo plugin started",
		"poll_interval", s.cfg.PollInterval, "locations", len(s.cfg.Locations))

	for {
		if ctx.Err() != nil {
			break
		}
		next := s.pollOnce(ctx, emit, reporter)
		timer := time.NewTimer(next)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	slog.Info("openmeteo plugin stopping")
	return nil
}

// pollOnce fetches and publishes every configured location once and
// returns the delay until the next poll: the configured poll interval,
// extended by any provider Retry-After (clamped). One failing location
// never suppresses the others.
func (s *Source) pollOnce(ctx context.Context, emit plugin.Emitter, reporter plugin.SourceHealthReporter) time.Duration {
	next := s.cfg.PollInterval
	succeeded := 0
	var firstErr error

	for _, loc := range s.cfg.Locations {
		if ctx.Err() != nil {
			break
		}
		message, err := s.fetchLocation(ctx, loc)
		if err != nil {
			if ctx.Err() != nil {
				// Shutting down: the cancellation is not a provider
				// failure and needs no warning.
				break
			}
			slog.Warn("location fetch failed", "location", loc.ID, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			// A rate-limited location lengthens the NEXT whole poll; the
			// remaining locations still run in this poll.
			if after, ok := retryAfter(err); ok && after > next {
				if after > maxRetryAfter {
					after = maxRetryAfter
				}
				next = after
			}
			continue
		}
		succeeded++
		if err := emit.EmitInformation(ctx, message); err != nil {
			slog.Warn("weather publish failed", "location", loc.ID, "error", err)
			continue
		}
		slog.Info("weather snapshot published", "location", loc.ID)
	}

	if reporter != nil {
		if succeeded == 0 && firstErr != nil {
			// Every location failed: the provider is not reachable.
			reporter.ReportSourceDegraded(firstErr)
		} else if succeeded > 0 {
			// At least one location succeeded: the source is operational;
			// individual failures are already logged above.
			reporter.ReportSourceHealthy()
		}
	}
	return next
}

// fetchLocation performs one provider request and builds the canonical
// weather information message for the location. valid_until is application
// freshness metadata (generated_at + 2×poll_interval), NOT a provider
// forecast validity guarantee.
func (s *Source) fetchLocation(ctx context.Context, loc Location) (core.InformationMessage, error) {
	resp, err := s.client.Fetch(ctx, loc, s.cfg.ForecastHours, s.cfg.ForecastDays)
	if err != nil {
		return core.InformationMessage{}, err
	}
	generated := s.now().UTC()
	validUntil := generated.Add(2 * s.cfg.PollInterval)
	snapshot, err := Normalize(loc, resp, generated)
	if err != nil {
		return core.InformationMessage{}, err
	}
	snapshot.ValidUntil = &validUntil
	// ProducerID is stamped by the manager from the configured source ID;
	// the plugin cannot know or choose it.
	return core.NewWeatherInformation("", snapshot)
}

// Register registers the openmeteo source plugin type.
func Register(reg *plugin.Registry) error {
	return reg.RegisterSource(Type, New)
}
