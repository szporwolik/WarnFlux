package adsb

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// Type is the plugin type name used in the YAML configuration.
const Type = "adsb"

// Defaults and bounds.
const (
	defaultPollInterval   = 10 * time.Second
	defaultRequestTimeout = 15 * time.Second
	defaultBaseAdsbLol    = "https://api.adsb.lol"
	defaultTrackWindow    = 5 * time.Minute

	minPollInterval = 5 * time.Second
	maxPollInterval = 5 * time.Minute
	minTrackWindow  = time.Minute
	maxTrackWindow  = 15 * time.Minute
	maxTimeout      = time.Minute
	maxRadiusKm     = 500
)

// Config is the plugin-specific configuration.
type Config struct {
	// Provider is the data source: "adsblol" (keyless internet API) or
	// "tar1090" (local readsb/tar1090 receiver — works without internet).
	Provider string `yaml:"provider"`
	// BaseURL overrides the provider endpoint (tests; also a local
	// tar1090 address such as http://10.0.0.20:8080).
	BaseURL string `yaml:"base_url"`
	// Latitude/Longitude override the area center (default: the APRS hub
	// position, so the coverage follows the configured radius).
	Latitude  *float64 `yaml:"latitude"`
	Longitude *float64 `yaml:"longitude"`
	// RadiusKM overrides the area radius (default: the APRS hub radius).
	RadiusKM float64 `yaml:"radius_km"`
	// PollInterval bounds one poll cycle. adsb.lol rate-limits anonymous
	// clients to roughly one request per 10 seconds.
	PollInterval time.Duration `yaml:"poll_interval"`
	// RequestTimeout bounds one provider request.
	RequestTimeout time.Duration `yaml:"request_timeout"`
	// TrackWindow is how long each aircraft's trail is kept (3-5 minutes).
	TrackWindow time.Duration `yaml:"track_window"`
}

// Source polls the configured provider and publishes one canonical
// aircraft snapshot per cycle.
type Source struct {
	cfg       Config
	client    *Client
	track     *tracker
	centerLat float64
	centerLon float64
	// now is the clock used for observation timestamps; injectable in
	// tests for deterministic trails.
	now func() time.Time
}

// New decodes and validates the plugin configuration. The APRS hub
// supplies the default area of interest (its position + radius).
func New(node *yaml.Node, hub *aprs.Hub) (plugin.SourcePlugin, error) {
	var cfg Config
	if err := plugin.DecodeConfig(node, &cfg); err != nil {
		return nil, err
	}
	if cfg.Provider == "" {
		cfg.Provider = ProviderAdsbLol
	}
	if cfg.Provider != ProviderAdsbLol && cfg.Provider != ProviderTar1090 {
		return nil, fmt.Errorf("adsb: provider must be %q or %q, got %q", ProviderAdsbLol, ProviderTar1090, cfg.Provider)
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.PollInterval < minPollInterval || cfg.PollInterval > maxPollInterval {
		return nil, fmt.Errorf("adsb: poll_interval must be between %s and %s, got %s", minPollInterval, maxPollInterval, cfg.PollInterval)
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}
	if cfg.RequestTimeout <= 0 || cfg.RequestTimeout > maxTimeout {
		return nil, fmt.Errorf("adsb: request_timeout must be >0 and at most %s, got %s", maxTimeout, cfg.RequestTimeout)
	}
	if cfg.TrackWindow == 0 {
		cfg.TrackWindow = defaultTrackWindow
	}
	if cfg.TrackWindow < minTrackWindow || cfg.TrackWindow > maxTrackWindow {
		return nil, fmt.Errorf("adsb: track_window must be between %s and %s, got %s", minTrackWindow, maxTrackWindow, cfg.TrackWindow)
	}

	if hub == nil || !hub.Enabled() {
		return nil, fmt.Errorf("adsb: the APRS hub is disabled (set aprs.enabled: true) — the area of interest comes from the hub")
	}
	centerLat, centerLon := hub.CenterLat(), hub.CenterLon()
	if cfg.Latitude != nil || cfg.Longitude != nil {
		if cfg.Latitude == nil || cfg.Longitude == nil {
			return nil, fmt.Errorf("adsb: latitude and longitude must be set together")
		}
		centerLat, centerLon = *cfg.Latitude, *cfg.Longitude
	}
	if centerLat < -90 || centerLat > 90 || centerLon < -180 || centerLon > 180 {
		return nil, fmt.Errorf("adsb: center out of range (%v, %v)", centerLat, centerLon)
	}
	radius := cfg.RadiusKM
	if radius == 0 {
		radius = hub.RadiusKM()
	}
	if radius < 1 || radius > maxRadiusKm {
		return nil, fmt.Errorf("adsb: radius_km must be between 1 and %d, got %v", maxRadiusKm, radius)
	}
	cfg.RadiusKM = radius

	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		baseURL = defaultBaseAdsbLol
	}
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("adsb: base_url must be a valid http(s) URL with a host, got %q", cfg.BaseURL)
	}
	cfg.BaseURL = strings.TrimRight(baseURL, "/")

	return &Source{
		cfg:       cfg,
		client:    NewClient(cfg.Provider, cfg.BaseURL, cfg.RequestTimeout),
		track:     newTracker(cfg.TrackWindow),
		centerLat: centerLat,
		centerLon: centerLon,
		now:       time.Now,
	}, nil
}

// Name returns the plugin type name.
func (s *Source) Name() string { return Type }

// Run polls the provider on the configured interval. Transient failures
// degrade the source and never terminate the run.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	reporter, _ := emit.(plugin.SourceHealthReporter)
	slog.Info("adsb plugin started",
		"provider", s.cfg.Provider, "poll_interval", s.cfg.PollInterval,
		"center", fmt.Sprintf("%.4f,%.4f", s.centerLat, s.centerLon),
		"radius_km", s.cfg.RadiusKM, "track_window", s.cfg.TrackWindow)

	for {
		if ctx.Err() != nil {
			break
		}
		n, err := s.pollOnce(ctx, emit)
		if err != nil && ctx.Err() == nil {
			slog.Warn("adsb poll failed", "provider", s.cfg.Provider, "error", err)
			if reporter != nil {
				reporter.ReportSourceDegraded(err)
			}
		} else if err == nil {
			if reporter != nil {
				reporter.ReportSourceHealthy()
			}
			slog.Debug("adsb snapshot published", "provider", s.cfg.Provider, "aircraft", n)
		}
		timer := time.NewTimer(s.cfg.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	slog.Info("adsb plugin stopping")
	return nil
}

// pollOnce fetches one provider response, filters it to the area of
// interest and publishes the canonical snapshot.
func (s *Source) pollOnce(ctx context.Context, emit plugin.Emitter) (int, error) {
	targets, _, err := s.client.Fetch(ctx, s.centerLat, s.centerLon, s.cfg.RadiusKM)
	if err != nil {
		return 0, err
	}

	now := s.now().UTC()
	s.track.prune(now)

	inArea := make([]ProviderTarget, 0, len(targets))
	for _, t := range targets {
		if distKm(s.centerLat, s.centerLon, t.Lat, t.Lon) > s.cfg.RadiusKM {
			continue
		}
		inArea = append(inArea, t)
	}

	snap := buildSnapshot(s.cfg.Provider, s.centerLat, s.centerLon, s.cfg.RadiusKM, inArea, s.track, now)
	payload, err := marshalSnapshot(snap)
	if err != nil {
		return 0, fmt.Errorf("marshal snapshot: %w", err)
	}
	message := core.InformationMessage{
		Source:      Type,
		Key:         "area",
		Kind:        "aircraft",
		GeneratedAt: now,
		Payload:     payload,
	}
	if err := emit.EmitInformation(ctx, message); err != nil {
		return 0, fmt.Errorf("publish: %w", err)
	}
	if stats, ok := emit.(plugin.SourceStatsReporter); ok {
		stats.ReportSourceStats(fmt.Sprintf("%d aircraft in range", len(inArea)))
	}
	return len(inArea), nil
}

// distKm is the haversine distance between two points.
func distKm(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371.0
	p1, p2 := lat1*math.Pi/180, lat2*math.Pi/180
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180
	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(p1)*math.Cos(p2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * R * math.Asin(math.Min(1, math.Sqrt(h)))
}

// Register registers the source plugin type. The APRS hub provides the
// default area of interest.
func Register(reg *plugin.Registry, hub *aprs.Hub) error {
	return reg.RegisterSource(Type, func(node *yaml.Node) (plugin.SourcePlugin, error) {
		return New(node, hub)
	})
}
