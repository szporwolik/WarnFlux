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
	// defaultPollInterval keeps the station inside the provider's daily
	// budget: adsb.lol allows 500 requests per 86400 s for anonymous
	// clients, so a 30-second poll (2880/day) exhausted the quota within
	// hours and the station then collected 429s until the window reset.
	defaultPollInterval = 5 * time.Minute
	// defaultRequestTimeout covers a COLD connection to a distant API
	// (fresh DNS/TCP/TLS setup, plus HTTP/2), which measured up to 8 seconds
	// on a slow link: 15 seconds left no margin and produced "degraded"
	// reports that said more about the network than about the provider.
	defaultRequestTimeout = 30 * time.Second
	defaultBaseAdsbLol    = "https://api.adsb.lol"
	defaultTrackWindow    = 5 * time.Minute

	minPollInterval = 5 * time.Second
	// maxPollInterval allows a cadence that leaves quota for other clients:
	// adsb.lol counts requests per PUBLIC IP, so every WarnFlux instance
	// (and every feeder) behind one NAT draws from the same 500/day. A
	// second instance on the default 5m cadence doubles the draw and
	// exhausts the window for both.
	maxPollInterval = time.Hour
	minTrackWindow  = time.Minute
	maxTrackWindow  = 15 * time.Minute
	maxTimeout      = time.Minute
	maxRadiusKm     = 500

	// maxRefusalBackoff bounds the doubling applied to one refusal episode:
	// each consecutive 429 doubles the wait, up to this ceiling.
	maxRefusalBackoff = 30 * time.Minute
	// maxRateLimitWait bounds how long the plugin honours the provider's
	// quota-window reset. It matches the clamp applied when the header is
	// parsed, because probing before the reset can only produce another
	// 429: the quota is empty until the advertised moment, and the poll
	// right after the reset succeeds. Hourly probes during a blocked window
	// were requests spent from an already empty budget.
	maxRateLimitWait = 24 * time.Hour
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
	// RadiusKM overrides the area radius (default: the APRS hub area radius).
	RadiusKM float64 `yaml:"radius_km"`
	// PollInterval bounds one poll cycle. adsb.lol rate-limits anonymous
	// clients, so keep it polite (30s is the safe default; the plugin
	// also honors 429 Retry-After when the provider pushes back).
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
	centerLat, centerLon := hub.AreaLat(), hub.AreaLon()
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
		radius = hub.AreaRadius()
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

// refusalDelay is how long to wait before the next poll after a refusal: the
// poll interval doubled once per consecutive 429 (bounded by
// maxRefusalBackoff), never shorter than the provider's Retry-After, and never
// shorter than the quota-window reset it advertises (bounded by
// maxRateLimitWait). A blocked provider used to be poked at the poll cadence
// for the whole outage — 77 pointless requests in one 6-hour episode.
func refusalDelay(interval time.Duration, refusals int, rl *rateLimitError) time.Duration {
	if interval <= 0 {
		interval = defaultPollInterval
	}
	wait := interval
	for i := 1; i < refusals && wait < maxRefusalBackoff; i++ {
		wait *= 2
	}
	wait = min(wait, maxRefusalBackoff)
	if rl != nil {
		if rl.retryAfter > wait {
			wait = rl.retryAfter
		}
		if rl.resetAfter > wait {
			wait = min(rl.resetAfter, maxRateLimitWait)
		}
	}
	return wait
}

// Run polls the provider on the configured interval. Transient failures
// degrade the source and never terminate the run; provider rate limits
// (429) back the next poll off by the advertised Retry-After window. An
// outage is logged as one episode: the first failed poll is the warning, the
// retries are counted, and the recovery reports the tally — one warning per
// poll turned a dead provider host into 48 warnings an hour that buried every
// other line of the log.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	reporter, _ := emit.(plugin.SourceHealthReporter)
	slog.Info("adsb plugin started",
		"provider", s.cfg.Provider, "poll_interval", s.cfg.PollInterval,
		"center", fmt.Sprintf("%.4f,%.4f", s.centerLat, s.centerLon),
		"radius_km", s.cfg.RadiusKM, "track_window", s.cfg.TrackWindow)

	// failures counts the failed polls of the current outage episode
	// (0 = healthy); outageSince anchors its start for the recovery report.
	// refusals counts consecutive rate-limit (429) answers: they grow the
	// step back, so a blocked quota is not poked at the poll cadence.
	failures := 0
	refusals := 0
	var outageSince time.Time

	for {
		if ctx.Err() != nil {
			break
		}
		n, err := s.pollOnce(ctx, emit)
		delay := s.cfg.PollInterval
		if err != nil && ctx.Err() == nil {
			if rl, ok := err.(*rateLimitError); ok {
				refusals++
				delay = max(delay, refusalDelay(s.cfg.PollInterval, refusals, rl))
			}
			failures++
			if failures == 1 {
				outageSince = time.Now()
				slog.Warn("adsb poll failed", "provider", s.cfg.Provider, "error", err, "retry_in", delay)
			} else {
				slog.Debug("adsb poll still failing", "provider", s.cfg.Provider, "error", err,
					"failed_attempts", failures, "retry_in", delay)
			}
			if reporter != nil {
				reporter.ReportSourceDegraded(err)
			}
		} else if err == nil {
			if failures > 0 {
				slog.Info("adsb poll recovered", "provider", s.cfg.Provider,
					"failed_attempts", failures, "outage", time.Since(outageSince).Round(time.Second))
				failures = 0
				refusals = 0
			}
			if reporter != nil {
				reporter.ReportSourceHealthy()
			}
			slog.Debug("adsb snapshot published", "provider", s.cfg.Provider, "aircraft", n)
		}
		timer := time.NewTimer(delay)
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
	reg.SetSourcePollBounds(Type, minPollInterval, maxPollInterval)
	return reg.RegisterSource(Type, func(node *yaml.Node) (plugin.SourcePlugin, error) {
		return New(node, hub)
	})
}
