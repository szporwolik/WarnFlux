// Package gddkia is a hazard source for the GDDKiA national-roads
// difficulties feed (https://www.archiwum.gddkia.gov.pl/dane/zima_html/utrdane.xml):
// road works, alternating traffic and closures, filtered to the
// configured geographic area (the APRS hub position + radius, overridable
// per plugin instance). Every event carries coordinates so the web UI can
// draw it on the map.
package gddkia

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/snapshotutil"
)

// Type is the plugin type name used in the YAML configuration.
const Type = "gddkia"

const (
	defaultPollInterval   = 15 * time.Minute
	defaultRequestTimeout = 10 * time.Second
	defaultBaseURL        = "https://www.archiwum.gddkia.gov.pl/dane/zima_html/utrdane.xml"

	minPollInterval   = time.Minute
	maxPollInterval   = 24 * time.Hour
	maxRequestTimeout = time.Minute
	// maxEntries bounds the parsed feed (national listing; ~500 today).
	maxEntries = 5000
)

// Config is the plugin-specific configuration.
type Config struct {
	PollInterval   time.Duration `yaml:"poll_interval"`
	RequestTimeout time.Duration `yaml:"request_timeout"`
	// BaseURL is the feed URL (default: the official archiwum endpoint);
	// tests override it with an httptest server.
	BaseURL string `yaml:"base_url"`
	// RadiusKM overrides the area radius; 0 = the APRS hub area radius.
	RadiusKM float64 `yaml:"radius_km"`
	// CenterLat/CenterLon override the area center; when unset the
	// configured APRS station position (aprs.latitude/longitude or the
	// gridsquare center) is used.
	CenterLat *float64 `yaml:"center_latitude"`
	CenterLon *float64 `yaml:"center_longitude"`
}

// Source polls the GDDKiA feed and ingests road difficulties in the
// configured area. Each fetch is a COMPLETE provider snapshot, so
// disappearances are reconciled against the SQLite active state.
type Source struct {
	cfg       Config
	client    *Client
	centerLat float64
	centerLon float64
	radiusKM  float64
}

// New decodes and validates the plugin-specific configuration. hub is the
// shared APRS hub: its configured position and radius define the default
// geographic scope (the same "cities + radius" the rest of the system
// uses). A nil hub is rejected — without a position there is no area.
func New(node *yaml.Node, hub *aprs.Hub) (plugin.SourcePlugin, error) {
	if hub == nil {
		return nil, fmt.Errorf("gddkia: the APRS hub is disabled, but its position and radius define the geographic scope")
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
	if cfg.RequestTimeout <= 0 || cfg.RequestTimeout > maxRequestTimeout {
		return nil, fmt.Errorf("request_timeout must be >0 and at most %s, got %s", maxRequestTimeout, cfg.RequestTimeout)
	}
	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("base_url must be a valid http(s) URL with a host, got %q", cfg.BaseURL)
	}
	cfg.BaseURL = baseURL

	centerLat, centerLon := hub.AreaLat(), hub.AreaLon()
	if cfg.CenterLat != nil || cfg.CenterLon != nil {
		if cfg.CenterLat == nil || cfg.CenterLon == nil {
			return nil, fmt.Errorf("center_latitude and center_longitude must be set together")
		}
		centerLat, centerLon = *cfg.CenterLat, *cfg.CenterLon
	}
	radius := cfg.RadiusKM
	if radius == 0 {
		radius = hub.AreaRadius()
	}
	if radius < 1 || radius > 1000 {
		return nil, fmt.Errorf("radius_km must be between 1 and 1000, got %v", radius)
	}

	return &Source{
		cfg:       cfg,
		client:    NewClient(cfg.BaseURL, cfg.RequestTimeout),
		centerLat: centerLat,
		centerLon: centerLon,
		radiusKM:  radius,
	}, nil
}

// Name returns the plugin type name.
func (s *Source) Name() string { return Type }

// Run polls the feed on the configured interval. Transient provider
// errors are logged and never terminate the run.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	reporter, _ := emit.(plugin.SourceHealthReporter)
	slog.Info("gddkia plugin started",
		"poll_interval", s.cfg.PollInterval, "radius_km", s.radiusKM,
		"center_lat", s.centerLat, "center_lon", s.centerLon)

	for {
		if ctx.Err() != nil {
			break
		}
		err := s.pollOnce(ctx, emit)
		if err != nil && ctx.Err() == nil {
			slog.Warn("gddkia poll failed", "error", err)
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
	slog.Info("gddkia plugin stopping")
	return nil
}

// pollOnce fetches the complete snapshot, filters it to the configured
// area, emits the surviving identities and reconciles disappearances.
func (s *Source) pollOnce(ctx context.Context, emit plugin.Emitter) error {
	doc, err := s.client.Fetch(ctx)
	if err != nil {
		return err
	}
	if len(doc.Utr) > maxEntries {
		return fmt.Errorf("feed has %d entries, bound is %d", len(doc.Utr), maxEntries)
	}

	now := time.Now()
	type pending struct {
		ev    core.HazardEvent
		count int
	}
	seen := make(map[string]*pending, len(doc.Utr))
	complete := true
	filtered := 0
	for i := range doc.Utr {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ev, err := normalize(doc.Utr[i], s.cfg.BaseURL)
		if err != nil {
			// Entries without usable coordinates cannot be placed in the
			// area, so they are out of scope, not a provider failure.
			slog.Debug("gddkia entry skipped", "entry", i, "reason", err.Error())
			filtered++
			continue
		}
		if aprs.DistanceKM(s.centerLat, s.centerLon, *ev.Latitude, *ev.Longitude) > s.radiusKM {
			filtered++
			continue
		}
		if p, ok := seen[ev.Key()]; ok {
			p.count++
			complete = false
			slog.Warn("gddkia feed contains a duplicate identity; snapshot marked incomplete", "event_key", ev.Key())
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
		return errors.New("gddkia: snapshot incomplete (duplicate identities); reconciliation skipped")
	}
	if _, err := snapshotutil.Reconcile(ctx, emit, sourceName, keys, now); err != nil && ctx.Err() == nil {
		slog.Warn("gddkia reconciliation failed", "error", err)
	}
	slog.Info("gddkia feed processed",
		"entries", len(doc.Utr), "in_area", len(keys), "filtered", filtered)
	if stats, ok := emit.(plugin.SourceStatsReporter); ok {
		stats.ReportSourceStats(fmt.Sprintf("%d / %d in area", len(keys), len(doc.Utr)))
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
