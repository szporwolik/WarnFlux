package imgw

import (
	"context"
	"encoding/json"
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
const Type = "imgw"

const (
	defaultPollInterval   = 5 * time.Minute
	defaultRequestTimeout = 10 * time.Second
	defaultBaseURL        = "https://danepubliczne.imgw.pl/api/data"

	minPollInterval   = time.Minute
	maxPollInterval   = time.Hour
	maxRequestTimeout = time.Minute
)

// Feed names select which official endpoints one plugin instance consumes.
const (
	feedMeteo = "meteo"
	feedHydro = "hydro"
)

// Normalized HazardEvent source namespaces (one per feed).
const (
	sourceMeteo = "imgw-meteo"
	sourceHydro = "imgw-hydro"
)

var validFeeds = map[string]bool{feedMeteo: true, feedHydro: true}

// Config is the plugin-specific configuration. The public IMGW data API
// requires no credentials.
type Config struct {
	PollInterval   time.Duration `yaml:"poll_interval"`
	RequestTimeout time.Duration `yaml:"request_timeout"`
	// BaseURL is the data API base (default: https://danepubliczne.imgw.pl/api/data);
	// tests override it with an httptest server.
	BaseURL string   `yaml:"base_url"`
	Feeds   []string `yaml:"feeds"`
	// Geography is the optional local-area filter (see geography.go).
	Geography *GeographyConfig `yaml:"geography"`
}

// Source polls the IMGW warning endpoints and ingests the current warning
// snapshots. Each successful fetch is a COMPLETE provider snapshot, so the
// source also reconciles disappearances against the authoritative SQLite
// active state (see reconcile.go).
type Source struct {
	cfg    Config
	client *Client
	// geo is the runtime-ready geographic policy (nil-safe when disabled).
	geo *geography
}

// New decodes and validates the plugin-specific configuration.
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
	if cfg.RequestTimeout <= 0 || cfg.RequestTimeout > maxRequestTimeout {
		return nil, fmt.Errorf("request_timeout must be >0 and at most %s, got %s", maxRequestTimeout, cfg.RequestTimeout)
	}
	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	baseURL = strings.TrimRight(baseURL, "/")
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("base_url must be a valid http(s) URL with a host, got %q", cfg.BaseURL)
	}
	cfg.BaseURL = baseURL

	if len(cfg.Feeds) == 0 {
		cfg.Feeds = []string{feedMeteo, feedHydro}
	}
	seen := make(map[string]bool, len(cfg.Feeds))
	for _, f := range cfg.Feeds {
		if !validFeeds[f] {
			return nil, fmt.Errorf("unknown feed %q (valid: meteo, hydro)", f)
		}
		if seen[f] {
			return nil, fmt.Errorf("duplicate feed %q", f)
		}
		seen[f] = true
	}

	g, err := buildGeography(cfg.Geography)
	if err != nil {
		return nil, err
	}

	return &Source{cfg: cfg, client: NewClient(cfg.BaseURL, cfg.RequestTimeout), geo: g}, nil
}

// Name returns the plugin type name.
func (s *Source) Name() string { return Type }

// Run polls the IMGW endpoints immediately and then every poll_interval.
// Snapshot reconciliation requires the SourceActiveEventReader capability:
// without it the source refuses to run instead of silently risking
// permanent stale warnings (e.g. withdrawn hydrological drought).
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	if _, ok := emit.(plugin.SourceActiveEventReader); !ok {
		return fmt.Errorf("emitter lacks the source active-event reader; IMGW snapshot disappearance reconciliation would be unsafe")
	}
	reporter, _ := emit.(plugin.SourceHealthReporter)
	slog.Info("imgw plugin started", "feeds", s.cfg.Feeds, "poll_interval", s.cfg.PollInterval)

	for {
		if ctx.Err() != nil {
			break
		}
		s.pollOnce(ctx, emit, reporter)
		timer := time.NewTimer(s.cfg.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	slog.Info("imgw plugin stopping")
	return nil
}

// pollOnce fetches and processes every enabled feed once and reports
// provider health: healthy only when every enabled feed produced a
// complete snapshot.
func (s *Source) pollOnce(ctx context.Context, emit plugin.Emitter, reporter plugin.SourceHealthReporter) {
	healthy := true
	now := time.Now()
	for _, feed := range s.cfg.Feeds {
		if ctx.Err() != nil {
			return // shutting down: no health report for a cancelled poll
		}
		if !s.pollFeed(ctx, emit, feed, now) {
			healthy = false
		}
	}
	if reporter != nil {
		if healthy {
			reporter.ReportSourceHealthy()
		} else {
			reporter.ReportSourceDegraded(errors.New("one or more IMGW feeds failed or produced an incomplete snapshot"))
		}
	}
}

// pollFeed processes one feed end-to-end and reports whether the provider
// snapshot was COMPLETE (fetch succeeded, JSON decoded, every item
// identified, no duplicates, count bounded). Disappearance reconciliation
// runs only for complete snapshots.
func (s *Source) pollFeed(ctx context.Context, emit plugin.Emitter, feed string, now time.Time) bool {
	body, err := s.client.Fetch(ctx, feed)
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		slog.Warn("IMGW feed fetch failed", "feed", feed, "error", err)
		return false
	}

	var keys map[string]bool
	var complete bool
	switch feed {
	case feedMeteo:
		keys, complete = s.processMeteo(ctx, emit, body)
	case feedHydro:
		keys, complete = s.processHydro(ctx, emit, body)
	}
	if !complete {
		return false
	}

	cancelled, err := reconcileSnapshots(ctx, emit, sourceFor(feed), keys, now)
	if err != nil {
		slog.Warn("IMGW snapshot reconciliation failed", "feed", feed, "error", err)
		return false
	}
	slog.Debug("IMGW feed processed", "feed", feed, "warnings", len(keys), "cancelled", cancelled)
	return true
}

// processMeteo decodes and ingests the meteorological snapshot.
func (s *Source) processMeteo(ctx context.Context, emit plugin.Emitter, body []byte) (map[string]bool, bool) {
	if err := requireJSONArray(body); err != nil {
		slog.Warn("IMGW meteo feed has an invalid top-level shape", "error", err)
		return nil, false
	}
	var items []meteoWarning
	if err := json.Unmarshal(body, &items); err != nil {
		slog.Warn("IMGW meteo feed is not valid JSON", "error", err)
		return nil, false
	}
	return s.ingestSnapshot(ctx, emit, sourceMeteo, len(items), func(i int) (core.HazardEvent, error) {
		ev, err := normalizeMeteo(items[i], s.cfg.BaseURL+"/warningsmeteo")
		if err != nil {
			return ev, err
		}
		match, areas, err := s.geo.matchesMeteo(items[i].Teryt)
		if err != nil {
			return ev, err
		}
		ev.Areas = areas
		if !match {
			return ev, errGeographicallyFiltered
		}
		return ev, nil
	})
}

// processHydro decodes and ingests the hydrological snapshot.
func (s *Source) processHydro(ctx context.Context, emit plugin.Emitter, body []byte) (map[string]bool, bool) {
	if err := requireJSONArray(body); err != nil {
		slog.Warn("IMGW hydro feed has an invalid top-level shape", "error", err)
		return nil, false
	}
	var items []hydroWarning
	if err := json.Unmarshal(body, &items); err != nil {
		slog.Warn("IMGW hydro feed is not valid JSON", "error", err)
		return nil, false
	}
	return s.ingestSnapshot(ctx, emit, sourceHydro, len(items), func(i int) (core.HazardEvent, error) {
		ev, err := normalizeHydro(items[i], s.cfg.BaseURL+"/warningshydro")
		if err != nil {
			return ev, err
		}
		if !s.geo.matchesHydro(ev.Areas) {
			return ev, errGeographicallyFiltered
		}
		return ev, nil
	})
}

// ingestSnapshot ingests one feed snapshot in TWO PHASES. Phase 1
// normalizes every item and counts identities (malformed items mark the
// snapshot incomplete but never block valid records; a duplicate identity
// marks it incomplete too). Phase 2 emits ONLY identities that occurred
// exactly once — an ambiguous duplicate is never arbitrarily emitted, so a
// corrupt snapshot cannot modify SQLite before being declared incomplete.
// Emit failures do NOT make the snapshot incomplete (the provider was still
// understood); they are logged and retried on the next poll.
func (s *Source) ingestSnapshot(ctx context.Context, emit plugin.Emitter, source string, count int, normalize func(i int) (core.HazardEvent, error)) (map[string]bool, bool) {
	if count > maxWarningsPerFeed {
		slog.Warn("IMGW feed exceeds the warning-count bound", "source", source, "count", count, "maximum", maxWarningsPerFeed)
		return nil, false
	}

	type pending struct {
		ev    core.HazardEvent
		count int
	}
	seen := make(map[string]*pending, count)
	complete := true
	for i := 0; i < count; i++ {
		if ctx.Err() != nil {
			return nil, false
		}
		ev, err := normalize(i)
		if err != nil {
			if errors.Is(err, errGeographicallyFiltered) {
				// Intentional policy filtering: the logical snapshot of THIS
				// source configuration excludes the event, but the provider
				// snapshot remains complete.
				continue
			}
			slog.Warn("IMGW item skipped; snapshot marked incomplete", "source", source, "item", i, "error", err)
			complete = false
			continue
		}
		if p, ok := seen[ev.Key()]; ok {
			p.count++
			complete = false
			slog.Warn("IMGW feed contains a duplicate identity; snapshot marked incomplete", "source", source, "event_key", ev.Key())
			continue
		}
		seen[ev.Key()] = &pending{ev: ev, count: 1}
	}

	keys := make(map[string]bool, len(seen))
	for key, p := range seen {
		if ctx.Err() != nil {
			return keys, false
		}
		if p.count != 1 {
			continue // ambiguous identity: never emitted
		}
		keys[key] = true
		if err := emit.Emit(ctx, p.ev); err != nil {
			if ctx.Err() != nil {
				return keys, false
			}
			slog.Warn("emit failed; retrying on the next poll", "event_key", key, "error", err)
			continue
		}
	}
	return keys, complete
}

func sourceFor(feed string) string {
	if feed == feedHydro {
		return sourceHydro
	}
	return sourceMeteo
}

// Register registers the imgw source plugin type.
func Register(reg *plugin.Registry) error {
	return reg.RegisterSource(Type, New)
}
