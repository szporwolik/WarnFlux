// Package burzedzis is a hazard source for the burze.dzis.net storm and
// severe-weather service: the official Polish weather warnings in force for
// the localities this installation follows, plus the provider's lightning
// discharge search around them.
//
// Two event sources come out of one instance, because an operator routes
// them differently: the warnings travel under "burzedzis" (a warning is a
// forecast with a validity window) and the lightning nowcast under
// "burzedzis-burza" (a storm is over the station right now, and the row is
// meant to be wired to the fast channels). Every alert carries the locality
// name it applies to, so a reader never has to guess which town is meant.
//
// What the plugin deliberately does not do: invent advice. The provider
// publishes no recommendation for a warning, so the instruction section
// stays empty — the description carries only the facts the provider gave
// (degree, window, coordinates).
package burzedzis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/burzedzis"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/snapshotutil"
	"github.com/szporwolik/WarnFlux/internal/severity"
)

// Type is the plugin type name used in the YAML configuration. It is also
// the source slug of the provider's warning events.
const Type = "burzedzis"

// TypeStorm is the source slug of the lightning nowcast events. It is not a
// plugin type: one instance publishes both, and the routing matrix offers
// both rows.
const TypeStorm = "burzedzis-burza"

const (
	defaultPollInterval   = 5 * time.Minute // the storm window is 10-20 minutes
	defaultRequestTimeout = 20 * time.Second
	// defaultLocalityRefresh re-resolves the configured locality names at
	// most this often; the directory answers are stable for years, so this
	// is a safety net, not a polling need.
	defaultLocalityRefresh = 24 * time.Hour
	defaultStormTTL        = 30 * time.Minute
	// defaultStormUpdate re-publishes an unchanged detection at most this
	// often (an escalated one goes out at once).
	defaultStormUpdate = 15 * time.Minute

	minPollInterval   = time.Minute
	maxPollInterval   = 24 * time.Hour
	maxRequestTimeout = time.Minute

	// MaxLocalities bounds one instance: every locality costs two requests
	// per poll (warnings + lightning), so the list is the request budget.
	// It is exported because the Config page enforces the same bound.
	MaxLocalities = 50
	// MaxLocalityNameLen bounds one configured name (the provider's
	// directory holds town names, not sentences).
	MaxLocalityNameLen = 64
	// maxAPIKeyFileSize bounds the optional key file.
	maxAPIKeyFileSize = 64 * 1024

	minStormRadiusKM  = 1
	maxStormRadiusKM  = 200
	maxStormLightings = 100000

	// fallbackStormWindow is used when the provider omits its window.
	fallbackStormWindow = 10 * time.Minute
)

// Config is the plugin-specific configuration.
type Config struct {
	// APIKey is the burze.dzis.net API key (account → API). Required.
	APIKey string `yaml:"api_key"`
	// APIKeyFile reads the key from a file instead (a Docker secret); the
	// two are mutually exclusive.
	APIKeyFile string `yaml:"api_key_file"`
	// BaseURL is the API root (default: the provider's own host); tests
	// override it with an httptest server.
	BaseURL string `yaml:"base_url"`

	PollInterval   time.Duration `yaml:"poll_interval"`
	RequestTimeout time.Duration `yaml:"request_timeout"`

	// Locations are the localities this installation follows, by the name
	// the provider's directory knows (diacritics included: "Niepołomice"
	// resolves, "Niepolomice" does not). The Config page edits this list.
	Locations []string `yaml:"locations"`
	// LocalityRefresh re-resolves the names at most this often.
	LocalityRefresh time.Duration `yaml:"locality_refresh"`

	// Degree severities map the provider's warning degrees onto canonical
	// WarnFlux severities (the defaults match IMGW's own scale).
	Degree1Severity string `yaml:"degree1_severity"`
	Degree2Severity string `yaml:"degree2_severity"`
	Degree3Severity string `yaml:"degree3_severity"`

	// Per-phenomenon floors: a warning never routes below its floor. The
	// wind family (funnel, storm, wind) is floored at severe by default,
	// because a thunderstorm or a strong-wind warning is dangerous even at
	// the first degree — the degrees only raise it further (3 → extreme).
	TornadoMinSeverity string `yaml:"tornado_min_severity"`
	StormMinSeverity   string `yaml:"storm_min_severity"`
	WindMinSeverity    string `yaml:"wind_min_severity"`
	RainMinSeverity    string `yaml:"rain_min_severity"`
	HeatMinSeverity    string `yaml:"heat_min_severity"`
	FrostMinSeverity   string `yaml:"frost_min_severity"`

	// StormTracking publishes the lightning nowcast (source
	// burzedzis-burza). Off means only warnings are published.
	StormTracking bool `yaml:"storm_tracking"`
	// StormRadiusKM is the search radius (default: the provider's 25 km).
	StormRadiusKM int `yaml:"storm_radius_km"`
	// StormMinLightnings is how many discharges in the window make one
	// event (default 1).
	StormMinLightnings int `yaml:"storm_min_lightnings"`
	// StormSeverity is the severity of a detection whose nearest discharge
	// is farther than StormNearKM.
	StormSeverity string `yaml:"storm_severity"`
	// StormNearKM / StormNearSeverity escalate when the nearest discharge
	// is close: 10 km → severe by default.
	StormNearKM       float64 `yaml:"storm_near_km"`
	StormNearSeverity string  `yaml:"storm_near_severity"`
	// StormDirectKM / StormDirectSeverity escalate again for a discharge
	// right over the locality: 3 km → extreme by default.
	StormDirectKM       float64 `yaml:"storm_direct_km"`
	StormDirectSeverity string  `yaml:"storm_direct_severity"`
	// StormTTL is the validity bucket of one detection: the event stays
	// valid to the end of its bucket and the next quiet poll cancels it
	// earlier.
	StormTTL time.Duration `yaml:"storm_ttl"`
	// StormUpdateInterval bounds how often a detection whose severity did
	// not change is re-published; the facts of a drifting storm change with
	// every poll, and every content change is a notification.
	StormUpdateInterval time.Duration `yaml:"storm_update_interval"`
}

// severityPlan is the validated severity mapping of one instance.
type severityPlan struct {
	byDegree map[int]string
	floors   map[string]string
}

// Source follows the configured localities: warnings on every poll, the
// lightning search with it.
type Source struct {
	cfg    Config
	client *burzedzis.Client
	plan   severityPlan
	// points holds the last GOOD coordinates per configured locality name.
	// A failed lookup never overwrites them: a momentary 503 must not blind
	// the station for a whole refresh interval.
	mu      sync.Mutex
	points  map[string]burzedzis.Locality
	unknown map[string]bool
	// retry names the localities whose lookup failed (transport, refusal,
	// unreadable answer). They are looked up again on the very next poll,
	// and a pass with failures never counts as a refresh.
	retry map[string]bool
	// refreshed is when the last COMPLETE pass ran: every configured name
	// answered (an unknown name is an answer too). It bounds the periodic
	// re-resolution.
	refreshed time.Time
	// storm remembers the last published detection per locality.
	storm map[string]stormState
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
		return nil, fmt.Errorf("burzedzis: poll_interval must be between %s and %s, got %s", minPollInterval, maxPollInterval, cfg.PollInterval)
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}
	if cfg.RequestTimeout <= 0 || cfg.RequestTimeout > maxRequestTimeout {
		return nil, fmt.Errorf("burzedzis: request_timeout must be >0 and at most %s, got %s", maxRequestTimeout, cfg.RequestTimeout)
	}
	if cfg.LocalityRefresh == 0 {
		cfg.LocalityRefresh = defaultLocalityRefresh
	}
	if cfg.LocalityRefresh < 0 {
		return nil, fmt.Errorf("burzedzis: locality_refresh must be 0 (resolve once) or positive, got %s", cfg.LocalityRefresh)
	}

	if cfg.APIKey != "" && cfg.APIKeyFile != "" {
		return nil, fmt.Errorf("burzedzis: api_key and api_key_file are mutually exclusive")
	}
	key := strings.TrimSpace(cfg.APIKey)
	if key == "" && cfg.APIKeyFile != "" {
		info, err := os.Stat(cfg.APIKeyFile)
		if err != nil {
			return nil, fmt.Errorf("burzedzis: stat api_key_file: %w", err)
		}
		if info.Size() > maxAPIKeyFileSize {
			return nil, fmt.Errorf("burzedzis: api_key_file is %d bytes, maximum %d", info.Size(), maxAPIKeyFileSize)
		}
		data, err := os.ReadFile(cfg.APIKeyFile)
		if err != nil {
			return nil, fmt.Errorf("burzedzis: read api_key_file: %w", err)
		}
		key = strings.TrimSpace(string(data))
	}
	if key == "" {
		return nil, fmt.Errorf("burzedzis: an api_key (or api_key_file) is required — the provider answers \"authentication required\" without one")
	}

	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		baseURL = burzedzis.DefaultBaseURL
	}
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		return nil, fmt.Errorf("burzedzis: base_url must be an http(s) URL, got %q", cfg.BaseURL)
	}
	cfg.BaseURL = strings.TrimRight(baseURL, "/")

	locations, err := NormalizeLocations(cfg.Locations)
	if err != nil {
		return nil, err
	}
	cfg.Locations = locations

	plan, err := buildPlan(&cfg)
	if err != nil {
		return nil, err
	}

	src := &Source{
		cfg:     cfg,
		client:  burzedzis.NewClient(cfg.BaseURL, key, cfg.RequestTimeout),
		plan:    plan,
		points:  make(map[string]burzedzis.Locality),
		unknown: make(map[string]bool),
		retry:   make(map[string]bool),
		storm:   make(map[string]stormState),
	}
	if len(locations) == 0 {
		slog.Warn("burzedzis: no localities configured; the source stays idle until the Config page (or the file) lists one",
			"base_url", cfg.BaseURL)
	}
	return src, nil
}

// buildPlan validates the severity mapping and fills in the defaults.
func buildPlan(cfg *Config) (severityPlan, error) {
	plan := severityPlan{
		byDegree: map[int]string{
			1: orDefault(cfg.Degree1Severity, severity.Moderate),
			2: orDefault(cfg.Degree2Severity, severity.Severe),
			3: orDefault(cfg.Degree3Severity, severity.Extreme),
		},
		floors: map[string]string{
			"tornado": orDefault(cfg.TornadoMinSeverity, severity.Severe),
			"storm":   orDefault(cfg.StormMinSeverity, severity.Severe),
			"wind":    orDefault(cfg.WindMinSeverity, severity.Severe),
			"rain":    orDefault(cfg.RainMinSeverity, severity.Minor),
			"heat":    orDefault(cfg.HeatMinSeverity, severity.Minor),
			"frost":   orDefault(cfg.FrostMinSeverity, severity.Minor),
		},
	}
	for degree, sev := range plan.byDegree {
		if !severity.Valid(sev) {
			return severityPlan{}, fmt.Errorf("burzedzis: unknown severity %q for degree %d (want unknown/minor/moderate/severe/extreme)", sev, degree)
		}
	}
	for _, ph := range burzedzis.Phenomena {
		floor := plan.floors[ph.Key]
		if !severity.Valid(floor) {
			return severityPlan{}, fmt.Errorf("burzedzis: unknown %s_min_severity %q (want unknown/minor/moderate/severe/extreme)", ph.Key, floor)
		}
		// The wind family is the one invariant an operator cannot lower:
		// a thunderstorm or a strong-wind warning is never a minor event.
		// Raising the floor (to extreme) stays allowed.
		if ph.Wind {
			rank, _ := severity.Rank(floor)
			if severeRank, _ := severity.Rank(severity.Severe); rank < severeRank {
				return severityPlan{}, fmt.Errorf("burzedzis: %s_min_severity must be %s or %s, got %q (a thunderstorm or strong-wind warning is never routed below severe)",
					ph.Key, severity.Severe, severity.Extreme, floor)
			}
		}
	}
	// Keep the config echoed in the startup log consistent with the plan.
	cfg.Degree1Severity = plan.byDegree[1]
	cfg.Degree2Severity = plan.byDegree[2]
	cfg.Degree3Severity = plan.byDegree[3]
	cfg.TornadoMinSeverity = plan.floors["tornado"]
	cfg.StormMinSeverity = plan.floors["storm"]
	cfg.WindMinSeverity = plan.floors["wind"]
	cfg.RainMinSeverity = plan.floors["rain"]
	cfg.HeatMinSeverity = plan.floors["heat"]
	cfg.FrostMinSeverity = plan.floors["frost"]

	if !cfg.StormTracking {
		return plan, nil
	}
	if cfg.StormRadiusKM == 0 {
		cfg.StormRadiusKM = burzedzis.DefaultStormRadiusKM
	}
	if cfg.StormRadiusKM < minStormRadiusKM || cfg.StormRadiusKM > maxStormRadiusKM {
		return severityPlan{}, fmt.Errorf("burzedzis: storm_radius_km must be between %d and %d, got %d", minStormRadiusKM, maxStormRadiusKM, cfg.StormRadiusKM)
	}
	if cfg.StormMinLightnings == 0 {
		cfg.StormMinLightnings = 1
	}
	if cfg.StormMinLightnings < 1 || cfg.StormMinLightnings > maxStormLightings {
		return severityPlan{}, fmt.Errorf("burzedzis: storm_min_lightnings must be between 1 and %d, got %d", maxStormLightings, cfg.StormMinLightnings)
	}
	if cfg.StormTTL == 0 {
		cfg.StormTTL = defaultStormTTL
	}
	if cfg.StormTTL < minPollInterval || cfg.StormTTL > 12*time.Hour {
		return severityPlan{}, fmt.Errorf("burzedzis: storm_ttl must be between %s and 12h, got %s", minPollInterval, cfg.StormTTL)
	}
	if cfg.StormUpdateInterval == 0 {
		cfg.StormUpdateInterval = defaultStormUpdate
	}
	if cfg.StormUpdateInterval < minPollInterval || cfg.StormUpdateInterval > cfg.StormTTL {
		return severityPlan{}, fmt.Errorf("burzedzis: storm_update_interval must be between %s and storm_ttl (%s), got %s", minPollInterval, cfg.StormTTL, cfg.StormUpdateInterval)
	}
	cfg.StormSeverity = orDefault(cfg.StormSeverity, severity.Moderate)
	if cfg.StormNearKM == 0 {
		cfg.StormNearKM = 10
	}
	cfg.StormNearSeverity = orDefault(cfg.StormNearSeverity, severity.Severe)
	if cfg.StormDirectKM == 0 {
		cfg.StormDirectKM = 3
	}
	cfg.StormDirectSeverity = orDefault(cfg.StormDirectSeverity, severity.Extreme)
	for name, sev := range map[string]string{"storm_severity": cfg.StormSeverity, "storm_near_severity": cfg.StormNearSeverity, "storm_direct_severity": cfg.StormDirectSeverity} {
		if !severity.Valid(sev) {
			return severityPlan{}, fmt.Errorf("burzedzis: unknown %s %q (want unknown/minor/moderate/severe/extreme)", name, sev)
		}
	}
	if cfg.StormDirectKM >= cfg.StormNearKM {
		return severityPlan{}, fmt.Errorf("burzedzis: storm_direct_km (%v) must be smaller than storm_near_km (%v)", cfg.StormDirectKM, cfg.StormNearKM)
	}
	if cfg.StormNearKM > float64(cfg.StormRadiusKM) {
		return severityPlan{}, fmt.Errorf("burzedzis: storm_near_km (%v) must not exceed storm_radius_km (%d)", cfg.StormNearKM, cfg.StormRadiusKM)
	}
	base, _ := severity.Rank(cfg.StormSeverity)
	near, _ := severity.Rank(cfg.StormNearSeverity)
	direct, _ := severity.Rank(cfg.StormDirectSeverity)
	if near < base || direct < near {
		return severityPlan{}, fmt.Errorf("burzedzis: the lightning severities must not decrease with closeness (storm_severity %q, storm_near_severity %q, storm_direct_severity %q)", cfg.StormSeverity, cfg.StormNearSeverity, cfg.StormDirectSeverity)
	}
	return plan, nil
}

func orDefault(v, fallback string) string {
	if v = strings.TrimSpace(v); v != "" {
		return v
	}
	return fallback
}

// NormalizeLocations cleans a configured locality list: names are trimmed,
// empties and case-insensitive duplicates are dropped, and the accepted
// bounds are enforced. It is shared with the Config page, so the panel can
// never write a list the loader would reject.
func NormalizeLocations(in []string) ([]string, error) {
	out := make([]string, 0, len(in))
	seen := make(map[string]bool, len(in))
	for _, raw := range in {
		name := strings.Join(strings.Fields(strings.TrimSpace(raw)), " ")
		if name == "" {
			continue
		}
		if len(name) > MaxLocalityNameLen {
			return nil, fmt.Errorf("burzedzis: locality %q is %d bytes, maximum %d", name, len(name), MaxLocalityNameLen)
		}
		key := strings.ToLower(name)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, name)
	}
	if len(out) > MaxLocalities {
		return nil, fmt.Errorf("burzedzis: %d localities configured, maximum %d (every one costs two requests per poll)", len(out), MaxLocalities)
	}
	return out, nil
}

// Name returns the plugin type name.
func (s *Source) Name() string { return Type }

// Run polls the provider immediately and then every poll_interval. Both
// event families are reconciled against the durable current state
// (SourceActiveEventReader): the warnings answer is a complete snapshot per
// point, so a warning the provider withdraws early is cancelled instead of
// hanging until its window ends, and a storm that moved away is cancelled by
// the next quiet poll.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	if _, ok := emit.(plugin.SourceActiveEventReader); !ok {
		return fmt.Errorf("emitter lacks the source active-event reader; burzedzis withdrawal reconciliation would be unsafe")
	}
	reporter, _ := emit.(plugin.SourceHealthReporter)
	slog.Info("burzedzis plugin started",
		"base_url", s.cfg.BaseURL, "poll_interval", s.cfg.PollInterval,
		"locations", s.cfg.Locations, "storm_tracking", s.cfg.StormTracking,
		"degrees", []string{s.cfg.Degree1Severity, s.cfg.Degree2Severity, s.cfg.Degree3Severity},
		"wind_floor", s.cfg.StormMinSeverity)

	for {
		if ctx.Err() != nil {
			break
		}
		err := s.pollOnce(ctx, emit)
		if err != nil && ctx.Err() == nil {
			slog.Warn("burzedzis poll failed", "error", err)
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
	slog.Info("burzedzis plugin stopping")
	return nil
}

// pollOnce resolves the localities (cached), publishes the warnings in
// force and, with storm_tracking on, the lightning nowcast.
func (s *Source) pollOnce(ctx context.Context, emit plugin.Emitter) error {
	if len(s.cfg.Locations) == 0 {
		if stats, ok := emit.(plugin.SourceStatsReporter); ok {
			stats.ReportSourceStats("no localities configured")
		}
		return nil
	}
	// A failed locality lookup degrades the source but must not stop the
	// cycle: the localities we already know keep being watched, and the
	// error still reaches the health report.
	resolveErr := s.resolve(ctx)
	if resolveErr != nil {
		slog.Warn("burzedzis locality resolution incomplete; polling the known localities", "error", resolveErr)
	}
	points, unknown := s.snapshotPoints()
	if len(points) == 0 {
		if resolveErr != nil {
			return fmt.Errorf("no locality could be resolved: %w", resolveErr)
		}
		return fmt.Errorf("none of the configured localities could be resolved (%s)", strings.Join(unknown, ", "))
	}

	var firstErr error
	failed := 0
	warnKeys := make(map[string]bool, len(points))
	warnEmitted := 0
	rejected := make([]rejectedPhenomenon, 0)
	for _, at := range points {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		snap, err := s.client.Warnings(ctx, at)
		if err != nil {
			failed++
			slog.Warn("burzedzis warnings fetch failed", "locality", at.Name, "error", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("locality %s: %w", at.Name, err)
			}
			continue
		}
		for _, rej := range snap.Rejected {
			// A phenomenon with a degree but no readable window is UNKNOWN,
			// not withdrawn: the alert already published for it must survive
			// this cycle, and the operator must see that the answer was
			// incomplete.
			rejected = append(rejected, rejectedPhenomenon{locality: slug(at.Name), phenomenon: rej.Phenomenon.Key})
			slog.Warn("burzedzis warning rejected: the provider published a degree without a readable window; keeping the alert already in force",
				"locality", at.Name, "phenomenon", rej.Phenomenon.Key, "field", rej.Field, "value", rej.Reason)
			if firstErr == nil {
				firstErr = fmt.Errorf("locality %s: the %s warning carries no readable %s (%q)", at.Name, rej.Phenomenon.Key, rej.Field, rej.Reason)
			}
		}
		for _, in := range snap.InForce {
			if !in.To.After(time.Now()) {
				continue // already over; the provider keeps it briefly
			}
			ev := s.warningEvent(in)
			warnKeys[ev.Key()] = true
			if err := emit.Emit(ctx, ev); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				slog.Warn("burzedzis emit failed; retrying on the next poll", "event_key", ev.Key(), "error", err)
				continue
			}
			warnEmitted++
		}
	}
	// Reconciliation only runs on a complete snapshot pass: a failed
	// locality must not be mistaken for a withdrawn warning. Both the pass
	// and the protection below are scoped to THIS instance's events by the
	// emitter (SourceActiveEventReader), so a second configured instance
	// following other localities never withdraws these alerts.
	if failed == 0 {
		reconcile := true
		if len(rejected) > 0 {
			// The rejected phenomena are unknown, so the events behind them
			// are put back into the "still in force" set: an unreadable date
			// must never read as a withdrawal. Everything else reconciles
			// normally, so a real withdrawal in the same cycle still lands.
			if err := protectRejected(ctx, emit, warnKeys, rejected); err != nil {
				reconcile = false
				slog.Warn("burzedzis reconciliation skipped: the alerts of the rejected phenomena cannot be protected", "error", err)
			}
		}
		if reconcile {
			if n, err := snapshotutil.Reconcile(ctx, emit, Type, warnKeys, time.Now()); err != nil {
				slog.Warn("burzedzis warning reconciliation failed", "error", err)
			} else if n > 0 {
				slog.Info("burzedzis withdrawn warnings cancelled", "count", n)
			}
		}
	} else {
		slog.Warn("burzedzis reconciliation skipped: the snapshot is incomplete", "failed_locality_queries", failed)
	}

	stormEmitted := 0
	if s.cfg.StormTracking {
		n, err := s.pollStorm(ctx, emit, points, failed == 0)
		stormEmitted = n
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			slog.Warn("burzedzis lightning search failed", "error", err)
		}
	}

	if stats, ok := emit.(plugin.SourceStatsReporter); ok {
		summary := fmt.Sprintf("%d localities · %d warnings", len(points), warnEmitted)
		if s.cfg.StormTracking {
			summary += fmt.Sprintf(" · %d lightning", stormEmitted)
		}
		if len(unknown) > 0 {
			summary += " · unknown: " + strings.Join(unknown, ", ")
		}
		if len(rejected) > 0 {
			names := make([]string, 0, len(rejected))
			for _, rej := range rejected {
				names = append(names, rej.phenomenon)
			}
			summary += " · incomplete answer: " + strings.Join(names, ", ")
		}
		stats.ReportSourceStats(summary)
	}
	slog.Info("burzedzis feed processed",
		"localities", len(points), "warnings", warnEmitted, "lightning", stormEmitted,
		"unknown_localities", unknown, "failed_queries", failed)
	if firstErr == nil {
		firstErr = resolveErr
	}
	return firstErr
}

// resolve looks the configured locality names up in the provider's
// directory. The two failures are deliberately kept apart:
//
//   - "the directory does not know this name" (ErrUnknownLocality) is a fact
//     about the NAME. The name is recorded, reported in the status line and
//     dropped from the point list — the Config page refuses such a name on
//     save, so this is the hand-edited-file case.
//   - every other failure (a refused request, HTTP 503, a timeout, an
//     unreadable answer) is a fact about the NETWORK. The coordinates already
//     known for that name are KEPT, the name is queued for the next poll, the
//     pass does not count as a refresh and the poll reports the source as
//     degraded. A momentary outage therefore never costs 24 hours of
//     monitoring.
func (s *Source) resolve(ctx context.Context) error {
	now := time.Now()
	s.mu.Lock()
	refreshDue := s.refreshed.IsZero() ||
		s.cfg.LocalityRefresh <= 0 || now.Sub(s.refreshed) >= s.cfg.LocalityRefresh
	due := make([]string, 0, len(s.cfg.Locations))
	for _, name := range s.cfg.Locations {
		if refreshDue || s.retry[name] {
			due = append(due, name)
			continue
		}
		if _, ok := s.points[name]; !ok && !s.unknown[name] {
			due = append(due, name)
		}
	}
	s.mu.Unlock()
	if len(due) == 0 {
		return nil
	}

	failed := false
	var firstErr error
	for _, name := range due {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		loc, err := s.client.Locality(ctx, name)
		if err != nil {
			if errors.Is(err, burzedzis.ErrUnknownLocality) {
				s.mu.Lock()
				s.unknown[name] = true
				delete(s.points, name)
				delete(s.retry, name)
				s.mu.Unlock()
				slog.Warn("burzedzis locality unknown to the provider (check the spelling on the Config page)", "locality", name, "error", err)
				continue
			}
			// Not a typo: keep what we have and come back next poll.
			failed = true
			s.mu.Lock()
			s.retry[name] = true
			s.mu.Unlock()
			slog.Warn("burzedzis locality lookup failed; keeping the last known coordinates", "locality", name, "error", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("locality %s: %w", name, err)
			}
			continue
		}
		s.mu.Lock()
		s.points[name] = loc
		delete(s.unknown, name)
		delete(s.retry, name)
		s.mu.Unlock()
	}

	s.mu.Lock()
	if !failed {
		s.refreshed = now
	}
	retrying := len(s.retry)
	s.mu.Unlock()
	if retrying > 0 {
		slog.Info("burzedzis localities pending a retry", "count", retrying, "next_poll", s.cfg.PollInterval)
	}
	return firstErr
}

// rejectedPhenomenon names one phenomenon the provider published a degree for
// without a readable window: the pair that identifies the alerts to protect.
type rejectedPhenomenon struct {
	locality   string
	phenomenon string
}

// protectRejected adds the live events of the rejected phenomena back into the
// "still in force" set, so the reconciliation of this cycle cannot read an
// unreadable date as a withdrawal. The read is scoped to this source instance
// (the emitter only returns the events it owns). The identity of a warning is
// "<locality>:<phenomenon>:<window start>", so the pair is matched as a prefix.
func protectRejected(ctx context.Context, emit plugin.Emitter, keys map[string]bool, rejected []rejectedPhenomenon) error {
	reader, ok := emit.(plugin.SourceActiveEventReader)
	if !ok {
		return fmt.Errorf("emitter lacks the source active-event reader")
	}
	active, err := reader.ListSourceActiveEvents(ctx, Type)
	if err != nil {
		return fmt.Errorf("read current active events: %w", err)
	}
	for _, ev := range active {
		for _, rej := range rejected {
			if strings.HasPrefix(ev.SourceID, rej.locality+":"+rej.phenomenon+":") {
				keys[ev.Key()] = true
				slog.Info("burzedzis alert kept: its window could not be read this cycle", "event_key", ev.Key())
			}
		}
	}
	return nil
}

// snapshotPoints returns the known coordinates in CONFIG order (the order the
// operator wrote them) plus the names the directory does not know. A name
// whose lookup failed keeps its last good coordinates and simply does not
// appear in the unknown list.
func (s *Source) snapshotPoints() ([]burzedzis.Locality, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	points := make([]burzedzis.Locality, 0, len(s.points))
	for _, name := range s.cfg.Locations {
		if loc, ok := s.points[name]; ok {
			points = append(points, loc)
		}
	}
	unknown := make([]string, 0, len(s.unknown))
	for _, name := range s.cfg.Locations {
		if s.unknown[name] {
			unknown = append(unknown, name)
		}
	}
	return points, unknown
}

// Register wires the plugin into the registry and declares the accepted
// poll-interval range, so the Config page can never write a value the
// plugin would refuse at startup.
func Register(reg *plugin.Registry) error {
	reg.SetSourcePollBounds(Type, minPollInterval, maxPollInterval)
	return reg.RegisterSource(Type, New)
}
