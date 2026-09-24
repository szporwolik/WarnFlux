package rso

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/severity"
)

// Type is the plugin type name used in the YAML configuration.
const Type = "rso"

const (
	defaultPollInterval   = 5 * time.Minute
	defaultRequestTimeout = 10 * time.Second
	defaultBaseURL        = "https://komunikaty.tvp.pl"

	minPollInterval   = time.Minute
	maxPollInterval   = time.Hour
	maxRequestTimeout = time.Minute
)

// sourceRSO is the provider-wide namespace of every RSO communication —
// the same communication is visible through multiple regional queries and
// must keep one identity.
const sourceRSO = "rso"

// officialVoivodeshipSlugs are the CURRENT official RSO voivodeship slugs
// (verified live from https://komunikaty.tvp.pl/wojewodztwa?_format=xml)
// plus the national wildcard. Configuration may also use the official
// display names (e.g. "małopolskie"), which canonicalize to these slugs.
var officialVoivodeshipSlugs = map[string]bool{
	"dolnoslaskie":        true,
	"kujawsko-pomorskie":  true,
	"lubelskie":           true,
	"lubuskie":            true,
	"lodzkie":             true,
	"malopolskie":         true,
	"mazowieckie":         true,
	"opolskie":            true,
	"podkarpackie":        true,
	"podlaskie":           true,
	"pomorskie":           true,
	"slaskie":             true,
	"swietokrzyskie":      true,
	"warminsko-mazurskie": true,
	"wielkopolskie":       true,
	"zachodniopomorskie":  true,
	"wszystkie":           true,
}

// officialVoivodeshipNames maps the official Polish display names to slugs.
var officialVoivodeshipNames = map[string]string{
	"dolnośląskie":        "dolnoslaskie",
	"kujawsko-pomorskie":  "kujawsko-pomorskie",
	"lubelskie":           "lubelskie",
	"lubuskie":            "lubuskie",
	"łódzkie":             "lodzkie",
	"małopolskie":         "malopolskie",
	"mazowieckie":         "mazowieckie",
	"opolskie":            "opolskie",
	"podkarpackie":        "podkarpackie",
	"podlaskie":           "podlaskie",
	"pomorskie":           "pomorskie",
	"śląskie":             "slaskie",
	"świętokrzyskie":      "swietokrzyskie",
	"warmińsko-mazurskie": "warminsko-mazurskie",
	"wielkopolskie":       "wielkopolskie",
	"zachodniopomorskie":  "zachodniopomorskie",
}

// Config is the plugin-specific configuration. The public XML integration
// requires no credentials.
type Config struct {
	PollInterval   time.Duration `yaml:"poll_interval"`
	RequestTimeout time.Duration `yaml:"request_timeout"`
	// BaseURL is the RSO portal base (default: https://komunikaty.tvp.pl);
	// tests override it with an httptest server.
	BaseURL string `yaml:"base_url"`
	// Voivodeships filters UPSTREAM via the official endpoint; default
	// [wszystkie]. "wszystkie" is exclusive (not combinable with regions).
	Voivodeships []string `yaml:"voivodeships"`
	// Filter is the optional high-signal policy (see filter.go).
	Filter *FileFilterConfig `yaml:"filter"`
}

// filePlaceConfig maps one configured place slug to its keyword fragments
// and the area tokens emitted on events classified there.
type filePlaceConfig struct {
	Keywords []string `yaml:"keywords"`
	Areas    []string `yaml:"areas"`
}

// fileLocalConfig is the optional installation-specific geography block.
// Nothing about the target area is hardcoded in the binary: keywords are
// regexp fragments (plain words work), places map keyword sets to the area
// tokens emitted on the event, roads are ids like a4/s7/dk75/dw964.
type fileLocalConfig struct {
	CoreKeywords     []string                   `yaml:"core_keywords"`
	PowiatKeywords   []string                   `yaml:"powiat_keywords"`
	NearbyKeywords   []string                   `yaml:"nearby_keywords"`
	Places           map[string]filePlaceConfig `yaml:"places"`
	Roads            []string                   `yaml:"roads"`
	SevereRoads      []string                   `yaml:"severe_roads"`
	CorridorKeywords []string                   `yaml:"corridor_keywords"`
	CoreAreas        []string                   `yaml:"core_areas"`
	CorridorArea     string                     `yaml:"corridor_area"`
}

type fileCorridor struct {
	Enabled *bool `yaml:"enabled"`
	KMFrom  *int  `yaml:"km_from"`
	KMTo    *int  `yaml:"km_to"`
}

// FileFilterConfig is the decoded filter block; defaults are applied and
// validated in New.
type FileFilterConfig struct {
	HighSignalOnly      *bool  `yaml:"high_signal_only"`
	ExcludeRCB          *bool  `yaml:"exclude_rcb"`
	ExcludeAirQuality   *bool  `yaml:"exclude_air_quality"`
	SuppressIMGWDupes   *bool  `yaml:"suppress_imgw_duplicates"`
	LocalMinSeverity    string `yaml:"local_min_severity"`
	RegionalMinSeverity string `yaml:"regional_min_severity"`
	// Corridor is the generic kilometer-corridor block. a4_corridor is
	// accepted as a legacy alias (Corridor wins when both are present).
	Corridor   *fileCorridor    `yaml:"corridor"`
	A4Corridor *fileCorridor    `yaml:"a4_corridor"`
	Local      *fileLocalConfig `yaml:"local"`
}

// Source polls the public RSO XML and ingests the current communication
// set. Each regional page-0 response is a complete current-state snapshot
// (verified live), so disappearances are reconciled against the
// authoritative SQLite active state for the combined snapshot.
type Source struct {
	cfg    Config
	client *Client
	// policy is the runtime-ready filter (nil = pass-through).
	policy *filterConfig
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
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("base_url must be a valid http(s) URL with a host, got %q", cfg.BaseURL)
	}
	cfg.BaseURL = baseURL

	if len(cfg.Voivodeships) == 0 {
		cfg.Voivodeships = []string{"wszystkie"}
	}
	canonical := make([]string, 0, len(cfg.Voivodeships))
	seen := make(map[string]bool, len(cfg.Voivodeships))
	for _, raw := range cfg.Voivodeships {
		slug := canonicalVoivodeship(raw)
		if slug == "" {
			return nil, fmt.Errorf("unknown voivodeship %q (use an official RSO slug)", raw)
		}
		if seen[slug] {
			return nil, fmt.Errorf("duplicate voivodeship %q", slug)
		}
		seen[slug] = true
		canonical = append(canonical, slug)
	}
	if seen["wszystkie"] && len(canonical) > 1 {
		return nil, fmt.Errorf("voivodeship \"wszystkie\" is exclusive and cannot be combined with regional slugs")
	}
	cfg.Voivodeships = canonical

	policy, err := buildFilterConfig(cfg.Filter)
	if err != nil {
		return nil, err
	}

	return &Source{cfg: cfg, client: NewClient(cfg.BaseURL, cfg.RequestTimeout), policy: policy}, nil
}

// buildFilterConfig validates and defaults the filter block. A nil block
// disables the policy entirely (pass-through, historic behaviour).
func buildFilterConfig(f *FileFilterConfig) (*filterConfig, error) {
	if f == nil {
		return nil, nil
	}
	p := &filterConfig{
		highSignalOnly:      boolDefault(f.HighSignalOnly, false),
		excludeRCB:          boolDefault(f.ExcludeRCB, true),
		excludeAirQuality:   boolDefault(f.ExcludeAirQuality, true),
		suppressIMGWDupes:   boolDefault(f.SuppressIMGWDupes, false),
		localMinSeverity:    "moderate",
		regionalMinSeverity: "severe",
		corridor:            corridorConfig{enabled: false},
	}
	if s := strings.TrimSpace(f.LocalMinSeverity); s != "" {
		p.localMinSeverity = s
	}
	if s := strings.TrimSpace(f.RegionalMinSeverity); s != "" {
		p.regionalMinSeverity = s
	}
	if !severity.Valid(p.localMinSeverity) {
		return nil, fmt.Errorf("filter.local_min_severity must be a canonical severity, got %q", p.localMinSeverity)
	}
	if !severity.Valid(p.regionalMinSeverity) {
		return nil, fmt.Errorf("filter.regional_min_severity must be a canonical severity, got %q", p.regionalMinSeverity)
	}
	corridor := f.Corridor
	if corridor == nil {
		corridor = f.A4Corridor
	}
	if corridor != nil {
		if corridor.Enabled != nil {
			p.corridor.enabled = *corridor.Enabled
		}
		if corridor.KMFrom != nil {
			p.corridor.kmFrom = *corridor.KMFrom
		}
		if corridor.KMTo != nil {
			p.corridor.kmTo = *corridor.KMTo
		}
		if p.corridor.kmFrom <= 0 || p.corridor.kmTo <= p.corridor.kmFrom {
			return nil, fmt.Errorf("filter.corridor requires 0 < km_from < km_to, got %d..%d", p.corridor.kmFrom, p.corridor.kmTo)
		}
	}
	if err := p.buildLocal(f.Local); err != nil {
		return nil, err
	}
	return p, nil
}

// buildLocal compiles the optional filter.local geography block. An
// absent block leaves every pattern nil: the policy then has no local
// scope and only the regional threshold applies.
func (p *filterConfig) buildLocal(l *fileLocalConfig) error {
	if l == nil {
		return nil
	}
	p.corePattern = keywordRE(l.CoreKeywords)
	p.powiatPattern = keywordRE(l.PowiatKeywords)
	p.nearbyPattern = keywordRE(l.NearbyKeywords)
	p.corridorLocs = keywordRE(l.CorridorKeywords)
	p.coreAreas = append([]string(nil), l.CoreAreas...)
	p.corridorArea = strings.TrimSpace(l.CorridorArea)

	p.placeAreas = make(map[string][]string, len(l.Places))
	p.cityPatterns = make(map[string]*regexp.Regexp, len(l.Places))
	for rawSlug, pl := range l.Places {
		slug := strings.TrimSpace(rawSlug)
		if slug == "" {
			return fmt.Errorf("filter.local.places: empty place slug")
		}
		re := keywordRE(pl.Keywords)
		if re == nil {
			return fmt.Errorf("filter.local.places.%s requires at least one keyword", slug)
		}
		p.cityPatterns[slug] = re
		p.placeAreas[slug] = append([]string(nil), pl.Areas...)
	}

	p.roadPatterns = make(map[string]*regexp.Regexp, len(l.Roads))
	for _, raw := range l.Roads {
		id := strings.ToLower(strings.TrimSpace(raw))
		if !roadIDRe.MatchString(id) {
			return fmt.Errorf("filter.local.roads entry %q must look like a4, s7, dk75 or dw964", raw)
		}
		p.roadPatterns[id] = roadRE(id)
	}

	var severe []string
	for _, raw := range l.SevereRoads {
		id := strings.ToLower(strings.TrimSpace(raw))
		if _, ok := p.roadPatterns[id]; !ok {
			return fmt.Errorf("filter.local.severe_roads entry %q must also be listed in roads", raw)
		}
		severe = append(severe, id)
	}
	sort.Strings(severe)
	p.severeRoadRe = keywordRE(severe)
	return nil
}

func boolDefault(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
}

// canonicalVoivodeship maps an official slug or display name (any case) to
// the official slug; "" means unknown (a typo fails configuration).
func canonicalVoivodeship(raw string) string {
	lower := strings.ToLower(strings.TrimSpace(raw))
	if officialVoivodeshipSlugs[lower] {
		return lower
	}
	if slug, ok := officialVoivodeshipNames[lower]; ok {
		return slug
	}
	return ""
}

// Name returns the plugin type name.
func (s *Source) Name() string { return Type }

// Run polls the RSO XML immediately and then every poll_interval. Snapshot
// reconciliation requires the SourceActiveEventReader capability: without
// it the source refuses to run instead of silently risking stale
// communications.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	if _, ok := emit.(plugin.SourceActiveEventReader); !ok {
		return fmt.Errorf("emitter lacks the source active-event reader; RSO snapshot disappearance reconciliation would be unsafe")
	}
	reporter, _ := emit.(plugin.SourceHealthReporter)
	slog.Info("rso plugin started", "voivodeships", s.cfg.Voivodeships, "poll_interval", s.cfg.PollInterval)

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
	slog.Info("rso plugin stopping")
	return nil
}

// Register registers the rso source plugin type.
func Register(reg *plugin.Registry) error {
	return reg.RegisterSource(Type, New)
}
