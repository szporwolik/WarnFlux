package rso

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/plugin"
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
}

// Source polls the public RSO XML and ingests the current communication
// set. Each regional page-0 response is a complete current-state snapshot
// (verified live), so disappearances are reconciled against the
// authoritative SQLite active state for the combined snapshot.
type Source struct {
	cfg    Config
	client *Client
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

	return &Source{cfg: cfg, client: NewClient(cfg.BaseURL, cfg.RequestTimeout)}, nil
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
