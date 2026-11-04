// Package gddkiadatex is a hazard source for the GDDKiA KPD platform: the
// national road-obstruction dataset, pulled over the DATEX II snapshot-pull
// SOAP service with a client-certificate WS-Security signature.
//
// It is a different provider from the retired "gddkia" XML feed (utrdane.xml):
// separate identities, separate source namespace, both may run side by side
// while the old one is still feeding.
package gddkiadatex

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/snapshotutil"
)

// Type is the plugin type name used in the YAML configuration.
const Type = "gddkia-datex"

const (
	defaultEndpoint       = "https://kpd.gddkia.gov.pl/addiod/ws_v34/snapshotPull"
	defaultPollInterval   = 15 * time.Minute
	defaultRequestTimeout = 60 * time.Second

	minPollInterval   = time.Minute
	maxPollInterval   = 24 * time.Hour
	maxRequestTimeout = 5 * time.Minute
	// maxRecords bounds the parsed publication (national listing: ~500 today).
	maxRecords = 20000
)

// Config is the plugin-specific configuration.
type Config struct {
	PollInterval   time.Duration `yaml:"poll_interval"`
	RequestTimeout time.Duration `yaml:"request_timeout"`
	// Endpoint is the snapshot-pull SOAP address.
	Endpoint string `yaml:"endpoint"`
	// CertFile is the client identity: a PKCS#12 (.p12/.pfx) file, or a PEM
	// certificate when CertKeyFile is set. Both are secrets: keep them in the
	// configuration volume, never in the repository.
	CertFile string `yaml:"cert_file"`
	// CertPassword unlocks a PKCS#12 file.
	CertPassword string `yaml:"cert_password"`
	// CertPasswordFile reads the password from a file instead (preferred: the
	// configuration file is readable by the panel's user only, but a separate
	// file keeps the secret out of anything that copies the YAML).
	CertPasswordFile string `yaml:"cert_password_file"`
	// CertKeyFile is the PEM private key that pairs with a PEM CertFile.
	CertKeyFile string `yaml:"cert_key_file"`
	// RadiusKM overrides the area radius; 0 = the APRS hub area radius.
	RadiusKM float64 `yaml:"radius_km"`
	// CenterLat/CenterLon override the area center; unset = the station area.
	CenterLat *float64 `yaml:"center_latitude"`
	CenterLon *float64 `yaml:"center_longitude"`
}

// Source polls the KPD snapshot-pull service. Each pull is a COMPLETE provider
// snapshot, so disappearances are reconciled against the active state.
type Source struct {
	cfg       Config
	client    *Client
	centerLat float64
	centerLon float64
	radiusKM  float64
	cert      CertificateInfo
}

// New decodes and validates the plugin configuration. hub provides the default
// geographic scope (without a station position there is no area to filter to).
func New(node *yaml.Node, hub *aprs.Hub) (plugin.SourcePlugin, error) {
	if hub == nil {
		return nil, fmt.Errorf("%s: the APRS hub is disabled, but its position and radius define the geographic scope", Type)
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
	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("endpoint must be a valid http(s) URL with a host, got %q", cfg.Endpoint)
	}
	cfg.Endpoint = endpoint

	password := cfg.CertPassword
	if file := strings.TrimSpace(cfg.CertPasswordFile); file != "" {
		raw, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("cert_password_file: %w", err)
		}
		password = strings.TrimSpace(string(raw))
	}
	key, cert, err := loadKeyPair(cfg.CertFile, cfg.CertKeyFile, password)
	if err != nil {
		return nil, err
	}
	s, err := newSigner(key, cert)
	if err != nil {
		return nil, err
	}

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
		client:    NewClient(cfg.Endpoint, s, cfg.RequestTimeout),
		centerLat: centerLat,
		centerLon: centerLon,
		radiusKM:  radius,
		cert:      info(cert),
	}, nil
}

// Name returns the plugin type name.
func (s *Source) Name() string { return Type }

// Run polls the service on the configured interval. A provider outage is one
// logged episode, not one warning per poll.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	reporter, _ := emit.(plugin.SourceHealthReporter)
	slog.Info("gddkia-datex plugin started",
		"endpoint", s.cfg.Endpoint, "poll_interval", s.cfg.PollInterval,
		"radius_km", s.radiusKM, "center_lat", s.centerLat, "center_lon", s.centerLon,
		"certificate", s.cert.Subject, "cert_expires", s.cert.NotAfter.Format(time.RFC3339))

	failures := 0
	var outageSince time.Time
	for {
		if ctx.Err() != nil {
			break
		}
		err := s.pollOnce(ctx, emit)
		switch {
		case err != nil && ctx.Err() != nil:
		case err != nil:
			failures++
			if failures == 1 {
				outageSince = time.Now()
				slog.Warn("gddkia-datex poll failed", "error", err, "retry_in", s.cfg.PollInterval)
			} else {
				slog.Debug("gddkia-datex poll still failing", "error", err,
					"failed_attempts", failures, "retry_in", s.cfg.PollInterval)
			}
			if reporter != nil {
				reporter.ReportSourceDegraded(err)
			}
		default:
			if failures > 0 {
				slog.Info("gddkia-datex poll recovered",
					"failed_attempts", failures, "outage", time.Since(outageSince).Round(time.Second))
				failures = 0
			}
			if reporter != nil {
				reporter.ReportSourceHealthy()
			}
		}
		timer := time.NewTimer(s.cfg.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	slog.Info("gddkia-datex plugin stopping")
	return nil
}

// pollOnce pulls the snapshot, filters it to the configured area, emits the
// survivors and reconciles the ones that disappeared.
func (s *Source) pollOnce(ctx context.Context, emit plugin.Emitter) error {
	body, err := s.client.Fetch(ctx)
	if err != nil {
		return err
	}
	doc, err := parseResponse(body)
	if err != nil {
		return err
	}

	now := time.Now()
	seen := make(map[string]core.HazardEvent)
	filtered, total := 0, 0
	for _, sit := range doc.Payload.Situations {
		for _, rec := range sit.Records {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			total++
			if total > maxRecords {
				return fmt.Errorf("publication carries more than %d records", maxRecords)
			}
			ev, err := normalize(rec, s.cfg.Endpoint)
			if err != nil {
				slog.Debug("gddkia-datex record skipped", "record", rec.ID, "reason", err.Error())
				filtered++
				continue
			}
			if aprs.DistanceKM(s.centerLat, s.centerLon, *ev.Latitude, *ev.Longitude) > s.radiusKM {
				filtered++
				continue
			}
			// The same record may repeat inside one publication; the last
			// version wins and the identity stays single.
			seen[ev.Key()] = ev
		}
	}

	keys := make(map[string]bool, len(seen))
	for key, ev := range seen {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		keys[key] = true
		if err := emit.Emit(ctx, ev); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Warn("gddkia-datex emit failed; retrying on the next poll", "event_key", key, "error", err)
			delete(keys, key)
		}
	}
	if _, err := snapshotutil.Reconcile(ctx, emit, sourceName, keys, now); err != nil && ctx.Err() == nil {
		slog.Warn("gddkia-datex reconciliation failed", "error", err)
	}
	slog.Info("gddkia-datex feed processed",
		"records", total, "in_area", len(keys), "filtered", filtered)
	if stats, ok := emit.(plugin.SourceStatsReporter); ok {
		stats.ReportSourceStats(fmt.Sprintf("%d / %d in area", len(keys), total))
	}
	if fr, ok := emit.(plugin.SourceFilterReporter); ok {
		fr.ReportSourceFiltered(filtered)
	}
	return nil
}

// Register registers the source plugin type.
func Register(reg *plugin.Registry, hub *aprs.Hub) error {
	reg.SetSourcePollBounds(Type, minPollInterval, maxPollInterval)
	return reg.RegisterSource(Type, func(node *yaml.Node) (plugin.SourcePlugin, error) {
		return New(node, hub)
	})
}
