// Command warnflux is the WarnFlux entry point.
//
// It loads a YAML configuration, runs the plugin manager (sources, durable
// ingestion and outputs) and shuts down cleanly on SIGINT/SIGTERM.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/actions"
	smtp "github.com/szporwolik/WarnFlux/internal/actions/smtp"
	"github.com/szporwolik/WarnFlux/internal/appinfo"
	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/aprspresence"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/geo"
	"github.com/szporwolik/WarnFlux/internal/ingest"
	"github.com/szporwolik/WarnFlux/internal/ingesthttp"
	"github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/metrics"
	"github.com/szporwolik/WarnFlux/internal/mqttpolicy"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/plugins"
	mqttout "github.com/szporwolik/WarnFlux/internal/plugins/outputs/mqtt"
	"github.com/szporwolik/WarnFlux/internal/radiocli"
	"github.com/szporwolik/WarnFlux/internal/routing"
	"github.com/szporwolik/WarnFlux/internal/severity"
	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
	"github.com/szporwolik/WarnFlux/internal/trail"
	"github.com/szporwolik/WarnFlux/internal/weatherreport"
	"github.com/szporwolik/WarnFlux/internal/web"
)

// resolveStoragePath fills in a database location when the configuration
// did not provide one: the database lands next to the executable, which
// keeps dev/debug runs self-contained (e.g. build/warnflux.db after a
// VS Code build task). An explicitly configured path is returned as-is.
func resolveStoragePath(path string, logger *slog.Logger) string {
	if strings.TrimSpace(path) != "" {
		return path
	}
	if exe, err := os.Executable(); err == nil {
		resolved := filepath.Join(filepath.Dir(exe), "warnflux.db")
		logger.Warn("storage.path not configured; creating the database next to the binary (dev/debug)",
			"storage_path", resolved)
		return resolved
	}
	logger.Warn("storage.path not configured and the executable directory is unavailable; using ./warnflux.db")
	return "warnflux.db"
}

// aprsManagerSink adapts the MQTT receiver manager to the APRS hub's
// publishing surface: hub topics go out under the first connected
// WarnFlux receiver's topic prefix. Canonical /events payloads are also
// dispatched locally first, so radio-message alarms never depend on the
// broker round-trip.
type aprsManagerSink struct {
	mgmt    *mqttreceiver.Manager
	ingress *dispatch.Ingress
	logger  *slog.Logger
}

func (s *aprsManagerSink) PublishRaw(suffix string, retained bool, payload []byte) error {
	if suffix == "events" {
		dispatchLocalEvent(s.ingress, s.logger, payload)
	}
	return s.mgmt.PublishRaw(suffix, retained, payload)
}

// dispatchLocalEvent enqueues one canonical /events payload into the
// LOCAL ingress before the broker publish: local routing (the durable
// inbox row) never depends on the broker round-trip. The loopback copy
// arriving through the receiver deduplicates at the delivery ledger.
// The explicit acceptance is returned so callers can confirm durable
// acceptance, report the emergency fallback and rejections separately.
func dispatchLocalEvent(ingress *dispatch.Ingress, logger *slog.Logger, payload []byte) dispatch.Acceptance {
	we, err := mqttreceiver.ParseEventPayload(payload)
	if err != nil {
		return dispatch.Rejected // hub-built payloads always parse; defensive only
	}
	ev := mqttreceiver.EventFromWire(we, "local", time.Now())
	switch acc := ingress.Enqueue(ev); acc {
	case dispatch.Rejected:
		logger.Warn("local dispatch rejected a radio event", "event_key", ev.Hazard.Key)
		return acc
	case dispatch.AcceptedEmergency:
		logger.Warn("radio event accepted WITHOUT durable storage (emergency mode; lost on restart)", "event_key", ev.Hazard.Key)
		return acc
	default:
		return acc
	}
}

// dispatchLocalChange enqueues one journal change as a canonical
// transition directly into the LOCAL ingress: SQLite and the radio
// suffice to serve the communication, the broker loopback is only the
// asynchronous sync copy for other instances (identical publisher +
// change ID, so the delivery ledger deduplicates the pair). inboxID is
// the durable inbox row the store committed in the same transaction as
// the journal change: the enqueue reuses it, so a crash between the
// commit and this dispatch leaves the row behind and inbox recovery
// re-delivers the alert after a restart.
func dispatchLocalChange(ingress *dispatch.Ingress, logger *slog.Logger, change core.EventChange, inboxID int64) {
	if change.ID == 0 {
		return // synthetic change without a journal record
	}
	ev := mqttreceiver.EventFromChange(change, "local", time.Now())
	ev.InboxID = inboxID
	switch ingress.Enqueue(ev) {
	case dispatch.Rejected:
		logger.Warn("local dispatch rejected a journal change", "change_id", change.ID, "event_key", ev.Hazard.Key)
	case dispatch.AcceptedEmergency:
		logger.Warn("journal change accepted WITHOUT durable storage (emergency mode; lost on restart)", "change_id", change.ID, "event_key", ev.Hazard.Key)
	}
}

// radioAlertTTL bounds a radio-raised /alert hazard (4 hours by
// default).
const radioAlertTTL = 4 * time.Hour

// hazardsFromMirror projects the dispatch mirror's active hazards,
// most severe first (Snapshot sorts them). minSeverity keeps only that
// level and above ("" = the full list).
func hazardsFromMirror(mirror *state.State, minSeverity string) []meshtastic.ActiveHazard {
	snap := mirror.Snapshot()
	filter := minSeverity != ""
	minRank := 0
	if filter {
		minRank, _ = severity.Rank(minSeverity)
	}
	out := make([]meshtastic.ActiveHazard, 0, len(snap.Hazards))
	for _, h := range snap.Hazards {
		if filter {
			if r, _ := severity.Rank(h.Severity); r < minRank {
				continue
			}
		}
		out = append(out, meshtastic.ActiveHazard{
			Headline:    h.Headline,
			Description: h.Description,
			EffectiveAt: h.EffectiveAt,
			ExpiresAt:   h.ExpiresAt,
		})
	}
	return out
}

// hazardsForRadio renders the active-hazard list for the public /hazard
// radio command: a header plus one Zulu-windowed line per hazard — the
// same lines the hourly emcom-channel digest broadcasts, so the command
// and the digest never disagree.
func hazardsForRadio(hazards []meshtastic.ActiveHazard) string {
	if len(hazards) == 0 {
		return "No active messages"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Active messages: %d", len(hazards))
	for _, hz := range hazards {
		b.WriteByte('\n')
		b.WriteString(meshtastic.HazardDigestLine(hz))
	}
	return b.String()
}

// weatherForRadio snapshots every current weather source — APRS weather
// stations plus the retained internet-provider state — into the shared
// weatherreport aggregation for the public /weather command. APRS
// stations echo back into the info state through the broker; those
// echoes are skipped so a station never counts twice.
func weatherForRadio(hub *aprs.Hub, mirror *state.State, now time.Time) weatherreport.Report {
	var readings []weatherreport.Reading
	var forecasts []weatherreport.Forecast
	aprsSeen := make(map[string]bool)
	if hub != nil {
		// The LOCAL hub cache is the primary APRS source: it is updated
		// before the infrastructure filter and before any broker
		// publication, so /weather stays honest off-grid (RF reception
		// with a dead broker). The mirror copy below only adds the
		// internet providers.
		for _, w := range hub.WeatherSnapshot(now) {
			aprsSeen[strings.ToLower(w.Callsign)] = true
			r := weatherreport.Reading{At: w.Time, Source: w.Callsign}
			if w.TemperatureC != nil {
				r.TempC = *w.TemperatureC
				r.HasTemp = true
			}
			if w.HumidityPct != nil {
				r.HumPct = *w.HumidityPct
				r.HasHum = true
			}
			readings = append(readings, r)
		}
	}
	if mirror != nil {
		for _, e := range mirror.Snapshot().Weather {
			ww := e.Weather
			if ww == nil {
				continue
			}
			name := ww.LocationName
			if name == "" {
				name = ww.LocationID
			}
			if aprsSeen[strings.ToLower(name)] {
				continue // APRS echo off the broker
			}
			r := weatherreport.Reading{At: ww.GeneratedAt, Source: ww.ProviderName}
			if ww.TemperatureC != nil {
				r.TempC = *ww.TemperatureC
				r.HasTemp = true
			}
			if ww.HumidityPct != nil {
				r.HumPct = *ww.HumidityPct
				r.HasHum = true
			}
			readings = append(readings, r)
			if len(ww.Daily) > 0 {
				d := ww.Daily[0]
				f := weatherreport.Forecast{At: ww.GeneratedAt, Source: ww.ProviderName}
				if d.TemperatureMaxC != nil {
					f.TempMaxC = *d.TemperatureMaxC
					f.HasMax = true
				}
				if d.TemperatureMinC != nil {
					f.TempMinC = *d.TemperatureMinC
					f.HasMin = true
				}
				forecasts = append(forecasts, f)
			}
		}
	}
	return weatherreport.Summarize(now, readings, forecasts)
}

// seedMeshtasticStations restores the heard-node list from the retained
// meshtastic/stations/# documents on the broker, so stations survive
// restarts the same way the APRS retained station state does. It retries
// with bounded backoff until a receiver connects or the context ends;
// the hub merges fresh documents without publishing and tombstones
// expired ones.
func seedMeshtasticStations(ctx context.Context, meshtasticHub *meshtastic.Hub, receivers *mqttreceiver.Manager, cfg *config.Config, logger *slog.Logger) {
	prefix := "warnflux"
	for _, r := range cfg.Dispatch.Receivers {
		if r.Enabled && r.WF.Enabled && r.WF.TopicPrefix != "" {
			prefix = r.WF.TopicPrefix
			break
		}
	}
	backoff := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		entries, err := receivers.Browse(ctx, "", prefix+"/meshtastic/stations/#",
			mqttreceiver.BrowseDefaultWindow, mqttreceiver.BrowseMaxEntries)
		if err == nil {
			seeded := 0
			for _, e := range entries {
				var doc struct {
					ID       string   `json:"id"`
					Name     string   `json:"name"`
					Short    string   `json:"short"`
					Sends    []string `json:"sends"`
					Lat      float64  `json:"lat"`
					Lon      float64  `json:"lon"`
					LastSeen string   `json:"last_seen"`
				}
				if json.Unmarshal([]byte(e.Payload), &doc) != nil || doc.ID == "" {
					continue
				}
				seen, err := time.Parse(time.RFC3339, doc.LastSeen)
				if err != nil {
					seen = time.Time{}
				}
				meshtasticHub.SeedNode(doc.ID, doc.Name, doc.Short, doc.Lat, doc.Lon, seen, doc.Sends)
				seeded++
			}
			logger.Info("meshtastic: restored heard stations", "count", seeded)
			return
		}
		logger.Debug("meshtastic: station restore retrying", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// version and commit are injected at build time via -ldflags:
//
//	-X main.version=vX.Y.Z -X main.commit=<sha>
//
// Development builds report "dev" / "unknown". The canonical version
// source of truth is the VERSION file at the repository root: release
// builds inject it via ldflags, and resolveVersion lets dev builds pick it
// up from next to the binary or from the working directory (cqops-style).
var (
	version = "dev"
	commit  = "unknown"
)

// resolveVersion upgrades a "dev" build to the version from a VERSION file
// when one is present next to the executable or in the working directory.
// Injected (non-dev) versions are returned unchanged; the file is trimmed
// so editors that add a trailing newline cannot corrupt it.
func resolveVersion(v string) string {
	if v != "dev" {
		return v
	}
	var candidates []string
	if exe, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(exe), "VERSION"))
	}
	candidates = append(candidates, "VERSION")
	for _, path := range candidates {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if trimmed := strings.TrimSpace(string(data)); trimmed != "" {
			return trimmed
		}
	}
	return v
}

// Shutdown bounds for the dispatch/action/web subsystems.
const (
	httpShutdownWait  = 5 * time.Second
	dispatchDrainWait = 5 * time.Second
	actionShutdownMax = 30 * time.Second
	// outboxRetention bounds the durable HTTP-ingest broker-sync backlog:
	// rows the broker could not confirm for this long are pruned, so a
	// prolonged outage cannot grow the database without bound.
	outboxRetention = 24 * time.Hour
	// commandRegistryRetention bounds the durable radio-command
	// registry: anchors older than this are pruned, so a command
	// retransmitted later starts its lifecycle fresh (a legitimate new
	// command, not a replay) and the table cannot grow without bound.
	commandRegistryRetention = 24 * time.Hour
)

// validateConfiguration mirrors run()'s static construction phase for
// -check-config: every strict YAML decoder and cross-validation runs
// (plugins, actions, MQTT receivers, ingest endpoints, web server), but
// nothing is started, no database is opened and no connection is made.
func validateConfiguration(cfg *config.Config, logger *slog.Logger, resolvedVersion string, met *metrics.Registry) error {
	registry := plugin.NewRegistry()
	hub, err := aprs.NewHub(aprs.HubConfig{
		Enabled:               cfg.APRS.Enabled,
		Callsign:              cfg.APRS.Callsign,
		Name:                  cfg.APRS.Name,
		Icon:                  cfg.APRS.Icon,
		GridSquare:            cfg.APRS.GridSquare,
		Latitude:              cfg.APRS.Latitude,
		Longitude:             cfg.APRS.Longitude,
		RadiusKM:              cfg.APRS.RadiusKM,
		AreaLatitude:          cfg.APRS.AreaLatitude,
		AreaLongitude:         cfg.APRS.AreaLongitude,
		AreaRadiusKM:          cfg.APRS.AreaRadiusKM,
		StationTTL:            cfg.APRS.StationTTL,
		BulletinTTL:           cfg.APRS.BulletinTTL,
		ExcludeInfrastructure: cfg.APRS.ExcludeInfrastructure,
		RouteMessages:         cfg.APRS.RouteMessages,
		Version:               resolvedVersion,
	}, logger)
	if err != nil {
		return fmt.Errorf("configure aprs hub: %w", err)
	}
	meshtasticHub, err := meshtastic.NewHub(meshtastic.Config{
		Enabled:       cfg.Meshtastic.Enabled,
		Device:        cfg.Meshtastic.Device,
		Transport:     cfg.Meshtastic.Transport,
		Host:          cfg.Meshtastic.Host,
		Baud:          cfg.Meshtastic.Baud,
		RouteMessages: cfg.Meshtastic.RouteMessages,
		NodeTTL:       cfg.Meshtastic.NodeTTL,
	}, logger)
	if err != nil {
		return fmt.Errorf("configure meshtastic hub: %w", err)
	}
	if err := plugins.RegisterBuiltins(registry, hub, meshtasticHub); err != nil {
		return fmt.Errorf("register built-in plugins: %w", err)
	}
	manager, err := plugin.NewManager(registry, cfg.Sources, cfg.Outputs,
		nil, nil, nil, plugin.ManagerOptions{
			ExpirationInterval: cfg.App.ExpirationInterval,
			ChangeRetention:    cfg.App.ChangeRetention,
			EventRetention:     cfg.App.EventRetention,
			Version:            resolvedVersion,
		}, logger)
	if err != nil {
		return fmt.Errorf("configure plugins: %w", err)
	}

	ingress := dispatch.NewIngress(cfg.Dispatch.QueueSize)
	mirror := state.New()
	traffic := mqttreceiver.NewTrafficBuffer(mqttreceiver.DefaultTrafficEntries)
	trails := trail.NewRecorder(trail.DefaultMaxTrails)

	actionRegistry := action.NewRegistry()
	if err := actions.RegisterAll(actionRegistry, hub, meshtasticHub); err != nil {
		return fmt.Errorf("register built-in actions: %w", err)
	}
	actionsMgr, err := action.NewManager(cfg.Actions, actionRegistry, logger, trails, met)
	if err != nil {
		return fmt.Errorf("configure actions: %w", err)
	}

	receivers, err := mqttreceiver.NewManager(cfg.Dispatch.Receivers, mirror, ingress, logger, traffic)
	if err != nil {
		return fmt.Errorf("configure mqtt receivers: %w", err)
	}

	var mainBroker config.IngestHTTP
	for _, o := range cfg.Outputs {
		if o.Enabled && o.Type == "mqtt" && o.Config != nil {
			var mc mqttout.Config
			if err := o.Config.Decode(&mc); err == nil {
				mainBroker = config.IngestHTTP{
					Broker:       mc.Broker,
					ClientID:     mc.ClientID,
					Username:     mc.Username,
					Password:     mc.Password,
					PasswordFile: mc.PasswordFile,
					TopicPrefix:  mc.TopicPrefix,
				}
			}
			break
		}
	}
	for _, ing := range cfg.IngestHTTP {
		if !ing.Enabled {
			continue
		}
		ing = ingesthttp.Resolve(ing, mainBroker)
		if ing.Broker == "" {
			return fmt.Errorf("configure ingest_http %q: no broker configured and no enabled mqtt output to inherit one from", ing.ID)
		}
		if _, err := ingesthttp.New(ing, logger); err != nil {
			return fmt.Errorf("configure ingest_http %q: %w", ing.ID, err)
		}
	}

	if cfg.Web.Enabled {
		if _, err := web.New(cfg.Web, mirror, receivers, receivers, manager, actionsMgr, hub,
			meshtasticHub, ingress, logger, resolvedVersion, commit, nil, nil, nil, nil, nil, nil, traffic, trails, met); err != nil {
			return fmt.Errorf("configure web: %w", err)
		}
	}
	return nil
}

func main() {
	fs := flag.NewFlagSet("warnflux", flag.ExitOnError)
	configPath := fs.String("config", config.DefaultConfigPath, "path to YAML configuration file")
	showVersion := fs.Bool("version", false, "print version and exit")
	checkConfig := fs.Bool("check-config", false, "validate the configuration (strict decode of every plugin/action/receiver) and exit without opening the database or connecting anywhere")
	fs.Parse(os.Args[1:])

	if *showVersion {
		fmt.Printf("warnflux %s (%s)\n", resolveVersion(version), commit)
		return
	}

	if err := run(*configPath, *checkConfig); err != nil {
		slog.Error("WarnFlux failed", "error", err)
		os.Exit(1)
	}
}

func run(configPath string, checkConfig bool) error {
	// Resolve the reported version once (dev builds may pick it up from
	// the repository VERSION file; release builds have it injected).
	resolvedVersion := resolveVersion(version)

	// In-memory log ring buffer: everything the process logs also lands
	// here and is served by the web UI's /logs viewer.
	logs := web.NewLogBuffer(web.DefaultLogLines)

	// Prometheus metrics registry: counters and gauges exposed on
	// /metrics (unauthenticated; counts only).
	met := metrics.New()

	// Phase 1 — static initialization. No background goroutines exist yet,
	// so a failure here leaves nothing running behind.
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	// Publish mask: which document families this station publishes to the
	// MQTT broker. The admin Config page toggles the same mask at runtime;
	// mqtt_publish in the config is the startup state.
	mqttpolicy.Set(cfg.MQTTPublish.Mask())

	logger, logCloser, err := newLogger(cfg.App, logs)
	if err != nil {
		return fmt.Errorf("configure logging: %w", err)
	}
	defer logCloser.Close()
	slog.SetDefault(logger)

	logger.Info("WarnFlux starting", "version", resolvedVersion, "commit", commit)

	// Installation-specific geography: the bundled TERYT table covers the
	// reference region only; geo.areas extends it with any units of this
	// installation's region, so the same binary serves the whole country.
	if len(cfg.Geo.Areas) > 0 {
		areas := make([]geo.Area, 0, len(cfg.Geo.Areas))
		for _, a := range cfg.Geo.Areas {
			areas = append(areas, geo.Area{
				Code: a.Code, Type: a.Type, Slug: a.Slug, Name: a.Name, Parents: a.Parents,
			})
		}
		if err := geo.Register(areas); err != nil {
			return fmt.Errorf("register geo.areas: %w", err)
		}
		logger.Info("geo areas registered", "count", len(areas))
	}

	// -check-config: construct every plugin, action, receiver and the web
	// server the same way a real run does (so every strict YAML decoder
	// and cross-validation runs), then exit. Nothing is started, the
	// database is never opened and no connection is ever made.
	if checkConfig {
		if err := validateConfiguration(cfg, logger, resolvedVersion, met); err != nil {
			return err
		}
		logger.Info("configuration valid", "path", configPath)
		return nil
	}

	// Dev/debug convenience: when the configuration does not provide a
	// database path, warn and place the database next to the binary
	// instead of failing startup. Production/Docker setups set
	// storage.path explicitly.
	cfg.Storage.Path = resolveStoragePath(cfg.Storage.Path, logger)

	logger.Info("configuration loaded", "path", configPath,
		"log_level", cfg.App.LogLevel, "log_file", cfg.App.LogFile,
		"storage_driver", cfg.Storage.Driver, "storage_path", cfg.Storage.Path)

	// The database is created and migrated on first startup. Close it only
	// after all workers have stopped.
	store, migration, err := sqlite.Open(cfg.Storage.Path)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer store.Close()

	if migration.From == 0 {
		logger.Info("database initialized", "schema_version", migration.To)
	} else if migration.To > migration.From {
		logger.Info("database migrated", "from", migration.From, "to", migration.To)
	}

	ingester := ingest.NewIngester(store, logger, met)

	// Build the plugin manager from the YAML plugin configuration. Unknown
	// types, duplicate IDs and malformed plugin configs fail here, before
	// any worker starts.
	registry := plugin.NewRegistry()

	// The APRS hub merges every APRS backend (aprs-inet now, aprs-radio
	// later) into one station state; plugins and actions receive it at
	// registration time.
	hub, err := aprs.NewHub(aprs.HubConfig{
		Enabled:               cfg.APRS.Enabled,
		Callsign:              cfg.APRS.Callsign,
		Name:                  cfg.APRS.Name,
		Icon:                  cfg.APRS.Icon,
		GridSquare:            cfg.APRS.GridSquare,
		Latitude:              cfg.APRS.Latitude,
		Longitude:             cfg.APRS.Longitude,
		RadiusKM:              cfg.APRS.RadiusKM,
		AreaLatitude:          cfg.APRS.AreaLatitude,
		AreaLongitude:         cfg.APRS.AreaLongitude,
		AreaRadiusKM:          cfg.APRS.AreaRadiusKM,
		StationTTL:            cfg.APRS.StationTTL,
		BulletinTTL:           cfg.APRS.BulletinTTL,
		ExcludeInfrastructure: cfg.APRS.ExcludeInfrastructure,
		RouteMessages:         cfg.APRS.RouteMessages,
		Version:               resolvedVersion, MessageRecorder: store}, logger)
	if err != nil {
		return fmt.Errorf("configure aprs hub: %w", err)
	}
	// The installation banner answers unknown slash commands on the radio
	// CLI and rides the periodic emcom presence beacon.
	identity := fmt.Sprintf("WarnFlux v%s - %s - %s",
		resolvedVersion, cfg.Web.Header1, cfg.Web.Domain)

	// The Meshtastic hub owns the Companion serial session to the Heltec
	// node; the source plugin, the meshtastic action and the admin page
	// share it. History is persisted like APRS messages.
	meshtasticHub, err := meshtastic.NewHub(meshtastic.Config{
		Enabled:              cfg.Meshtastic.Enabled,
		Device:               cfg.Meshtastic.Device,
		Transport:            cfg.Meshtastic.Transport,
		Host:                 cfg.Meshtastic.Host,
		Baud:                 cfg.Meshtastic.Baud,
		RouteMessages:        cfg.Meshtastic.RouteMessages,
		NodeTTL:              cfg.Meshtastic.NodeTTL,
		EmcomChannel:         cfg.Meshtastic.EmcomChannel,
		WatchChannel:         cfg.Meshtastic.WatchChannel,
		EmcomInterval:        cfg.Meshtastic.EmcomInterval,
		EmcomHazardsInterval: cfg.Meshtastic.EmcomHazardsInterval,
		EmcomIdentity:        identity,
	}, logger)
	if err != nil {
		return fmt.Errorf("configure meshtastic hub: %w", err)
	}
	meshtasticHub.SetRecorder(store)
	// The heard-node directory persists in SQLite so restarts and quiet
	// periods do not empty the node list.
	meshtasticHub.SetNodeStore(store)
	// Meshtastic direct-message routing trusts registered operators: the
	// sender's node id (8 hex) must belong to a user's registered mesh
	// node list. Without the gate no mesh message becomes a hazard event.
	meshtasticHub.SetSenderGate(func(id string) string {
		owners, err := store.MeshtasticOwners()
		if err != nil {
			logger.Warn("meshtastic: sender allow-list load failed", "error", err)
			return ""
		}
		return owners[id]
	})
	// APRS message routing only trusts registered operators: the sender's
	// base callsign (SSID-insensitive) must appear on a user's APRS
	// callsign list. Without the gate no message becomes a hazard event.
	hub.SetSenderGate(func(base string) bool {
		calls, err := store.AllAPRSCallsigns()
		if err != nil {
			logger.Warn("aprs: sender allow-list load failed", "error", err)
			return false
		}
		for _, c := range calls {
			if c == base {
				return true
			}
		}
		return false
	})

	// The shared radio CLI: /help lists the commands, /debug fires the
	// debug alarm; both APRS messages and Meshtastic direct messages
	// answer through the same interpreter (future topics plug in here).
	// Unknown slash messages answer with the installation banner.
	radioCLI := radiocli.New(identity)
	hub.SetCLI(radioCLI)
	meshtasticHub.SetCLI(radioCLI)

	if err := plugins.RegisterBuiltins(registry, hub, meshtasticHub); err != nil {
		return fmt.Errorf("register built-in plugins: %w", err)
	}
	// Per-alert notification audit trail: the routing engine, the action
	// workers and TrailAware outputs (the MQTT event stream) record why
	// each alert was or was not delivered; the web UI serves it on
	// /notifications. Created before the manager so outputs receive it.
	trails := trail.NewRecorder(trail.DefaultMaxTrails)
	manager, err := plugin.NewManager(registry, cfg.Sources, cfg.Outputs,
		ingester.Ingest, ingester.Expire, store, plugin.ManagerOptions{
			ExpirationInterval: cfg.App.ExpirationInterval,
			ChangeRetention:    cfg.App.ChangeRetention,
			EventRetention:     cfg.App.EventRetention,
			Version:            resolvedVersion,
			TrailRecorder:      trails,
		}, logger)
	if err != nil {
		return fmt.Errorf("configure plugins: %w", err)
	}

	// Cursor lifecycle: create a cursor for every enabled output before any
	// worker starts (new outputs replay the retained journal from 0) and
	// drop cursors of outputs that are no longer configured, so cleanup and
	// pending stats always operate on the authoritative set of outputs.
	var enabledOutputs []storage.OutputRef
	for _, o := range cfg.Outputs {
		if o.Enabled {
			enabledOutputs = append(enabledOutputs, storage.OutputRef{ID: o.ID, Type: o.Type})
		}
	}
	if err := store.SyncOutputs(context.Background(), enabledOutputs); err != nil {
		return fmt.Errorf("sync output cursors: %w", err)
	}

	// Dispatch subsystem: one canonical bounded ingress plus the mirrored
	// retained MQTT state (per receiver, never persisted). The ingress
	// gets the durable inbox: acceptance is persisted before routing, so
	// a full queue or a crash no longer loses accepted events.
	ingress := dispatch.NewIngress(cfg.Dispatch.QueueSize)
	ingress.SetInboxWriteTimeout(cfg.Dispatch.InboxWriteTimeout)
	ingress.SetInbox(store)
	mirror := state.New()
	// The active hazards from the dispatch state mirror, most severe
	// first (Snapshot sorts them), one Zulu-windowed line per hazard.
	// The hourly emcom-channel digest speaks severe-and-above only —
	// pushing every minor road-works entry onto the mesh would drown
	// the channel in noise. The /hazard command keeps the full list
	// (an operator explicitly asked for it).
	hazardSource := func() []meshtastic.ActiveHazard {
		return hazardsFromMirror(mirror, "")
	}
	digestSource := func() []meshtastic.ActiveHazard {
		return hazardsFromMirror(mirror, severity.Severe)
	}
	meshtasticHub.SetActiveHazardSource(digestSource)
	// /hazard: a public command — every active hazard, one Zulu-windowed
	// line each (the same form the hourly digest broadcasts). The
	// channels split the reply into one message per line.
	radioCLI.Register("hazard", "active messages", func(string) radiocli.Result {
		return radiocli.Result{Handled: true, Reply: hazardsForRadio(hazardSource())}
	})
	// /weather: a public command — the region average of every current
	// weather reading (APRS stations + internet providers, gross errors
	// rejected) plus the averaged forecast when one is held. Built for
	// EMCOM: it scales from many sources down to a single station.
	radioCLI.Register("weather", "weather", func(string) radiocli.Result {
		return radiocli.Result{Handled: true, Reply: weatherForRadio(hub, mirror, time.Now()).Text()}
	})
	// /alert: authorized operators raise a severe hazard straight from
	// the radio — "severe alert with the information", default 4-hour
	// expiry, distributed through the standard routing matrix.
	radioCLI.RegisterRestricted("alert", "alert", func(args string) radiocli.Result {
		headline := strings.TrimSpace(args)
		if headline == "" {
			return radiocli.Result{Handled: true, Reply: "Missing parameter: /alert <text>"}
		}
		return radiocli.Result{
			Handled: true,
			Alert:   &radiocli.AlertSpec{Headline: headline, TTL: radioAlertTTL},
			Reply:   "OK: alert raised",
		}
	})

	// LOCAL-FIRST source pipeline: every journal change (ingest or
	// expiration) is dispatched into the local ingress directly — SQLite
	// + radio suffice, no broker round-trip. The store committed the
	// change's inbox row in the same transaction as the journal record,
	// so the live enqueue reuses it (never writes a second row) and a
	// crash between the commit and the dispatch is healed by inbox
	// recovery. The MQTT output keeps syncing the same change to the
	// broker for other instances.
	ingester.SetDispatchSink(func(change core.EventChange, inboxID int64) {
		dispatchLocalChange(ingress, logger, change, inboxID)
	})

	// Inbound MQTT traffic ring buffer: every frame the receivers ingest
	// lands here and is served by the web UI's /traffic viewer.
	traffic := mqttreceiver.NewTrafficBuffer(mqttreceiver.DefaultTrafficEntries)

	// ActionPlugins: explicit routing only. Unknown types fail here, before
	// any worker starts (even for disabled entries).
	actionRegistry := action.NewRegistry()
	if err := actions.RegisterAll(actionRegistry, hub, meshtasticHub); err != nil {
		return fmt.Errorf("register built-in actions: %w", err)
	}
	actionsMgr, err := action.NewManager(cfg.Actions, actionRegistry, logger, trails, met)
	if err != nil {
		return fmt.Errorf("configure actions: %w", err)
	}
	// Durable delivery: the action workers claim jobs from SQLite
	// (payload, recipients, attempts, next-attempt deadline) and record
	// the result after each execution, so a restart never loses a
	// queued notification.
	actionsMgr.SetDeliveryStore(store)
	// Staleness oracle consulted directly before transmission: an alert
	// superseded by a newer version, cancelled (locally or remotely) or
	// expired while its job waited in the queue must never hit the
	// radio. The shared lifecycle ledger covers local, remote and panel
	// messages; the local events table remains the second layer for
	// expiry of locally-ingested events.
	actionsMgr.SetDeliveryGate(func(ctx context.Context, req action.ActionRequest) bool {
		if h := req.Event.Hazard; h != nil {
			blocked, err := store.LifecycleBlocks(ctx, h.Publisher, h.Key, h.ChangeID)
			if err != nil {
				logger.Warn("actions: lifecycle lookup failed", "event_key", h.Key, "error", err)
				return true // never suppress on a lookup failure
			}
			if blocked {
				return false
			}
			verdict, err := store.HazardActive(ctx, h.Publisher, h.Key, time.Now())
			if err != nil {
				logger.Warn("actions: delivery freshness lookup failed", "event_key", h.Key, "error", err)
				return true // read error: never suppress on a lookup failure
			}
			return verdict != storage.HazardInactive
		}
		return true
	})
	// Recovery sweep for the async-failure ledger: a crash between the
	// failure-marker write and the job re-arm (pre-atomic versions, or
	// any future split) would leave an accepted job with a durable
	// marker — re-arm it now, so the retry runs within the attempt
	// budget. The atomic recorder path (RecordMeshFailureAndRequeue)
	// can no longer create that state, this is the safety net.
	if n, err := store.RequeueMarkedDeliveries(context.Background(), time.Now()); err != nil {
		logger.Warn("actions: marked-delivery recovery failed", "error", err)
	} else if n > 0 {
		logger.Info("actions: marked deliveries re-armed after restart", "jobs", n)
	}

	// MQTT receivers: independent input clients (never the publisher).
	// Construction failures are fatal; connection failures are not.
	receivers, err := mqttreceiver.NewManager(cfg.Dispatch.Receivers, mirror, ingress, logger, traffic)
	if err != nil {
		return fmt.Errorf("configure mqtt receivers: %w", err)
	}

	// The APRS hub publishes its station/packet/message feeds through the
	// first connected WarnFlux receiver (same broker, same topic prefix);
	// routed message events also dispatch locally first.
	hub.SetSink(&aprsManagerSink{mgmt: receivers, ingress: ingress, logger: logger})

	// /alert and /debug confirm what the LOCAL pipeline accepted:
	// durable (inbox row), emergency (RAM-only fallback) or rejection.
	// The broker publish is only the asynchronous sync copy — its
	// failure never downgrades a durably accepted local event, and a
	// locally rejected event never syncs.
	eventAcceptor := func(payload []byte) dispatch.Acceptance {
		acc := dispatchLocalEvent(ingress, logger, payload)
		if acc == dispatch.Rejected {
			return acc
		}
		if err := receivers.PublishRaw("events", false, payload); err != nil && logger != nil {
			logger.Warn("command event broker sync failed (local acceptance unaffected)", "error", err)
		}
		return acc
	}
	hub.SetEventAcceptor(eventAcceptor)

	// Restart-proof command registry: the durable anchor row (written
	// transactionally with the inbox acceptance) holds the lifecycle
	// times AND the recorded result of a previously accepted command.
	// The events table is the provider-ingest registry — radio command
	// acceptance never writes it — so it is consulted first for legacy
	// anchors and the command registry follows. A stored result proves
	// the job executed before: the hub replays it instead of executing
	// the command again, and the original TTL is recovered.
	eventTimes := func(ctx context.Context, key string) (time.Time, time.Time, string, bool) {
		if eff, exp, ok, err := store.EventTimes(ctx, key); err == nil && ok {
			return eff, exp, "", true
		} else if err != nil && logger != nil {
			logger.Debug("radio event times lookup failed", "key", key, "error", err)
		}
		eff, exp, result, ok, err := store.CommandTimes(ctx, key)
		if err != nil && logger != nil {
			logger.Debug("radio command registry lookup failed", "key", key, "error", err)
		}
		return eff, exp, result, ok
	}
	hub.SetEventTimesResolver(eventTimes)
	meshtasticHub.SetEventTimesResolver(eventTimes)

	// Heard Meshtastic nodes feed the broker as retained station documents
	// under meshtastic/stations/<key12>; expired nodes are tombstoned.
	meshtasticHub.SetStationSink(func(ctx context.Context, topic string, retained bool, payload []byte) error {
		return receivers.PublishRaw(topic, retained, payload)
	})

	// Meshtastic rx/tx messages feed the broker as non-retained documents
	// on meshtastic/messages, mirroring the APRS message feed.
	meshtasticHub.SetMessageSink(func(ctx context.Context, topic string, retained bool, payload []byte) error {
		return receivers.PublishRaw(topic, retained, payload)
	})

	// Meshtastic direct messages from registered operators feed the alarm
	// pipeline: canonical /events documents on the events stream. They
	// dispatch locally FIRST (durable inbox, no broker dependency); the
	// broker publish is the asynchronous sync copy for other instances.
	meshtasticHub.SetEventSink(func(ctx context.Context, topic string, retained bool, payload []byte) error {
		dispatchLocalEvent(ingress, logger, payload)
		return receivers.PublishRaw(topic, retained, payload)
	})

	// /alert and /debug confirmations track the local acceptance exactly
	// like APRS.
	meshtasticHub.SetEventAcceptor(eventAcceptor)

	// APRS weather stations feed the canonical weather pipeline: every
	// decoded weather report becomes a retained info/<prefix> topic and a
	// dashboard card, exactly like the openmeteo source's snapshots.
	hub.SetWeatherSink(func(ctx context.Context, rep aprs.WeatherReport) error {
		msg, err := rep.ToInformation()
		if err != nil {
			return err
		}
		return manager.EmitInformation(ctx, "aprs", msg)
	})

	// Public HTTP ingest endpoints: API-key-protected publishers. Each
	// enabled instance accepts hazard messages and publishes them to the
	// main broker (inherited from the first enabled mqtt output unless
	// the instance overrides it), where the receiver/routing flow picks
	// them up. A failed initial broker connect is non-fatal: the endpoint
	// answers 503 until paho's background reconnects succeed.
	var mainBroker config.IngestHTTP
	for _, o := range cfg.Outputs {
		if o.Enabled && o.Type == "mqtt" && o.Config != nil {
			var mc mqttout.Config
			if err := o.Config.Decode(&mc); err == nil {
				mainBroker = config.IngestHTTP{
					Broker:       mc.Broker,
					ClientID:     mc.ClientID,
					Username:     mc.Username,
					Password:     mc.Password,
					PasswordFile: mc.PasswordFile,
					TopicPrefix:  mc.TopicPrefix,
				}
			}
			break
		}
	}

	var ingestInstances []*ingesthttp.Instance
	ingestHandlers := make(map[string]http.Handler)
	for _, ing := range cfg.IngestHTTP {
		if !ing.Enabled {
			// A deliberately disabled input answers with an explicit
			// error instead of a misleading 404.
			ingestHandlers[ing.ID] = ingesthttp.Disabled(logger)
			continue
		}
		ing = ingesthttp.Resolve(ing, mainBroker)
		if ing.Broker == "" {
			return fmt.Errorf("configure ingest_http %q: no broker configured and no enabled mqtt output to inherit one from", ing.ID)
		}
		if ing.ClientID == mainBroker.ClientID {
			logger.Warn("ingest_http: client_id equals the mqtt output client_id; the two connections will kick each other off the broker",
				"instance", ing.ID)
		}
		inst, err := ingesthttp.New(ing, logger)
		if err != nil {
			return fmt.Errorf("configure ingest_http %q: %w", ing.ID, err)
		}
		// LOCAL-FIRST: the endpoint dispatches into the durable inbox
		// before any broker I/O, so the MQTT publish mask can never
		// lose an accepted message. The durable outbox records the
		// broker publication before the 202, so a disconnected broker
		// neither rejects the request nor loses its cross-instance
		// sync.
		inst.SetIngress(ingress)
		inst.SetOutbox(store)
		// Atomic acceptance (P1): the inbox row, the lifecycle record
		// and the outbox row commit in ONE transaction before the event
		// is handed to the routing worker — a rejected commit accepts
		// nothing, and a crash can never leave an accepted local
		// notification without its MQTT sync.
		inst.SetAcceptor(store)
		// Builder-mode identity: stamp the persistent instance UUID and
		// a monotonic version onto every builder event so the routing
		// engine records them in the lifecycle ledger and the delivery
		// gate blocks jobs a later cancellation supersedes. A missing
		// ID (read failure) degrades to the legacy publisher-less form.
		if pid, err := store.InstanceID(context.Background()); err != nil {
			logger.Warn("ingest_http: publisher id unavailable; builder events run without lifecycle versioning",
				"instance", ing.ID, "error", err)
		} else {
			inst.SetPublisherID(pid)
		}
		if err := inst.Start(); err != nil {
			logger.Warn("ingest_http: initial broker connect failed (requests are accepted locally and synced once the broker returns)",
				"instance", ing.ID, "error", err)
		}
		ingestInstances = append(ingestInstances, inst)
		ingestHandlers[ing.ID] = inst
	}
	defer func() {
		for _, inst := range ingestInstances {
			inst.Close()
		}
	}()

	// Authenticated web UI. The listening socket is created before the
	// application declares readiness (bind failure is startup-critical).
	var webSrv *web.Server
	if cfg.Web.Enabled {
		// The admin user is the web auth account: it exists in the users
		// table as the read-only first row, and its stored password is
		// synced to the YAML/secret value so the directory always matches
		// the config.
		adminPassword := cfg.Web.Auth.Password
		if cfg.Web.Auth.PasswordFile != "" {
			data, err := os.ReadFile(cfg.Web.Auth.PasswordFile)
			if err != nil {
				return fmt.Errorf("web: read auth.password_file: %w", err)
			}
			adminPassword = strings.TrimRight(string(data), "\r\n")
		}
		if err := store.EnsureAdminUser(cfg.Web.Auth.Username, adminPassword); err != nil {
			logger.Warn("web: ensure admin user failed", "error", err)
		}
		webSrv, err = web.New(cfg.Web, mirror, receivers, receivers, manager, actionsMgr, hub, meshtasticHub, ingress, logger, resolvedVersion, commit, store, store, store, store, ingestHandlers, logs, traffic, trails, met)
		if err != nil {
			return fmt.Errorf("configure web: %w", err)
		}
		// Low-disk alarm: the health page and /metrics report the free
		// space against storage.min_free_mb (0 disables the alarm).
		webSrv.SetStorageAlarm(cfg.Storage.MinFreeMB * 1024 * 1024)
		// Broker resync: after every receiver (re)connect the panel's
		// local-first state (communications, EMCOM networks) is
		// republished, so broker outages never lose it permanently.
		receivers.SetResync(func() {
			go webSrv.SyncBrokerState()
		})
		// Password-reset emails ride the first enabled smtp action's
		// configuration; without one the self-service flow degrades to
		// "contact an administrator".
		for _, a := range cfg.Actions {
			if !a.Enabled || a.Type != smtp.Type || a.Config == nil {
				continue
			}
			var sc smtp.Config
			if err := a.Config.Decode(&sc); err == nil {
				webSrv.SetPasswordResetMailer(func(to, subject, text string) error {
					return smtp.Direct(context.Background(), sc, []string{to}, subject, text)
				})
			}
			break
		}
		if err := webSrv.Bind(); err != nil {
			return err
		}
	}

	// Phase 2 — runtime. From here on, provider failures are isolated by
	// the plugin framework instead of terminating the process.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Mobile APRS stations crossing the operational range announce on
	// the meshtastic group channel (the emcom channel — only when one
	// is configured). The watcher starts with a silent baseline and
	// announces transitions afterwards, debounced against edge
	// flapping.
	if hub != nil && meshtasticHub.Enabled() && cfg.Meshtastic.EmcomChannel > 0 {
		presence := aprspresence.New(aprspresence.Options{
			Channel: cfg.Meshtastic.EmcomChannel,
			Send: func(ctx context.Context, text string) error {
				return meshtasticHub.SendChannelText(ctx, cfg.Meshtastic.EmcomChannel, text, "system")
			},
			Stations: hub.Stations,
			Lat:      hub.AreaLat(),
			Lon:      hub.AreaLon(),
			RadiusKM: hub.AreaRadius(),
			Logger:   logger,
		})
		go presence.Run(ctx)
	}

	// Durable HTTP-ingest broker sync: each endpoint's outbox worker
	// publishes the accepted payloads whenever its broker connection is
	// up (mask applied at publish time), so the endpoints accept locally
	// regardless of the broker state.
	for _, inst := range ingestInstances {
		go inst.RunOutbox(ctx)
	}

	// Startup order: action workers → receivers → Router core → HTTP.
	// The offline-mode switch rides on top: the startup state comes from
	// web.offline_mode and the admin Config page toggles it at runtime.
	// Applying it BEFORE Start means internet sources never even start
	// while the station is offline.
	if cfg.Web.OfflineMode {
		manager.SetOffline(true)
		actionsMgr.SetOffline(true)
	}
	if webSrv != nil {
		webSrv.SetOfflineHandler(func(on bool) {
			manager.SetOffline(on)
			actionsMgr.SetOffline(on)
		})
	}
	actionsMgr.Start(ctx)
	receivers.StartAll()
	// Restore heard Meshtastic nodes from the retained broker documents, so
	// the station list survives restarts the same way the APRS retained
	// station state does. Bounded retries until a receiver connects.
	go seedMeshtasticStations(ctx, meshtasticHub, receivers, cfg, logger)
	if hub.Enabled() {
		hub.Start(ctx)
	}

	// Group routing rule engine: the consumer of the dispatch ingress. It
	// evaluates every hazard transition against the group rules (severity
	// threshold + assigned actions/outputs) with periodic rule reloads.
	routingCtx, cancelRouting := context.WithCancel(ctx)
	defer cancelRouting()
	ruleEngine := routing.New(store, actionsMgr, logger, action.AppInfo{
		Version: resolvedVersion,
		Header1: cfg.Web.Header1,
		Domain:  cfg.Web.Domain,
		RepoURL: appinfo.RepoURL,
	}, trails, met)
	ruleEngine.SetInbox(store)
	var routingWG sync.WaitGroup
	routingWG.Add(1)
	go func() {
		defer routingWG.Done()
		ruleEngine.Run(routingCtx, ingress.Events())
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		manager.Run(ctx)
	}()

	// Durable action-fire ledger maintenance: COMPLETED delivery history
	// older than the configured retention is pruned at startup and then
	// hourly. Pending jobs (saved, running or a scheduled retry) never
	// age out. A negative retention disables pruning entirely.
	if cfg.App.NotificationRetention > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			prune := func() {
				cutoff := time.Now().Add(-cfg.App.NotificationRetention)
				n, err := store.PruneActionFires(cutoff)
				if err != nil {
					logger.Warn("routing: fire ledger prune failed", "error", err)
					return
				}
				if n > 0 {
					logger.Debug("routing: fire ledger pruned (completed history)", "removed", n)
				}
			}
			prune()
			ticker := time.NewTicker(time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					prune()
				}
			}
		}()
	}

	// Durable dispatch-inbox maintenance: ordinary retention removes ONLY
	// rows whose hazard carries an authoritative expiry that has already
	// passed — the staleness gates would suppress them anyway. Pending
	// messages (no expiry, future expiry) are never aged out by clock age
	// alone, and every removal is an explicit outcome recorded in the
	// audit log. Below the low-disk alarm threshold the cutoff shortens
	// so auxiliary data stops competing with primary storage on a full
	// card. A retention of 0 disables pruning.
	if cfg.Dispatch.InboxRetention > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			interval := cfg.App.ExpirationInterval
			if interval <= 0 {
				interval = time.Minute
			}
			minFree := cfg.Storage.MinFreeMB * 1024 * 1024
			prune := func() {
				now := time.Now()
				cutoff := now.Add(-cfg.Dispatch.InboxRetention)
				lowDisk := false
				if minFree > 0 {
					if free, err := store.FreeBytes(ctx); err != nil {
						logger.Warn("storage: free space check failed", "error", err)
					} else if free < minFree {
						lowDisk = true
						short := now.Add(-dispatch.LowDiskInboxRetention)
						if short.After(cutoff) {
							cutoff = short
						}
						logger.Warn("storage: low disk space, shortening inbox retention",
							"free_mb", free/1024/1024, "min_free_mb", cfg.Storage.MinFreeMB)
					}
				}
				n, err := store.PruneInbox(ctx, cutoff)
				if err != nil {
					logger.Warn("dispatch: inbox prune failed", "error", err)
					return
				}
				if n > 0 {
					logger.Info("dispatch: inbox pending-expiry prune",
						"dropped_expired", n, "low_disk", lowDisk)
					if err := store.RecordAudit("system", "inbox-expiry-prune",
						fmt.Sprintf("%d expired pending inbox rows removed (validity policy)", n), now); err != nil {
						logger.Warn("dispatch: inbox prune audit failed", "error", err)
					}
				}
				// Panel tombstone pruning: expired compose communications
				// and deleted EMCOM networks stop being useful once the
				// broker has seen their tombstone sync.
				if n, err := store.PruneComposeHazards(ctx, cutoff); err != nil {
					logger.Warn("compose: tombstone prune failed", "error", err)
				} else if n > 0 {
					logger.Debug("compose: tombstones pruned", "removed", n)
				}
				// Compose auto-expiry reconciliation: the AfterFunc timers
				// live only in this process, so a restart must re-apply
				// every expires_at that passed in the meantime.
				if webSrv != nil {
					if n, err := webSrv.AutoExpireCompose(ctx); err != nil {
						logger.Warn("compose: auto-expiry reconciliation failed", "error", err)
					} else if n > 0 {
						logger.Info("compose: auto-expiry reconciled", "expired", n)
					}
				}
				if n, err := store.PruneEmcomNetworks(ctx, cutoff); err != nil {
					logger.Warn("emcom: tombstone prune failed", "error", err)
				} else if n > 0 {
					logger.Debug("emcom: tombstones pruned", "removed", n)
				}
			}
			prune()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					prune()
				}
			}
		}()
	}

	// Durable APRS message history: pruned to the retention bound at
	// startup and then hourly (the store also prunes on every insert).
	{
		wg.Add(1)
		go func() {
			defer wg.Done()
			prune := func() {
				n, err := store.PruneAPRSMessages(ctx, storage.APRSMessageRetentionEntries)
				if err != nil {
					logger.Warn("aprs: message history prune failed", "error", err)
					return
				}
				if n > 0 {
					logger.Debug("aprs: message history pruned", "removed", n)
				}
			}
			prune()
			ticker := time.NewTicker(time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					prune()
				}
			}
		}()
	}

	// Durable HTTP-ingest outbox: rows the broker could not confirm for
	// longer than outboxRetention are pruned hourly, so a prolonged
	// broker outage cannot grow the database without bound. The local
	// delivery already happened; only the cross-instance sync of stale
	// requests is dropped. Still-valid rows are never pruned: beyond
	// the capacity bound appends are rejected with an explicit 503
	// until publishing drains the queue, and the fullness alarm below
	// warns before that happens.
	{
		wg.Add(1)
		go func() {
			defer wg.Done()
			prune := func() {
				n, err := store.PruneOutbox(ctx, time.Now().Add(-outboxRetention))
				if err != nil {
					logger.Warn("ingest_http: outbox prune failed", "error", err)
					return
				}
				if n > 0 {
					logger.Info("ingest_http: outbox aged rows pruned", "removed", n)
				}
				// Fullness alarm: a still-valid backlog near the bound is
				// about to turn every append into an explicit 503.
				if backlog, err := store.OutboxCount(ctx); err == nil && backlog >= storage.OutboxCapacity*9/10 {
					logger.Warn("ingest_http: outbox backlog high — still-valid rows near capacity; appends are rejected with 503 until publishing drains",
						"backlog", backlog, "capacity", storage.OutboxCapacity)
				}
				// Async-failure sweep: re-arm any accepted job whose
				// action+version still carries a failure marker (safety
				// net for the atomic recorder path).
				if n, err := store.RequeueMarkedDeliveries(ctx, time.Now()); err != nil {
					logger.Warn("actions: marked-delivery sweep failed", "error", err)
				} else if n > 0 {
					logger.Info("actions: marked deliveries re-armed", "jobs", n)
				}
			}
			prune()
			ticker := time.NewTicker(time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					prune()
				}
			}
		}()
	}

	// Durable radio-command registry: anchors older than the retention
	// window are pruned at startup and then hourly, so the registry
	// cannot grow without bound while replays (short retransmission
	// windows) always find their anchor.
	{
		wg.Add(1)
		go func() {
			defer wg.Done()
			prune := func() {
				n, err := store.PruneCommandEvents(ctx, time.Now().Add(-commandRegistryRetention))
				if err != nil {
					logger.Warn("radio: command registry prune failed", "error", err)
					return
				}
				if n > 0 {
					logger.Debug("radio: command registry pruned", "removed", n)
				}
			}
			prune()
			ticker := time.NewTicker(time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					prune()
				}
			}
		}()
	}

	if webSrv != nil {
		serveErr := make(chan error, 1)
		go webSrv.Serve(serveErr)
		go func() {
			select {
			case err := <-serveErr:
				logger.Error("http server failed", "error", err)
				stop()
			case <-ctx.Done():
			}
		}()
		webSrv.MarkReady()
	}
	logger.Info("ready",
		"web_enabled", cfg.Web.Enabled,
		"receivers", len(cfg.Dispatch.Receivers),
		"actions", len(cfg.Actions),
		"ingest_http", len(ingestHandlers))

	<-ctx.Done()
	logger.Info("WarnFlux stopping")

	// Shutdown order: stop HTTP state-changing work → stop receiver intake
	// → stop Router core (sources, ingestion, outputs, existing semantics)
	// → drain dispatch ingress → drain/close actions → disconnect receiver
	// clients. SQLite closes last via defer.
	if webSrv != nil {
		httpCtx, cancelHTTP := context.WithTimeout(context.Background(), httpShutdownWait)
		if err := webSrv.Shutdown(httpCtx); err != nil {
			logger.Warn("http shutdown", "error", err)
		}
		cancelHTTP()
	}

	receivers.StopIntakeAll()

	// Stop the rule engine before the ingress is drained/closed: it is the
	// ingress consumer, and no action may be submitted after the action
	// manager starts shutting down below.
	cancelRouting()
	routingWG.Wait()

	// The manager stops sources, drains ingestion and closes outputs with
	// bounded timeouts.
	wg.Wait()

	drainCtx, cancelDrain := context.WithTimeout(context.Background(), dispatchDrainWait)
	_ = ingress.Drain(drainCtx)
	cancelDrain()
	ingress.StopIntake()

	actionCtx, cancelActions := context.WithTimeout(context.Background(), actionShutdownMax)
	if err := actionsMgr.Shutdown(actionCtx); err != nil {
		logger.Error("action shutdown incomplete", "error", err)
	}
	cancelActions()

	receivers.DisconnectAll()

	logger.Info("WarnFlux stopped")
	return nil
}

// newLogger builds the application logger. Output always goes to stdout so
// Docker and systemd keep working; when app.log_file is set, output is also
// written to a rotating file of at most app.log_max_size_mb, keeping up to
// app.log_max_backups rotated copies. capture additionally receives every
// line (the in-memory ring buffer behind the /logs viewer).
func newLogger(app config.App, capture io.Writer) (*slog.Logger, io.Closer, error) {
	level := app.SlogLevel()
	if app.LogFile == "" {
		logger := slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, capture), &slog.HandlerOptions{Level: level}))
		return logger, nopCloser{}, nil
	}

	rotator := &lumberjack.Logger{
		Filename:   app.LogFile,
		MaxSize:    app.LogMaxSizeMB,
		MaxBackups: app.LogMaxBackups,
		LocalTime:  true,
	}
	logger := slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, rotator, capture), &slog.HandlerOptions{
		Level: level,
	}))
	return logger, rotator, nil
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }
