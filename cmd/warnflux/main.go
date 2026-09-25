// Command warnflux is the WarnFlux entry point.
//
// It loads a YAML configuration, runs the plugin manager (sources, durable
// ingestion and outputs) and shuts down cleanly on SIGINT/SIGTERM.
package main

import (
	"context"
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
	"github.com/szporwolik/WarnFlux/internal/appinfo"
	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/geo"
	"github.com/szporwolik/WarnFlux/internal/ingest"
	"github.com/szporwolik/WarnFlux/internal/ingesthttp"
	"github.com/szporwolik/WarnFlux/internal/metrics"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/plugins"
	mqttout "github.com/szporwolik/WarnFlux/internal/plugins/outputs/mqtt"
	"github.com/szporwolik/WarnFlux/internal/routing"
	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
	"github.com/szporwolik/WarnFlux/internal/trail"
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
// WarnFlux receiver's topic prefix.
type aprsManagerSink struct {
	mgmt *mqttreceiver.Manager
}

func (s *aprsManagerSink) PublishRaw(suffix string, retained bool, payload []byte) error {
	return s.mgmt.PublishRaw(suffix, retained, payload)
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
		StationTTL:            cfg.APRS.StationTTL,
		ExcludeInfrastructure: cfg.APRS.ExcludeInfrastructure,
		RouteMessages:         cfg.APRS.RouteMessages,
		Version:               resolvedVersion,
	}, logger)
	if err != nil {
		return fmt.Errorf("configure aprs hub: %w", err)
	}
	if err := plugins.RegisterBuiltins(registry, hub); err != nil {
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
	if err := actions.RegisterAll(actionRegistry, hub); err != nil {
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
			ingress, logger, resolvedVersion, commit, nil, nil, nil, traffic, trails, met); err != nil {
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
		StationTTL:            cfg.APRS.StationTTL,
		ExcludeInfrastructure: cfg.APRS.ExcludeInfrastructure,
		RouteMessages:         cfg.APRS.RouteMessages,
		Version:               resolvedVersion,
	}, logger)
	if err != nil {
		return fmt.Errorf("configure aprs hub: %w", err)
	}
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

	if err := plugins.RegisterBuiltins(registry, hub); err != nil {
		return fmt.Errorf("register built-in plugins: %w", err)
	}
	manager, err := plugin.NewManager(registry, cfg.Sources, cfg.Outputs,
		ingester.Ingest, ingester.Expire, store, plugin.ManagerOptions{
			ExpirationInterval: cfg.App.ExpirationInterval,
			ChangeRetention:    cfg.App.ChangeRetention,
			EventRetention:     cfg.App.EventRetention,
			Version:            resolvedVersion,
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
	// retained MQTT state (per receiver, never persisted).
	ingress := dispatch.NewIngress(cfg.Dispatch.QueueSize)
	mirror := state.New()

	// Inbound MQTT traffic ring buffer: every frame the receivers ingest
	// lands here and is served by the web UI's /traffic viewer.
	traffic := mqttreceiver.NewTrafficBuffer(mqttreceiver.DefaultTrafficEntries)

	// Per-alert notification audit trail: the routing engine and the
	// action workers record why each alert was or was not delivered;
	// the web UI serves it on /notifications.
	trails := trail.NewRecorder(trail.DefaultMaxTrails)

	// ActionPlugins: explicit routing only. Unknown types fail here, before
	// any worker starts (even for disabled entries).
	actionRegistry := action.NewRegistry()
	if err := actions.RegisterAll(actionRegistry, hub); err != nil {
		return fmt.Errorf("register built-in actions: %w", err)
	}
	actionsMgr, err := action.NewManager(cfg.Actions, actionRegistry, logger, trails, met)
	if err != nil {
		return fmt.Errorf("configure actions: %w", err)
	}

	// MQTT receivers: independent input clients (never the publisher).
	// Construction failures are fatal; connection failures are not.
	receivers, err := mqttreceiver.NewManager(cfg.Dispatch.Receivers, mirror, ingress, logger, traffic)
	if err != nil {
		return fmt.Errorf("configure mqtt receivers: %w", err)
	}

	// The APRS hub publishes its station/packet/message feeds through the
	// first connected WarnFlux receiver (same broker, same topic prefix).
	hub.SetSink(&aprsManagerSink{mgmt: receivers})

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
		if err := inst.Start(); err != nil {
			logger.Warn("ingest_http: initial broker connect failed (endpoint will answer 503 until connected)",
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
		webSrv, err = web.New(cfg.Web, mirror, receivers, receivers, manager, actionsMgr, hub, ingress, logger, resolvedVersion, commit, store, ingestHandlers, logs, traffic, trails, met)
		if err != nil {
			return fmt.Errorf("configure web: %w", err)
		}
		if err := webSrv.Bind(); err != nil {
			return err
		}
	}

	// Phase 2 — runtime. From here on, provider failures are isolated by
	// the plugin framework instead of terminating the process.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Startup order: action workers → receivers → Router core → HTTP.
	actionsMgr.Start(ctx)
	receivers.StartAll()
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

	// Durable action-fire ledger maintenance: rows older than the
	// configured retention are pruned at startup and then hourly.
	// A negative retention disables pruning entirely.
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
					logger.Debug("routing: fire ledger pruned", "removed", n)
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
