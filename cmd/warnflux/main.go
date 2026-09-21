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
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/ingest"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/plugins"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
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

// version and commit are injected at build time via -ldflags:
//
//	-X main.version=vX.Y.Z -X main.commit=<sha>
//
// Development builds report "dev" / "unknown".
var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	// Developer demo of the core pipeline; not a stable public interface.
	if len(os.Args) >= 2 && os.Args[1] == "demo" {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
		slog.SetDefault(logger)
		if err := runDemo(logger); err != nil {
			slog.Error("WarnFlux demo failed", "error", err)
			os.Exit(1)
		}
		return
	}

	fs := flag.NewFlagSet("warnflux", flag.ExitOnError)
	configPath := fs.String("config", config.DefaultConfigPath, "path to YAML configuration file")
	showVersion := fs.Bool("version", false, "print version and exit")
	fs.Parse(os.Args[1:])

	if *showVersion {
		fmt.Printf("warnflux %s (%s)\n", version, commit)
		return
	}

	if err := run(*configPath); err != nil {
		slog.Error("WarnFlux failed", "error", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	// Phase 1 — static initialization. No background goroutines exist yet,
	// so a failure here leaves nothing running behind.
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	logger, logCloser, err := newLogger(cfg.App)
	if err != nil {
		return fmt.Errorf("configure logging: %w", err)
	}
	defer logCloser.Close()
	slog.SetDefault(logger)

	logger.Info("WarnFlux starting", "version", version, "commit", commit)

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

	ingester := ingest.NewIngester(store, logger)

	// Build the plugin manager from the YAML plugin configuration. Unknown
	// types, duplicate IDs and malformed plugin configs fail here, before
	// any worker starts.
	registry := plugin.NewRegistry()
	if err := plugins.RegisterBuiltins(registry); err != nil {
		return fmt.Errorf("register built-in plugins: %w", err)
	}
	manager, err := plugin.NewManager(registry, cfg.Sources, cfg.Outputs,
		ingester.Ingest, ingester.Expire, store, plugin.ManagerOptions{
			ExpirationInterval: cfg.App.ExpirationInterval,
			ChangeRetention:    cfg.App.ChangeRetention,
			EventRetention:     cfg.App.EventRetention,
			Version:            version,
		}, logger)
	if err != nil {
		return fmt.Errorf("configure plugins: %w", err)
	}

	// Cursor lifecycle: create a cursor for every enabled output before any
	// worker starts (new outputs replay the retained journal from 0) and
	// drop cursors of outputs that are no longer configured, so cleanup and
	// pending stats always operate on the authoritative set of outputs.
	var enabledOutputIDs []string
	for _, o := range cfg.Outputs {
		if o.Enabled {
			enabledOutputIDs = append(enabledOutputIDs, o.ID)
		}
	}
	if err := store.SyncOutputs(context.Background(), enabledOutputIDs); err != nil {
		return fmt.Errorf("sync output cursors: %w", err)
	}

	// Phase 2 — runtime. From here on, provider failures are isolated by
	// the plugin framework instead of terminating the process.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		manager.Run(ctx)
	}()

	<-ctx.Done()
	logger.Info("WarnFlux stopping")

	// The manager stops sources, drains ingestion and closes outputs with
	// bounded timeouts. store.Close runs via defer afterwards.
	wg.Wait()

	logger.Info("WarnFlux stopped")
	return nil
}

// newLogger builds the application logger. Output always goes to stdout so
// Docker and systemd keep working; when app.log_file is set, output is also
// written to a rotating file of at most app.log_max_size_mb, keeping up to
// app.log_max_backups rotated copies. File logging exists for non-Docker /
// non-systemd deployments; stdout remains the primary logging contract.
func newLogger(app config.App) (*slog.Logger, io.Closer, error) {
	level := app.SlogLevel()
	if app.LogFile == "" {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
		return logger, nopCloser{}, nil
	}

	rotator := &lumberjack.Logger{
		Filename:   app.LogFile,
		MaxSize:    app.LogMaxSizeMB,
		MaxBackups: app.LogMaxBackups,
		LocalTime:  true,
	}
	logger := slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, rotator), &slog.HandlerOptions{
		Level: level,
	}))
	return logger, rotator, nil
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }
