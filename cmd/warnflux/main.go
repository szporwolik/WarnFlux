// Command warnflux is the WarnFlux entry point.
//
// It loads a YAML configuration, optionally connects to an MQTT broker and
// publishes a periodic heartbeat ("ping") message.
package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"

	"warnflux/internal/config"
	"warnflux/internal/ingest"
	"warnflux/internal/mqtt"
	"warnflux/internal/plugin"
	"warnflux/internal/plugins"
	"warnflux/internal/storage/sqlite"
)

//go:embed version.txt
var versionFile string

const (
	disconnectGracePeriod = 250 * time.Millisecond
)

// version is read from version.txt at build time. That file is the single
// source of truth, also used by the release workflow.
var version = strings.TrimSpace(versionFile)

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

	configPath := flag.String("config", config.DefaultConfigPath, "path to YAML configuration file")
	flag.Parse()

	if err := run(*configPath); err != nil {
		slog.Error("WarnFlux failed", "error", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
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

	logger.Info("WarnFlux starting", "version", version)
	logger.Info("configuration loaded", "path", configPath,
		"log_level", cfg.App.LogLevel, "log_file", cfg.App.LogFile,
		"mqtt_enabled", cfg.MQTT.Enabled,
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
	manager, err := plugin.NewManager(registry, cfg.Sources, cfg.Outputs, ingester.Ingest, logger)
	if err != nil {
		return fmt.Errorf("configure plugins: %w", err)
	}

	// ctx is cancelled on SIGINT or SIGTERM; all work is tied to it.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var (
		client *mqtt.Client
		wg     sync.WaitGroup
	)

	// Plugin manager: source supervisors, ingestion workers and outputs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		manager.Run(ctx)
	}()

	// Expiration worker: periodically marks stale active events as expired.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ingester.RunExpiration(ctx, cfg.App.ExpirationInterval)
	}()

	if cfg.MQTT.Enabled {
		client = mqtt.New(cfg.MQTT, logger)
		if err := client.Connect(ctx); err != nil {
			return fmt.Errorf("connect to MQTT broker: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			client.PingLoop(ctx, cfg.MQTT.PingInterval, version)
		}()
	} else {
		logger.Info("MQTT disabled, no connection will be made")
	}

	<-ctx.Done()
	logger.Info("WarnFlux stopping")

	// All workers (expiration, ping loop) exit once ctx is cancelled; wait
	// for them before closing connections so no goroutine is left behind.
	wg.Wait()
	if client != nil {
		client.Disconnect(disconnectGracePeriod)
	}

	// store.Close runs via defer after workers have stopped.
	logger.Info("WarnFlux stopped")
	return nil
}

// newLogger builds the application logger. Output always goes to stdout so
// Docker and systemd keep working; when app.log_file is set, output is also
// written to a rotating file of at most app.log_max_size_mb, keeping up to
// app.log_max_backups rotated copies.
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
