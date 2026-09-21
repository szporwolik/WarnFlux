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
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"warnflux/internal/config"
	"warnflux/internal/mqtt"
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

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.App.SlogLevel(),
	}))
	slog.SetDefault(logger)

	logger.Info("WarnFlux starting", "version", version)
	logger.Info("configuration loaded", "path", configPath,
		"log_level", cfg.App.LogLevel, "mqtt_enabled", cfg.MQTT.Enabled)

	// ctx is cancelled on SIGINT or SIGTERM; all work is tied to it.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var (
		client *mqtt.Client
		wg     sync.WaitGroup
	)

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

	// The ping loop exits once ctx is cancelled; wait for it before
	// closing the connection so no goroutine is left behind.
	wg.Wait()
	if client != nil {
		client.Disconnect(disconnectGracePeriod)
	}

	logger.Info("WarnFlux stopped")
	return nil
}
