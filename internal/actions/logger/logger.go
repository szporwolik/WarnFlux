// Package logger is the built-in proof-of-concept action demonstrating the
// ActionPlugin architecture: registry, config decoding, queue isolation,
// worker, timeout, health and shutdown.
//
// It is NOT wired to the dispatch ingress: tests (and later rules) invoke
// it explicitly through Manager.Submit.
package logger

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

// Type is the action type name used in the YAML configuration.
const Type = "logger"

// Config is the action-specific configuration.
type Config struct {
	// Level controls which log level dispatch events are written at.
	Level string `yaml:"level"`
}

type loggerAction struct {
	id     string
	level  slog.Level
	logger *slog.Logger
}

// New builds a logger action instance from its raw YAML configuration.
func New(node *yaml.Node) (action.Plugin, error) {
	var cfg Config
	if node != nil {
		if err := node.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("logger: decode config: %w", err)
		}
	}
	level := slog.LevelInfo
	if cfg.Level != "" {
		switch strings.ToLower(cfg.Level) {
		case "debug":
			level = slog.LevelDebug
		case "info":
			level = slog.LevelInfo
		case "warn":
			level = slog.LevelWarn
		case "error":
			level = slog.LevelError
		default:
			return nil, fmt.Errorf("logger: config.level %q must be one of debug|info|warn|error", cfg.Level)
		}
	}
	return &loggerAction{id: "logger", level: level, logger: slog.Default()}, nil
}

func (p *loggerAction) Name() string { return p.id }

// Execute logs concise event metadata only. It never logs full payloads.
func (p *loggerAction) Execute(ctx context.Context, req action.ActionRequest) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	ev := req.Event
	attrs := []any{"action", p.id, "kind", ev.Kind, "receiver", ev.Origin.ReceiverID}
	switch ev.Kind {
	case dispatch.EventHazardTransition:
		if ev.Hazard != nil {
			attrs = append(attrs, "type", ev.Hazard.Type, "source", ev.Hazard.Source,
				"event_key", ev.Hazard.Key, "severity", ev.Hazard.Hazard.Severity)
		}
	case dispatch.EventMQTTMessage:
		if ev.MQTT != nil {
			attrs = append(attrs, "topic", ev.MQTT.Topic, "bytes", len(ev.MQTT.Payload))
		}
	}
	p.logger.Log(ctx, p.level, "dispatch event", attrs...)
	return nil
}

func (p *loggerAction) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return nil
}
