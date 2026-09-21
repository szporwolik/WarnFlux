// Package demo implements a tiny built-in source plugin that periodically
// emits a deterministic synthetic HazardEvent. It exists purely to validate
// and demonstrate the plugin architecture and is intended for development
// and testing only.
package demo

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"

	"warnflux/internal/core"
	"warnflux/internal/plugin"
)

// Type is the plugin type name used in the YAML configuration.
const Type = "demo"

// Config is the plugin-specific configuration.
type Config struct {
	// Interval between synthetic events. Default 30s.
	Interval time.Duration `yaml:"interval"`
}

// Source emits one synthetic event per interval.
type Source struct {
	cfg Config
	seq atomic.Int64
}

// New decodes and validates the plugin-specific configuration.
func New(node *yaml.Node) (plugin.SourcePlugin, error) {
	var cfg Config
	if err := plugin.DecodeConfig(node, &cfg); err != nil {
		return nil, err
	}
	if cfg.Interval == 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.Interval < 0 {
		return nil, fmt.Errorf("interval must not be negative, got %s", cfg.Interval)
	}
	return &Source{cfg: cfg}, nil
}

// Name returns the plugin type name.
func (s *Source) Name() string { return Type }

// Run emits a synthetic event on every tick until ctx is cancelled.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			n := s.seq.Add(1)
			event := core.HazardEvent{
				Source:   Type,
				SourceID: fmt.Sprintf("%04d", n),
				Event:    "Drill",
				Severity: "minor",
				Headline: "Synthetic demo event",
				Status:   core.StatusActive,
			}
			if err := emit.Emit(ctx, event); err != nil {
				return err
			}
		}
	}
}

// Register registers the demo source plugin type.
func Register(reg *plugin.Registry) error {
	return reg.RegisterSource(Type, New)
}
