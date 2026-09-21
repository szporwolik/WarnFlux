// Package plugin defines the contracts, registry, supervision and health
// tracking for compiled-in WarnFlux plugins.
//
// Plugins in this project are ordinary Go packages compiled into the
// WarnFlux binary and selected through the YAML configuration — not
// dynamic libraries. Because they run in-process, the supervision here
// provides fault isolation against ordinary bugs (panics, errors, hangs),
// not a security sandbox.
package plugin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// Emitter is handed to a source plugin by WarnFlux. It is the only way
// for a source to hand normalized events to the core pipeline; sources must
// not access storage or other core internals directly.
type Emitter interface {
	Emit(ctx context.Context, event core.HazardEvent) error
}

// SourcePlugin obtains hazard information and emits normalized HazardEvent
// values. WarnFlux owns the lifecycle: Run must return when ctx is
// cancelled.
type SourcePlugin interface {
	Name() string
	Run(ctx context.Context, emit Emitter) error
}

// SourceFactory builds a SourcePlugin from its raw plugin-specific
// configuration. The factory decodes and validates the configuration itself;
// the core knows nothing about provider-specific fields.
type SourceFactory func(config *yaml.Node) (SourcePlugin, error)

// OutputPlugin receives meaningful EventChange values from the core. Handle
// must respect ctx (bounded by the configured runtime timeout) and must
// treat the change as read-only.
type OutputPlugin interface {
	Name() string
	Handle(ctx context.Context, change core.EventChange) error
}

// OutputFactory builds an OutputPlugin from its raw plugin-specific
// configuration.
type OutputFactory func(config *yaml.Node) (OutputPlugin, error)

// Closer is an optional interface for plugins that hold resources which
// should be released cleanly when the application shuts down. The worker
// invokes it (with a timeout and panic recovery) after the plugin stops.
type Closer interface {
	Close() error
}

// Status is an application health snapshot that status-publishing outputs
// (such as the retained MQTT status topic) may consume.
type Status struct {
	Version          string
	Uptime           time.Duration
	DatabaseHealthy  bool
	PendingChanges   int
	OldestPendingAge time.Duration
	Sources          []PluginStatus
	Outputs          []PluginStatus
}

// StatusPublisher is an optional output plugin interface: the worker calls
// PublishStatus periodically (never concurrently with Handle while it
// behaves) using the interval reported by StatusInterval.
//
// Status publication health is AUXILIARY:
//   - a PublishStatus error is logged and never suspends the output or
//     counts toward its delivery-failure threshold;
//   - a PublishStatus call that ignores its timeout violates its context
//     contract: status publishing is then DISABLED for that output
//     instance for the rest of the process (hazard delivery continues),
//     and the abandoned callback may overlap later Handle calls —
//     implementers MUST respect ctx.
type StatusPublisher interface {
	PublishStatus(ctx context.Context, status Status) error
	StatusInterval() time.Duration
}

// DecodeConfig decodes the raw plugin configuration node into a typed
// config struct using strict field matching, so misspelled configuration
// keys fail fast during startup. A missing config node decodes to the zero
// value, letting plugins apply their own defaults.
func DecodeConfig(node *yaml.Node, out any) error {
	var data []byte
	if node == nil || node.Kind == 0 {
		data = []byte("null\n")
	} else {
		encoded, err := yaml.Marshal(node)
		if err != nil {
			return fmt.Errorf("encode plugin config: %w", err)
		}
		data = encoded
	}

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(out); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode plugin config: %w", err)
	}
	return nil
}
