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
//
// Emit deep-copies the event and returns nil ONLY after the core has
// durably persisted and classified it (SQLite transaction committed). A
// non-nil error means the event was NOT persisted; the source may retry —
// event identity and fingerprint dedup make retries safe. During shutdown
// Emit returns an error instead of accepting ownership it cannot honor.
type Emitter interface {
	Emit(ctx context.Context, event core.HazardEvent) error

	// EmitInformation forwards a non-hazard informational message
	// (latest-state data such as a weather snapshot) to
	// information-capable outputs. It is auxiliary best-effort delivery:
	// unlike Emit there is no durable journal, no cursor and no
	// at-least-once guarantee. A non-nil error means the message was not
	// published (or no information-capable output exists); the source may
	// simply retry on its next poll.
	EmitInformation(ctx context.Context, message core.InformationMessage) error
}

// SourcePlugin obtains hazard information and emits normalized HazardEvent
// values. WarnFlux owns the lifecycle: Run must return when ctx is
// cancelled.
type SourcePlugin interface {
	Name() string
	Run(ctx context.Context, emit Emitter) error
}

// SourceHealthReporter is an OPTIONAL capability implemented by the
// emitter handed to a source plugin: sources that detect provider-level
// health can report operational state. The supervisor still owns the
// lifecycle states (starting/running/stopping/stopped); this interface
// only reports provider operational health (healthy / degraded), which
// surfaces in the source's plugin status.
type SourceHealthReporter interface {
	ReportSourceHealthy()
	ReportSourceDegraded(err error)
}

// SourceActiveEventReader is an OPTIONAL capability implemented by the
// emitter handed to a source plugin: it returns the CURRENT active events
// of one source from the authoritative SQLite current-state table
// (read-only, paged internally — no direct database access). Full-snapshot
// sources need it to reconcile provider disappearances (cancellations)
// across process restarts. A source whose correctness depends on this
// capability should fail clearly when it is absent instead of silently
// running without reconciliation.
type SourceActiveEventReader interface {
	ListSourceActiveEvents(ctx context.Context, source string) ([]core.HazardEvent, error)
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

// InformationPublisher is an OPTIONAL output capability: outputs that
// implement it additionally receive non-hazard informational messages from
// sources. Outputs that do not implement it simply never see information.
// Information publishing must never affect hazard delivery: failures are
// logged, not counted toward the hazard failure threshold, and must not
// suspend or advance anything hazard-related.
type InformationPublisher interface {
	PublishInformation(ctx context.Context, message core.InformationMessage) error
}

// ActiveStateSeeder is an OPTIONAL output capability for retained
// current-active hazard views (e.g. the MQTT /active/# topics). At startup
// the output worker seeds the CURRENT active events from SQLite (the
// authoritative state) so the view can be rebuilt after a process restart
// even when the durable journal is fully acknowledged. SeedActiveState MUST
// be a LOCAL, non-blocking desired-state registration (no network I/O): the
// publication is the output's own concern via ActiveStateRehydrater. A
// non-nil error is logged and never affects hazard delivery.
type ActiveStateSeeder interface {
	SeedActiveState(event core.HazardEvent) error
}

// ActiveStateRehydrater is an OPTIONAL output capability: the output
// republishes its desired active state in ONE bounded background pass. The
// worker triggers it once after startup seeding; implementations must
// return immediately (the pass itself runs asynchronously) so journal
// delivery never waits on the network.
type ActiveStateRehydrater interface {
	RehydrateActiveState()
}

// OutputFactory builds an OutputPlugin from its raw plugin-specific
// configuration.
type OutputFactory func(config *yaml.Node) (OutputPlugin, error)

// RuleRef identifies the group a rule-routed delivery targets. Outputs that
// implement RuleFeed use it to publish under a group-scoped topic.
type RuleRef struct {
	GroupID     int64
	GroupName   string
	MinSeverity string
}

// RuleFeed is an OPTIONAL output capability: outputs implementing it
// additionally receive severity-filtered, group-routed deliveries from the
// rule engine, in ADDITION to the global journal stream. Rule-routed
// delivery is best-effort: HandleRule failures are logged by the caller
// and never affect journal cursors or hazard delivery.
//
// Ownership contract: HandleRule may be called CONCURRENTLY with Handle
// (and with other HandleRule calls) from different goroutines, so the
// implementation must synchronize its own shared state. The context is
// bounded by the caller.
type RuleFeed interface {
	HandleRule(ctx context.Context, change core.EventChange, ref RuleRef) error
}

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
