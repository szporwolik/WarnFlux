# Writing WarnFlux plugins

WarnFlux plugins are **compiled-in integrations**: ordinary Go packages
in this repository, registered in `internal/plugins/plugins.go` and selected
through the YAML configuration. They are not dynamic libraries, and no
arbitrary code is loaded at runtime. A community contribution goes through
code review and tests, is compiled into the binary, and is then enabled
through YAML.

## Contract summary

| Interface | Method | Purpose |
|-----------|--------|---------|
| `plugin.SourcePlugin` | `Name() string` | stable instance description for logs/status |
| | `Run(ctx, plugin.Emitter) error` | provider loop; must return when `ctx` is cancelled |
| `plugin.OutputPlugin` | `Name() string` | stable instance description |
| | `Handle(ctx, core.EventChange) error` | deliver one change; `ctx` is bounded by `runtime.timeout` |
| `plugin.Closer` (optional) | `Close() error` | release resources on shutdown (bounded timeout, panic-recovered) |
| `plugin.StatusPublisher` (output, optional) | `PublishStatus(ctx, plugin.Status) error` | publish the application status snapshot |
| | `StatusInterval() time.Duration` | how often `PublishStatus` is called |

Factories:

- `plugin.SourceFactory = func(*yaml.Node) (SourcePlugin, error)`
- `plugin.OutputFactory = func(*yaml.Node) (OutputPlugin, error)`

Use `plugin.DecodeConfig(node, &cfg)` to decode the raw configuration node
with strict field matching: misspelled keys fail at startup instead of
being silently ignored.

## Source plugin example

```go
package example

import (
    "context"
    "errors"
    "time"

    "gopkg.in/yaml.v3"

    "github.com/szporwolik/WarnFlux/internal/core"
    "github.com/szporwolik/WarnFlux/internal/plugin"
)

type Config struct {
    Interval time.Duration `yaml:"interval"`
}

type Source struct{ cfg Config }

func (s *Source) Name() string { return "example" }

func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
    ticker := time.NewTicker(s.cfg.Interval)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return nil
        case <-ticker.C:
            // fetch provider data, build a core.HazardEvent ...
            event := core.HazardEvent{Source: "example", SourceID: id, Event: "Flood"}
            if err := emit.Emit(ctx, event); err != nil {
                return err
            }
        }
    }
}

// New decodes and validates the plugin-specific YAML config.
func New(node *yaml.Node) (plugin.SourcePlugin, error) {
    var cfg Config
    if err := plugin.DecodeConfig(node, &cfg); err != nil {
        return nil, err
    }
    if cfg.Interval <= 0 {
        return nil, errors.New("interval must be positive")
    }
    return &Source{cfg: cfg}, nil
}

func Register(reg *plugin.Registry) error {
    return reg.RegisterSource("example", New)
}
```

## Output plugin example

```go
func (o *Output) Name() string { return "example" }

func (o *Output) Handle(ctx context.Context, change core.EventChange) error {
    // deliver the change; ctx is bounded by runtime.timeout
    return nil
}

func (o *Output) Close() error { return nil } // optional

func Register(reg *plugin.Registry) error {
    return reg.RegisterOutput("example", New)
}
```

Then add one line in `internal/plugins/plugins.go`:

```go
if err := example.Register(reg); err != nil {
    return err
}
```

## What the framework guarantees

For **sources**:

- `Run` is invoked by a supervisor in its own goroutine.
- Panics are recovered and logged with a stack trace.
- A failing source is restarted with bounded backoff, unless the manager is
  shutting down (never restarted after cancellation).
- `Emit` deep-copies the event, so mutating the event after `Emit` returns
  cannot corrupt stored state.
- `Emit` applies bounded backpressure when the ingestion queue is full —
  it returns an error instead of silently dropping hazard events.

For **outputs**:

- Each output has its own worker polling the durable journal; one slow or
  broken output cannot delay another.
- `Handle` is called with a per-call timeout context.
- **Single flight**: at most one `Handle` call per plugin instance is ever
  in flight; a handler that ignores `ctx` is never invoked again (the
  worker waits for it or, during shutdown, abandons it).
- Consecutive failures suspend the plugin; periodic recovery probes resume
  delivery after a success. Failures and probes are visible in the status
  snapshot.
- A change is acknowledged only after `Handle` returns `nil` — delivery is
  at-least-once across restarts.

## Mandatory rules

- Respect context cancellation; do not block past it.
- Do not panic intentionally; do not call `os.Exit`.
- Do not create unmanaged permanent goroutines or unbounded channels.
- Use request contexts and finite timeouts for network calls.
- Do not access storage, ingestion internals or other plugins directly.
- Do not bypass the emitter / change contract.
- Do not log secrets; do not use global mutable state.
- Validate the configuration before starting; return meaningful errors.
- Treat incoming `core.EventChange` values as read-only.

## HTTP client guidance

Plugins calling APIs must use the request context, a finite connect/request
timeout, a reasonable User-Agent, and bounded response body sizes where
practical. A remote endpoint must not be able to hold a plugin connection
forever.

## Contribution requirements for a plugin PR

- a typed configuration struct decoded strictly from YAML
- configuration validation
- unit tests
- documentation in this file or the plugin package
- reasonable network timeouts and context cancellation support
- no direct database access, no dependency on other plugins
- no secrets in logs, no unbounded goroutines or channels
- no process termination calls, no unnecessary large dependencies

## Security note

Because plugins execute inside the WarnFlux process, the supervision
provides **fault isolation, not a security sandbox**. A malicious plugin can
still call `os.Exit`, consume all memory or ignore cancellation. Plugins are
reviewed code compiled into the binary; do not enable plugins you do not
trust.
