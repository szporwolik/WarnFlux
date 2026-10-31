# Writing WarnFlux plugins

> **Convention:** a plugin ships a `README.md` in its own package directory
> (`internal/plugins/sources/<name>/README.md` or
> `internal/plugins/outputs/<name>/README.md`). The local README is the
> authoritative documentation for provider-specific configuration, behavior
> and limitations; this guide covers only framework contracts, and a new
> plugin without its README is incomplete. The older provider integrations
> (`adsb`, `gddkia`, `gios`, `meshtastic`) are still being backfilled. See
> [`internal/plugins/sources/openmeteo/README.md`](../internal/plugins/sources/openmeteo/README.md)
> for the informational source plugin reference and
> [`internal/plugins/sources/burzedzis/README.md`](../internal/plugins/sources/burzedzis/README.md)
> for a hazard source that publishes two slugs.

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
- `Emit` deep-copies the event and returns **nil only after the event has
  been durably persisted** (SQLite transaction committed). A non-nil error
  means the event was not persisted — check the error and retry on a later
  poll; identity/fingerprint dedup make retries safe. During shutdown Emit
  returns an error instead of accepting ownership it cannot honor.
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
- Status heartbeat (`StatusPublisher`) health is **auxiliary** and is
  accounted separately from hazard delivery:

  | Behavior | Consequence |
  |----------|-------------|
  | `Handle` failure | counts toward the failure threshold → suspension + probes |
  | `PublishStatus` failure | logged only, never counted, never suspends |
  | `PublishStatus` ignores its timeout (context-contract violation) | status publishing is **disabled for that output instance** for the rest of the process; hazard delivery continues. The abandoned callback may overlap later `Handle` calls — implementers MUST respect `ctx`. |
  | `PublishInformation` failure | logged only, never counted, never suspends; information is best-effort latest-state |
  | `PublishInformation` ignores its timeout (context-contract violation) | information publishing is **disabled for that output instance** for the rest of the process; the worker returns to its main loop immediately without waiting for the late result, so a hung information callback can never block hazard delivery. At most ONE abandoned information goroutine can ever exist per output. |

- **Active-state reconstruction** (`ActiveStateSeeder` +
  `ActiveStateRehydrater`): outputs with these optional capabilities
  rebuild their materialized retained active view at startup. The worker
  seeds the CURRENT active events paged from the authoritative SQLite
  state (never by replaying the journal) via `SeedActiveState`, which
  MUST be a LOCAL, non-blocking desired-state registration (no network
  I/O) — so startup reconstruction can never serialize on a slow broker
  or delay journal delivery. Afterwards the worker triggers
  `RehydrateActiveState` ONCE; implementations must return immediately
  (the pass itself runs in the background). A seeding callback that
  violates its timeout disables seeding for the rest of that startup
  pass (at most one abandoned goroutine per output).

- **Single owner**: one output worker owns all journal-driven callbacks
  into a plugin instance (`Handle`, `PublishStatus`, `PublishInformation`,
  `Close`). The only exception is a callback that violated its timeout and
  was deliberately abandoned — Go cannot forcibly kill a goroutine, so an
  abandoned auxiliary callback may overlap later `Handle` calls.
  Implementers MUST respect `ctx`.
- **Information queue semantics**: information messages are coalesced by
  source+producer+key+kind in a bounded latest-value queue per output
  (128 unique pending identities; updates to an already-pending key never
  consume an extra slot). A new key when the queue is full returns an
  error to the source; replacing an existing key always succeeds. The
  queue is intentionally NOT a delivery log: only the newest snapshot per
  identity matters, and a lost information snapshot is acceptable.
- A change is acknowledged only after `Handle` returns `nil` — delivery is
  at-least-once across restarts.
- **Durable identity**: an output's configured `id` **and plugin `type`**
  pair is its persisted journal consumer identity. Renaming the ID or
  changing the type creates a new consumer whose cursor starts at 0
  (replaying retained journal history); disabling an output removes its
  cursor. Legacy cursors written before the durable type identity existed
  (schema < v4) carry an empty type and are treated as new consumers after
  the upgrade — a conservative one-time replay with possible duplicate
  delivery, never silent loss.

## APRS backends and the shared hub

APRS integrations (the `aprs-inet` source today, the `aprs-radio` KISS
source later) do NOT publish their own MQTT topics. They feed parsed
packets into the shared **APRS hub** (`internal/aprs`), constructed once
from the top-level `aprs:` configuration and handed to plugin factories at
registration:

```go
hub, _ := aprs.NewHub(aprs.HubConfig{...}, logger)
if err := plugins.RegisterBuiltins(reg, hub); err != nil { ... }
```

The hub owns the topic layout and merges every backend, which guarantees
the two design invariants:

1. **No duplicate topics** — every nearby station has exactly ONE retained
   state document (`<prefix>/aprs/stations/<CALLSIGN>`), regardless of how
   many backends heard it. Identical packets observed twice are merged
   (the document's `received_via` list records the backends) and the
   non-retained packet feed publishes each unique content digest once.
2. **Permanent state on the broker** — station documents are retained and
   expire only after `aprs.station_ttl` (deleted with an empty retained
   payload). Our own station document is published at startup.

The hub also routes outbound APRS messages (rx/tx messaging): a backend
implementing `aprs.Transmitter` registers while connected, and
`Hub.SendMessage` picks the first ready transmitter. The built-in `aprs`
action uses this to notify ham callsigns from routed hazard events. A new
backend therefore only needs to (a) parse frames into `aprs.Packet` and
call `hub.Observe`, and (b) optionally implement `aprs.Transmitter` — the
hub, the MQTT topics and the action wiring stay untouched.

Every outgoing SMS goes through `gsm.TransliterateGSM7` first, which renders
Polish text as ASCII **and folds the GSM-7 extension characters** (`|`, `[`,
`]`, `{`, `}`, `~`, `^`, `\\`, `€`) into basic-table equivalents. That fold is
not cosmetic: in text mode (`AT+CMGF=1`, `AT+CSCS="IRA"`) this modem family
answers `+CMS ERROR 305` ("invalid text mode parameter") for a body carrying
one of those characters and the message never leaves the modem — the SMS
banner (`identity | hint`) and every routed action message (ending in
`[WF-...]`) failed that way, while plain notifications went through.

The same pattern covers the GSM channel: the built-in `sms` action sends
notifications through the shared GSM hub (the serial AT modem session)
to the configured phones plus the routed group members' registered
numbers, and inbound SMS commands (`/help`, `/hazard`, `/alert`, ...)
enter the pipeline as the `gsm` source. Only directory-registered phone
numbers are ever answered or allowed to run commands — unknown senders
are dropped outright (spam hygiene). The one exception is the national
Alert RCB broadcast sender, which is never answered but generates a
24-hour severe `rcb` hazard through the routing matrix.

## Deliveries to a shared destination

A routed alert becomes one delivery job per matching group. That is right for
actions that address a per-group recipient list (mail, SMS, APRS, Meshtastic):
each group's own members must be addressed by that group's own message. It is
WRONG for an action that posts to one place shared by everyone — a Discord
channel, a webhook — where two matching groups would post the same alert
twice, under one identity nobody can tell apart (reported P2).

Concretely, the built-in `discord` (one channel), `http_webhook` (one URL,
payload built from the event alone) and `logger` (one log, line names the
action — never the group) actions implement it; `smtp`, `sms`, `aprs` and
`meshtastic` do not, because they address each group's own members.

An action plugin of that kind therefore implements the optional capability:

```go
func (a *myAction) SharedDestination() bool { return true }
```

The routing engine asks the action manager
(`action.SharedDestinationReporter`) and collapses the deliveries of every
matching group into ONE job per alert: the first matching group owns the
delivery, the others contribute their recipients (all recipient lists are
union-merged, case-insensitively, keeping the first occurrence and the order)
and are counted as matched. A group below its own threshold never contributes,
and an action whose capability cannot be read keeps the per-group behaviour.

## Message wording on narrow channels

APRS gives an alert 67 characters, SMS one GSM-7 segment, Meshtastic a couple
of hundred: none of them fits a provider description. The rule every channel
follows is *what happened beats where beats the ID*:

- the **headline** carries the message when it adds information;
- when the headline merely repeats the event label — GDDKiA, RSO and some IMGW
  records publish the same string in both fields — the **first paragraph of the
  description** takes its place (`internal/alerttext`). Without this rule a
  production frame read `SOSNA SEV: Utrudnienia na A4 ID:WF-1023233601` for a
  hazard whose description said "Jerzmanowice ul. Jurajska … wypadek autobusu
  MPK … DK 94 zablokowana (314km)": the title twice and nothing else;
- the **area list and coordinates** are used only when no description exists
  (SMS keeps at most two areas: they are machine slugs such as
  `corridor:a4-balice-tarnow`, not something a person reads);
- the **message ID** always rides along and its room is reserved before the
  text is assembled, so the tail of a description is cut before the ID is lost;
- APRS drops the prefix and then the severity before it shortens the body — the
  body is the part that says what happened.

A provider description is condensed by `alerttext.Summary`: the note block after
a blank line is dropped ("IMGW probability: 70%.") and the rest is cut at a word
boundary. Punctuation is never used to end it — Polish descriptions are full of
abbreviations ("ul.", "pow."), and cutting at the first period left
"Jerzmanowice ul" as the whole message.

## Mandatory rules

- Respect context cancellation; do not block past it.
- **Check every `Emit` error.** `Emit` returning nil is the durable-
  persistence acknowledgment; a retry after an error is deduplicated
  safely.
- Do not panic intentionally; do not call `os.Exit`.
- Do not create unmanaged permanent goroutines or unbounded channels.
- Use request contexts and finite timeouts for network calls.
- Do not access storage, ingestion internals or other plugins directly.
- Do not bypass the emitter / change contract.
- Do not log secrets; do not use global mutable state.
- Validate the configuration before starting; return meaningful errors.
- Treat incoming `core.EventChange` values as read-only.
- Do not set `received_at` / `updated_at` / `first_seen_at` /
  `last_seen_at`: these are core-owned ingestion metadata.

## Provider identity and lifecycle guidance

- **`SourceID` must be the stable upstream alert identity.** Never derive
  it from the fetch timestamp, a per-run random value, or a hash of mutable
  content. If the upstream protocol genuinely lacks identity, derive a
  stable identifier from immutable protocol fields and document it — dedup
  depends on it.
- **Cancellation is explicit:** a provider cancelling an event must emit
  `StatusCancelled`. An event merely missing from one poll is NOT an
  automatic cancellation unless the provider protocol explicitly defines
  disappearance as termination.
- **Bounded inputs:** every HTTP adapter must enforce a finite response
  size limit, a connect/request timeout and context-aware I/O. The core's
  size caps are a last-resort safety net, not a substitute for provider
  bounds.
- **A switched-off source withdraws its events.** Events live until their own
  `expires_at`, and road works carry expiries months ahead, so a source the
  operator disabled would keep its warnings "live" on the panel and the public
  page for as long as they were valid. At startup the station therefore
  cancels the active events of every source INSTANCE that is disabled (or no
  longer in the configuration) and removes their retained documents.
  Ownership is decided by the event's `Producer` (the configured instance id,
  e.g. `imgw-warnings`), never by comparing the event's source slug with a
  plugin type: IMGW publishes `imgw-meteo`/`imgw-hydro` under the type `imgw`,
  and the slug comparison withdrew the warnings of a RUNNING plugin at every
  restart. A plugin whose event source differs from its type declares the
  mapping once (`plugin.RegisterEventSources`). Rows without a producer are
  left alone and reported — a stale warning is cheaper than hiding a valid
  alarm.
- **The withdrawal is journalled.** The status change and the journal record
  commit in one transaction (`Store.WithdrawEvent`), and the MQTT output removes
  the retained document from its cursor, which is only acknowledged after a
  successful publish. A broker that is unreachable at withdrawal time is
  therefore fixed on the next start instead of leaving the document behind
  forever; and no inbox row is written, so withdrawing an operator's source
  never notifies subscribers.

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
