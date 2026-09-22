# WarnFlux

WarnFlux aggregates hazard and emergency information from pluggable
sources, normalizes it into a common event model, deduplicates and tracks
each event's lifecycle in SQLite, and delivers every meaningful change to
outputs such as MQTT **at least once**.

```text
YAML config
    ↓
WarnFlux
    ↓
sources → bounded queue → ONE ordered ingest worker
    ↓
normalized HazardEvent → key + fingerprint + lifecycle (SQLite)
    ↓
durable change journal → independent per-output workers
    ↓
MQTT (<prefix>/events stream + retained <prefix>/status snapshot)
```

## Status

The core pipeline, plugin framework, durable journal and the MQTT output are
implemented and covered by unit, integration, fuzz and race-detector tests.
The built-in `demo` source exists for development. The `openmeteo` source
publishes ordinary weather snapshots to retained MQTT information topics —
**it does NOT generate HazardEvents**. **Real hazard providers (CAP,
MeteoAlarm, GDACS, IMGW, …) are intentionally not implemented yet** — they
are the next step; the plugin contracts are designed for them.

## Requirements

- Go 1.26 or newer (for local builds)
- An MQTT broker such as [Mosquitto](https://mosquitto.org/) (optional)

## Architecture

- **HazardEvent** (`internal/core`) is the normalized event model every
  source produces. Identity is the stable key `source:source_id`;
  mutable fields are never part of identity.
- **Fingerprint** is a deterministic SHA-256 hash of the content fields
  (category, type, severity, urgency, certainty, texts, times, location,
  areas, URL). Lifecycle status and ingestion metadata are excluded, so
  the same normalized event always hashes identically.
- **Ingestion** compares fingerprints and yields `new`, `duplicate` (only
  `last_seen_at` refreshes), `updated` or `cancelled`. Duplicates never
  reach outputs. A stale provider event whose expiry has lapsed is a
  duplicate that remains expired — an event is re-activated (`updated`)
  only when the provider makes it live again (a future expiry or none).
- **Emit is durable**: a source's `Emit` call returns nil only AFTER the
  event has been committed to SQLite and classified. A non-nil error means
  the event was not persisted and the source may retry (identity +
  fingerprint dedup make retries safe).
- **Change journal**: every meaningful transition is written atomically
  with the event update into a durable journal with monotonically
  increasing IDs. Each journal record carries an **immutable JSON
  snapshot** of the complete event state at the moment of that transition;
  the `events` table is current state, the journal is history. Outputs poll
  and acknowledge their own cursor, giving **at-least-once delivery** that
  survives restarts and crashes.
- **Output cursors**: at startup the store synchronizes one cursor row per
  enabled output (created before any delivery, removed for outputs that no
  longer exist). A newly enabled output starts at cursor 0 and receives all
  changes still present in the retained journal. Cleanup, pending stats and
  polling all operate on this same authoritative cursor set.
- **Expiration**: a maintenance loop atomically marks active events with
  `expires_at <= now` as `expired`, journaling a change for each.
  Re-ingesting the same already-expired provider event is a duplicate, not
  a reactivation; an event becomes active again only when the provider
  makes it live again (a future expiry or none).

## Build

```bash
go build -o build/warnflux ./cmd/warnflux
```

The version is embedded at build time:

```bash
go build -ldflags "-X main.version=0.1.0 -X main.commit=$(git rev-parse HEAD)" \
  -o build/warnflux ./cmd/warnflux
```

`warnflux --version` prints `warnflux <version> (<commit>)`.

## Run locally

```bash
cp config.example.yaml config.yaml
# edit config.yaml to match your broker
./build/warnflux --config config.yaml
```

If `--config` is omitted, `./config.yaml` in the current directory is used.

## Configuration

WarnFlux is configured with a single YAML file (default `config.yaml`).
The file is capped at 1 MiB — far beyond any realistic configuration, but
enough to keep accidental pathological input from being read into memory.

All settings live in the YAML file; defaults are applied for missing values.

| Key | Default | Description |
|-----|---------|-------------|
| `app.log_level` | `info` | `debug`, `info`, `warn` or `error` |
| `app.log_file` | (stdout only) | optional rotating log file path |
| `app.log_max_size_mb` | `10` | rotate the log file after this many megabytes |
| `app.log_max_backups` | `5` | how many rotated log files to keep |
| `app.expiration_interval` | `1m` | how often expired events are checked (Go duration) |
| `app.change_retention` | `24h` | how long acknowledged journal records are kept |
| `app.event_retention` | `720h` | how long cancelled/expired current-state records are kept (active events are never cleaned) |
| `storage.driver` | `sqlite` | only `sqlite` is supported for now |
| `storage.path` | *(binary dir)* | SQLite database path. When omitted, WarnFlux **warns** and creates `warnflux.db` next to the binary (dev/debug convenience). Set it explicitly in Docker, e.g. `/data/warnflux.db`. |

Plugin instances are configured in the `sources:` and `outputs:` sections:

```yaml
sources:
  - id: demo
    type: demo            # registered plugin type
    enabled: false
    runtime:              # framework-level supervision options
      restart: true
      shutdown_timeout: 10s
    config:               # plugin-specific, decoded strictly by the plugin
      interval: 30s

outputs:
  - id: mqtt-main
    type: mqtt
    enabled: false
    runtime:
      timeout: 10s
      failure_threshold: 5
    config:
      broker: tcp://localhost:1883
      client_id: warnflux-home   # required: unique per WarnFlux instance
      username: ""
      password: ""        # mutually exclusive with password_file
      password_file: ""   # e.g. /run/secrets/mqtt-password
      topic_prefix: warnflux
      qos: 1                # 1 or 2 (0 is rejected: best-effort transport)
      heartbeat_interval: 30s   # 0 disables periodic status publication
```

Every instance needs a unique `id` and a known `type`. Unknown types and
invalid or duplicate IDs are startup errors.

> **Output IDs are durable consumer identities.** The journal cursor is
> keyed by an output's `id` — renaming the ID creates a new consumer.
> Disabling an output removes its cursor; re-enabling it later creates a
> new cursor at 0, so it replays journal entries that are still retained
> (entries already removed by retention cannot be replayed). The ID, not
> the plugin type, defines the identity.

## MQTT output

The MQTT output publishes to two topics under `topic_prefix`
(`warnflux` in the examples below):

| Topic | Retained | QoS | Content |
|-------|----------|-----|---------|
| `warnflux/events` | no | `qos` from config (**1 or 2**; 0 is rejected) | one JSON message per meaningful event change |
| `warnflux/status` | yes | `qos` from config | periodic application health snapshot (`heartbeat_interval`) |

Messages are UTF-8 JSON. Field names use `lower_snake_case`; the
`schema_version` field identifies the wire format (currently `1`).

> **Delivery guarantee and QoS.** The durable journal provides
> at-least-once delivery at the plugin boundary: a change is acknowledged
> only after the plugin reports success, and unacknowledged changes are
> re-delivered after a restart. MQTT QoS 0 would make the network hop
> best-effort, so it is rejected at configuration time — `qos` must be `1`
> or `2` (default `1`). While the broker is unreachable, publishes fail
> and the changes stay pending; paho's automatic reconnection backoff is
> capped (30 s) so pending hazards are flushed promptly once the broker
> returns.

### Event messages — `warnflux/events`

Published whenever an event transitions: `new`, `updated`, `cancelled`
(by a source) or `expired` (by the expiration worker). Duplicates are
never published. After a restart, unacknowledged changes are re-published
(at-least-once) — if you need exactly-once, deduplicate on `change_id`
**within one instance** (see the field table below for its scope).

```json
{
  "schema_version": 1,
  "change_id": 42,
  "change_type": "new",
  "event_key": "meteoalarm:2.49.0.1.616.0.DEU",
  "event": {
    "source": "meteoalarm",
    "source_id": "2.49.0.1.616.0.DEU",
    "category": "met",
    "event": "Rain",
    "severity": "orange",
    "urgency": "expected",
    "certainty": "likely",
    "headline": "Heavy rain expected",
    "description": "Heavy rain with local thunderstorms over North Rhine-Westphalia.",
    "instruction": "Avoid low-lying areas and secure loose objects.",
    "effective_at": "2026-01-01T00:00:00Z",
    "expires_at": "2026-01-02T00:00:00Z",
    "latitude": 51.23,
    "longitude": 7.03,
    "areas": ["DE-NW", "DE-RP"],
    "status": "active",
    "source_url": "https://example.org/alert/2.49.0.1.616.0.DEU",
    "received_at": "2026-01-01T00:05:00Z",
    "updated_at": "2026-01-01T00:05:00Z"
  }
}
```

Top-level fields:

| Field | Type | Meaning |
|-------|------|---------|
| `schema_version` | int | wire format version (currently `1`); bump means breaking change |
| `change_id` | int64 | journal ID, monotonic **within one WarnFlux SQLite database lifetime**; if the database is recreated, or two WarnFlux instances publish to the same topic, the same integer can appear again — deduplicate on `change_id` only together with your instance's `topic_prefix` |
| `change_type` | string | `new`, `updated`, `cancelled` or `expired` |
| `event_key` | string | `source:source_id` — the stable logical upstream event identity |
| `event` | object | full normalized event snapshot — see below |

`event` fields:

| Field | Type | Meaning |
|-------|------|---------|
| `source` | string | normalized lowercase provider name (e.g. `meteoalarm`); with `source_id` forms the stable identity `source:source_id` |
| `source_id` | string | provider-specific event identifier |
| `category` | string | provider category code (e.g. `met`) |
| `event` | string | event type (e.g. `Rain`) |
| `severity` | string | provider-defined; CAP-style vocabulary: `Minor`, `Moderate`, `Severe`, `Extreme`, `Unknown` |
| `urgency` | string | CAP-style: `Immediate`, `Expected`, `Future`, `Past`, `Unknown` |
| `certainty` | string | CAP-style: `Observed`, `Likely`, `Possible`, `Unlikely`, `Unknown` |
| `headline` | string | short human-readable headline |
| `description` | string | full description |
| `instruction` | string | recommended protective action |
| `effective_at` | RFC3339 string, optional | when the alert takes effect; omitted when not provided |
| `expires_at` | RFC3339 string, optional | when the alert expires; omitted = never expires |
| `latitude` | float, optional | event location; always present together with `longitude` or both omitted |
| `longitude` | float, optional | see `latitude` |
| `areas` | array of strings | affected areas (trimmed, de-duplicated) |
| `status` | string | lifecycle state: `active`, `cancelled` or `expired` (never `updated` — that is a change type, not a state) |
| `source_url` | string | link to the original provider page |
| `received_at` | RFC3339 string | when the source delivered the event (ingestion metadata) |
| `updated_at` | RFC3339 string | when the persisted content last changed |

Empty values are serialized as `""` (strings) or `[]` (areas); optional
fields (`effective_at`, `expires_at`, `latitude`, `longitude`) are
omitted entirely when absent. Severity/urgency/certainty are passed
through as provided by the source adapter; built-in conventions follow
the CAP vocabulary listed above.

### Status snapshot — `warnflux/status`

Published every `heartbeat_interval` (set it to `0` to disable) and
**retained**: a new subscriber immediately receives the latest snapshot.
This is the replacement for the old "ping" topic.

If the process disappears without disconnecting (crash, kill -9, network
cut), the broker publishes a **retained last will** with
`"state": "offline"` on the same topic, so consumers can distinguish a
dead instance from a stale heartbeat. On graceful shutdown WarnFlux
publishes the same retained offline status before disconnecting (a clean
DISCONNECT also cancels the broker-side will). The will payload is fixed at
plugin construction: its `generated_at` reflects when the MQTT output
instance configured its will; `state=offline` is authoritative regardless
of the timestamp.

```json
{
  "schema_version": 1,
  "service": "warnflux",
  "state": "running",
  "generated_at": "2026-09-22T12:00:00Z",
  "version": "0.1.0",
  "uptime_seconds": 3600,
  "database_healthy": true,
  "pending_changes": 0,
  "oldest_pending_age_seconds": 0,
  "sources": [
    {
      "id": "demo",
      "type": "demo",
      "state": "running",
      "consecutive_failures": 0,
      "restart_count": 0
    }
  ],
  "outputs": [
    {
      "id": "mqtt-local",
      "type": "mqtt",
      "state": "running",
      "consecutive_failures": 0,
      "restart_count": 0,
      "last_error": ""
    }
  ]
}
```

| Field | Meaning |
|-------|---------|
| `service` | always `warnflux` |
| `state` | `running` while the process is alive |
| `generated_at` | UTC observation time of the snapshot (RFC3339) — consumers should treat a retained snapshot as stale when it stops advancing |
| `version` | build version (`dev` for development builds) |
| `uptime_seconds` | seconds since the manager started |
| `database_healthy` | whether the **last status database query succeeded** (not a full integrity check) |
| `pending_changes` | journal changes not yet acknowledged by every enabled output; **0 when no outputs are configured** (retained changes are history, not a backlog) |
| `oldest_pending_age_seconds` | age of the oldest pending change |
| `sources` / `outputs` | one entry per plugin instance: `id`, `type`, `state` (`starting`, `running`, `degraded`, `suspended`, `stopping`, `stopped`, `disabled`), `consecutive_failures`, `restart_count`, `last_error` (truncated to 2048 bytes with an explicit marker) |

### How to subscribe

All messages (topics are prefixed with the configured `topic_prefix`):

```bash
mosquitto_sub -h localhost -p 1883 -t 'warnflux/#' -v
```

Only event changes:

```bash
mosquitto_sub -h localhost -p 1883 -t 'warnflux/events' -v
```

Fetch the latest retained status snapshot once and exit:

```bash
mosquitto_sub -h localhost -p 1883 -t 'warnflux/status' -C 1 -v
```

With authentication:

```bash
mosquitto_sub -h broker.example.com -p 1883 \
  -u warnflux -P '<password>' \
  -t 'warnflux/#' -v
```

Capture the next 5 event messages and exit (`change_id` is monotonic
within one WarnFlux database and allows consumer-side deduplication
per instance):

```bash
mosquitto_sub -h localhost -p 1883 -t 'warnflux/events' -C 5 -v
```

Without local mosquitto clients, subscribe from a container:

```bash
docker run --rm eclipse-mosquitto:2 mosquitto_sub \
  -h broker.example.com -p 1883 -u warnflux -P '<password>' \
  -t 'warnflux/#' -v
```


## Delivery guarantee

- Every event transition is committed to the journal **in the same SQLite
  transaction** as the state change (`synchronous=FULL`, WAL), together
  with the immutable event snapshot for that transition.
- At startup the store creates one cursor per enabled output **before any
  delivery**; a change is acknowledged only after the plugin reports
  success, and an output that never succeeds keeps its cursor at 0 —
  cleanup can never delete its undelivered changes.
- After a restart, unacknowledged changes are re-polled and re-delivered:
  **at-least-once**. Consumers of the MQTT stream should deduplicate on
  `change_id` if exactly-once semantics are required.

## Storage

Events are persisted in SQLite via the pure-Go `modernc.org/sqlite` driver
(no CGO). The database and schema are created automatically on first start;
later schema versions are migrated in-place by append-only steps (versioned
with `PRAGMA user_version`), each in its own transaction. An old binary
refuses to touch a database created by a newer one.

| Setting | Value | Why |
|---------|-------|-----|
| `journal_mode` | `WAL` | crash-safe journal; readers do not block writers |
| `synchronous` | `FULL` | journal durable across power loss, not just process crashes |
| `busy_timeout` | `5000` | wait for locks instead of failing immediately |
| `foreign_keys` | `ON` | integrity checks if relations are added later |
| connections | `1` | serialized access: no `SQLITE_BUSY` by design |

Schema versions: **v1** events table → **v2** ms expiry + change journal +
output cursors → **v3** immutable `event_snapshot` per journal row.
Databases created by the pre-release v2 schema have their old journal rows
backfilled with the CURRENT event state (documented limitation: pre-v3
rows cannot be reconstructed historically). All changes written after v3
carry exact historical snapshots.

**Zero outputs:** when no outputs are enabled, retained changes are
history, not pending deliveries — `PendingStats` reports 0 and cleanup
removes changes once they are older than `change_retention`. Adding an
output later replays only what is still retained.

**Event retention:** cancelled/expired current-state records are deleted
once the provider has NOT been observed for `app.event_retention` (default
30 days) — observation is tracked by machine-time `last_seen_at_ms`, so a
provider that keeps repeating a stale event keeps it retained. Active
events are never cleaned. After an event ages out, WarnFlux has
intentionally forgotten its lifecycle: if the provider later reports the
same event again it may become `new` once more — `event_retention` defines
how long lifecycle memory is preserved after provider disappearance. The
change journal (which carries immutable snapshots) is unaffected and stays
bounded by `app.change_retention`.

**Event-size caps:** as a final safety net, `HazardEvent.Validate` rejects
pathological payloads (description > 32 KiB, headline/URL > 2 KiB,
instruction > 8 KiB, short fields > 256 B, more than 512 areas or an area
label longer than 2 KiB). Caps are byte-based and intentionally generous —
they bound SQLite, journal and MQTT payload sizes, not legitimate hazard
content.

### Timestamp model

| Field | Meaning | Set by |
|-------|---------|--------|
| `effective_at` | provider event effective time | provider |
| `expires_at` | provider event expiry | provider |
| `first_seen_at` | first time WarnFlux observed the logical event | core |
| `last_seen_at` / `last_seen_at_ms` | most recent provider observation (retention key) | core |
| `received_at` | first receipt time; attached to the immutable output snapshot | core |
| `updated_at` | time the persisted content/lifecycle last changed | core |

Provider plugins must not set `received_at` / `updated_at` / `first_seen_at`
/ `last_seen_at`; provider-origin timestamps belong in dedicated fields
(`effective_at`, `expires_at`, or future provider-specific ones).

## Docker

```bash
docker build -t warnflux .
docker run --rm \
  -v ./config.yaml:/config.yaml:ro \
  warnflux \
  --config /config.yaml
```

The image is a multi-stage `distroless/static-debian12:nonroot` build that
runs as a non-root user (UID 65532), exposes no ports, and pre-creates
writable `/data` and `/logs` directories.

| Mount | Purpose | Optional? |
|-------|---------|-----------|
| `/config.yaml` | configuration | required (read-only) |
| `/data` | SQLite database (`storage.path`) | recommended — otherwise the database is lost on recreation |
| `/logs` | rotating log files (`app.log_file`) | optional |

With **bind mounts**, host directories must be writable by UID 65532
(e.g. `sudo chown 65532:65532 ./data ./logs`). Named volumes initialize
from the image and work out of the box.

### Docker Compose (hardened)

```yaml
services:
  warnflux:
    image: ghcr.io/<owner>/warnflux:latest
    restart: unless-stopped
    read_only: true
    cap_drop: [ALL]
    security_opt:
      - no-new-privileges
    tmpfs:
      - /tmp
    volumes:
      - ./config.yaml:/config.yaml:ro
      - warnflux-data:/data
      - warnflux-logs:/logs
    command: ["--config", "/config.yaml"]

volumes:
  warnflux-data:
  warnflux-logs:
```

### Deploy from GitHub Container Registry

```bash
docker run -d \
  --name warnflux \
  --restart unless-stopped \
  -v ./config.yaml:/config.yaml:ro \
  -v warnflux-data:/data \
  -v warnflux-logs:/logs \
  ghcr.io/<owner>/warnflux:latest \
  --config /config.yaml
```

Prefer a specific version tag (e.g. `ghcr.io/<owner>/warnflux:v0.1.0`)
for reproducible deployments.

## Plugins

WarnFlux plugins are **compiled-in integrations**: ordinary Go packages
registered in `internal/plugins/plugins.go` and selected through YAML.
They are not dynamic libraries. Details, mandatory implementation rules and
contribution requirements: see [docs/plugins.md](docs/plugins.md).

Built-in plugins:

| Plugin | Kind | Purpose |
|--------|------|---------|
| `demo` | source | synthetic development events |
| `openmeteo` | source | periodically publishes current weather + forecast for configured coordinates to retained MQTT information topics (see its [README](internal/plugins/sources/openmeteo/README.md)) — **does NOT generate HazardEvents** |
| `mqtt` | output | hazard event stream, retained status, retained information topics |

### Failure isolation

- plugin panics are recovered, logged with a stack trace and isolated
- every source runs under its own supervisor with bounded backoff and is
  restarted on failure (unless the manager is shutting down)
- every output has its own worker: per-call timeout, single-flight handler
  invocation (a wedged handler is never called again), and failure
  suspension with periodic recovery probes
- a broken output cannot block other outputs or the core
- queues are bounded; a full queue applies backpressure instead of
  silently dropping hazard events
- secrets are never logged

> Because built-in plugins run inside the WarnFlux process, this is
> fault isolation, not a security sandbox. Community plugins must undergo
> code review.

## Releases

CI (`pull_request` and pushes to `main`) runs format checks, `go vet`, the
test suite with the race detector and `govulncheck`. Tagging a semantic
version (`v0.1.0`) triggers the release workflow, which builds:

- `warnflux-linux-amd64` and `warnflux-linux-arm64`
- `warnflux-windows-amd64.exe`
- `SHA256SUMS`
- the Docker image (`linux/amd64`, `linux/arm64`) published to
  `ghcr.io/<owner>/warnflux` with `v0.1.0`, `v0.1` and `latest` tags
  (no floating `v0` major tag before 1.0) and OCI labels

## Demo

```bash
go run ./cmd/warnflux demo
```

The demo exercises the pipeline against a temporary database:

```text
first event            → new
same event again       → duplicate
changed severity       → updated
cancelled event        → cancelled
repeated cancellation  → duplicate
expiration check       → expired
event without expiry   → stays active
```

## Tests

```bash
go test ./...
go test -race ./...
go test -fuzz=FuzzNormalizeValidate -fuzztime=30s ./internal/core/
```

## Project layout

```text
cmd/warnflux/      application entry point, --version, demo command
internal/config/       YAML configuration loading and validation
internal/core/         normalized event model, identity, fingerprint
internal/ingest/       dedup/update/cancel pipeline
internal/storage/      EventStore contract + SQLite implementation
internal/plugin/       plugin contracts, registry, supervision, workers
internal/plugins/      compiled-in source/output integrations (each with its own README.md)
config.example.yaml    example configuration
.github/workflows/     ci.yml + release.yml
docs/                  plugin guide and project docs
```

## License

[Apache-2.0](LICENSE)

```