# WarnFlux

> **Alerts in. Action out.**

<p align="center">
  <img src="assets/logo.png" alt="WarnFlux logo" width="144">
</p>

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
MQTT (<prefix>/events stream + retained <prefix>/active/# current-active
view + retained <prefix>/status snapshot)
    ↓
MQTT broker(s)
    ↓
MQTT receivers (one or more independent input clients)
    ↓
canonical dispatch ingress (hazard_transition + mqtt_message)
    ↓
Rules [future] → selected ActionPlugins (SMS / email / Discord / …)
```

## Status

The core pipeline, plugin framework, durable journal and the MQTT output are
implemented and covered by unit, integration, fuzz and race-detector tests.
WarnFlux publishes both a realtime hazard change stream (`/events`,
non-retained) and a retained current-active hazard view (`/active/#`,
materialized from SQLite) so late-joining clients immediately discover all
active hazards — see
[`internal/plugins/outputs/mqtt/README.md`](internal/plugins/outputs/mqtt/README.md).
The `openmeteo` source
publishes ordinary weather snapshots to retained MQTT information topics —
**it does NOT generate HazardEvents**. Real hazard sources:

- **IMGW-PIB warnings** — meteorological `warningsmeteo` and hydrological
  `warningshydro`, with snapshot disappearance reconciliation
  ([README](internal/plugins/sources/imgw/README.md))
- **Regionalny System Ostrzegania (RSO)** — public XML communications
  with upstream voivodeship filtering and multi-region merge
  ([README](internal/plugins/sources/rso/README.md))

**Other real hazard providers (CAP, MeteoAlarm, GDACS, …) are intentionally
not implemented yet** — they are the next step; the plugin contracts are
designed for them.

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
| `app.notification_retention` | `720h` | how long action-fire ledger rows are kept; bounds the dedup window (negative = never prune) |
| `storage.driver` | `sqlite` | only `sqlite` is supported for now |
| `storage.path` | *(binary dir)* | SQLite database path. When omitted, WarnFlux **warns** and creates `warnflux.db` next to the binary (dev/debug convenience). Set it explicitly in Docker, e.g. `/data/warnflux.db`. |

Plugin instances are configured in the `sources:` and `outputs:` sections:

```yaml
sources:
  - id: imgw-warnings
    type: imgw           # registered plugin type
    enabled: true
    runtime:             # framework-level supervision options
      restart: true
      shutdown_timeout: 10s
    config:              # plugin-specific, decoded strictly by the plugin
      poll_interval: 5m
      request_timeout: 10s

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
      "id": "imgw-warnings",
      "type": "imgw",
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

**Notification dedup:** every action firing is claimed in a durable
ledger keyed by `(group, action, event identity)` before the action is
submitted. The `/events` MQTT stream is at-least-once, so a retained or
replayed transition (for example everything re-delivered after a restart)
cannot re-fire the same notification — the ledger survives restarts in
SQLite. Each new journal `change_id` is a fresh identity, so real updates
still notify. Ledger rows are pruned by age: `app.notification_retention`
(30 days by default) bounds both the table size and the dedup window; a
negative value disables pruning entirely.

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
runs as a non-root user (UID 65532), exposes port 8080 for the web UI, and
pre-creates writable `/data` and `/logs` directories plus the `/config`
convention directory.

| Mount | Purpose | Optional? |
|-------|---------|-----------|
| `/config/config.yaml` | configuration | required (read-only) |
| `/data` | SQLite database (`storage.path`) | recommended — otherwise the database is lost on recreation |
| `/logs` | rotating log files (`app.log_file`) | optional |

The container entrypoint runs `warnflux --config /config/config.yaml`;
set `storage.path: /data/warnflux.db` and `web.listen: ":8080"` inside
the container configuration. Build args `VERSION` and `COMMIT` inject the
version and commit shown in the UI footer (the release workflow sets both
automatically).

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

## Integrated Dispatcher (one application)

WarnFlux now includes the Dispatcher and the authenticated Web UI in
**one binary, one Docker container and one SQLite database**
(`/data/warnflux.db` — there is no second database).

### Two MQTT clients is intentional

Using two MQTT client connections to the same broker is intentional:

| Role | Config | Direction | Semantics |
|------|--------|-----------|-----------|
| MQTT **OutputPlugin** | `outputs[].type=mqtt` | publishes | durable Router output: hazard transitions, retained active view, status |
| MQTT **Receiver** | `dispatch.mqtt_receivers[]` | subscribes | dispatch INPUT: consumes frames for the canonical dispatch ingress |

The publisher and a receiver connecting to the same broker MUST use
**different MQTT client IDs**.

### OutputPlugin vs ActionPlugin

- **OutputPlugin** — automatic durable delivery of Router changes.
  Example: MQTT.
- **ActionPlugin** — explicitly invoked by the group routing rule engine
  through `Manager.Submit`. Examples: SMS, email, Discord, CAT.
  Built-in actions: `logger` (proof of concept) and `smtp` (one email per
  routed dispatch event; STARTTLS or implicit TLS/SMTPS, AUTH PLAIN,
  password via `password` or a `password_file` secret). The matched
  group's member emails are delivered as hidden Bcc copies (one SMTP
  transaction per unique address), paced by a per-action rate limit
  (`rate_limit_per_minute`, evenly spaced, default 30). ActionPlugins
  never automatically receive MQTT events.

### Group routing rule engine

Every group is a notification channel with a routing matrix: each assigned
**action** instance carries its own minimum severity
(`unknown` < `minor` < `moderate` < `severe` < `extreme`). Received
`hazard_transition` events are evaluated against the cached group rules
(reloaded every 10 s): an event fires exactly the actions whose threshold
it satisfies. Output plugins are deliberately NOT part of the matrix —
they already receive every journal change by default, so a per-group
output assignment would be redundant. The matrix is edited in the web UI
under **Groups → Routing** (per-action severity selects).

### Canonical dispatch events

Every receiver feeds the same bounded ingress with one of two kinds:

- `hazard_transition` — strictly parsed WarnFlux `/events` messages
  (`new`/`updated`/`cancelled`/`expired`); retained `/active/#`, `/info/#`
  and `/status` messages update mirrored state but never become transitions.
- `mqtt_message` — raw generic frames from additional subscriptions, with
  an opaque byte payload (no JSON/UTF-8 assumptions).

Events carry the receiver ID so future rules can select by origin, and
state is namespaced by receiver: two brokers publishing identical topics
never overwrite each other. Retained-state bounds: 10 000 active hazards,
10 000 info entries, 1 MiB MQTT payload. Generic frames are not republished
(no implicit MQTT bridge — loop safety) and not stored as history.

### Multi-broker receivers

```yaml
dispatch:
  mqtt_receivers:
    - id: local
      broker: tcp://mosquitto:1883
      client_id: warnflux-dispatch-local
      warnflux:
        enabled: true
        topic_prefix: warnflux

    - id: remote
      broker: tcp://10.10.10.10:1883
      client_id: warnflux-dispatch-remote
      subscriptions:
        - topic: "remote/#"
          qos: 1
```

Each receiver owns its connection, client ID, credentials, subscriptions,
reconnect state and health; one receiver failing never stops another, the
Router core or the Web UI. `warnflux.enabled` automatically subscribes
`<prefix>/events`, `<prefix>/active/#`, `<prefix>/info/#`, `<prefix>/status`
with strict protocol parsing.

### Web UI

The authenticated admin UI (`web.enabled: true`) is server-rendered with
embedded assets (no CDN), session login, CSRF protection, `/healthz` and
`/readyz`, and 5-second partial polling. Dashboard sections: System, MQTT
connections, Weather, Active warnings, Sources/Outputs (from the Router
plugin manager) and Actions. The dashboard consumes the MQTT-facing
contract through the receivers — it never reads active hazards straight
from SQLite — so local and remote WarnFlux instances appear on the
same path. Web credentials support `password` or `password_file`.

### Public HTTP ingest endpoints

`ingest_http` configures zero or more public, API-key-protected ingest
endpoints, one per instance. A remote scraper POSTs a hazard message; the
endpoint publishes it to `warnflux/events` on the configured broker, and
the regular receiver → routing matrix → actions flow handles it exactly
like a message from an upstream WarnFlux instance (other consumers on the
broker see it too).

Each instance has its own `api_key` (at least 16 characters, presented as
`Authorization: Bearer <key>`). The instance `id` is the event source
stamped on builder-mode alerts and appears as a source row in the Groups
routing matrix, so alerts from a scraper can be routed at their own
severity thresholds.

Hardening options per instance: `previous_key` / `previous_key_file` are
accepted alongside `api_key` for zero-downtime key rotation, `allowed_cidrs`
restricts the source networks (403 otherwise), `rate_limit_per_minute`
bounds the request rate (0 = default 60, negative = unlimited; 429 carries
`Retry-After`). Every request is audited in the log with its request id
(`X-Request-ID`, echoed in the response), source IP and result, and the
counters (accepted/rejected/auth_failed/rate_limited/forbidden) appear on
the Health page and on `/metrics`.

Broker settings are optional: an instance without `broker` inherits the
broker, credentials and topic prefix of the **primary MQTT output** (the
first enabled output with `type: mqtt`) — all plugins push to the one main
broker. `client_id` defaults to `warnflux-ingest-<id>`. Fill the fields in
per instance only to publish elsewhere (additional brokers remain sources
via `dispatch.mqtt_receivers`).

Two accepted payload shapes on `POST /api/v1/ingest/<id>`:

1. **Wire mode** — the canonical `/events` payload, exactly as published on
   the MQTT topic:

   ```json
   {
     "schema_version": 1,
     "change_id": 42,
     "change_type": "new",
     "event_key": "imgw-meteo:123",
     "event": {
       "source": "imgw-meteo", "source_id": "123",
       "event": "Strong wind", "severity": "severe",
       "headline": "Strong wind warning",
       "areas": ["powiat wielicki"],
       "status": "active",
       "received_at": "2026-09-23T10:00:00Z",
       "updated_at": "2026-09-23T10:00:00Z"
     }
   }
   ```

2. **Builder mode** — a simplified alert; the endpoint assembles the wire
   payload (source = instance id, timestamps = now, status = active):

   ```bash
   curl -X POST https://example.com/api/v1/ingest/news \
     -H "Authorization: Bearer <key>" \
     -H "Content-Type: application/json" \
     -d '{"severity":"severe","event":"Pożar","headline":"Pożar w lesie",
          "areas":["gmina Niepołomice"],"source_id":"scraper-7"}'
   ```

   Builder fields: `severity` (required, canonical), `event` or `headline`
   (at least one required), optional `transition` (`new` default, `updated`,
   `cancelled`, `expired`), `source_id` (when omitted, a hash of the body —
   identical re-posts produce the same `event_key`), plus `urgency`,
   `certainty`, `description`, `instruction`, `areas`, `effective_at`,
   `expires_at`, `latitude`, `longitude`, `category`, `source_url`.

Responses: `202` accepted, `400` invalid payload, `401` bad key, `403`
source address not allowed, `413` oversized (> 1 MiB), `429` rate limited,
`503` broker unavailable.

## Metrics

`GET /metrics` exposes a small Prometheus text-format endpoint
(unauthenticated; counters only, never content or secrets):

- `warnflux_source_polls_total` / `warnflux_source_errors_total` /
  `warnflux_events_filtered_total` per source,
- `warnflux_events_ingested_total`, `warnflux_events_duplicates_total`,
  `warnflux_events_active`,
- `warnflux_notifications_total{action,result}` and
  `warnflux_notification_retry_total`,
- `warnflux_mqtt_connected`, `warnflux_dispatch_queue_depth`,
  `warnflux_pending_changes`,
- `warnflux_ingest_http_requests_total{instance,result}`.

## Plugins

WarnFlux plugins are **compiled-in integrations**: ordinary Go packages
registered in `internal/plugins/plugins.go` and selected through YAML.
They are not dynamic libraries. Details, mandatory implementation rules and
contribution requirements: see [docs/plugins.md](docs/plugins.md).

Built-in plugins:

| Plugin | Kind | Purpose |
|--------|------|---------|
| `imgw` | source | real HAZARD source: IMGW-PIB `warningsmeteo`/`warningshydro` with snapshot reconciliation (see its [README](internal/plugins/sources/imgw/README.md)) |
| `rso` | source | real HAZARD source: RSO public XML with upstream voivodeship filtering (see its [README](internal/plugins/sources/rso/README.md)) |
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

The canonical version source of truth is the **`VERSION` file at the
repository root** (one line, plain semantic version, e.g. `0.1.0`).
Release builds inject it into the binary via ldflags; dev builds fall back
to reading a `VERSION` file next to the binary or in the working
directory, so `go run ./cmd/warnflux --version` from the repo root
reports the real version.

To release: bump `VERSION`, push, then tag `v<version>` — the release
workflow cross-checks the tag against `VERSION` and fails when they
mismatch, so a release can never publish a version that was not bumped
intentionally.

CI (`pull_request` and pushes to `main`) runs format checks (including a
`VERSION` format validation), `go vet`, the test suite with the race
detector and `govulncheck`. Tagging a semantic version (`v0.1.0`) triggers
the release workflow, which builds:

- `warnflux-linux-amd64`, `warnflux-linux-arm64` and
  `warnflux-linux-armv7` (32-bit Raspberry Pi OS)
- `SHA256SUMS`
- the Docker image (`linux/amd64`, `linux/arm64`, `linux/arm/v7`)
  published to
  `ghcr.io/<owner>/warnflux` with `v0.1.0`, `v0.1` and `latest` tags
  (no floating `v0` major tag before 1.0) and OCI labels

## Tests

```bash
go test ./...
go test -race ./...
go test -fuzz=FuzzNormalizeValidate -fuzztime=30s ./internal/core/
```

## Project layout

```text
cmd/warnflux/          application entry point, --version
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