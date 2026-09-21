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
The built-in `demo` source exists for development. **Real hazard providers
(CAP, MeteoAlarm, GDACS, IMGW, …) are intentionally not implemented yet** —
they are the next step; the plugin contracts are designed for them.

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
  reach outputs. An event whose expiry has lapsed is re-activated
  (`updated`) when a source reports it again.
- **Change journal**: every meaningful transition is written atomically
  with the event update into a durable journal with monotonically
  increasing IDs. Outputs poll and acknowledge their own cursor, giving
  **at-least-once delivery** that survives restarts and crashes.
- **Expiration**: a maintenance loop atomically marks active events with
  `expires_at <= now` as `expired`, journaling a change for each.

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

All settings live in the YAML file; defaults are applied for missing values.

| Key | Default | Description |
|-----|---------|-------------|
| `app.log_level` | `info` | `debug`, `info`, `warn` or `error` |
| `app.log_file` | (stdout only) | optional rotating log file path |
| `app.log_max_size_mb` | `10` | rotate the log file after this many megabytes |
| `app.log_max_backups` | `5` | how many rotated log files to keep |
| `app.expiration_interval` | `1m` | how often expired events are checked (Go duration) |
| `app.change_retention` | `24h` | how long acknowledged journal records are kept |
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
      client_id: warnflux
      username: ""
      password: ""        # mutually exclusive with password_file
      password_file: ""   # e.g. /run/secrets/mqtt-password
      topic_prefix: warnflux
      qos: 1
      heartbeat_interval: 30s   # 0 disables periodic status publication
```

Every instance needs a unique `id` and a known `type`. Unknown types,
duplicate IDs and malformed plugin configuration are startup errors.

## MQTT output

The MQTT output publishes to two topics under `topic_prefix`:

| Topic | Retained | Content |
|-------|----------|---------|
| `<prefix>/events` | no | one JSON message per event change (`new`, `updated`, `cancelled`, `expired`) |
| `<prefix>/status` | yes | periodic application status snapshot (`heartbeat_interval`) |

Event message example (fields use `lower_snake_case`; `schema_version`
identifies the wire format):

```json
{
  "schema_version": 1,
  "change_id": 42,
  "change_type": "new",
  "event": {
    "source": "meteoalarm",
    "source_id": "2.49.0.1.616.0.DEU",
    "category": "met",
    "event": "Rain",
    "severity": "orange",
    "headline": "Heavy rain expected",
    "effective_at": "2026-01-01T00:00:00Z",
    "expires_at": "2026-01-02T00:00:00Z",
    "areas": ["DE-NW", "DE-RP"],
    "status": "active"
  }
}
```

The status snapshot reports version, uptime, database health, pending
journal changes and per-plugin state.

### Verify with mosquitto_sub

```bash
mosquitto_sub -h localhost -t 'warnflux/#' -v
```

## Delivery guarantee

- Every event transition is committed to the journal **in the same SQLite
  transaction** as the state change (`synchronous=FULL`, WAL).
- Each output keeps an independent acknowledgment cursor; a change is
  acknowledged only after the plugin reports success.
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

Prefer a specific version tag (e.g. `ghcr.io/<owner>/warnflux:0.1.0`)
for reproducible deployments.

## Plugins

WarnFlux plugins are **compiled-in integrations**: ordinary Go packages
registered in `internal/plugins/plugins.go` and selected through YAML.
They are not dynamic libraries. Details, mandatory implementation rules and
contribution requirements: see [docs/plugins.md](docs/plugins.md).

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
  `ghcr.io/<owner>/warnflux` with `v0.1.0`, `v0.1`, `v0` and `latest`
  tags and OCI labels

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
internal/plugins/      built-in plugins (demo source, MQTT output)
config.example.yaml    example configuration
.github/workflows/     ci.yml + release.yml
docs/                  plugin guide and project docs
```

## License

[MIT](LICENSE)

```