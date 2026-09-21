# WarnFlux

WarnFlux aggregates hazard and emergency information from multiple
sources and routes normalized events to outputs such as MQTT.

The current version provides the persistent core pipeline; source adapters
are added next.

```text
YAML config
    ↓
WarnFlux
    ↓
normalized HazardEvent → deduplication + lifecycle → SQLite
    ↓
MQTT connection + periodic ping
```

## Requirements

- Go 1.26 or newer (for local builds)
- An MQTT broker such as [Mosquitto](https://mosquitto.org/) (optional)

## Architecture

```text
Sources (CAP, MeteoAlarm, GDACS, USGS, ... — not implemented yet)
        ↓
Normalization: source data → HazardEvent
        ↓
Deduplication + lifecycle: event key + fingerprint + status
        ↓
Persistence: SQLite
        ↓
Outputs: EventChange → MQTT, ... (only the ping today)
```

- **HazardEvent** (`internal/core`) is the common normalized event model all
  source adapters will produce.
- **Event identity** is the stable key `source:source_id` (for example
  `meteoalarm:2.49.0.1.616...`). Mutable fields are never part of identity.
- **Fingerprint** is a deterministic SHA-256 hash of the content fields
  (category, event type, severity, urgency, certainty, texts, times,
  location, areas, status, URL). Ingestion metadata such as `ReceivedAt` or
  last-seen timestamps is excluded, so the same normalized event always
  hashes identically.
- **Ingestion** compares the stored fingerprint with the incoming one and
  yields `new`, `duplicate` (only `last_seen_at` is refreshed), `updated`
  or `cancelled`. Only meaningful changes produce an `EventChange` for
  outputs; duplicates never do.
- **Expiration** is a periodic worker that persistently marks active events
  with `expires_at <= now` as `expired`. Events without an expiry stay
  active until a source cancels them (or a future source-specific policy
  says otherwise).

## Project layout

```text
cmd/warnflux/    application entry point + demo command + version.txt
internal/config/     YAML configuration loading and validation
internal/core/       normalized event model, identity, fingerprint
internal/ingest/     dedup/update/cancel pipeline + expiration worker
internal/storage/    EventStore interface + SQLite implementation
internal/mqtt/       MQTT client and ping publisher
config.example.yaml  example configuration
.github/workflows/   Build and Publish workflow (release.yml)
build/               build output (binary + sample config, git-ignored)
```

## Build

```bash
go build -o build/warnflux ./cmd/warnflux
```

Or use the VS Code build task (**Ctrl+Shift+B**), which builds into
`build/` and copies `config.example.yaml` there as `build/config.yaml`
when that file does not exist yet.

The version string is embedded at build time from
`cmd/warnflux/version.txt` (the single source of truth; the release
workflow reads the same file).

## Run locally

```bash
cp config.example.yaml config.yaml
# edit config.yaml to match your broker
./build/warnflux --config config.yaml
```

If `--config` is omitted, `./config.yaml` in the current working directory
is used. After a VS Code build you can also run from the sample config:
`./build/warnflux --config build/config.yaml`.

## Configuration

All settings live in the YAML file. Defaults are applied for missing values:

| Key                  | Default          | Description                                        |
|----------------------|------------------|----------------------------------------------------|
| `app.log_level`      | `info`           | `debug`, `info`, `warn` or `error`                 |
| `app.log_file`       | (stdout only)    | optional rotating log file path                    |
| `app.log_max_size_mb`| `10`             | rotate the log file after this many megabytes      |
| `app.log_max_backups`| `5`              | how many rotated log files to keep                 |
| `app.expiration_interval` | `1m`        | how often expired events are checked (Go duration) |
| `storage.driver`     | `sqlite`         | only `sqlite` is supported for now                 |
| `storage.path`       | `warnflux.db`| SQLite database file path                          |
| `mqtt.enabled`       | `false`          | connect to the MQTT broker on startup              |
| `mqtt.broker`        | –                | broker URL, e.g. `tcp://localhost:1883`            |
| `mqtt.client_id`     | `warnflux`   | client identifier for the broker                   |
| `mqtt.username`      | –                | username (empty for anonymous access)              |
| `mqtt.password`      | –                | password (empty for anonymous access)              |
| `mqtt.topic_prefix`  | `warnflux`   | topic prefix for published messages                |
| `mqtt.qos`           | `1`              | QoS for published messages: `0`, `1` or `2`        |
| `mqtt.ping_interval` | `30s`            | interval between heartbeat pings (Go duration)     |

Credentials are never written to the logs.

## Logging

Logs always go to stdout, so `docker logs` and journald keep working. Set
`app.log_file` to additionally write to a rotating file: once it exceeds
`app.log_max_size_mb`, it is renamed with a timestamp suffix and up to
`app.log_max_backups` older copies are retained. Credentials never appear
in logs.

## Storage

Events are persisted in a local [SQLite](https://www.sqlite.org/) database
using the pure-Go `modernc.org/sqlite` driver (no CGO). `storage.path`
configures the file; the database and schema are created automatically on
first startup, and later schema versions are migrated in-place — an
existing database is never deleted or recreated.

Schema changes are append-only migration steps in
`internal/storage/sqlite` (versioned via SQLite's `PRAGMA user_version`).
Each step runs in its own transaction: a failed step rolls back completely
and is retried on the next startup. An old binary refuses to touch a
database created by a newer one.

SQLite settings chosen for a long-running daemon:

| Setting        | Value    | Why                                                        |
|----------------|----------|------------------------------------------------------------|
| `journal_mode` | `WAL`    | crash-safe journal; readers do not block writes            |
| `synchronous`  | `NORMAL` | durable across process crashes; warning data does not need `FULL` |
| `busy_timeout` | `5000`   | wait for locks instead of failing immediately              |
| `foreign_keys` | `ON`     | integrity checks if relations are added later              |
| connections    | `1`      | single-process, low volume: serialized access, no `SQLITE_BUSY` |

Unexpected process termination cannot corrupt the database (WAL); at most
the last committed transaction may be lost on power failure.

## Ping heartbeat

While MQTT is enabled, WarnFlux publishes a JSON ping every
`mqtt.ping_interval` (default 30 seconds) to `<topic_prefix>/status/ping`,
using the QoS from the configuration and `retain` set to `false`:

```json
{"type":"ping","service":"warnflux","version":"dev","timestamp":"2026-09-21T12:00:00Z"}
```

The timestamp is the current time in UTC.

## Verify with mosquitto_sub

With a broker running on `localhost`, subscribe to all WarnFlux topics:

```bash
mosquitto_sub -h localhost -t 'warnflux/#' -v
```

You should see a `warnflux/status/ping` message every 30 seconds.

## Docker

```bash
docker build -t warnflux .
docker run --rm \
  -v ./config.yaml:/config.yaml:ro \
  warnflux \
  --config /config.yaml
```

Note: without a volume for `/data`, the SQLite database lives in the
container's ephemeral filesystem and is lost when the container is
recreated. For anything but a quick smoke test, use a persistent volume as
shown below.

The image is built with a multi-stage Dockerfile, runs as a non-root user and
exposes no ports — WarnFlux is an MQTT client, not a server. The image
pre-creates a writable `/data` directory for that non-root user.

When using a named volume (as in the Compose example), Docker initializes it
from the image, so `/data` and `/logs` are writable out of the box. With a
**bind mount** to a host directory instead, that host directory must be
writable by UID `65532` (e.g. `sudo chown 65532:65532 ./data ./logs`).

### Deploy from GitHub Container Registry

Every release is published to `ghcr.io/<owner>/warnflux`, tagged with
the version and `latest`. Deployment needs only a single `config.yaml` file;
volumes are used for persistence:

| Mount         | Purpose                                          | Optional?                              |
|---------------|--------------------------------------------------|----------------------------------------|
| `/config.yaml`| configuration                                    | required (read-only)                   |
| `/data`       | SQLite database (`storage.path`)                 | recommended — otherwise the database is lost on recreation |
| `/logs`       | rotating log files (`app.log_file`)              | optional — logs go to stdout regardless |

A typical config for containers points the database at the volume and
optionally enables a rotating log file:

```yaml
storage:
  driver: sqlite
  path: /data/warnflux.db

app:
  log_file: /logs/warnflux.log
```

Get the latest image:

```bash
docker pull ghcr.io/<owner>/warnflux:latest
```

Run it (this also pulls the image on first use):

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

Follow the logs with `docker logs -f warnflux`.

To update to the latest release:

```bash
docker pull ghcr.io/<owner>/warnflux:latest
docker rm -f warnflux
docker run -d \
  --name warnflux \
  --restart unless-stopped \
  -v ./config.yaml:/config.yaml:ro \
  -v warnflux-data:/data \
  -v warnflux-logs:/logs \
  ghcr.io/<owner>/warnflux:latest \
  --config /config.yaml
```

For reproducible deployments, replace `latest` with a specific version tag,
e.g. `ghcr.io/<owner>/warnflux:0.1.0`.

### Docker Compose

```yaml
services:
  warnflux:
    image: ghcr.io/<owner>/warnflux:latest
    restart: unless-stopped
    volumes:
      - ./config.yaml:/config.yaml:ro
      - warnflux-data:/data
      - warnflux-logs:/logs
    command: ["--config", "/config.yaml"]

volumes:
  warnflux-data:
  warnflux-logs:
```

### Releases

`.github/workflows/release.yml` implements the Build and Publish flow:

- runs when a pull request targeting `main` is merged (or manually via
  `workflow_dispatch`)
- reads the version from `cmd/warnflux/version.txt`
- runs `go vet` and the tests
- builds Linux and Windows amd64 binaries
- drafts a GitHub release `v<version>` with release notes taken from the
  merged pull request and the binaries attached
- builds and pushes the Docker image to `ghcr.io/<owner>/warnflux`,
  tagged with the version and `latest`

To make a release: bump the version in `cmd/warnflux/version.txt`, merge
the pull request, then review and publish the draft release on GitHub.

## Demo

`warnflux demo` exercises the core pipeline against a temporary SQLite
database (removed afterwards):

```text
first event            → new
same event again       → duplicate
changed severity       → updated
cancelled event        → cancelled
repeated cancellation  → duplicate
expiration check       → expired
event without expiry   → stays active
```

```bash
go run ./cmd/warnflux demo
```

## Tests

```bash
go test ./...
```