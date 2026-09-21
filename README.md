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

## Plugins

### Philosophy

WarnFlux plugins are **compiled-in integrations**: ordinary Go packages
in this repository, selected and configured through the YAML configuration.
They are not dynamic libraries, and no arbitrary code is loaded at runtime.
A community contribution goes through code review and tests, is compiled
into the binary, and is then enabled through YAML.

### Categories

- **Source plugins** obtain hazard information (CAP, GDACS, IMGW, …) and
  emit normalized `HazardEvent` values through the `Emitter` interface.
- **Output plugins** receive meaningful `EventChange` values from the core
  (currently the built-in MQTT output).

Plugins never bypass the core: validation, identity, fingerprinting,
deduplication, persistence, lifecycle and routing are owned by WarnFlux.
A source cannot access the database, and an output cannot access other
plugins.

### Configuration

```yaml
sources:
  - id: demo
    type: demo            # registered plugin type
    enabled: false
    runtime:              # framework-level supervision options
      restart: true
      startup_timeout: 15s
      shutdown_timeout: 10s
    config:               # plugin-specific, decoded by the plugin itself
      interval: 30s

outputs:
  - id: mqtt-main
    type: mqtt
    enabled: true
    runtime:
      timeout: 10s
      failure_threshold: 5
    config:
      broker: tcp://localhost:1883
      client_id: warnflux-events
      topic_prefix: warnflux
      qos: 1
```

Every instance needs a unique `id` (shared namespace between sources and
outputs) and a known `type`. Disabled plugins are never instantiated.
Unknown types, duplicate IDs and malformed plugin configuration are startup
errors: WarnFlux refuses to start.

### Failure isolation

- plugin panics are recovered, logged with a stack trace and isolated
- every source runs under its own supervisor with bounded exponential
  backoff (1s → 2s → … → max 1m, reset after a healthy run)
- every output has its own bounded queue and worker with a per-call timeout
- a broken MQTT server cannot block other outputs, and vice versa
- repeated output failures suspend the plugin; periodic recovery probes
  reset the failure counter on success
- queues are bounded; a full queue applies backpressure and reports an
  error instead of silently dropping hazard events
- plugin failures are logged with `plugin_id`, `plugin_type` and the error;
  secrets are never logged

> Because built-in plugins execute inside the WarnFlux process, this is
> fault isolation rather than a security sandbox. A malicious or severely
> broken plugin can still call `os.Exit`, consume all memory, spawn
> unmanaged goroutines or ignore context cancellation. Community plugins
> must undergo code review.

### Writing a plugin

A source plugin implements two methods:

```go
package example

type Source struct{ cfg Config }

func (s *Source) Name() string { return "example" }

func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
    // provider-specific polling loop; return when ctx is cancelled
    return nil
}

// New decodes and validates the plugin-specific YAML config.
func New(node *yaml.Node) (plugin.SourcePlugin, error) {
    var cfg Config
    if err := plugin.DecodeConfig(node, &cfg); err != nil {
        return nil, err
    }
    // validate cfg here
    return &Source{cfg: cfg}, nil
}

func Register(reg *plugin.Registry) error {
    return reg.RegisterSource("example", New)
}
```

An output plugin is equally small:

```go
func (o *Output) Name() string { return "example" }

func (o *Output) Handle(ctx context.Context, change core.EventChange) error {
    // deliver the change; ctx is bounded by runtime.timeout
    return nil
}

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

Mandatory rules for plugin implementations:

- respect context cancellation; do not block past it
- do not panic intentionally; do not call `os.Exit`
- do not create unmanaged permanent goroutines or unbounded channels
- use request contexts and finite timeouts for network calls
- do not access storage, ingestion internals or other plugins directly
- do not bypass the emitter / change contract
- do not log secrets; do not use global mutable state
- validate the configuration before starting; return meaningful errors

### Contribution requirements

A plugin pull request must include:

- a typed configuration struct decoded strictly from YAML
- configuration validation
- unit tests
- README / configuration documentation
- reasonable network timeouts and context cancellation support
- no direct database access, no dependency on other plugins
- no secrets in logs, no unbounded goroutines or channels
- no process termination calls and no unnecessary large dependencies

### HTTP client guidance

Plugins calling APIs must use the request context, a finite connect/request
timeout, a reasonable User-Agent, and bounded response body sizes where
practical. A remote endpoint must not be able to hold a WarnFlux plugin
connection forever.

## Project layout

```text
cmd/warnflux/    application entry point + demo command + version.txt
internal/config/     YAML configuration loading and validation
internal/core/       normalized event model, identity, fingerprint
internal/ingest/     dedup/update/cancel pipeline + expiration worker
internal/storage/    EventStore interface + SQLite implementation
internal/plugin/     plugin contracts, registry, supervision, status
internal/plugins/    built-in source and output plugins
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

Plugin instances are configured in the `sources:` and `outputs:` sections;
see the [Plugins](#plugins) section.

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