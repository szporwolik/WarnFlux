# WarnFlux

WarnFlux aggregates hazard and emergency information from multiple
sources and routes normalized events to outputs such as MQTT.

This is the initial version: it only provides the application skeleton.

```text
YAML config
    ↓
WarnFlux
    ↓
MQTT connection
    ↓
periodic ping
```

Hazard sources and additional outputs are intentionally not implemented yet;
the code is structured so they can be added later.

## Requirements

- Go 1.26 or newer (for local builds)
- An MQTT broker such as [Mosquitto](https://mosquitto.org/) (optional)

## Project layout

```text
cmd/warnflux/    application entry point + version.txt
internal/config/     YAML configuration loading and validation
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

| Key                 | Default         | Description                                        |
|---------------------|-----------------|----------------------------------------------------|
| `app.log_level`     | `info`          | `debug`, `info`, `warn` or `error`                 |
| `mqtt.enabled`      | `false`         | connect to the MQTT broker on startup              |
| `mqtt.broker`       | –               | broker URL, e.g. `tcp://localhost:1883`            |
| `mqtt.client_id`    | `warnflux`  | client identifier for the broker                   |
| `mqtt.username`     | –               | username (empty for anonymous access)              |
| `mqtt.password`     | –               | password (empty for anonymous access)              |
| `mqtt.topic_prefix` | `warnflux`  | topic prefix for published messages                |
| `mqtt.qos`          | `1`             | QoS for published messages: `0`, `1` or `2`        |
| `mqtt.ping_interval`| `30s`           | interval between heartbeat pings (Go duration)     |

Credentials are never written to the logs.

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

The image is built with a multi-stage Dockerfile, runs as a non-root user and
exposes no ports — WarnFlux is an MQTT client, not a server.

### Deploy from GitHub Container Registry

Every release is published to `ghcr.io/<owner>/warnflux`, tagged with
the version and `latest`. Deployment needs only a single `config.yaml` file.

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
  ghcr.io/<owner>/warnflux:latest \
  --config /config.yaml
```

For reproducible deployments, replace `latest` with a specific version tag,
e.g. `ghcr.io/<owner>/warnflux:0.1.0`.

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

## Tests

```bash
go test ./...
```