# WarnFlux

> **Alerts in. Action out.**

![WarnFlux logo](assets/logo.png)

WarnFlux aggregates hazard information from pluggable sources, normalizes
it into one event model, tracks each event's lifecycle in SQLite and
delivers every meaningful change to outputs. **Early days — this is just
the beginning of the project.**

## What it does

- **Sources** (compiled-in plugins): IMGW-PIB warnings (meteo + hydro),
  RSO communications, Open-Meteo weather, APRS-IS.
- **Core**: normalized `HazardEvent` with stable identity and content
  fingerprint; new / updated / cancelled / expired lifecycle in SQLite;
  durable change journal with at-least-once delivery to outputs.
- **Outputs**: MQTT (event stream + retained active view + retained
  status snapshot), SMTP e-mail, HTTP webhooks.
- **Dispatch**: MQTT receivers feed a bounded ingress; the group routing
  matrix fires actions (SMTP, webhooks, APRS messages to hams) per
  severity threshold.
- **APRS**: a shared hub merges backends (APRS-IS now, KISS radio later)
  and publishes one retained station document per nearby station; the
  public page maps them with a weather-radar overlay.
- **Web UI**: public home page (active hazards + radio map) and an
  authenticated admin area (dashboard, MQTT traffic + topic browser,
  users, groups, health, logs, notifications).

## Quick start

Requirements: Go 1.26+, an MQTT broker (e.g.
[Mosquitto](https://mosquitto.org/)).

```bash
git clone https://github.com/szporwolik/WarnFlux
cd WarnFlux
cp config.example.yaml config.yaml   # edit to match your broker
go build -o warnflux ./cmd/warnflux
./warnflux --config config.yaml
```

`warnflux --version` prints the build version and commit. The database
(SQLite, pure Go — no CGO) is created and migrated automatically.

## Configuration

Everything lives in one YAML file — `config.example.yaml` documents every
option with comments: `app` (logging, retention), `storage` (SQLite),
`sources`, `outputs`, `dispatch.mqtt_receivers`, `web`, `actions`,
`ingest_http`, `aprs`.

Plugin instances follow one shape:

```yaml
sources:
  - id: imgw-warnings
    type: imgw           # registered plugin type
    enabled: true
    runtime:             # supervision: restart, shutdown_timeout
      restart: true
    config:              # plugin-specific, decoded strictly
      poll_interval: 5m
```

Output `id`s are durable consumer identities of the journal — renaming one
creates a new consumer.

## MQTT contract

All topics live under the configured `topic_prefix` (`warnflux` in the
examples). Payloads are UTF-8 JSON with `schema_version` (currently `1`)
and `lower_snake_case` fields.

| Topic | Retained | Content |
|-------|----------|---------|
| `warnflux/events` | no | one message per event transition (`new`/`updated`/`cancelled`/`expired`) |
| `warnflux/active/#` + `warnflux/active-list` | yes | current active hazards |
| `warnflux/info/#` | yes | weather snapshots |
| `warnflux/status` | yes | application health (last-will publishes `state: offline`) |
| `warnflux/aprs/stations/<CALLSIGN>` | yes | merged APRS station state |
| `warnflux/aprs/packets`, `warnflux/aprs/messages` | no | live APRS feeds |

Subscribe to everything:

```bash
mosquitto_sub -h localhost -p 1883 -t 'warnflux/#' -v
```

Delivery is **at-least-once** (durable journal + per-output cursors in
SQLite); deduplicate on `change_id` per instance if you need exactly-once.

## Docker

```bash
docker build -t warnflux .
docker run --rm -v ./config.yaml:/config/config.yaml:ro warnflux --config /config/config.yaml
```

The image runs as a non-root user and expects the database on a mounted
`/data` volume (`storage.path: /data/warnflux.db`).

## Web UI

One authenticated admin area (`web.enabled: true`), server-rendered with
embedded assets (no CDN), session login and CSRF protection:

- `/` — public page: active hazards, radio stations map
- `/dashboard` — system status, MQTT connections, weather, plugins, actions
- `/traffic` — MQTT traffic viewer + retained-topic browser
- `/groups` / `/users` — group routing matrix, roles, per-user APRS callsigns
- `/health` `/logs` `/notifications` `/test` — admin tools

The MQTT publisher (`outputs[].type=mqtt`) and the receivers
(`dispatch.mqtt_receivers[]`) are separate MQTT clients — they must use
different client IDs. Receivers are the dispatch INPUT; each has its own
connection, credentials and subscriptions and fails independently.

## Public ingest endpoints

`ingest_http` configures API-key-protected HTTP endpoints (one per
instance): a remote scraper POSTs a hazard, the endpoint publishes it to
`warnflux/events` on the configured broker and the normal receiver →
routing → actions flow handles it. Bearer key, CIDR allowlist and rate
limit per instance.

## Documentation

- [config.example.yaml](config.example.yaml) — every option, commented
- [docs/plugins.md](docs/plugins.md) — plugin contracts and rules
- [docs/weather-schema.md](docs/weather-schema.md) — weather wire format
- per-plugin READMEs under `internal/plugins/`

Plugins are compiled-in Go packages registered in
`internal/plugins/plugins.go` — not dynamic libraries.

## Development

```bash
go test ./...            # unit + integration + fuzz
go test -race ./...
go vet ./...
```

Releases: bump `VERSION`, push, tag `vX.Y.Z` — CI builds multi-arch
binaries and the Docker image (`ghcr.io/<owner>/warnflux`), and fails
when the tag does not match `VERSION`.

## License

[Apache-2.0](LICENSE)

```