# WarnFlux

> **Alerts in. Action out.**

![WarnFlux logo](assets/logo.png)

## ⚠️ Work in progress — do not install

**This repository contains a very early, pre-release version of the
product.** WarnFlux is under active development and is **not ready to be
installed, deployed or used for real-world operation**. Please do not
build your own installation from this code yet.

- Everything here is experimental and unfinished.
- There is no stable release, no upgrade path and no compatibility
  promise: configuration, storage, APIs and wire formats may change at
  any time, without notice and without migration paths.
- There are no availability, security or data-integrity guarantees.
- An installation made from this code today can break, lose data or
  change behavior with the next commit.

If you are looking for a working alert/EMCOM system to run, **this is not
the place yet** — come back once a stable release is announced.

## What WarnFlux is (planned)

A local-first EMCOM hub: it aggregates hazard information from pluggable
sources (official warnings and incident feeds, weather and air quality,
radio and mesh traffic — see *Data sources* below), tracks a durable event
lifecycle in SQLite and delivers every meaningful change to outputs, with
APRS/Meshtastic radio gateways and a web panel. Most of these pieces exist
in early form and are being hardened; none of them is considered
production-ready.

## Data sources

Data belongs to the providers, not to WarnFlux. Each one is an opt-in source
plugin (`sources:` in the configuration), and every alert names the provider
it came from and links back to it.

| Provider | What it contributes |
|---|---|
| [IMGW-PIB](https://danepubliczne.imgw.pl/) | Meteorological and hydrological warnings |
| [GIOŚ](https://www.gios.gov.pl/) | Air-quality exceedances (PJP API) and serious industrial accidents |
| [GDDKiA](https://www.gov.pl/web/gddkia) | Winter road conditions and closures |
| [RSO](https://komunikaty.tvp.pl/) | Government security-centre alerts |
| [burze.dzis.net](https://burze.dzis.net/) | Point storm/wind warnings and lightning detection |
| [Open-Meteo](https://open-meteo.com/) | Forecasts and air quality *(informational)* |
| [NOAA Aviation Weather](https://aviationweather.gov/) | METAR observations *(informational)* |
| [adsb.lol](https://adsb.lol/) — or a local readsb/tar1090 receiver | Aircraft traffic *(informational)* |
| [APRS-IS](https://www.aprs-is.net/) and [Meshtastic](https://meshtastic.org/) | Radio and LoRa mesh traffic |
| RCB (Alert RCB) and operator systems | Alerts received through the GSM modem and the REST API |

## For developers

Contributions and testing are welcome. The commands below are the
development entry point, **not an installation guide**.

- [config.example.yaml](config.example.yaml) — every option, commented
- [docs/plugins.md](docs/plugins.md) — plugin contracts and rules
- [docs/weather-schema.md](docs/weather-schema.md) — weather wire format
- [docs/durability.md](docs/durability.md) — delivery & durability guarantees
- [docs/offline-mode.md](docs/offline-mode.md) — off-grid operation
- [docs/resilience.md](docs/resilience.md) — hardening notes

```bash
go build ./...   # development build only
go test ./...
go test -race ./...
go vet ./...
```

## License

[Apache-2.0](LICENSE)
