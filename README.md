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
sources (IMGW-PIB meteo/hydro, RSO, Open-Meteo weather, APRS-IS), tracks
a durable event lifecycle in SQLite and delivers every meaningful change
to outputs, with APRS/Meshtastic radio gateways and a web panel. Most of
these pieces exist in early form and are being hardened; none of them is
considered production-ready.

## For developers

Contributions and testing are welcome. The commands below are the
development entry point, **not an installation guide**.

- [CONTRIBUTING.md](CONTRIBUTING.md) — how to contribute
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
