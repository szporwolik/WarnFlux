# WarnFlux

> **Alerts in. Action out.**
>
> Running live as **[SOSNA](https://sosna.sp9moa.pl)** — the community alert
> station of the [Niepołomice Amateur Radio Club SP9MOA](https://sp9moa.pl).

![WarnFlux logo](assets/logo.png)

## Status: deep testing on one live station

WarnFlux is not finished, and it is not pretending to be. The code is being
tested in depth on a **single live installation**: **SOSNA** — *Społeczny
Otwarty System Notyfikacji i Alertów* ("Community Open Notification and Alert
System") — run by the **[Niepołomice Amateur Radio Club SP9MOA](https://sp9moa.pl)**
for Kraków and the neighbouring municipalities in southern Poland.

SOSNA is where every change lands first: real feeds from public providers, real
APRS and LoRa radios, a real GSM modem, real operators on duty and real people
waiting for a warning to arrive in time. What you see in this repository is the
engine behind that station, hardened against the failures a 24/7 station
actually meets — dropped radio sessions, a flapping uplink, a modem that
re-enumerates, a provider that changes its schema.

**See it working: <https://sosna.sp9moa.pl>** — live hazard map, message archive
and the station's own health. That is the best picture of what WarnFlux is meant
to be.

### Not ready for other installations yet

Running WarnFlux yourself is **not supported at this stage**, and the code is
not published with that expectation:

- configuration, storage schema, APIs and wire formats still change without a
  migration path;
- there is no stable release, no upgrade promise and no compatibility policy;
- there are no availability, security or data-integrity guarantees for a
  deployment you build from these sources;
- an installation made from today's code can break, lose data or change
  behaviour with the next commit.

The club runs one instance, deliberately, so that every defect is found where it
hurts least and fixed before anyone else depends on it. When the project is
ready to be installed elsewhere, this page will say so — until then, watch
SOSNA, read the sources, open an issue, and come back for the release.

## What WarnFlux is

A local-first EMCOM hub: it aggregates hazard information from pluggable sources
(official warnings and incident feeds, weather and air quality, radio and mesh
traffic — see *Data sources* below), tracks a durable event lifecycle in SQLite
and delivers every meaningful change to the outputs — e-mail, SMS, Discord,
APRS, Meshtastic — with a web panel for the operators on duty.

Everything described here runs on the SOSNA station today. It is still being
hardened: see the status section above before pointing it at anything you care
about.

## Data sources

Data belongs to the providers, not to WarnFlux. Each one is an opt-in source
plugin (`sources:` in the configuration), and every alert names the provider
it came from and links back to it.

| Provider | What it contributes |
|---|---|
| [IMGW-PIB](https://danepubliczne.imgw.pl/) | Meteorological and hydrological warnings |
| [GIOŚ](https://www.gios.gov.pl/) | Air-quality exceedances (PJP API) and serious industrial accidents |
| [GDDKiA](https://www.gov.pl/web/gddkia) | Winter road conditions and closures |
| [GDDKiA KPD](https://kpd.gddkia.gov.pl/) | National road obstructions (DATEX II): works, accidents, closures with the affected stretch drawn on the map |
| [RSO](https://komunikaty.tvp.pl/) | Government security-centre alerts |
| [burze.dzis.net](https://burze.dzis.net/) | Point storm/wind warnings and lightning detection |
| [Open-Meteo](https://open-meteo.com/) | Forecasts and air quality *(informational)* |
| [NOAA Aviation Weather](https://aviationweather.gov/) | METAR observations *(informational)* |
| [adsb.lol](https://adsb.lol/) — or a local readsb/tar1090 receiver | Aircraft traffic *(informational)* |
| [APRS-IS](https://www.aprs-is.net/) and [Meshtastic](https://meshtastic.org/) | Radio and LoRa mesh traffic |
| RCB (Alert RCB) and operator systems | Alerts received through the GSM modem and the REST API |

## For developers

Contributions, bug reports and testing are welcome — the review rounds that
shaped this code all came from running it on a real station and writing down
what broke. The commands below are the development entry point, **not an
installation guide**.

- [config.example.yaml](config.example.yaml) — every option, commented
- [docs/plugins.md](docs/plugins.md) — plugin contracts and rules
- [docs/weather-schema.md](docs/weather-schema.md) — weather wire format
- [docs/durability.md](docs/durability.md) — delivery & durability guarantees
- [docs/offline-mode.md](docs/offline-mode.md) — off-grid operation
- [docs/time-sync.md](docs/time-sync.md) — keeping the station clock right (NTP, GPS, LAN time server)
- [docs/resilience.md](docs/resilience.md) — hardening notes
- [docs/operations.md](docs/operations.md) — runbook: what runs where, boot sequence, health checks, upgrades, troubleshooting

```bash
go build ./...   # development build only
go test ./...
go test -race ./...
go vet ./...
```

## License

[Apache-2.0](LICENSE)
