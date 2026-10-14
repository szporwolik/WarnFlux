# gddkia — national-road difficulties (GDDKiA)

The General Directorate for National Roads and Motorways publishes its
current road difficulties as one XML feed. WarnFlux turns the entries inside
its own operational area into hazard events (category `road`), so works,
alternating traffic and closures reach the groups that asked for them.

- Feed: `GET https://www.archiwum.gddkia.gov.pl/dane/zima_html/utrdane.xml`
  (the whole country in one document, ~500 entries today; the plugin refuses
  anything beyond 5000 entries as a transport/parsing risk).
- One entry → one hazard event. Every event carries the coordinates the feed
  provides, so the home map can draw it.

## Severity

| Feed state | Severity | Event type |
|---|---|---|
| `droga_zamknieta` true/1/tak | `severe` | Road closure |
| `ruch_wahadlowy` true/1/tak | `moderate` | Alternating traffic |
| anything else (works, mowing, …) | `minor` | Road works |

The provider's own type field is kept as `ProviderSeverity` for diagnostics;
it never participates in routing. The event type is a canonical English name
that every channel renders in the reader's language (`i18n.EventName` with
the `event.*` catalog keys).

## Area scope

The plugin follows the APRS hub's operational area — the same "position +
radius" the rest of the system uses:

- default center/radius: `aprs.latitude`/`longitude` (or the gridsquare) and
  `aprs.radius_km`;
- override per instance with `center_latitude`/`center_longitude` and
  `radius_km` (1..1000 km).

The APRS hub is therefore REQUIRED: without it the plugin refuses to start
(`the APRS hub is disabled, but its position and radius define the geographic
scope`) rather than silently publishing the whole country.

## Event identity and lifecycle

The identity is a stable hash of the entry's own identifying fields (road
number, kilometre, section name, creation time, coordinates and the
provider's type), so re-polling updates the event instead of duplicating it.
The feed is a COMPLETE snapshot of the country: a difficulty that disappears
from it is cancelled by the next poll
(`snapshotutil.Reconcile`), and an expired event is left to the core expiry
worker. Areas are normalized to road tokens (`droga:79`), which the mail and
the panel render as "roads: 79".

## Configuration

```yaml
sources:
  - id: gddkia-roads
    type: gddkia
    enabled: true
    config:
      poll_interval: 15m      # 1m..24h (default 15m)
      request_timeout: 10s    # <=1m
      # radius_km: 60         # default: aprs.radius_km
      # center_latitude: 50.02
      # center_longitude: 20.13
```

- The feed is a Polish government service: POLAND-ONLY, and it is suspended
  by the offline-mode switch (see `docs/offline-mode.md`).
- One instance is the norm; several instances with different centres are
  possible but each one downloads the same national document.
