# gios — serious industrial accidents (GIOŚ register)

The Chief Inspectorate of Environmental Protection keeps the official
register of serious industrial accidents (*rejestr poważnych awarii*). The
plugin follows the register for the configured administrative scope and turns
the relevant entries into hazard events, so an accident in the area reaches
the groups that asked for it.

- API: `GET https://dane.gios.gov.pl/api/powazne-awarie` (paginated register,
  year by year).
- POLAND-ONLY, and it is suspended by the offline-mode switch (see
  `docs/offline-mode.md`).
- The register is small and historical: in Małopolska it holds a handful of
  entries for the last decade. A poll therefore often finds nothing — that is
  the normal state, not a fault.

## Scope

Two filters decide what becomes an alert:

1. **Administrative scope** — `wojewodztwo` (one of the provider's 16
   voivodeship names) and optionally `powiat`. When `wojewodztwo` is empty the
   plugin derives it ONCE from the station's home position (a reverse
   geocoding call to `reverse_url`, Nominatim by default) and caches it.
2. **Geographic scope** — the entry's locality is geocoded through the GUGiK
   ULDK service (`geocoder_url`) and must fall inside the operational radius
   (default: `aprs.radius_km`, override with `radius_km` /
   `center_latitude` / `center_longitude`).

`rok` overrides the register year; 0 (the default) means the current year.

## Severity

| Entry type (`rodzaj_zdarzenia`) | Severity | Event type | Category |
|---|---|---|---|
| contains `emisja`, `wyciek` or `uwolnienie` | `severe` | Chemical release | `chemical` |
| everything else | `moderate` | Industrial accident | `industrial` |

The provider's own type text is kept as `ProviderSeverity` for diagnostics
and appears in the headline (`<locality> — <type>`); it never participates in
routing. Both event types are canonical English names that every channel
translates for the reader (`i18n.EventName`).

## Event identity and lifecycle

The identity is a stable hash of the entry's identifying fields (locality,
type, source of the event and its description), so re-polling updates the
event instead of duplicating it. Each fetch is a COMPLETE provider snapshot
for the year and the administrative scope, so an entry that disappears is
cancelled by the next poll (`snapshotutil.Reconcile`); events expire through
the core expiry worker. `EffectiveAt` is the entry's date (the register
publishes a date without a time, parsed as Europe/Warsaw midnight).

Areas are normalized to administrative tokens (`wojewodztwo:małopolskie`,
`powiat:wielicki`), which is what the routing matrix and the area matching
consume.

## Configuration

```yaml
sources:
  - id: gios-awarie
    type: gios
    enabled: true
    config:
      poll_interval: 6h        # 30m..7d (default 6h — the register is slow)
      request_timeout: 10s     # <=1m
      # wojewodztwo: małopolskie   # empty = derived from the home position
      # powiat: wielicki
      # rok: 0                   # 0 = the current year
      # radius_km: 60            # default: aprs.radius_km
      # center_latitude: 50.02
      # center_longitude: 20.13
      # geocoder_url: https://uldk.gugik.gov.pl
      # reverse_url: https://nominatim.openstreetmap.org
```

The geocoders are third-party services: the reverse lookup happens at most
once per run, and a locality is resolved once and cached for the process
lifetime, so a steady station makes a handful of geocoding calls rather than
one per poll and per entry.

A geocoding failure is treated as a transient provider problem, not as data:
the entry is skipped AND the snapshot is marked incomplete, so the
reconciliation does not cancel anything on partial data (the same rule the
other snapshot sources follow).
