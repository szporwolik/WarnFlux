# IMGW-PIB warnings source plugin

## Provider

[Instytut Meteorologii i Gospodarki Wodnej – Państwowy Instytut Badawczy
(IMGW-PIB)](https://danepubliczne.imgw.pl/apiinfo) — public warning data
API. The plugin consumes exactly two documented endpoints:

- `https://danepubliczne.imgw.pl/api/data/warningsmeteo` — meteorological
  warnings (degree 1–3, TERYT areas)
- `https://danepubliczne.imgw.pl/api/data/warningshydro` — hydrological
  warnings (degree 1–3 plus ungraded drought, voivodeship/basin areas)

**Attribution (required by IMGW):**

> Źródłem pochodzenia danych jest Instytut Meteorologii i Gospodarki
> Wodnej – Państwowy Instytut Badawczy.
>
> Dane Instytutu Meteorologii i Gospodarki Wodnej – Państwowego Instytutu
> Badawczego zostały przetworzone.

WarnFlux normalizes and processes the source data; IMGW requires
processed data to be attributed accordingly. Do not remove or obscure the
IMGW origin of the data.

## Configuration

```yaml
sources:
  - id: imgw-warnings
    type: imgw
    enabled: true

    runtime:
      restart: true
      shutdown_timeout: 10s

    config:
      poll_interval: 5m
      request_timeout: 10s

      feeds:
        - meteo
        - hydro
```

| Field | Default | Meaning |
|-------|---------|---------|
| `poll_interval` | `5m` | Time between polls (1m..1h) |
| `request_timeout` | `10s` | Per-request HTTP timeout (>0..1m) |
| `base_url` | `https://danepubliczne.imgw.pl/api/data` | Endpoint base (tests override this) |
| `feeds` | `[meteo, hydro]` | Which official feeds to consume; `meteo`, `hydro`, or both |

No API keys or credentials are needed for the public API.

## Behavior

- Polls immediately at startup, then every `poll_interval`. Both feeds are
  fetched sequentially; one failing feed never suppresses the other.
- Each successful fetch is a COMPLETE provider snapshot of the current
  warning set.

### Snapshot / disappearance reconciliation

Because IMGW returns the CURRENT warning set, a warning that disappears
from a complete snapshot is treated as withdrawn:

- still present → deduplicated/updated by the core
- new → ingested as NEW
- disappeared with a future `ExpiresAt` → CANCELLED
- disappeared with `ExpiresAt` already in the past → left to the core
  expiration worker (EXPIRED)
- disappeared with NO `ExpiresAt` (e.g. withdrawn hydrological drought)
  → CANCELLED

Reconciliation reads the authoritative SQLite active state through the
source active-event reader capability, so it also works after a process
restart — it never depends on plugin-local memory. Reconciliation runs
ONLY for complete snapshots: a failed fetch, malformed JSON, an
unidentifiable item, duplicate identities or an oversized response make
the snapshot incomplete and cancel nothing.

### Special "no products" response

IMGW reports "no warnings currently" as HTTP 404 with
`{"status":false,"message":"No products were found"}`. Exactly this
condition is a successful EMPTY snapshot (equal to `[]`) — a provider
outage is never mistaken for a mass cancellation.

### Field mapping

| Provider | WarnFlux |
|----------|--------------|
| meteo `id` | `SourceID` (used directly) |
| meteo `nazwa_zdarzenia` / hydro `zdarzenie` | `Event`, `Headline` |
| meteo `stopien` / hydro `stopień` 1,2,3 | `Severity` moderate/severe/extreme |
| hydro `stopień` `-1` (drought, bezstopniowe) | `Severity` unknown |
| unknown/unexpected degree | `Severity` unknown (logged) |
| `obowiazuje_od` / `data_od` | `EffectiveAt` |
| `obowiazuje_do` / `data_do` | `ExpiresAt` (year ≥ 9999 → nil) |
| meteo `teryt` | `Areas`: `teryt:<code>` (sorted, deduplicated) |
| hydro `obszary` | `Areas`: `wojewodztwo:<w>`, `zlewnia:<code>`, `obszar:<w>, <opis>` |
| `tresc` / `przebieg` + probability + comment | `Description` (deterministic) |

`Urgency` and `Certainty` stay empty — IMGW exposes probability but no CAP
urgency/certainty, and no values are invented. `Instruction` stays empty.
`ReceivedAt`/`UpdatedAt` are core-owned and never set by the plugin.
TERYT and basin codes are never geo-resolved.

### Hydrological drought

IMGW defines hydrological drought as an ungraded (bezstopniowe),
indefinite warning: degree `-1` and `data_do` of `9999-12-31 23:59:59`
("year ≥ 9999" → `ExpiresAt = nil`). Such warnings stay active until
withdrawn upstream; snapshot reconciliation is what eventually cancels
them.

### Hydrological identity

Hydrological warnings have no globally unique ID, so the identity is
derived deterministically from `sha256(year(data_od), numer, normalized
biuro)` — content changes (description, probability, expiry, areas) never
change the identity, so ordinary updates stay UPDATED, not new hazards.
Changing the year, number or office produces a different identity.

### Timestamps

IMGW timestamps are naive Polish local times parsed in `Europe/Warsaw`
(`2006-01-02 15:04:05`, DST-aware), never UTC. Malformed required
timestamps make the item unidentifiable and the snapshot incomplete.

### Source health

Healthy when every enabled feed produces a complete snapshot; degraded
when any feed fails or is incomplete. Emit/MQTT delivery failures belong
to the core and do not degrade source health. Ordinary provider failures
are logged and retried on the next poll — the supervisor is not restarted.

## Known limitations

- No geographic filtering (TERYT allowlists, voivodeship filters,
  geofencing) yet — the full authoritative warning set is ingested.
- Probability is preserved only in the description text.
- Provider publication time is not modeled on `HazardEvent` yet.

## MQTT topics produced

This plugin emits `HazardEvent`s; it does not publish MQTT directly:

```text
warnflux/events
warnflux/active/imgw-meteo/<sha256(event_key)>
warnflux/active/imgw-hydro/<sha256(event_key)>
```
