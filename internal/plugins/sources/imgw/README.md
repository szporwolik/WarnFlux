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

      geography:
        enabled: true
        include:
          - gmina:niepolomice
          - miasto:krakow
          - powiat:wielicki
          - powiat:bochenski
```

| Field | Default | Meaning |
|-------|---------|---------|
| `poll_interval` | `5m` | Time between polls (1m..1h) |
| `request_timeout` | `10s` | Per-request HTTP timeout (>0..1m) |
| `base_url` | `https://danepubliczne.imgw.pl/api/data` | Endpoint base (tests override this) |
| `feeds` | `[meteo, hydro]` | Which official feeds to consume; `meteo`, `hydro`, or both |
| `geography.enabled` | `false` | enable local-area filtering |
| `geography.include` | – | target units as `<type>:<slug>`; unknown names fail startup |

No API keys or credentials are needed for the public API.

## Geographic filtering and TERYT resolution

A small static TERYT resolver (`internal/geo`) bundles the official GUS
TERYT units needed by this installation, with explicit parent hierarchy:

| TERYT | Unit |
|-------|------|
| `12` | województwo małopolskie |
| `1201` | powiat bocheński |
| `1202` | powiat brzeski |
| `1206` | powiat krakowski |
| `1216` | powiat tarnowski |
| `1217` | powiat tatrzański |
| `1219` | powiat wielicki |
| `1201011` | miasto Bochnia |
| `1201012` | gmina Bochnia (wiejska) |
| `1219043` | gmina Niepołomice |
| `1219044` | miasto Niepołomice |
| `1219045` | gmina Niepołomice (obszar wiejski) |
| `1219053` | gmina Wieliczka |
| `1219054` | miasto Wieliczka |
| `1261011` | miasto Kraków |

Matching is HIERARCHICAL, never string-prefix based:

```text
warning area == target
OR warning area is an ancestor of target
OR warning area is a descendant of an explicitly included broader target
```

So a warning for `powiat wielicki` matches target `gmina:niepolomice`
(Niepołomice lies inside that powiat), a gmina-level warning matches a
`powiat:wielicki` target, and `powiat tatrzański` never matches. Severity
alone never makes a geographically irrelevant warning relevant.

- Every provider TERYT entry is resolved BEFORE deciding: a warning
  passes when AT LEAST ONE of its areas intersects the configured
  geography.
- **Unknown TERYT codes** (provider evolution) are preserved verbatim,
  logged at debug level, never invent a name, and never match by
  themselves; they do not make the snapshot incomplete.
- **Malformed/implausible** TERYT values keep their existing validation
  (item skipped, snapshot incomplete).
- Raw `teryt:<code>` identifiers are RETAINED for technical/debug
  purposes, and enriched with resolved tokens
  (`powiat:wielicki`, `gmina:niepolomice`, …). Human-facing outputs
  (SMTP body, dashboard) render known areas via the shared display helper
  as e.g. `powiat tatrzański (TERYT 1217)`.

### Hydrological warnings

Hydro warnings use voivodeships, basin codes and free text, not a clean
TERYT list, so hydro geography is conservative: a warning passes when its
normalized description clearly intersects the local area (Niepołomice
villages, Kraków, Wieliczka, Bochnia, Kłaj, Targowisko, Szarów, Gdów,
the Drwinka/Serafa local waters). A plain `małopolskie` without any local
reference does not pass; broad rivers alone (Wisła, Raba) do not pass
because their basins span far beyond the target. Full basin-level GIS
resolution is deliberately out of scope and is not approximated.

### Filtering and snapshot correctness

Geographic filtering is intentional policy, not provider failure:

- a filtered warning does NOT make the provider snapshot incomplete and
  does NOT degrade source health,
- the reconciliation key set contains only accepted warnings: a warning
  that stops intersecting the configured geography is cancelled by the
  next complete snapshot,
- unknown TERYT codes do not cause provider-wide snapshot failure,
- malformed critical provider data keeps its existing safety semantics.

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
| meteo `teryt` | `Areas`: `teryt:<code>` + resolved `<type>:<slug>` (sorted, deduplicated) |
| hydro `obszary` | `Areas`: `wojewodztwo:<w>`, `zlewnia:<code>`, `obszar:<w>, <opis>` |
| `tresc` / `przebieg` + probability + comment | `Description` (deterministic) |

`Urgency` and `Certainty` stay empty — IMGW exposes probability but no CAP
urgency/certainty, and no values are invented. `Instruction` stays empty.
`ReceivedAt`/`UpdatedAt` are core-owned and never set by the plugin.
Basin codes are never geo-resolved; TERYT codes are resolved through the
static mapping above (unknown codes stay raw `teryt:<code>`).

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

- Hydro geography is conservative text matching, not basin-level GIS:
  precise basin/river resolution is documented as out of scope above.
- Probability is preserved only in the description text.
- Provider publication time is not modeled on `HazardEvent` yet.

## MQTT topics produced

This plugin emits `HazardEvent`s; it does not publish MQTT directly:

```text
warnflux/events
warnflux/active/imgw-meteo/<sha256(event_key)>
warnflux/active/imgw-hydro/<sha256(event_key)>
```
