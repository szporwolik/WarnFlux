# RSO (Regionalny System Ostrzegania) source plugin

## Provider

[RSO](https://www.gov.pl/web/mswia/regionalny-system-ostrzegania) — public
warning/communication system operated via the TVP communications portal.
Integration documentation: <https://www.komunikaty.tvp.pl/Info/Integration>.

This plugin uses ONLY the documented public XML integration:

```text
https://komunikaty.tvp.pl/komunikatyxml/{WOJEWODZTWO}/{KATEGORIA}/{PAGE}?_format=xml
https://komunikaty.tvp.pl/wojewodztwa?_format=xml
https://komunikaty.tvp.pl/kategorie?_format=xml
```

No authenticated CAP API, no HTML scraping, no JSON endpoint.

## Configuration

```yaml
sources:
  - id: rso
    type: rso
    enabled: true

    runtime:
      restart: true
      shutdown_timeout: 10s

    config:
      poll_interval: 5m
      request_timeout: 10s

      voivodeships:
        - malopolskie
        - slaskie
        - swietokrzyskie
```

| Field | Default | Meaning |
|-------|---------|---------|
| `poll_interval` | `5m` | Time between polls (1m..1h) |
| `request_timeout` | `10s` | Per-request HTTP timeout (>0..1m) |
| `base_url` | `https://komunikaty.tvp.pl` | Portal base (tests override this) |
| `voivodeships` | `[wszystkie]` | Official RSO slugs to query; `wszystkie` = whole country and is EXCLUSIVE (not combinable with regional slugs) |

Supported official slugs (current, verified live):

```text
dolnoslaskie  kujawsko-pomorskie  lubelskie  lubuskie  lodzkie
malopolskie   mazowieckie         opolskie   podkarpackie  podlaskie
pomorskie     slaskie             swietokrzyskie  warminsko-mazurskie
wielkopolskie zachodniopomorskie  wszystkie
```

Official Polish display names (`małopolskie`, `śląskie`, …) are also
accepted and canonicalize to the slugs. A typo fails configuration.

### High-signal filter (recommended for local installations)

The optional `filter` block turns RSO from a mirror of the regional feed
into a HIGH-SIGNAL local source. All policy decisions run in-process, are
deterministic and never touch the network:

```yaml
    config:
      voivodeships:
        - malopolskie

      filter:
        high_signal_only: true
        exclude_rcb: true
        exclude_air_quality: true
        suppress_imgw_duplicates: true
        local_min_severity: moderate
        regional_min_severity: severe
        corridor:
          enabled: true
          km_from: 400
          km_to: 503
```

| Field | Default | Meaning |
|-------|---------|---------|
| `high_signal_only` | `false` | apply the severity/geographic thresholds (exclusions below still apply) |
| `exclude_rcb` | `true` | discard RCB communications unconditionally (they are handled by a dedicated path; a serious RCB message is still not emitted — intentional deduplication) |
| `exclude_air_quality` | `true` | discard air-quality / smog / PM10 / PM2.5 notices (a dedicated source covers them; toxic smoke from a fire or a chemical release is civil protection and is NOT discarded) |
| `suppress_imgw_duplicates` | `false` | discard plain RSO copies of IMGW meteo/hydro warnings; a communication adding a distinct civil-protection consequence (evacuation, road closure, water trouble, infrastructure failure) is kept |
| `local_min_severity` | `moderate` | minimum severity for the local core (from `filter.local`) and the corridor |
| `regional_min_severity` | `severe` | minimum severity for the whole voivodeship without a local match |
| `corridor` | disabled unless configured | optional kilometre window (km_from < km_to) treated as the road corridor; `a4_corridor` is accepted as a legacy alias |

## Endpoint and filtering

Filtering happens in two layers. UPSTREAM: each configured voivodeship is
one request to

```text
/komunikatyxml/<slug>/wszystkie/0?_format=xml
```

Page `0` returns the full list without pagination (verified live); the
category is always `wszystkie` for now. Responses are bounded (8 MiB,
4096 items per region).

## Snapshot semantics

The page-0 list was VERIFIED LIVE as the current applicable communication
set (all observed `valid_to` timestamps are in the future; disappearing
items are expired or withdrawn, not archived). Therefore:

- a complete combined snapshot enables disappearance reconciliation
  (CANCELLED for withdrawn non-expired communications; already-expired
  ones stay with the core expiration worker),
- reconciliation reads the authoritative SQLite active state through the
  source active-event reader, so it survives process restarts,
- reconciliation runs ONLY for a complete snapshot: any regional fetch
  failure, malformed XML, wrong root element, pagination-count mismatch,
  missing/conflicting identity or over-limit feed disables it.

## Field mapping

| Provider | WarnFlux |
|----------|--------------|
| `<id>` | `SourceID` (stable provider identity, used directly) |
| `<title>` | `Event`, `Headline` |
| `<content>` (fallback `<shortcut>`) | `Description` (whitespace-normalized) |
| `<valid_from>` | `EffectiveAt` (naive Polish local → `Europe/Warsaw`, DST-aware) |
| `<valid_to>` | `ExpiresAt` (empty → nil) |
| `<provinces>` | `Areas`: `wojewodztwo:<slug>` (fallback: the queried region slug, never `wszystkie`) |

- `Category` stays empty — the list items do not carry a category.
- `Severity` is always `unknown`: RSO exposes no severity/priority with
  documented semantics, and none is invented.
- `Urgency`, `Certainty`, `Instruction` stay empty.
- `SourceURL` is the filtered XML list URL (the old `/komunikaty/<id>/detale`
  URL currently returns 404, so the list URL is the reliable public
  reference).
- `ReceivedAt`/`UpdatedAt` are core-owned and never set by the plugin.

With the `filter` block enabled the mapping is enriched:

- **`rso_alarm` is NOT severity.** The provider does not document
  `rso_alarm` as a severity scale; it is never consulted. Severity is
  inferred only from explicit semantics in `title` / `shortcut` /
  `content`.
- Explicit Polish warning degrees map deterministically:
  `1 → moderate`, `2 → severe`, `3 → extreme` (forms like
  `ostrzeżenie pierwszego stopnia`, `ostrzeżenie 1 stopnia`,
  `stopień: 2`, `stopień zagrożenia: 3`). Unrelated numbers are never
  treated as degrees.
- Semantic severity rules cover water-supply incidents (unfit water,
  microbiological contamination, boil orders → `severe`; conditional
  fitness → `moderate`), roads (target-road complete closure → `severe`,
  one lane/alternating traffic → `moderate`, routine works → `minor`
  unless a complete closure is announced), hydrology (`stan alarmowy` →
  `severe`, `stan ostrzegawczy` → `moderate`) and civil protection
  (evacuation, explosion, gas/chemical release, major fire → `severe`).
- **No hard blacklist of local information:** `test syren`, `ćwiczenia`,
  `szczepienie lisów`, `uwaga hałas` etc. are classified normally (usually
  `minor`/`unknown`) and fall under the threshold — they are never
  hard-rejected just for containing those words.
- `Category` is set only when confident: `road`, `water`, `hydrology`,
  `weather` or `civil-protection`.
- `Areas` are enriched with normalized tokens when confident:
  `gmina:niepolomice`, `powiat:wielicki`, `miasto:krakow`,
  `miasto:wieliczka`, `miasto:bochnia`, `droga:a4`, `droga:dk75`,
  `droga:dw964`, `corridor:a4-balice-tarnow`. Nothing is invented from
  weak textual evidence.

### Geographic relevance

Classification and geography are separate concepts: an event is first
classified, then geographically scoped, then emitted or suppressed.

- **Core area:** Niepołomice and gmina Niepołomice (Podłęże, Staniątki,
  Wola Batorska, Wola Zabierzowska, Zabierzów Bocheński, Chobot,
  Ochmanów, Słomiróg, Suchoraba, Zagórze, Zakrzów, Zakrzowiec) plus
  `powiat wielicki` → `moderate`+ is emitted.
- **Cities:** Kraków, Wieliczka, Bochnia → `moderate`+.
- **A4 Balice–Tarnów corridor:** A4 kilometre references in the configured
  window (tolerant of `435 km`, `435,6 km`, `435.6 km`, `435+600`,
  `km 435+600`) or corridor location names (Balice, Kraków, Bieżanów,
  Wieliczka, Podłęże, Niepołomice, Targowisko, Szarów, Kłaj, Bochnia,
  Brzesko, Wierzchosławice, Tarnów) → `moderate`+.
- **Other roads** (DK75, DK94, DW964, also DW965/966/967, S7): relevant
  only with a direct local place match or an explicitly relevant section —
  a road number alone never makes an event local.
- An **A4 event outside the corridor and without a local place is never
  accepted** just because it contains `A4` (`A4 zablokowana, 97 km,
  kierunek Wrocław` → rejected).
- **Whole voivodeship** without a local match: only `severe`/`extreme`
  passes (a generic first-degree voivodeship-wide warning is normally NOT
  emitted).

### Filtering and snapshot correctness

Policy filtering is intentional, not provider failure:

- a filtered item does NOT mark the combined snapshot incomplete,
- a filtered item does NOT degrade source health,
- the reconciliation key set contains only accepted items: an event that
  was relevant before and stops satisfying the policy (or disappears
  upstream) is cancelled by the next complete snapshot,
- provider failures (fetch, malformed XML, wrong root, pagination
  mismatch, identity conflicts) keep their existing incomplete-snapshot
  semantics and still disable reconciliation.

## Multi-region merge

All configured regional feeds form ONE logical snapshot: the same provider
`id` seen in several regions is merged (areas unioned, sorted,
deduplicated) and emitted once. The same `id` with CONFLICTING non-area
content makes the identity ambiguous: it is not emitted and the combined
snapshot is incomplete (no reconciliation).

## Failure semantics

- HTTP failure, timeout, oversized body, malformed XML, wrong root,
  pagination mismatch or an unidentifiable item ⇒ that region is
  incomplete ⇒ no disappearance reconciliation for the combined snapshot.
- A failed region never looks like "all its warnings disappeared".
- Source health: healthy only when the combined snapshot is complete;
  degraded otherwise. Emit/MQTT failures belong to the core and do not
  degrade source health.

## Known limitations

- No category filtering configuration yet (always `wszystkie`).
- No geolocation / powiat resolution; only voivodeship filtering.
- Severity is unknown for every RSO item until the provider documents
  degree semantics.

## MQTT topics produced

This plugin emits `HazardEvent`s only; it does not publish MQTT directly:

```text
warnflux/events
warnflux/active/rso/<sha256(event_key)>
```
