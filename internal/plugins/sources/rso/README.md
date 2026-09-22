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

## Endpoint and filtering

Filtering happens UPSTREAM: each configured voivodeship is one request to

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
