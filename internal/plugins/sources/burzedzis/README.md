# burzedzis — burze.dzis.net storm warnings and lightning nowcast

[burze.dzis.net](https://burze.dzis.net) republishes the official Polish
weather warnings (the IMGW degrees) for any point in Poland and runs its own
lightning-discharge detection. One plugin instance follows a list of
localities and publishes **two** sources:

| source slug | what it carries |
|---|---|
| `burzedzis` | the warnings in force (funnel, storm, wind, rain, heat, frost) |
| `burzedzis-burza` | the lightning discharges detected around a locality |

Two rows, because the two are routed differently: a lightning detection is
"a storm is over us right now" (a fast channel), a first-degree frost warning
is not.

## API

SOAP over `POST https://burze.dzis.net/soap.php` (the published WSDL is the
provider's documentation). The plugin uses three operations:

- `miejscowosc` — locality name → coordinates. **Diacritic-sensitive**: the
  directory knows `Niepołomice`, not `Niepolomice`, and answers `0/0` for
  anything it does not know.
- `ostrzezenia_pogodowe` — the warnings in force for a point: degree (`0` =
  not in force, `1..3` = first/second/third degree) and a UTC window per
  phenomenon. The answer is a COMPLETE snapshot for that point, so a warning
  the provider withdraws early is cancelled instead of hanging until its
  window ends.
- `szukaj_burzy` — lightning discharges around a point within a radius
  (default 25 km) over a rolling 10/15/20 minute window: how many, how far
  the nearest one was and from which direction.

### The 0/0 trap

An unknown locality answers `0/0`, and `0/0` is at the same time the
provider's **"return test data"** switch: a full set of third-degree warnings
plus a 999-discharge storm. A mistyped town would therefore turn into fake
severe alerts. Two guards:

- `internal/burzedzis` refuses `0/0` with `ErrUnknownLocality` — coordinates
  only ever come from a successful directory lookup, never from a guess.
- The Config page checks every submitted name against the directory before
  writing it, and refuses the save naming the unknown ones.

An unresolved name (resolved once, cached) is reported in the source's status
line (`unknown: …`) and logged — a typo shows up as a named gap, never as
silence.

### The two kinds of failure are kept apart

Only the provider's own 0/0 answer (ErrUnknownLocality) marks a name as
unknown and drops its coordinates. Everything else — a refused request, HTTP
503, a timeout, an unreadable answer — is a fact about the network, not about
the name:

- the coordinates already resolved for that locality are KEPT, so the station
  keeps watching through a provider outage;
- the name is queued and looked up again on the very next poll (a transient
  failure never waits for `locality_refresh`);
- the pass does not count as a completed refresh, and the poll reports the
  source as degraded.

Treating a 503 as "unknown locality" used to wipe the point list and mark the
cache fresh, which cost 24 hours of monitoring after one bad minute — the
tests pin both behaviours (`TestResolveSurvivesProviderFailure`,
`TestResolveDistinguishesUnknownFromUnreachable`).

### An answer has to prove it is the operation's payload

`xml.Unmarshal` succeeding proves nothing: a maintenance page, a plain
`<html>` document or an empty SOAP body all parse cleanly and leave every
field at its zero value. Because the warnings answer is a COMPLETE snapshot
per point (a degree of 0 means "not in force"), reading such a document as
"nothing is in force" would reconcile every live warning of the source away —
a silent withdrawal of real alerts.

So the client validates what it decoded, before the adapter ever sees it:

- the operation element and its `return` payload must be present, and the
  fields the provider always ships (all six warning phenomena with their
  windows, `liczba`/`odleglosc`/`okres`, `y`/`x`) must be there — decoding is
  into pointers, so a MISSING element is distinguishable from a legitimate 0;
- a document that fails those checks returns an error (`the answer carries no
  <operation>/return payload …`), which makes the pass fail, which in turn
  skips the reconciliation — no cancellation, and the poll reports the source
  as degraded;
- a COMPLETE envelope whose window is merely unreadable is a different thing:
  that phenomenon is rejected by name (`Rejected`) and drops out of the alert
  set, while the rest of the answer is used.

## Severity mapping

Degrees map onto the canonical scale (`1 → moderate`, `2 → severe`,
`3 → extreme`, configurable), and every phenomenon has a **floor**:

| phenomenon | XML field | default floor |
|---|---|---|
| trąba powietrzna (funnel) | `traba` | severe |
| burza (thunderstorm) | `burza` | severe |
| silny wiatr (strong wind) | `wiatr` | severe |
| intensywne opady (rain) | `opad` | minor (the degree scale decides) |
| upał (heat) | `upal` | minor |
| mróz (frost) | `mroz` | minor |

The floor only ever *raises*: a thunderstorm warning is severe at the first
degree and extreme at the third. The wind family is the one invariant an
operator cannot lower — `storm_min_severity` (and its siblings) accept
`severe` or `extreme`, and the loader refuses anything below, so "a storm is
never routed as a minor event" holds whatever the configuration says.

The lightning nowcast escalates with proximity to the nearest discharge:

| nearest discharge | default severity |
|---|---|
| farther than `storm_near_km` (10 km) | `moderate` |
| ≤ `storm_near_km` | `severe` |
| ≤ `storm_direct_km` (3 km) | `extreme` |

## Event identity and lifecycle

- Warnings: one event per **locality + phenomenon + warning window**
  (`niepolomice:storm:202610131400`). A re-issued degree updates the same
  event; a shifted forecast window is a new event and the old one is
  cancelled by the next poll.
- Lightning: one event per **locality** while a storm lasts, updated as the
  discharges move closer or fade. The validity is pinned to a `storm_ttl`
  bucket and re-published at most once per `storm_update_interval`, so a
  drifting count does not re-notify on every poll; the first quiet poll
  cancels it, and the validity retires it if the station stops.
- Reconciliation reads the durable current state
  (`SourceActiveEventReader`), so it is restart-safe and skipped whenever a
  locality query failed — an incomplete snapshot must never look like "the
  provider withdrew everything".

Every event carries the locality name in its headline, its areas and its
description, plus the provider's coordinates. The instruction section stays
empty: the provider publishes no recommendation, and the station does not
invent one.

## Requests

One `ostrzezenia_pogodowe` and one `szukaj_burzy` per locality per poll; the
directory is resolved once and cached for `locality_refresh` (default 24h).
`poll_interval` therefore *is* the request budget — keep it at or below 15
minutes, because the provider's storm window is 10–20 minutes (a longer
interval could miss a window entirely).

## Configuration

```yaml
sources:
  - id: burze-main
    type: burzedzis
    enabled: true
    config:
      api_key: "…"            # required (api_key_file for a Docker secret)
      locations: ["Niepołomice", "Kraków"]
      poll_interval: 5m
      storm_tracking: true
```

`locations` is edited on the Config page ("Storm localities"); it is read at
startup, so a change applies after a restart. The list is limited to 50
localities per instance and 64 bytes per name, and the loader refuses a list
outside those bounds — so the panel can never write something the station
would refuse to start with.

## Tests

`internal/burzedzis` covers the wire protocol (request shape, the `0/0`
guard, the SOAP fault, the snapshot decode, the provider's own test-data
shapes). The plugin tests cover the severity priorities, the configuration
bounds, the event identity, the dedup of unchanged detections, the
withdrawal reconciliation and the incomplete-snapshot guard.
