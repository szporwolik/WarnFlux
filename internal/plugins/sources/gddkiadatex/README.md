# gddkia-datex — GDDKiA KPD (DATEX II snapshot pull)

Hazard source for the national road-obstruction dataset published by
**GDDKiA** through the **KPD platform** (Krajowa Platforma Danych). The station
pulls the complete current set with one signed SOAP call and turns every
obstruction into a WarnFlux event with coordinates, validity window and a stable
identity.

This is a different provider from the retired `gddkia` XML feed
(`utrdane.xml`): different identities, a different source namespace
(`gddkia-datex`), and both may run side by side during a migration.

## Registration and credentials

Access requires registration with GDDKiA. They issue a **client certificate**
(a `.p12`) per integrator, and it is a **secret** — whoever holds it can pull the
data in the station's name:

* it lives in the configuration volume (`/config` in the container,
  `/srv/docker/warnflux/config` on the host), mode `0600`, owned by the panel's
  user;
* `.gitignore` rejects `*.p12`, `*.pfx`, `*.pem`, `*.key`, `*.pass` and
  `/secrets/`, so a stray copy anywhere in the tree cannot be committed. This is
  a safety net, not a licence to keep it in the working copy;
* the password goes to its own file (`cert_password_file`) rather than inline, so
  copying `config.yaml` for support does not copy the secret;
* an expired certificate is reported at start-up (one line), not as a failed
  pull every interval;
* rotating it means: drop the new `.p12` in, update the password file, restart
  the station.

Configuration:

```yaml
sources:
  - id: gddkia-datex
    type: gddkia-datex
    enabled: true
    config:
      poll_interval: 15m
      request_timeout: 90s          # the national snapshot is ~2 MB
      endpoint: https://kpd.gddkia.gov.pl/addiod/ws_v34/snapshotPull
      cert_file: /config/kpd-client.p12
      cert_password_file: /config/kpd-client.pass
      # cert_key_file: /config/kpd-client.key   # PEM pair instead of PKCS#12
      # radius_km / center_latitude / center_longitude override the area
```

Without `aprs` (the hub supplies the area center and radius) the plugin refuses
to start: a national feed without a geographic scope would ingest the whole
country.

## The protocol, and why the code looks the way it does

The service speaks DATEX II over SOAP and authenticates **only** through a
WS-Security signature in the SOAP header:

* the certificate travels as a `BinarySecurityToken` (`wsu:Id="BinaryToken1"`);
* the `Body` element is signed (RSA-SHA256, exclusive C14N) and referenced by
  `URI="#_1"`, its `Id` living in the `wsu` namespace;
* `KeyInfo` carries a `SecurityTokenReference` back to that token.

A request without it is rejected with `No WS-Security header found`; a signature
that does not match the canonical form the server computes is rejected with
`The signature or decryption was invalid`. Two details are load-bearing and were
each a bug during development:

1. **The reference URI must not be empty.** The `Id` attribute is `u:Id`, so the
   signer has to be told that name — otherwise it emits `URI=""` and the server
   rejects the message.
2. **The namespaces the Body uses must be declared on the Body itself.**
   Exclusive canonicalisation of a subtree needs them locally, and the service
   hashes the same subtree.

Both are pinned by `signer_test.go`.

## What is mapped

| DATEX | WarnFlux |
|---|---|
| `situationRecord/@id` | `SourceID` — stable across polls, so one obstruction keeps one message number |
| `xsi:type` | `Event` (`MaintenanceWorks` → "Road works", …) |
| `validity/validityStatus` | records that are not `active` are skipped |
| `validityTimeSpecification` | `EffectiveAt` / `ExpiresAt` |
| `generalPublicComment` (Polish value) | `Description` |
| `locationReference/pointByCoordinates` | `Latitude` / `Longitude` |
| `openlrPointLocationReference` (first + last reference point) | `Path` — the stretch the map draws as a line |
| `pointAlongLinearElement/linearElement/roadNumber` | `Headline` suffix and the `droga:<nr>` area token |

Severity: DATEX `severity` wins when the provider sends one (`high` → severe,
`highest` → extreme). The national obstruction profile sends **none** — verified
against the live response, 519 records with zero `<severity>` elements — so the
record type decides:

| Record type | Severity | Why |
|---|---|---|
| `Accident`, `VehicleFire`, `WrongWayDriving`, `InfrastructureDamageObstruction` | **severe** | an incident in progress on the carriageway |
| `GeneralObstruction`, `Obstruction`, `VehicleObstruction`, `AbnormalTraffic`, weather/road conditions | **moderate** | part of the road is blocked |
| `MaintenanceWorks`, lane/speed/rerouting management | **minor** | planned works with warning signs |

Anything unrecognised is `moderate`. The table is one map in `adapter.go`
(`typeSeverity`) — a station that disagrees edits it rather than having the
plugin guess from the free-text comment.

The one exception is a **planned total closure**: the provider has no field for
it, but the comment says so ("całkowite zamknięcie", "droga zamknięta"). Such a
record is raised from minor to moderate. The phrase list is explicit, in
`closurePhrases`, so the behaviour is predictable and testable.

Records without point geometry cannot be placed in the area and are counted as
filtered, not as a provider failure.

## Stretches (OpenLR)

Most records describe a **stretch**, not a pin: KPD localises them with OpenLR,
which carries the first and the last reference point of the segment. The plugin
keeps both (`Path`, at most two points) and the map draws the straight line
between them — enough to see which part of the road is affected, without
inventing intermediate geometry. In the live national publication 436 of 525
records carry such a pair.

Geometry is deliberately **not** part of the content fingerprint: when the
provider shifts a stretch (or a row was stored before geometry existed), the
store updates the line silently on the next pull, with no journal entry and
therefore no re-notification. An incoming record without OpenLR never wipes
stored geometry.

## Behaviour

* every pull is a **complete snapshot**, so events that disappear are reconciled
  (withdrawn) by `snapshotutil` — a withdrawal publishes the removal, it does not
  silently drop the alert;
* a provider outage is logged as **one episode** (first failure warns, retries go
  to debug, recovery reports the tally), so a retired endpoint cannot produce
  ninety warnings a day;
* the certificate is validated at start-up: an expired one is refused there
  instead of failing every pull;
* the source is marked internet-backed, so the offline-mode switch suspends it.

## Limitations

* **Poland only.** The feed covers Polish national roads.
* **Local time without an offset.** KPD writes timestamps as
  `2026-06-18T11:00:00` in Polish local time; the plugin parses them in the
  station's zone.
* **Snapshot only.** The pull model is what GDDKiA recommends for recovery and
  periodic refresh; the push-on-occurrence mode is not implemented (it would need
  an inbound endpoint).
* **No delta semantics.** A changed record arrives as a new version of the same
  id; the event keeps its identity and the panel shows the update time.
