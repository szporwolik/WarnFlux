# adsb — aircraft traffic around the station

The ADS-B plugin shows what is flying in the station's area of interest. It is
an **informational** source: it never produces hazard events and never enters
the routing matrix. What it publishes is a map layer — the home page draws the
current traffic, including a short trail per aircraft.

- kind: `aircraft` (retained MQTT information document, `docs/weather-schema.md`
  documents the sibling weather kinds; the aircraft payload is described below)
- one document per poll, replacing the previous one: `type`, `schema_version`,
  `generated_at`, `provider`, `center_latitude`/`center_longitude`,
  `radius_km` and `aircraft[]`
- each aircraft: `icao24`, `callsign`, `category`, position, `altitude_m`,
  `on_ground`, `speed_kmh`, `track_deg`, `vertical_rate_m_s`, `seen_at` and an
  optional `trail[]` (position + timestamp, 3–15 minutes back)
- a raw position older than `track_window` drops out of the trail, and an
  aircraft that stops reporting disappears with the next document

## Providers

| `provider` | Endpoint | Notes |
|---|---|---|
| `adsblol` (default) | `https://api.adsb.lol` | Keyless community API. **Quota: 500 requests per 86400 s** for anonymous clients, so the default 5-minute poll (~288/day) is the ceiling worth using — a 30 s poll (2880/day) burns the daily budget in hours and then collects 429s until the window resets. |
| `tar1090` | `base_url` (e.g. `http://10.0.0.20:8080`) | A local readsb/tar1090 receiver on the LAN. No internet needed, so this is the off-grid option. |

A 429 is honored, and there are three sources of the wait, longest wins:

- the advertised `Retry-After` window;
- the plugin's own backoff: the poll interval doubles with every consecutive
  refusal (up to 30 minutes) and resets on the first success;
- `x-ratelimit-reset` — the seconds until the provider's quota window resets.
  The public endpoint answers a 429 with the *daily* window (up to 86400 s) and
  no `Retry-After` at all; the plugin waits for that reset, capped at one hour
  so a recovered quota is noticed the same hour instead of the next day.

Without the last two the station poked a blocked provider at its normal
cadence: one production outage collected 77 pointless requests over 6 hours.

## Area scope

The aircraft list covers the same "position + radius" as the rest of the
system: `aprs.latitude`/`longitude` (or the gridsquare centre) and
`aprs.radius_km`, overridable per instance with `latitude`/`longitude` and
`radius_km` (1..500 km). The APRS hub is therefore required — it defines the
area.

## Configuration

```yaml
sources:
  - id: adsb-main
    type: adsb
    enabled: true
    config:
      provider: adsblol        # or: tar1090 (+ base_url)
      poll_interval: 5m        # 5s..5m (default 5m — the provider allows 500 req/day)
      request_timeout: 15s     # <=1m
      track_window: 5m         # 1m..15m — how long a trail is kept
      # base_url: "http://10.0.0.20:8080"
      # latitude: 50.02
      # longitude: 20.13
      # radius_km: 60          # default: aprs.radius_km
```

The area centre comes from the APRS hub, so the aircraft layer always covers
exactly the area the station claims to watch — one knob to move, not two.

## An outage is one log entry, not one per poll

A provider host that simply stops answering (a total `adsb.lol` outage
recorded on the production station) failed every poll, and each failure was
a WARN — 48 an hour, which buried every other line of the log and made one
outage look like a dozen separate faults. The episode is now reported once:

- the first failed poll logs `adsb poll failed` (WARN) with the error and
  the retry delay;
- the following failed polls are counted and logged at DEBUG
  (`adsb poll still failing`, with `failed_attempts`);
- the first successful poll afterwards logs `adsb poll recovered` with
  `failed_attempts` and the outage duration.

The source health report is unaffected: it is marked degraded on every failed
poll and healthy again on the first success.

## Why the default request timeout is 30 seconds

`adsblol` answers from a distant edge, and a cold connection (DNS + TCP +
TLS + HTTP/2 setup) measured up to 8 seconds here — the old 15-second budget
left no margin and turned a slow network into a "source degraded" report.
The timeout is a safety net, not a latency budget. A 429 is a different
matter: it is honored explicitly, by backing the next poll off.
