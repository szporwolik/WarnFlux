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
| `adsblol` (default) | `https://api.adsb.lol` | Keyless community API. Rate-limited for anonymous clients: keep `poll_interval` polite and expect the occasional 429. |
| `tar1090` | `base_url` (e.g. `http://10.0.0.20:8080`) | A local readsb/tar1090 receiver on the LAN. No internet needed, so this is the off-grid option. |

A 429 is honored: the plugin backs the next poll off by the advertised
`Retry-After` window (or its own backoff, whichever is longer) instead of
hammering the provider.

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
      poll_interval: 30s       # 5s..5m (default 30s)
      request_timeout: 15s     # <=1m
      track_window: 5m         # 1m..15m — how long a trail is kept
      # base_url: "http://10.0.0.20:8080"
      # latitude: 50.02
      # longitude: 20.13
      # radius_km: 60          # default: aprs.radius_km
```

The area centre comes from the APRS hub, so the aircraft layer always covers
exactly the area the station claims to watch — one knob to move, not two.

## Why the default request timeout is 30 seconds

`adsblol` answers from a distant edge, and a cold connection (DNS + TCP +
TLS + HTTP/2 setup) measured up to 8 seconds here — the old 15-second budget
left no margin and turned a slow network into a "source degraded" report.
The timeout is a safety net, not a latency budget. A 429 is a different
matter: it is honored explicitly, by backing the next poll off.
