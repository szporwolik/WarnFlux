# Open-Meteo informational source plugin

## Purpose

Periodically fetches ordinary weather data from
[Open-Meteo](https://open-meteo.com/) for configured geographical locations
and publishes the latest weather snapshot to a retained MQTT information
topic.

**This plugin publishes INFORMATION, not HazardEvents.** Weather data never
enters the SQLite hazard journal, never creates hazard lifecycles and never
reaches the `/events` topic. It is best-effort latest-state delivery: a
missing weather update is acceptable; a lost committed HazardEvent is not.

## Provider

[Open-Meteo](https://open-meteo.com/) — the free public weather forecast API
(`https://api.open-meteo.com/v1/forecast`). Commercial customers can use the
customer endpoint with an API key.

## Configuration

| Field | Default | Meaning |
|-------|---------|---------|
| `poll_interval` | `15m` | Time between fetches (1m..24h) |
| `request_timeout` | `10s` | Per-request HTTP timeout |
| `forecast_hours` | `48` | Hourly forecast length (1..168) |
| `forecast_days` | `7` | Daily forecast length (1..16) |
| `api_key` | `""` | Commercial API key (mutually exclusive with `api_key_file`) |
| `api_key_file` | `""` | Read the key from a file (≤64 KiB, line endings trimmed) |
| `base_url` | provider-selected | Explicit endpoint override (testing/custom deployments) |
| `locations` | required | One or more locations (`id`, `latitude`, `longitude`, optional `name`) |

Location IDs are lowercase slugs (`^[a-z0-9][a-z0-9._-]{0,63}$`) and become
MQTT topic segments. Latitude must be -90..90, longitude -180..180, both
finite. Duplicate IDs are rejected.

## Defaults

- Poll interval: **15 minutes** (immediate fetch on startup, then every interval).
- Forecast: **48 hourly hours + 7 daily days**.
- Up to 64 locations, fetched sequentially (one request per location).

## API key behavior

- No key configured → public endpoint `https://api.open-meteo.com/v1/forecast`.
- Key configured and no `base_url` → commercial endpoint
  `https://customer-api.open-meteo.com/v1/forecast`, with `apikey=<key>` on
  the provider request.
- Explicit `base_url` always wins.

The key is never logged and never appears in errors (provider URLs in
transport errors are redacted).

## Example YAML

```yaml
sources:
  - id: weather-home
    type: openmeteo
    enabled: true

    runtime:
      restart: true
      shutdown_timeout: 10s

    config:
      poll_interval: 15m
      request_timeout: 10s

      forecast_hours: 48
      forecast_days: 7

      # Free Open-Meteo API:
      api_key: ""
      api_key_file: ""

      locations:
        - id: home
          name: Home
          latitude: 50.0000
          longitude: 20.0000
```

## MQTT topic

One retained topic per configured location:

```text
<topic_prefix>/info/openmeteo/<producer_id>/<location_id>/weather
```

Example: `warnflux/info/openmeteo/weather-home/home/weather` (with the
source configured as `id: weather-home`).

Every successful poll **replaces** the retained message for that location.
The message is published with the MQTT output's configured QoS and
`retain=true`. Weather is never published to `<prefix>/events`.

## MQTT payload

The published JSON is the **canonical, provider-neutral weather schema v1**
documented in [`docs/weather-schema.md`](../../../../docs/weather-schema.md)
— units, condition enum, timestamp semantics, optional fields and the
compatibility policy all live there. Open-Meteo WMO weather codes are
mapped to the canonical condition enum by this plugin's adapter
(`adapter.go`); the raw code is preserved as
`provider_condition_code`.

`generated_at` is the WarnFlux processing time and `valid_until` is
`generated_at + 2 × poll_interval`; provider forecast timestamps
(`current.time`, `hourly[].time`, `daily[].date`, `sunrise`, `sunset`) come
from the provider, resolved in the provider-reported timezone.

## Current fields

`temperature_2m`, `relative_humidity_2m`, `apparent_temperature`, `is_day`,
`precipitation`, `rain`, `showers`, `snowfall`, `weather_code`,
`cloud_cover`, `pressure_msl`, `surface_pressure`, `wind_speed_10m`,
`wind_direction_10m`, `wind_gusts_10m`.

## Hourly fields

`temperature_2m`, `relative_humidity_2m`, `apparent_temperature`,
`precipitation_probability`, `precipitation`, `weather_code`, `cloud_cover`,
`pressure_msl`, `wind_speed_10m`, `wind_direction_10m`, `wind_gusts_10m`.

## Daily fields

`weather_code`, `temperature_2m_max`, `temperature_2m_min`,
`apparent_temperature_max`, `apparent_temperature_min`,
`precipitation_probability_max`, `precipitation_sum`, `wind_speed_10m_max`,
`wind_gusts_10m_max`, `wind_direction_10m_dominant`, `sunrise`, `sunset`.

## Polling behavior

The plugin fetches immediately on startup, then every `poll_interval`.
Locations are fetched sequentially; one failing location never suppresses
the others. Provider timestamps are parsed in the provider-resolved timezone
(`timezone=auto`); an invalid or missing timezone fails that location — no
malformed timestamps are ever published. Forecast arrays are length-checked
before indexing, and non-finite numeric values are rejected.

## Failure/retry behavior

Transient failures (network errors, DNS failures, HTTP 429/500/503,
malformed responses) are logged and skipped: the plugin keeps running and
retries on the next scheduled poll. The source supervisor is NOT restarted
for ordinary provider errors. `Retry-After` on 429/503 is honored, clamped
to at most 5 minutes. Configuration errors (invalid locations, bad slugs,
invalid ranges) fail at startup.

## Information vs HazardEvent semantics

- Weather snapshots travel through `EmitInformation` and the retained
  `/info/...` topics only.
- They are never stored in SQLite, never journaled, never cursor-tracked and
  never replayed.
- Information publish failures are logged and do not affect hazard delivery:
  no suspension, no failure counting, no cursor movement.

## Data attribution

Every payload carries `"attribution": "Weather data by Open-Meteo"`.

## Known limitations

- One HTTP request per location per poll (no batching).
- Latest state only: no weather history is stored.
- CAP-style polygons/geocodes are not represented; `areaDesc`-like mapping
  is out of scope for this plugin.
- Weather data is informational only — it is not an official warning
  authority and no hazards are derived from it.
