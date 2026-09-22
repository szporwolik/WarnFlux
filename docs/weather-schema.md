# Weather information schema v1

This document is the public contract for the **provider-neutral weather
information** published by WarnFlux sources on MQTT information topics.
Every weather provider (Open-Meteo today; MET Norway, IMGW, WeatherAPI, …
later) must produce exactly this schema, so consumers parse one JSON format
regardless of the data source.

> Weather information is **not** a HazardEvent: it is latest-state
> informational data, never stored in SQLite, never journaled and never
> delivered through the `/events` topic.

## Compatibility policy

- `schema_version = 1`.
- Existing field meanings never change.
- New optional fields may be added at any time.
- Enum values may only be added compatibly (never removed or redefined).
- Breaking changes require a `schema_version` bump.
- The schema is versioned by WarnFlux, **not** by provider.

## Topic

```text
<topic_prefix>/info/<provider>/<producer_id>/<location_id>/weather
```

| Segment | Meaning | Example |
|---------|---------|---------|
| `provider` | provider type slug | `openmeteo` |
| `producer_id` | configured source plugin instance ID | `weather-home` |
| `location_id` | configured location ID within the producer | `home` |

Retained: `true`. QoS: the MQTT output's configured QoS. Every successful
poll replaces the retained message.

Consumers can subscribe to `warnflux/info/+/+/home/weather` and parse
all weather providers identically.

## Units (fixed)

| Quantity | Unit |
|----------|------|
| temperature | Celsius |
| wind speed | km/h |
| precipitation | mm |
| snowfall | cm |
| pressure | hPa |
| humidity, cloud cover, probability | % (0..100) |
| direction | degrees (0..360) |
| timestamps | RFC3339 |

`generated_at` and `valid_until` are always UTC on the wire: the canonical
serializer normalizes any input time zone to UTC, so adapters may pass any
`time.Time` zone. Provider forecast timestamps (`current.time`,
`hourly[].time`, `sunrise`, `sunset`) are offset-aware RFC3339 and keep
their location offset (they are NOT converted to UTC); `daily[].date` is
`YYYY-MM-DD`.

## Freshness

`generated_at` is when WarnFlux built the snapshot.
`valid_until` (optional) is application freshness metadata
(`generated_at + 2 × poll_interval` for Open-Meteo) — consumers should use
it to reject stale retained data. It is **not** a provider forecast
validity guarantee. A location that is removed from configuration leaves
its retained message behind; consumers must rely on `valid_until` (there is
no automatic retained-topic cleanup).

## Condition enum

Stable WarnFlux condition values (all providers):

```text
clear
mainly_clear
partly_cloudy
overcast
fog
drizzle
freezing_drizzle
rain
freezing_rain
snow
snow_grains
showers
snow_showers
thunderstorm
thunderstorm_hail
unknown
```

Providers map their native codes (e.g. Open-Meteo WMO codes) into this
enum inside their adapter; a provider that cannot classify a condition
uses `unknown` (consumers never need provider-specific handling). The raw
provider code is retained as optional metadata in
`provider_condition_code` — absent (`null`) when the provider has no raw
code, and never an empty string.

## Payload structure

```json
{
  "schema_version": 1,
  "type": "weather",
  "generated_at": "2026-09-22T12:00:00Z",
  "valid_until": "2026-09-22T12:30:00Z",

  "provider": {
    "id": "openmeteo",
    "name": "Open-Meteo",
    "attribution": "Weather data by Open-Meteo"
  },

  "location": {
    "id": "home",
    "name": "Home",
    "latitude": 50.0000,
    "longitude": 20.0000,
    "timezone": "Europe/Warsaw",
    "elevation_m": 250
  },

  "current": {
    "time": "2026-09-22T14:00:00+02:00",
    "temperature_c": 18.2,
    "apparent_temperature_c": 17.5,
    "relative_humidity_pct": 71,
    "condition": "partly_cloudy",
    "provider_condition_code": "2",
    "precipitation_mm": 0.0,
    "rain_mm": 0.0,
    "showers_mm": 0.0,
    "snowfall_cm": 0.0,
    "cloud_cover_pct": 42,
    "pressure_msl_hpa": 1017.4,
    "surface_pressure_hpa": 986.2,
    "wind_speed_kmh": 12.1,
    "wind_direction_deg": 245,
    "wind_gusts_kmh": 21.0,
    "is_day": true
  },

  "hourly": [
    {
      "time": "2026-09-22T15:00:00+02:00",
      "temperature_c": 18.5,
      "condition": "partly_cloudy",
      "provider_condition_code": "2",
      "precipitation_probability_pct": 10
    }
  ],

  "daily": [
    {
      "date": "2026-09-22",
      "condition": "partly_cloudy",
      "provider_condition_code": "2",
      "temperature_max_c": 20.4,
      "temperature_min_c": 10.2
    }
  ]
}
```

### Field semantics

- **Optional measurements are omitted when unavailable** (`omitempty`).
  A missing field means "unavailable", never zero: `0.0` precipitation is
  real measured zero.
- `location.latitude` / `location.longitude` are the **configured**
  requested coordinates (the identity of the location), not
  provider-adjusted coordinates.
- `location.timezone` is the provider-resolved IANA timezone; canonical
  validation rejects values that are not loadable timezones.
- `location.elevation_m` is the provider-reported elevation (the
  coordinates are the configured ones; the elevation is provider data).
- `provider.attribution` is the provider's required attribution text; it
  is data provenance, not WarnFlux branding.
- `hourly` is strictly ascending by time; `daily` strictly ascending by
  real calendar date (`time.Parse("2006-01-02")` — `2026-02-30` or
  `banana` are rejected); duplicates are rejected.
- A snapshot must contain at least one of `current`, `hourly` or `daily`
  (observation-only and forecast-only providers are supported).

## Canonical bounds

The canonical model is bounded **before** serialization, so a buggy
adapter cannot build a giant snapshot (the 256 KiB information payload cap
is the last resort, not the only gate). Canonical validation rejects:

| Limit | Value |
|-------|-------|
| `hourly` entries | max 1000 |
| `daily` entries | max 366 |
| `provider.name` | max 256 bytes |
| `provider.attribution` | max 2048 bytes |
| `location.name` | max 512 bytes |
| `location.timezone` | max 128 bytes |
| `provider_condition_code` | max 128 bytes |

These limits are deliberately far above normal operational use
(Open-Meteo normalizes 48 hourly / 7 daily entries) so future providers
are not artificially restricted. All public-model text must be valid
UTF-8; oversized values are **rejected, never truncated** — canonical
provider data is not silently modified.

## Stability of the pipeline

Provider adapters produce the internal `core.WeatherSnapshot` model and
call the single canonical serializer (`core.MarshalWeatherSnapshot` /
`core.NewWeatherInformation`). Providers must never define their own
weather wire DTOs. Adding a provider means: provider client → provider
model → adapter (`Normalize`) → canonical model → serializer. The MQTT
schema does not change.
