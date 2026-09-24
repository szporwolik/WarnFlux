# METAR source plugin

Polls current METAR aviation observations from the
[NOAA Aviation Weather Center](https://aviationweather.gov/api/data/metar)
and publishes each configured airport as a canonical **informational
weather snapshot** (`kind: weather`) on the retained MQTT information
topics. It does **not** generate `HazardEvent`s.

The feed carries every station's position (`lat`/`lon`), so airports
appear on the weather map automatically — no coordinates are configured.

## Configuration

```yaml
- id: metar-epkk
  type: metar
  enabled: true

  runtime:
    restart: true
    shutdown_timeout: 10s

  config:
    poll_interval: 15m
    request_timeout: 10s
    # base_url: "https://aviationweather.gov/api/data"   # tests only

    stations:
      - id: EPKK
        name: "Kraków Balice (EPKK)"   # display override, optional
        timezone: "Europe/Warsaw"      # IANA zone, optional (default: UTC)
```

`poll_interval` must be between `5m` and `24h`; `station.id` must be a
4-letter ICAO code.

## Normalization

- Temperature (°C) and dew point come from the feed; relative humidity is
  derived with the Magnus formula.
- Wind speed/gusts are converted from knots to km/h (`×1.852`); a gust of
  `0` means "not reported" and is omitted.
- Altimeter setting is reported in the station's local unit: values below
  100 are treated as inHg and converted to hPa (`×33.8638816`), larger
  values are already hPa.
- The canonical condition is derived from the weather phenomena in the raw
  report (`TS`, `FZRA`, `SHSN`, `SN`, `RA`, `DZ`, `FG`/`BR`, ...), falling
  back to the dominant cloud layer (`OVC`/`BKN` → overcast, `SCT` →
  partly_cloudy, `FEW` → mainly_clear, `CLR`/`SKC` → clear).
- Location ID is the lowercase ICAO code (canonical slug), e.g. `epkk`;
  the display name defaults to the provider's airport name.

## Topics

`<topic_prefix>/info/metar/<producer_id>/<station_id>/weather`
