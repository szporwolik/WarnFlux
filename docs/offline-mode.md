# Offline mode — off-grid operation and the internet audit

WarnFlux can run a station fully off-grid. The **offline-mode switch**
(admin Config page, `/config`) suspends everything internet-backed and
switches the maps to a local tile tree. The switch takes effect
**immediately**; `web.offline_mode` in the config sets the startup state.

## What the switch does

| Area | Offline behavior |
|---|---|
| Internet sources | Suspended (`imgw`, `openmeteo`, `rso`, `gios`, `giosaq`, `gddkia`, `metar`, `adsb`, `aprs-inet`). Their runs are cancelled and no restart is scheduled until the station goes online again. Health page shows them as amber `SUSPENDED`. |
| Internet actions | Held, not executed: `smtp`, `http_webhook`, `discord` workers wait with their queued requests (durable jobs stay `saved`, attempts are not spent). On going online, everything delivers. |
| Password reset | Self-service email reset is unavailable ("contact an administrator") — no email leaves the station. |
| Maps | The browser uses the **bundled Leaflet** (no unpkg) and the station's own tile tree (`/tiles/{z}/{x}/{y}.jpg`). MapLibre (vector styles) and the RainViewer radar are internet features and are skipped. |
| Local functions | **Untouched**: SQLite storage, the local MQTT broker, APRS radio (KISS/TNC), MeshCore (serial), the EMCOM panel, local radio routing, the archive, the admin UI. |

## Local map tiles

- `web.tiles_dir` points at a Leaflet raster tree: `{z}/{x}/{y}.jpg`
  (the `build/WF_Map` bundle is an example). The station serves it under
  `/tiles/`.
- Docker: mount the tree read-only into the container and set
  `tiles_dir` accordingly, e.g.
  `-v ./WF_Map:/data/tiles:ro` with `tiles_dir: /data/tiles`.
  Every operator can drop in their own tiles without rebuilding the image.
- Empty `tiles_dir` → maps work only online.

## Internet audit (where WarnFlux touches the network)

Everything listed here is **server-side** and covered by the switch,
except where noted. The browser only ever fetches from the station
itself plus the map providers listed under *Frontend*.

### Sources (suspended offline)

| Plugin | Remote host | Notes |
|---|---|---|
| `imgw` | `danepubliczne.imgw.pl` | IMGW warnings |
| `openmeteo` | `api.open-meteo.com` (customer endpoint with API key) | Forecast + air quality |
| `rso` | `komunikaty.tvp.pl` | Regional alerts XML |
| `gios` | `dane.gios.gov.pl`, geocoders `services.gugik.gov.pl`, `nominatim.openstreetmap.org` | Serious-incident feed |
| `giosaq` | `api.gios.gov.pl` | Air-quality stations |
| `gddkia` | `archiwum.gddkia.gov.pl` | Winter road conditions |
| `metar` | `aviationweather.gov` | METAR |
| `adsb` | `api.adsb.lol` or the operator's LAN Tar1090 | Aircraft |
| `aprs-inet` | `rotate.aprs2.net:14580` | APRS-IS feed (TX fails over to the radio when suspended) |

### Actions (held offline)

| Action | Remote | Notes |
|---|---|---|
| `smtp` | Configured SMTP host | Also backs the password-reset mailer |
| `http_webhook` | Configured webhook URL | Generic JSON POST |
| `discord` | Discord webhook URL | One POST per event |

### Local-only (never suspended)

`aprs-radio` (KISS/TNC), `meshcore` (serial), the MQTT receiver/output
(the broker is operator-configured — keep it local), `logger`,
`aprs`/`aprsout`/`meshcore` actions, HTTP ingest (local broker).

### Frontend (the browser)

- **Leaflet** is bundled in the binary (`/static/leaflet/`, BSD-2) —
  no unpkg request ever, online or offline.
- **Online only**: OpenFreeMap vector styles via MapLibre GL (jsdelivr
  CDN), raster tiles from OpenStreetMap/Esri, RainViewer radar. None of
  these are requested while the switch is on.
- **Offline**: `/tiles/` from the station's own tree.
- Everything else (fonts, weather icons, APRS symbol sheets, Bootstrap
  icons, all `/api/*` and `/partials/*` calls) is local.

### Explicitly absent

No NTP client, no update checks, no telemetry, no external config
fetches, no fonts/CDN imports in CSS. Deep links in notifications and
reset emails are *links*, not requests made by the server.

## Caveats

- A runtime toggle does not rewrite the config file: set
  `web.offline_mode: true` to boot offline after a restart.
- The MQTT broker is assumed to be local; a public broker configured in
  `dispatch.mqtt_receivers`/`outputs` is an internet path the switch
  does not (and cannot) police.
