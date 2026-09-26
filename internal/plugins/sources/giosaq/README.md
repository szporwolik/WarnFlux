# giosaq — GIOŚ air-quality exceedances

Official Chief Inspectorate of Environmental Protection (GIOŚ) PJP API
source: active information about exceedances of the information, alarm,
admissible and target air-quality levels.

- Endpoint: `GET /v1/rest/levels/getInformationAboutExceeding` (2
  requests/minute — poll at least every 2 minutes, 5m default).
- Station directory: `GET /v1/rest/station/findAll?size=500` (one request
  covers the country; refreshed every 24h by default, provides coordinates
  so the home map can draw the alerts).
- The feed is a paginated history (newest first). The plugin reads only
  the first page and keeps records newer than `max_age` (24h default), so
  historical pages never re-surface as alerts.
- One record → one hazard event (category `environment`, source `giosaq`),
  severity mapped from the official norm type:

  | keyword | default severity |
  |---|---|
  | `alarmowy` (alarm level) | severe |
  | `informowania` (information level) | moderate |
  | `dopuszczalnego`/`docelowego` | minor |

- The affected zone (e.g. `PL1203 strefa małopolska`) becomes the event
  area; `zones` filters by case-insensitive substrings.
- Events carry stable identities (norm type + station + recorded hour), so
  re-polling updates instead of duplicating; there is no reconciliation
  (the feed is a history) — events retire through their own expiry.

The routing matrix (Groups page) offers `giosaq` as a source row, so the
official exceedance alerts can be forwarded per group, exactly like IMGW
and RSO.
