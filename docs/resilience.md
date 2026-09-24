# Network resilience and offline (UPS) autonomy

WarnFlux is designed around one rule: **the station must keep serving its
area when the internet uplink is gone.** The APRS radio channel (KISS →
Direwolf) is the primary working network in that state; every internet-fed
source degrades independently and recovers on its own.

## Startup never depends on the internet

Everything network-reachable is constructed but not connected during
startup. The process boots with: SQLite (WAL, `synchronous=FULL`,
`busy_timeout=5000` — crash-safe across a UPS power loss), the APRS hub,
plugin/action registries, the web listener (bind failures are fatal — they
are local configuration errors), and the MQTT receivers in background
auto-reconnect mode. A failed initial broker connect is non-fatal; the
ingest HTTP endpoints answer 503 until paho reconnects.

## What keeps working offline

- **APRS radio (aprs-radio)**: local KISS link to Direwolf; RX feeds the
  hub (stations, messages, weather), TX carries outbound messages. The hub
  prefers radio for unknown-origin addressees and falls back across
  backends, so messages still flow while aprs-inet is disconnected.
- **Hub / dispatch / rules / actions**: the whole event pipeline from an RF
  message to an outbound notification is local (SQLite + in-memory).
- **Web UI**: served locally, including the dashboard, map, weather tab and
  the message/notification trails.
- **SQLite**: WAL journaling survives sudden power loss.

## What degrades (and how)

| Component | Behavior without internet |
|---|---|
| aprs-inet | reconnects with exponential backoff (2s → 2min); a session that ran ≥ 1 min resets the backoff so recovery after a single drop is fast |
| IMGW / GIOŚ / GDDKiA / RSO / Open-Meteo / METAR | poll fails → source marked degraded on the health page; snapshots are skipped, never half-applied (reconciliation only on complete data) |
| SMTP action | per-call deadline, bounded retries with backoff, then a clear trail entry (`/notifications`) |
| MQTT output | at-least-once journal: the worker suspends on repeated failures and replays from the journal cursor after recovery |
| ingest HTTP | 503 until the broker connection is back |

All HTTP clients carry explicit timeouts; all dials carry deadlines; plugin
crashes are isolated by the supervisor (bounded exponential backoff, max
1 min, panic recovery). One broken provider can never take the station
down.

## Outbound message funnel

Every human-readable outbound message (APRS tx, SMTP subject/body) passes
through `internal/sanity` before transmission — see the package docs there.
The future LLM reviewer plugs into the same chokepoint (`TODO(llm)`).
