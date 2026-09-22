# MQTT output plugin

The single built-in MQTT output: WarnFlux's only MQTT connection
implementation. Sources and informational plugins publish through it; no
other plugin creates its own MQTT client.

## Topics

| Topic | Retained | QoS | Content |
|-------|----------|-----|---------|
| `<prefix>/events` | no | configured `qos` (1 or 2; 0 rejected) | one JSON message per durable hazard change |
| `<prefix>/active/<source>/<sha256(event_key)>` | yes | configured `qos` | CURRENT active hazards (materialized view of SQLite state) |
| `<prefix>/status` | yes | configured `qos` | application health snapshot |
| `<prefix>/info/<source>/<producer_id>/<key>/<kind>` | yes | configured `qos` | latest-state informational messages |

The `<prefix>/events` topic is HazardEvent-only; information is never
published there.

## Active hazards (`/active/#`)

The retained `/active/...` view is the materialized MQTT copy of the
**authoritative SQLite current hazard state**: a new client subscribing to
`warnflux/active/#` immediately receives every currently active
hazard, no provider update required. It is reconstructed at output startup
from SQLite and republished automatically after every broker (re)connect,
so it survives both a broker restart with lost retained state and a
WarnFlux restart with an empty broker.

- The final topic level is the lowercase hex SHA-256 of the logical
  `event_key` (64 characters) — raw keys are never placed in topics. The
  full key stays inside the payload.
- Active events are published with the canonical `active_hazard` payload
  (schema version 1, type `active_hazard`, the same hazard event wire
  block as `/events`).
- Cancelled/expired events delete the retained topic (zero-length retained
  payload), so late subscribers never see them as active; the transition
  remains available on `/events`.
- Delivery ordering per journal change: `/events` first, then the active
  view; the journal is acknowledged only after BOTH succeed. A retry may
  duplicate `/events` (at-least-once) but never silently diverge the
  active view.
- The desired active cache is in-memory only (currently active hazards,
  never history) and exists solely to republish after reconnects; there is
  no MQTT-state database.
- On graceful shutdown the active retained topics are NOT deleted: they
  describe provider hazard state, not process liveness. Consumers combine
  them with `<prefix>/status` (`state: offline`) and `expires_at`.

## Client usage

```text
Realtime hazard transitions:   SUB warnflux/events
Current active hazards:        SUB warnflux/active/#
Weather / information state:   SUB warnflux/info/#
Service status:                SUB warnflux/status
```

`/events` is the ordered change stream (new/updated/cancelled/expired);
`/active/#` is the current active hazard set.

## Delivery semantics

- **Hazard events**: durable at-least-once via the SQLite journal — a
  change is acknowledged only after MQTT confirms the publish (PUBACK for
  QoS ≥ 1), and unacknowledged changes are redelivered after restarts.
- **Status**: auxiliary heartbeat; a status callback that violates its
  timeout is disabled for the output's lifetime.
- **Information**: auxiliary latest-state best-effort. Failures are logged
  and never affect hazard delivery, failure accounting or cursors. A
  contract-violating information callback is disabled for the output's
  lifetime; the worker returns to hazard delivery immediately (the
  abandoned call can never block hazards).
- **Stale retained information**: information topics are retained so late
  subscribers receive the latest snapshot, but there is no automatic
  retained-topic cleanup. If a source stops publishing (removed
  configuration, outage), the last retained value stays on the broker —
  consumers should check `valid_until` and treat expired retained values
  as stale.

## Configuration

- `broker` (required), `client_id` (required; unique per instance — two
  clients sharing an ID kick each other off the broker).
- `topic_prefix` (default `warnflux`).
- `qos` (default 1; 0 rejected).
- `username` / `password` / `password_file` (mutually exclusive password
  sources; the file is bounded to 64 KiB and never logged).
- `heartbeat_interval` (status heartbeat; 0 disables).

TLS: use an `ssl://` broker URL; Paho's TLS defaults apply. Reconnection is
automatic with a bounded backoff (max 30 s) so pending hazards flush
promptly after the broker returns. On graceful shutdown the output
publishes a retained `state:"offline"` status before disconnecting; on
abnormal termination the configured last will provides it.
