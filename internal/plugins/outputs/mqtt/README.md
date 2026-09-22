# MQTT output plugin

The single built-in MQTT output: WarnFlux's only MQTT connection
implementation. Sources and informational plugins publish through it; no
other plugin creates its own MQTT client.

## Topics

| Topic | Retained | QoS | Content |
|-------|----------|-----|---------|
| `<prefix>/events` | no | configured `qos` (1 or 2; 0 rejected) | one JSON message per durable hazard change |
| `<prefix>/status` | yes | configured `qos` | application health snapshot |
| `<prefix>/info/<source>/<producer_id>/<key>/<kind>` | yes | configured `qos` | latest-state informational messages |

The `<prefix>/events` topic is HazardEvent-only; information is never
published there.

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
