# Delivery and durability guarantees

WarnFlux moves every alert through three stages with different durability
semantics. This document states exactly what survives a crash or a broker
outage at each stage — and what does not.

## 0. Offline architecture (SQLite + radio suffice)

The architectural invariant: **local SQLite and the local radio must
suffice to accept, store, serve and transmit a communication. Internet
and the MQTT broker EXTEND the station (remote instances, public feeds,
WAN media); their outage must never interrupt the local path.**

Concretely:

- **Accept** — a receiver, the panel or a radio frame persists the
  canonical event in SQLite before anything else (dispatch inbox rows,
  `compose_hazards`, `emcom_networks`, the events table).
- **Route** — every producer dispatches directly into the LOCAL ingress:
  source ingests and expirations through the ingest dispatch sink
  (`EventFromChange`, stamped with the publisher UUID and journal change
  ID), panel communications and radio events through their local-first
  paths. The broker loopback is only the asynchronous sync copy; the
  identical dedup identity collapses the pair.
- **Transmit** — action workers claim delivery jobs from SQLite and talk
  to the radio (MeshCore serial, APRS KISS) directly. WAN media (SMTP,
  Discord, APRS-IS) are additional actions whose failure is isolated per
  action.
- **Serve** — the public home page, the map endpoint and the admin
  warnings panel read the active view from the LOCAL database first; the
  MQTT mirror only fills documents the local record does not own (other
  instances' publications).
- **Sync** — the durable journal with per-output cursors, persistent
  receiver sessions and the resync hook reconcile the broker whenever it
  returns; missed documents are re-published (including tombstones), and
  received `/events` are redelivered by the broker's session queue.

Startup never requires the network: sources, outputs and receivers
connect lazily and failures stay isolated; `/healthz` and `/readyz`
report process and storage readiness only.

## 1. Output publishing (journal → MQTT)

**Guarantee: at-least-once per output.**

Every meaningful event transition is written to the durable SQLite journal
(`changes`) atomically with the event state. Each output plugin has a
cursor (`output_cursors`) advanced only after the plugin acknowledged the
delivery. After a restart, unacknowledged changes are re-delivered in
journal order.

- A crash between the MQTT publish and the cursor advance re-publishes the
  same change (at-least-once).
- The `/events` payloads carry the instance's persistent **publisher UUID**
  (`instance_meta`), so independent publishers never collide downstream.

## 2. Dispatch acceptance (wire → routing evaluation)

**Guarantee: explicit acceptance result; durable-first; at-least-once
evaluation; atomic hand-off to delivery.**

Every canonical event accepted from a receiver (MQTT or HTTP ingest) is
persisted to the dispatch inbox (`dispatch_inbox`) **before** it enters
the live queue. The inbox row is consumed **only** by
`CommitInboxDelivery`, which persists every delivery job the evaluation
produced and deletes the inbox row **in one transaction** — either all
jobs exist durably and the row is gone, or neither happened.

**Receiver recovery (the /events stream survives outages):**

- Receivers use a **persistent MQTT session** by default
  (`clean_session: false`): the broker keeps the subscriptions and queues
  QoS≥1 messages while the receiver is disconnected and redelivers them
  on the next (re)connect — the recovery protocol for the non-retained
  `/events` stream without replay bookkeeping.
- **Receipt is acknowledged only after the durable write**: the message
  handler ACKs a frame only when its events were accepted by the ingress
  (or deliberately consumed). An event REJECTED by a full intake is left
  unacknowledged, so the broker keeps it queued and redelivers it.
- Duplicates are expected and tolerated: the routing ledger deduplicates
  per (group, action, event, publisher/change ID) and the staleness gates
  suppress replayed alerts whose hazard already expired.

Acceptance is a **tri-state result**, never a silent fallback:

- **durable** — the inbox row exists before the live queue is offered. A
  full live queue defers the event to inbox recovery instead of dropping
  it; a crash between acceptance and evaluation re-delivers the event
  after the restart (recovery runs on the refresh tick).
- **emergency** — the inbox write failed (full or damaged card) or no
  inbox is attached: the event is delivered now but lives only in RAM.
  The result is **visible and auditable**, never green: the health page
  turns the dispatch queue row amber with an EMERGENCY badge and a
  running count, `/metrics` exposes
  `warnflux_dispatch_accepts_total{durability="emergency"}` and
  `warnflux_dispatch_inbox_failures_total`, and every receiver logs the
  degradation per event.
- **rejected** — neither the inbox nor the live queue took the event; it
  is lost and counted.

Each inbox write is bounded by `dispatch.inbox_write_timeout` (default
2 s): the intake never blocks the receiver callback longer than the
deadline — a stuck database degrades the event to emergency acceptance
instead of hanging MQTT ingestion.

**Auxiliary-data policy (a full card must not kill acceptance silently):**

- `dispatch.inbox_retention` (default 24 h) prunes unevaluated inbox rows
  older than the cutoff — stale alerts the staleness gates would suppress
  anyway. Below the `storage.min_free_mb` alarm threshold the cutoff
  shortens to 5 minutes, so auxiliary data stops competing with primary
  storage.
- The health page reports the free space on the database filesystem and
  turns the Database row red below the threshold; `/metrics` exposes
  `warnflux_storage_free_bytes` and `warnflux_inbox_backlog`.

- **Without a validly loaded routing snapshot the event stays pending**:
  an evaluation on an empty rule set is not a deliberate result, so the
  engine refuses to consume the row and recovery retries after the next
  successful rule load.
- A deliberate no-match (no cell matches, severity below every threshold,
  cancelled/expired transitions, non-routed kinds) consumes the inbox row
  with zero jobs — the result was intended.
- If the commit itself fails (ledger down), the engine falls back to the
  in-memory submission path and **keeps the inbox row pending**:
  recovery re-evaluates the event and persists the jobs once the ledger
  heals (at-least-once, duplicates possible).

**Local-first panel and radio (a down broker must not break local
delivery):**

Panel communications (compose), EMCOM readiness levels and routed
APRS/MeshCore messages are **saved to the local database first**
(`compose_hazards`, `emcom_networks` or the dispatch inbox row) and
**dispatched directly into the local ingress** — the radio TX and local
notifications work even when the broker is unreachable. The MQTT
documents are asynchronous, best-effort sync copies for other instances;
the resync hook republishes the current local state on every receiver
(re)connect, and deleted/expired entries publish tombstones so a stale
broker copy can never revive them.

## 3. Action completion (routing evaluation → notification)

**Guarantee: durable job queue; at-least-once per (group, action, event).**

Routing persists a full delivery job (`action_fires`, one row per
group × action × event × publisher) that carries the **execution payload**
(event + recipients), the **attempt counter** and the **next-attempt
deadline**. The action workers claim jobs from SQLite and record the
result **after** each execution — the in-memory queue is never the source
of truth:

- `saved` — the job is in the durable queue and WILL be executed (first
  execution or scheduled retry). Replays of the transition deduplicate:
  they see the job is pending and do not enqueue a second one.
- `running` — a worker claimed the job. If the process dies before
  settling, the stale-claim sweep re-queues the job after the claim lease
  (5 minutes) expires, so the alert still executes after a restart.
- `accepted` — the transport accepted the transmission (the device/relay
  queued or sent it). Terminal: replays deduplicate. This is the strongest
  stage most channels can prove.
- `confirmed` — the transport proved recipient-level confirmation (only
  channels whose protocol offers it, via the optional `ConfirmingPlugin`
  interface). Terminal: replays deduplicate.
- `failed` — an execution attempt failed. A transient failure schedules
  the next attempt at the retry deadline; once the attempt budget is
  spent the job is terminally failed, and a **replayed transition
  re-arms it** with a fresh budget instead of deduplicating it away.

A power loss between "job queued" and "transmitted" therefore retries on
restart, and an execution that burns all attempts is retried by the next
replay of the transition. Channel floods without delivery
acknowledgements (e.g. MeshCore group messages) stop at `accepted` — the
device took the message, the protocol has no stronger signal.

**Staleness policy (recovered alerts must not hit the radio):**

- At scheduling time the engine refuses to start the notification
  machine for a `new`/`updated` transition whose `ExpiresAt` already
  passed, and — when the store offers the freshness oracle
  (`HazardActive`) — for a hazard whose stored state is already
  cancelled/expired: an older update never outranks a known
  cancellation. The skip is deliberate (the inbox row is consumed); a
  failed oracle lookup never suppresses an alert.
- Directly before transmission the worker re-checks the payload's
  `ExpiresAt` and the same oracle. A job that aged out while it waited
  settles as `expired` — a terminal state that deduplicates replays —
  instead of reaching the device. The message-type policy is therefore:
  send while active, mark expired once the hazard is no longer
  worth notifying.

## What is NOT guaranteed

- The audit trail (`trail`) is not durable: it is diagnostics, never the
  source of truth. The in-memory queue survives only as the fallback path
  when the ledger itself fails (best-effort, undeduplicated).
- The retained active-view topics on the broker are best-effort mirrors;
  the journal is authoritative.
- Duplicates are possible at every stage (at-least-once): consumers must
  deduplicate by `publisher + source + event_key + change_id`.
- A crash **during** an action execution can transmit twice (the job may
  re-run after recovery): at-least-once, not exactly-once.
