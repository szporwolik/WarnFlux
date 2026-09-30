# Delivery and durability guarantees

WarnFlux moves every alert through three stages with different durability
semantics. This document states exactly what survives a crash or a broker
outage at each stage — and what does not.

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

**Guarantee: durable acceptance; at-least-once evaluation; atomic
hand-off to delivery.**

Every canonical event accepted from a receiver (MQTT or HTTP ingest) is
persisted to the dispatch inbox (`dispatch_inbox`) **before** it enters
the live queue. The inbox row is consumed **only** by
`CommitInboxDelivery`, which persists every delivery job the evaluation
produced and deletes the inbox row **in one transaction** — either all
jobs exist durably and the row is gone, or neither happened.

- A full live queue defers the event to inbox recovery instead of dropping
  it; a crash between acceptance and evaluation re-delivers the event
  after the restart (recovery runs on the refresh tick).
- If the inbox write itself fails, acceptance falls back to the live queue
  only (the event is not durable).
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
