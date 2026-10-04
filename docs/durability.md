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
  `compose_hazards`, `emcom_networks`, the events table). A source
  ingest commits its journal record and its dispatch inbox row **in one
  transaction**: a crash between them can never strand a journaled
  alert without a notification path — inbox recovery re-delivers it
  after the restart.
- **Route** — every producer dispatches directly into the LOCAL ingress:
  source ingests and expirations through the ingest dispatch sink
  (`EventFromChange`, stamped with the publisher UUID and journal change
  ID), panel communications and radio events through their local-first
  paths. The broker loopback is only the asynchronous sync copy; the
  identical dedup identity collapses the pair.
- **Transmit** — action workers claim delivery jobs from SQLite and talk
  to the radio (Meshtastic serial, APRS KISS) directly. WAN media (SMTP,
  Discord, APRS-IS) are additional actions whose failure is isolated per
  action. A Meshtastic command whose caller was cancelled is rejected
  before any I/O (nothing reaches the serial wire), and a write that is
  in flight when the cancellation lands is never abandoned mid-frame:
  it settles within a bounded grace or the session is recreated, so the
  next command can never interleave with a stray frame. A SHORT write
  is never success either: the remainder of the frame is written in a
  loop bounded by the transport write deadline, and any failed or
  partial frame recreates the session before the next command.
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

**Source pipeline (journal → local dispatch):**

- A source ingest or expiration writes its inbox row **in the same
  transaction as the journal record** (`Change.InboxID`). The live
  enqueue reuses that row (never writes a second one), so a crash
  between the commit and the dispatch leaves the row behind and inbox
  recovery evaluates it after the restart.
- A re-polled duplicate produces no journal change and therefore needs
  no repair: the first change is already durably accepted.

**HTTP ingest (local-first + durable outbox):**

- The endpoint accepts when the LOCAL pipeline accepted (durable inbox
  row) AND the intended broker publication is persisted in the durable
  outbox (`ingest_outbox`) — **the broker connection is never a
  precondition**: a disconnected broker answers 202 with both rows
  committed.
- A background outbox worker publishes the rows to `<prefix>/events`
  whenever the broker is connected and mirrors the retained active view;
  a row is deleted only after the broker confirmed the publish
  (at-least-once — a crash or a failure retries on the next tick, and
  rows survive restarts). The publish mask is applied at publish time: a
  fully masked category defers the row, never drops the local delivery.
- **Capacity never drops a still-valid alarm.** A long broker outage
  can fill the queue past its bound; capacity pressure may remove only
  *provably obsolete* rows — expired non-corrective rows (a corrective
  row's expiry does not prove the broker retired the earlier active
  document, so cancellations/expirations are protected until the ACK or
  a newer version of the same identity), or rows superseded by a newer
  version. When the backlog alone exceeds the bound, new appends fail
  with an explicit **503 and no partial write** (`ErrOutboxFull`), and
  the operator gets a fullness warning in the logs before (and at) the
  point where appends start being rejected. The backlog drains to the
  broker and acceptance resumes on its own.
- **The version guard survives the ACK.** Every accepted append records
  the highest version per event identity in a durable watermark that
  outlives the queue rows: a cancellation that was published and
  acknowledged still blocks a delayed older version from re-publishing
  the alert (the queue-drain comparison alone would let it through).
  The watermark lives until the event's own expiry passes (then the
  broker mirror is self-expired and the guard is pointless).
- `/metrics` exposes `warnflux_ingest_outbox_backlog` so a degraded
  broker sync is visible to the operator.

**Panel lifecycle (EMCOM / compose):**

- Panel-issued transitions carry an identity and a version: EMCOM
  transitions are stamped with the instance's publisher UUID and the
  network state version (`updated_at_ms`) as their change ID, and
  `SaveEmcomNetwork` records the lifecycle row **in the same transaction
  as the network state** (compose mirrors its state the same way).
  Dropping a network back to monitoring (or deleting it) therefore
  blocks a previously queued activation through the delivery gate — a
  radio that comes back can never transmit the stale raise.
- A **failed lifecycle write never acks the inbox**: the engine records
  the transition before any terminal skip, and on a write error the
  event stays PENDING — inbox recovery retries the record until the
  ledger accepts it, so a cancellation is never silently lost from the
  queue.
- The freshness oracle (`HazardActive`) applies the **same publisher
  identity** as the lifecycle ledger: a local cancellation of an event
  key never suppresses an independent publisher's active transition for
  that key — only this instance's own records (and legacy
  identity-less payloads) are governed by the local events table.

**Retained-view deletions survive restarts (MQTT /active/#):**

- An output that maintains retained broker documents persists its
  unresolved deletions (`output_pending_deletes`) **before the journal
  ack** — the acked change never replays, so the persisted set is the
  only memory of a delete collected while the category was masked or
  failed. On startup the worker restores the set before the active
  seeding and the rehydration pass deletes every still-retired retained
  topic; a key that is active again cleans its restored delete (the
  active document wins).

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

- `dispatch.inbox_retention` (default 24 h) is the pending-message
  validity policy, not an age-based reaper: rows older than the cutoff
  are removed ONLY when their hazard carries an authoritative expiry
  that has already passed (the staleness gates would suppress it
  anyway). Each removal is an explicit, audited outcome (`system` /
  `inbox-expiry-prune` in the dashboard audit log). Messages without an
  expiry never age out. Below the `storage.min_free_mb` alarm threshold
  the cutoff shortens to 5 minutes, so auxiliary data stops competing
  with primary storage.
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
APRS/Meshtastic messages are **saved to the local database first**
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
acknowledgements (e.g. Meshtastic group messages) stop at `accepted` — the
device took the message, the protocol has no stronger signal.

**Group retries resume, never restart (Meshtastic):**

- The Meshtastic action paces one alert to the whole routed group under
  a single bounded call deadline — a large group can exceed it mid-list.
- Every successful transmission (the channel broadcast and each direct
  message) is recorded in a durable, **version-keyed progress ledger**
  (`publisher + event key + change id + recipient + channel`) before the
  action moves on, so the ledger is independent of the message history:
  a historical delivered message with the same text never suppresses a
  NEW alert version.
- A retry skips the broadcast and every recipient whose exact version
  already has a progress row and transmits only the unfinished sends:
  early recipients are never repeated, and the last member is reached
  instead of being starved by the deadline.
- An unreadable ledger fails open (the send goes out again): a repeated
  alert is always safer than a lost one.

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
