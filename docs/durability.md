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

**Guarantee: durable acceptance; at-least-once evaluation.**

Every canonical event accepted from a receiver (MQTT or HTTP ingest) is
persisted to the dispatch inbox (`dispatch_inbox`) **before** it enters
the live queue. The routing engine acknowledges the row only after it has
evaluated the event.

- A full live queue defers the event to inbox recovery instead of dropping
  it.
- A crash between acceptance and evaluation re-delivers the event after
  the restart (recovery runs on the refresh tick).
- If the inbox write itself fails, acceptance falls back to the live queue
  only (the event is not durable).
- The inbox row is acknowledged even when the evaluation ends in
  "no matching route" or a rejected action submission — durability for the
  action stage lives in the delivery jobs below, not in the inbox.

## 3. Action completion (routing evaluation → notification)

**Guarantee: at-least-once per (group, action, event); retried on replay.**

Routing persists a delivery job (`action_fires`, one row per
group × action × event × publisher) **before** submitting the action and
settles it **only after** the action accepted the request:

- `running` / `failed` jobs are retried when the same transition replays
  (including after restarts).
- `succeeded` jobs deduplicate replays.
- A crash between submission and settlement leaves the job retryable.
- Channel floods without delivery acknowledgements (e.g. MeshCore group
  messages) are "sent" once the device accepted them — the protocol has no
  stronger signal.

## What is NOT guaranteed

- The live in-memory queue and the audit trail (`trail`) are not durable:
  they are diagnostics, never the source of truth.
- The retained active-view topics on the broker are best-effort mirrors;
  the journal is authoritative.
- Duplicates are possible at every stage (at-least-once): consumers must
  deduplicate by `publisher + source + event_key + change_id`.
