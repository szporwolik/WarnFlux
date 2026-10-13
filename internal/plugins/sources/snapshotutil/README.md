# snapshotutil — helpers shared by snapshot sources

Not a plugin: this package registers nothing and takes no configuration. It
holds the two algorithms the full-snapshot sources need verbatim — `imgw`,
`rso`, `gios`, `gddkia` and `burzedzis` all import them, and a private copy
per provider would be the same code drifting apart:

- `ParseWarsawLocal("2026-10-13 14:05:00")` — parses a naive Polish provider
  timestamp in Europe/Warsaw (CET/CEST, DST-aware). Polish providers publish
  wall-clock time without a zone, and reading it as UTC shifts every alert by
  one or two hours.
- `Reconcile(ctx, emit, source, snapshotKeys, now)` — cancels the active
  events of one source that the latest COMPLETE provider snapshot no longer
  contains. It reads the durable current state through
  `plugin.SourceActiveEventReader`, so it is restart-safe and never depends on
  plugin-local memory; events whose validity already lapsed are left to the
  core expiry worker.

Contract for callers, learned the hard way: reconcile **only on a complete
pass**. A partially fetched snapshot (one provider page or one locality
failed) must skip the call entirely, otherwise a network hiccup looks like
"the provider withdrew everything" and cancels live alerts. Every snapshot
source documents that rule next to its poll loop.
