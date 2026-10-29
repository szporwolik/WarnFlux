# Daily admin status mail

Once a day the station sends **one** branded mail to the operator. It answers
"what happened in the last 24 hours, and is anything broken?" without opening
the panel or the log.

## What the mail contains

- **Verdict** — `OK`, `DEGRADED` or `INCOMPLETE`, taken from the **shared
  system-health assessment** (`internal/health`) — the very same one the
  dashboard health card shows: source/output plugins, MQTT receivers, actions,
  hardware probes, the database and the dispatch queue. A disconnected receiver,
  a degraded action, a missing modem or a broken database makes it `DEGRADED`,
  exactly as it does in the panel. The verdict also rides the subject line, so
  the inbox list already answers the question.
- **Station facts** — version and commit, uptime, the reporting window and the
  moment the mail was built.
- **Traffic in the window** — SMS, Meshtastic and APRS messages received and
  sent, hazards new / active now / ended, notifications dispatched and failed.
  The message counts come from **durable per-minute aggregates**
  (`traffic_counters`, written in the same transaction as the message) rather
  than from the history tables, whose retention keeps only the newest few
  thousand rows: a busy day is now counted exactly, and the window is snapped
  to the minute so the printed span is exactly the span that was counted.
- **System health** — every finding behind the verdict (subsystem, component,
  detail), the pending-notification count and — when part of the station could
  not be read — the inputs that were unavailable. It is never a bare "OK": a
  report that could not look says so.
- **Services** — every plugin with its live state, its last poll summary, the
  last error for a degraded row and the consecutive-failure count. Problem rows
  are listed first, switched-off rows last.

## Configuration

```yaml
admin_digest:
  enabled: true
  at: "06:00"          # station-local clock time
  to: ["admin@example.org"]   # optional; empty = every admin account with an e-mail
```

It rides the **first enabled `smtp` action**, exactly like the password-reset
mail: without one nothing is sent and the log says so. The message uses the same
branded template as every other WarnFlux mail (plain-text alternative + HTML with
the inline logo) and the system notification language.

The anchor is re-armed after every send with the next occurrence of `at`, so a
restart never skips the report and a DST change moves the wall clock rather than
the anchor.

## Missing data is never zero

A broken counter query (a locked or corrupt database) no longer produces a mail
whose traffic section shows zeros — the numbers would read as "a quiet day".
The traffic section then carries a single `Counter read: unavailable` line, the
verdict becomes `INCOMPLETE` and the mail names the inputs it could not read.
The mail still goes out: losing the report because one query failed would hide
the very problem it was meant to report.

## What it deliberately does not do

- It never sends a second copy: one mail a day, whether or not the verdict is
  `OK`. A "only when something is wrong" mode would make silence ambiguous.
- It does not attach the log or the database: those live in the panel and on
  disk, and a mailbox is the wrong place for them.
