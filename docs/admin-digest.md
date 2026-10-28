# Daily admin status mail

Once a day the station sends **one** branded mail to the operator. It answers
"what happened in the last 24 hours, and is anything broken?" without opening
the panel or the log.

## What the mail contains

- **Verdict** — `OK` or `DEGRADED`, derived from the plugin states (a degraded,
  stopped or suspended plugin makes it `DEGRADED`). The verdict also rides the
  subject line, so the inbox list already answers the question.
- **Station facts** — version and commit, uptime, the reporting window and the
  moment the mail was built.
- **Traffic in the window** — SMS, Meshtastic and APRS messages received and
  sent, hazards new / active now / ended, notifications dispatched and failed.
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

## What it deliberately does not do

- It never sends a second copy: one mail a day, whether or not the verdict is
  `OK`. A "only when something is wrong" mode would make silence ambiguous.
- It does not attach the log or the database: those live in the panel and on
  disk, and a mailbox is the wrong place for them.
