# APRS-IS source plugin (aprs-inet)

## Purpose

Connects to an [APRS-IS](http://www.aprs-is.net/) server, logs in with our
callsign + validation passcode, subscribes to a range filter around the
configured position (top-level `aprs.gridsquare` + `aprs.radius_km`) and
feeds every received frame into the shared **APRS hub**
(`internal/aprs`). The hub merges all backends into:

- one **retained station-state document per nearby station**:
  `<topic_prefix>/aprs/stations/<CALLSIGN>` (position, icon, course/speed,
  comment, distance, last heard, received-via backends), deleted with an
  empty retained payload after `aprs.station_ttl`;
- the **non-retained packet feed**: `<topic_prefix>/aprs/packets`;
- the **message feed**: `<topic_prefix>/aprs/messages` — APRS text
  messages addressed to our callsign (rx) and messages WarnFlux sends
  (tx).

The plugin also implements `aprs.Transmitter`: while connected it injects
outbound APRS messages back into APRS-IS, so the built-in **aprs action**
(and future rule wiring) can notify hams directly. APRS-IS routes such
messages through the nearest i-gate to the target station.

**This plugin publishes station state and messages, not HazardEvents.**
Nothing here touches the hazard journal or the `/events` topic.

## Complementary backends (no duplicate topics)

aprs-inet is the first of two planned backends; **aprs-radio** (KISS/TNC
over radio, real RX/TX) plugs into the same hub. Because both feed
`Hub.Observe` and the hub owns the topic layout:

- every station has **exactly one** state topic regardless of how many
  backends heard it — packets observed by both backends are merged, the
  `received_via` list records who heard the station;
- identical packets (same content digest) are published on the packet feed
  only once;
- outbound messages go through the hub, which picks the first ready
  transmitter — radio when it is connected, APRS-IS otherwise.

## Configuration

| Field | Default | Meaning |
|-------|---------|---------|
| `callsign` | `aprs.callsign` | APRS-IS login callsign (validated with the passcode) |
| `passcode` | required | APRS-IS validation code (mutually exclusive with `passcode_file`) |
| `passcode_file` | `""` | Read the passcode from a file (≤64 KiB) |
| `server` | `rotate.aprs2.net:14580` | APRS-IS host:port |
| `filter` | automatic | Overrides the default range filter `r/<lat>/<lon>/<radius_km>` |
| `connect_timeout` | `15s` | One dial attempt (1s..1m) |
| `read_timeout` | `10m` | One read deadline; exceeding it forces a reconnect (30s..1h) |

The passcode is never logged. On connection loss the plugin reconnects
with exponential backoff (2s..2m) and reports degraded health; the hub
keeps the station state — the retained MQTT documents simply age out per
`aprs.station_ttl`.

## Message format

Received APRS messages (`:TO       :text{id` frames) addressed to our
callsign are published on `aprs/messages`:

```json
{"schema_version":1,"direction":"rx","from":"SP9XYZ-7","to":"SP9MOA-10",
 "text":"hello ops","received_at":"2026-09-23T12:00:00Z","via":"aprs-inet"}
```

Outbound messages are sent as
`SP9MOA-10>APRS,TCPIP*::SP9XYZ   :text` frames and confirmed on the same
topic with `"direction":"tx"`. Message text is capped at 67 characters.
