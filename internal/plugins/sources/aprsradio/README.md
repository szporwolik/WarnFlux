# aprs-radio — KISS radio backend

Connects WarnFlux to a **KISS TNC server over plain TCP** (Direwolf, aprsd,
a hardware TNC bridge...). While connected it:

- **receives**: every decoded AX.25 UI frame is parsed and fed into the
  shared APRS hub, so stations heard over RF merge into the same
  `warnflux/aprs/stations/<CALLSIGN>` documents as the APRS-IS feed
  (origin `rf`);
- **transmits**: it registers as the hub's outbound transmitter, so APRS
  text messages (from the `aprs` and `aprs-out` actions) are encoded as
  AX.25 UI frames and pushed through the TNC to the digipeater network
  (default path `WIDE1-1`).

## Configuration

```yaml
sources:
  - id: aprs-radio-main
    type: aprs-radio
    enabled: true

    runtime:
      restart: true
      shutdown_timeout: 10s

    config:
      server: "127.0.0.1:8001"   # KISS server host:port (required)
      path: ["WIDE1-1"]          # digipeater path for outbound frames
      connect_timeout: 15s
      read_timeout: 10m          # timeout is NOT an error; reconnect only after 3x of silence
      max_frame_bytes: 2048
```

Requires `aprs.enabled: true`.

## Parallel operation

`aprs-inet` and `aprs-radio` run side by side and complement each other:

- both feed the hub at the same time — RF-heard packets enrich the
  APRS-IS view (and vice versa), stations heard on RF are marked `rf`;
- outbound messages prefer the radio for RF-heard stations (reaching hams
  without any internet hop) and fall back to APRS-IS for internet-only
  stations or when the TNC is down — so in a crisis with no network the
  radio becomes the primary path automatically.

## Message acks

Outbound messages sent with ack tracking (the `aprs-out` action) carry a
`{id}` suffix. When the addressee acknowledges over RF, the ack frame
arrives through the same KISS stream and the hub wakes the waiting sender.
