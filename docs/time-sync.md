# Time sync — a station that knows the time

WarnFlux stamps everything it publishes: warnings, SMS, APRS and Meshtastic
messages, the archive, the daily digest. A station whose clock is wrong sends
*wrong* information, and an off-grid station has nobody to ask. So the image
keeps the clock itself:

| Situation | Source |
|---|---|
| The network answers | **NTP** (`ntp_servers`), a few milliseconds of precision |
| No network, GPS receiver attached | the **GPS** (NMEA `RMC`/`ZDA` over a serial port) |
| No network, no GPS | the clock free-runs, and the panel says so |

And in every case the station **is the LAN's time source**: a small SNTP server
answers on UDP 123, so the other devices in the network can take their time from
the station while it runs off-grid.

## How it runs

The discipline is the **same image in a second process**:

```sh
warnflux --config /config/config.yaml -time-sync
```

Why a separate process and not a goroutine inside the panel:

* stepping the host clock needs **`CAP_SYS_TIME`**, and the panel (which parses
  radio traffic, SMS, MQTT payloads and HTTP requests) must never carry that
  capability;
* the capability is only effective for **root** — a non-root process has no
  ambient capabilities, so the time container runs as `user: "0:0"` while the
  panel keeps running as the image's non-root user with `cap_drop: [ALL]`;
* with the current Docker default seccomp profile `clock_settime` is allowed
  *only* when `CAP_SYS_TIME` is present, so no custom profile is needed. Verified
  probe (an invalid clock id reaches the syscall and returns `EINVAL`, instead of
  the `EPERM` a blocked syscall returns):

  ```
  docker run --rm alpine ...                         clock_settime(-1) -> errno=1 (EPERM)
  docker run --rm --cap-add SYS_TIME alpine ...      clock_settime(-1) -> errno=22 (EINVAL)
  ```

Without the capability the daemon still **serves** the time and still logs every
pass — it just cannot move the clock, and it says so (`time: cannot step the
clock … the container needs CAP_SYS_TIME`).

## Configuration

```yaml
time_sync:
  enabled: true
  ntp_servers: ["pool.ntp.org"]
  interval: 6h                  # hours, not minutes: a clock drifts by seconds/week
  gps_device: "/dev/ttyACM0"    # empty = no GPS
  gps_baud: 9600
  step_threshold: 250ms         # below this the clock is left alone
  serve: true                   # SNTP server for the LAN
  serve_listen: ":123"
  status_file: "/data/time-sync.json"
```

Details that matter:

* **`interval`** is the delay between discipline passes. Six hours is plenty: the
  clock drifts by seconds per week, and a step while the radio transmits only
  makes timestamps jump.
* **`step_threshold`** keeps the daemon from chasing noise. A correction below
  it is measured and logged, not applied.
* **GPS without a fix** is rejected on purpose: a receiver with no satellite fix
  still prints its own (wrong) clock, so RMC status must be `A`, the date must be
  plausible (≥ 2020) and the correction below 30 days. Three samples are taken
  and the **median** is used, so one corrupt sentence cannot move the clock.
* **`status_file`** is rewritten after every pass. The dashboard's time-zone row
  and the daily digest read it and report where the time came from: `clock from
  gps, 2h ago`, or amber `FREE-RUNNING` when there is no reference.

## Compose

The panel and the discipline share the configuration and the data volume; only
the time container gets the capability, the clock device and the UDP port:

```yaml
services:
  warnflux:
    # unchanged: read_only, cap_drop [ALL], user non-root, device for the modem

  warnflux-time:
    image: ghcr.io/szporwolik/warnflux:<same tag as warnflux>
    container_name: warnflux-time
    restart: unless-stopped
    command: ["--config", "/config/config.yaml", "-time-sync"]
    # Stepping the clock needs the capability to be effective for the process,
    # so this one container runs as root; it serves nothing but time.
    user: "0:0"
    cap_drop: [ALL]
    cap_add: [SYS_TIME]
    security_opt:
      - no-new-privileges
    read_only: true
    tmpfs:
      - /tmp
    volumes:
      - ./config:/config:ro
      - ./data:/data            # status file read by the panel
    ports:
      - "123:123/udp"
    # The GPS receiver is NOT listed here: `devices:` is resolved when the
    # container is created, so a missing /dev/ttyACM0 would make Docker refuse
    # to create it at all (exit 255, no retry). It lives in a separate overlay.
```

Ready-made overlays in this repository:

| File | Purpose |
|---|---|
| `deploy/warnflux/compose.time-sync.yml` | the discipline container (capability, UDP 123, shared config/data) |
| `deploy/warnflux/compose.time-sync.devices.yml` | the GPS receiver (`/dev/ttyACM0`) — add it once the device is plugged in |

```sh
docker compose -f compose.yml -f compose.time-sync.yml \
               -f compose.time-sync.devices.yml up -d warnflux-time
```

## The host side: one owner, and a fallback

A host that keeps its own NTP client running will fight the container — two
disciplinarians stepping one clock. So the station owns the clock while it runs,
and the host takes over **only while the station cannot keep it**:

```
station alive  ──►  warnflux-time steps the host clock, host timesyncd stopped
station gone   ──►  warnflux-clock-guard starts the host timesyncd
```

`warnflux-clock-guard.sh` (run by `warnflux-clock-guard.timer` every 2 minutes)
treats the station as the owner while **both** hold:

* the `warnflux-time` container is running, and
* the last discipline pass is not overdue (`time_sync.status_file` → `next_at`,
  plus a grace, default 15 min).

Otherwise it starts the host's `systemd-timesyncd`, which takes the time from
the ordinary pool (`timesyncd-warnflux-fallback.conf`) — never from the
station's own server, because the server and the station die together. Once the
station is back, the next check hands the clock back and stops the host client
again.

```sh
install -m 0755 deploy/warnflux-clock-guard.sh /usr/local/sbin/
install -m 0644 deploy/systemd/warnflux-clock-guard.service deploy/systemd/warnflux-clock-guard.timer \
               /etc/systemd/system/
install -m 0644 deploy/systemd/timesyncd-warnflux-fallback.conf \
               /etc/systemd/timesyncd.conf.d/10-warnflux-fallback.conf
systemctl daemon-reload
systemctl enable --now systemd-timesyncd        # the guard stops it on the next check
systemctl enable --now warnflux-clock-guard.timer
```

Hosts that use `chrony` instead of `systemd-timesyncd` should disable it the
same way (`sudo systemctl disable --now chrony`) and skip the guard — a
`chrony` configuration pointed at the station (`server <station-ip> iburst`) is
the other way to keep one owner, at the price of following the station's clock
even when it is wrong.

Check it by hand:

```sh
/usr/local/sbin/warnflux-clock-guard.sh          # prints what it decided
systemctl list-timers warnflux-clock-guard.timer
systemctl is-active systemd-timesyncd            # inactive while the station runs
```

## Verifying

```sh
# what the daemon last did
cat /srv/docker/warnflux/data/time-sync.json

# is the SNTP server answering?
chronyd -Q 'server 10.0.0.123 iburst'

docker logs --tail 20 warnflux-time
# time: clock is good  source=ntp detail=pool.ntp.org offset=3ms threshold=250ms
```

## Failure modes and what the panel shows

| Symptom | Meaning |
|---|---|
| `clock without a reference: …` (amber) | no NTP answer and no usable GPS: the clock free-runs from its last correction |
| `clock: no discipline pass yet` | the process just started |
| `last pass 13h ago` | the discipline process is not running (container down, `interval` misconfigured) |
| `time: cannot step the clock …` in the log | the container lost `CAP_SYS_TIME` or runs as non-root |
| `gps reports an implausible date (no satellite fix?)` | antenna unplugged / no sky view |
