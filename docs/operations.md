# Operating a station

A runbook for a deployed WarnFlux station: what runs where, what happens at
boot, how to verify a healthy instance, and what to check when something is off.
It assumes the Docker Compose deployment described in
[`deploy/README.md`](../deploy/README.md).

## 1. What runs

A station is four Compose projects, each with its own directory under
`/srv/docker/`:

| Project | Image | Role |
|---|---|---|
| `mosquitto` | `eclipse-mosquitto:2` | The internal broker. Every plugin talks through it. |
| `direwolf` | `aprs-direwolf:2` | APRS TNC on the radio (KISS over TCP). |
| `warnflux-time` | `ghcr.io/szporwolik/warnflux:<tag>` | The same image as the panel, but it keeps the host clock (GPS/NTP) and answers SNTP on UDP 123. See [time-sync.md](time-sync.md). |
| `warnflux` | `ghcr.io/szporwolik/warnflux:<tag>` | The station itself: panel, plugins, storage. |

The panel listens on 8080 inside its container; the published host port is
whatever the Compose file maps it to (`8080` on a dev box, `8090` in the
reference deployment behind a reverse proxy). `GET /healthz` answers `ok` with
200 and needs no authentication — use it as the liveness probe.

State lives in `/srv/docker/warnflux/data/warnflux.db` (SQLite, WAL mode) and
in the broker's retained topics. Both are needed to know what the station
currently warns about: the database is the truth, the retained topics are the
outbound mirror of that truth.

Configuration and secrets live in `/srv/docker/warnflux/config/` (owned by the
container user, `0750`). Tokens — Discord webhook, SMTP password, GSM PIN —
belong there and never in this repository.

## 2. What happens at boot

There is no RTC on the usual single-board host, so at power-on the clock can be
hours off, and USB devices enumerate in an arbitrary order. Neither may decide
whether the station comes up. Three pieces handle that:

| Unit | What it does |
|---|---|
| `warnflux-boot-up.timer` → `warnflux-boot-up.service` | The boot path **and** the self-healing loop. Waits for `NTPSynchronized=yes`, waits for every mapped device, then brings the stacks up in dependency order: broker → radio → clock → station. |
| `warnflux-clock-guard.timer` → `warnflux-clock-guard.service` | Decides who owns the clock: the station container while it is alive and its last sync is recent, otherwise `systemd-timesyncd`. |
| `systemd-timesyncd` (enabled) | The fallback clock. The guard only stops it, never disables it, so it is back after every boot. |

Docker's `restart: unless-stopped` policy also starts the containers directly at
boot, before the clock is right — that is why a broker with TLS can come up
`Exited 255` and then be fixed by the boot-up pass a moment later. The correct
sequence is therefore: **let the timer do its job, then look**.

`warnflux-boot-up.service` is a one-shot pass, not a state: it finishes
`inactive` after every run. It must, otherwise the timer has no next activation
to schedule. The timer re-runs it five minutes after the previous pass finished
(`OnUnitInactiveSec`), which is what makes the stack self-healing: pull a device,
`docker compose down`, or let a container crash, and the next pass puts it back.

### Devices are always addressed by ID

`devices:` entries in a Compose file are resolved when the container is
*created*, so a missing device is an `Exited 255` with no retry. And the
`ttyUSB*` / `ttyACM*` numbering changes between boots, so pinning `/dev/ttyUSB0`
is a bug waiting for a power cut. Map by udev ID instead:

```yaml
devices:
  - /dev/serial/by-id/usb-FTDI_...-if00-port0:/dev/modem
  - /dev/serial/by-id/usb-u-blox_AG_-_GPS_GNSS_Receiver_...-if00:/dev/gps
```

## 3. Verifying a healthy station

```sh
# host units
systemctl --failed                          # expect: nothing
systemctl is-active warnflux-boot-up.timer  # expect: active
systemctl show warnflux-boot-up.service -p Result   # last pass: success
systemctl list-timers warnflux-boot-up.timer        # NEXT must be scheduled

# containers
docker ps --format '{{.Names}}\t{{.Status}}'        # four stacks Up, restarts=0

# the station
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8090/healthz
docker logs warnflux --since 30m 2>&1 | grep -cE 'level=(ERROR|WARN)'   # expect: 0

# retained outbound state (must match the number of active events)
docker exec mosquitto mosquitto_sub -h localhost -t 'warnflux/active/#' -W 2 -v | wc -l

# clock
docker logs warnflux-time --tail 20 | grep -E 'clock is good|clock stepped'
```

A pass that reports `NEXT = -` in `list-timers` means the self-healing loop is
dead, not that everything is fine.

## 4. Upgrading

```sh
docker pull ghcr.io/szporwolik/warnflux:vX.Y.Z
# set the tag in /srv/docker/warnflux/compose.yml and in the time project
docker compose -f /srv/docker/warnflux/compose.yml up -d
docker compose -f /srv/docker/warnflux-time/compose.yml up -d
```

Always pass the same overlays the boot script uses (`compose.devices.yml`,
`compose.time-sync.yml`, and their device overlays when present), otherwise
`up -d` silently drops the device mappings. Take a copy of the database and the
config directory into `/srv/backup/` before the upgrade; the migration runs at
container start and is not reversible.

After the upgrade, run the checks from section 3 and confirm the panel shows the
new version in its health card.

## 5. When something is off

| Symptom | First thing to check |
|---|---|
| A container is `Exited 255` right after boot | Its device is missing or was renamed → `ls -l /dev/serial/by-id/`, then wait for the next boot-up pass. |
| The panel is up but the clock card shows amber | `docker logs warnflux-time`; a `clock stepped` line right after boot is normal (Docker starts the stack before the clock is synchronised). |
| Nothing on APRS, everything else fine | `docker logs direwolf`, and the RF side: squelch, antenna, feedline — a station with a deaf receiver still hears the internet feed. |
| Broker shows no `warnflux/active/#` while the panel lists warnings | Outbound publishing is behind a durable cursor; a broker restart is caught up automatically (see [durability.md](durability.md)). |
| Warnings disappeared after a restart | A disabled source withdraws its own events. Check the plugin's `enabled` flag before assuming data loss. |
| Panel slow or the database grows | `sqlite3 warnflux.db 'PRAGMA quick_check;'`, then look at the WAL size — a multi-megabyte WAL just means a checkpoint is pending. |

## 6. Known limitations

- **No RTC.** Every boot starts with a wrong clock and a short window in which
  TLS-dependent services may fail once before the boot-up pass fixes them.
- **USB order is not stable.** Anything pinned to `/dev/ttyUSB*` or
  `/dev/ttyACM*` will eventually break on a reboot; use by-id paths.
- **HID devices have no by-id path.** The AIOC PTT interface is still addressed
  as `/dev/hidrawN`, so a udev rule is the only way to make it stable.
- **RF reception is physical.** Software can tell you the frame counter; it
  cannot tell you the antenna is disconnected.
- **One broker, one database, no replication.** Backups are copies, not a
  cluster: restoring means stopping the stack, replacing the file, starting it
  again.
