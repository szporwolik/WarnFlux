# Deployment: a station that starts without its hardware

Two failure modes kept the station dark after a reboot of the production VM:

1. **Docker**: `devices:` is resolved when the container is **created**. A missing
   host path makes `docker compose up` fail with
   `error gathering device information while adding custom device` (exit 255) and
   Docker does **not** retry — `restart: unless-stopped` cannot help, because the
   container was never created.
2. **Kernel**: a freshly installed kernel that is missing its
   `linux-modules-extra` package ships no `cdc_acm`/`usbserial`/`option`/
   `snd-usb-audio`, so `/dev/ttyUSB0`, `/dev/serial/by-id/…` and `/dev/snd/pcm*`
   never appear (see `warnflux-usb-modules.service` below).

The layout in this directory makes both harmless: the station comes up, logs what
is missing and reports it on the panel's health card (`Hardware`), and picks the
device up by itself once it is plugged in.

## Files

| File | Purpose |
|---|---|
| `warnflux/compose.devices.yml`, `aprs/compose.devices.yml` | Device overlays. The base `compose.yml` on the host carries **no** `devices:` block, so the containers can always be created. |
| `warnflux/compose.time-sync.yml` | The clock-discipline container (NTP/GPS + SNTP server for the LAN). Same image, own process, `CAP_SYS_TIME`, UDP 123 — see `docs/time-sync.md`. |
| `warnflux/compose.time-sync.devices.yml` | GPS receiver overlay for that container (`/dev/ttyACM0`), added only once the device is present. |
| `warnflux-clock-guard.sh`, `systemd/warnflux-clock-guard.{service,timer}` | Host-side guard: the station owns the clock, the host's `systemd-timesyncd` is started only while the station cannot keep it. |
| `systemd/timesyncd-warnflux-fallback.conf` | `systemd-timesyncd` drop-in used by that fallback (ordinary NTP pool, never the station itself). |
| `warnflux-boot-up.sh` + `systemd/warnflux-boot-up.{service,timer}` | **The boot path.** Waits for a synchronised clock and for every mapped device, then starts the stacks in dependency order; the same script runs from a 2-minute timer as the self-healing pass. |
| `warnflux-stack-up.sh` (deprecated) | Waits up to 120 s for the device, then `docker compose up -d` **with** the overlay when it exists, **without** it when it does not. Superseded by `warnflux-boot-up.sh`. |
| Device mapping rule | Always map **by-id** paths (`/dev/serial/by-id/...`), never a bare `/dev/ttyUSB0` or `/dev/ttyACM0`: the numbering swaps between devices after a re-enumeration, and a number hands the container the wrong device without any error. |
| `warnflux-stack-reconcile.sh` | Compares "device present on the host" with "device mapped into the container" and recreates only that service when they differ — a running container never gains a device node. |
| `systemd/*.service`, `systemd/*.timer` | Autostart on boot plus the 2-minute reconcile timer (USB hotplug without a manual restart). |

## Install on the host

```sh
install -m 0755 deploy/warnflux-stack-up.sh deploy/warnflux-stack-reconcile.sh /usr/local/bin/
install -m 0644 deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/
install -m 0644 deploy/warnflux/compose.devices.yml /srv/docker/warnflux/
install -m 0644 deploy/aprs/compose.devices.yml    /srv/docker/aprs/
systemctl daemon-reload
systemctl enable --now warnflux-stack.service aprs-direwolf.service
systemctl enable --now warnflux-stack-reconcile.timer
```

The host's `compose.yml` files must be the *base* variant: same services, no
`devices:`/`group_add`. Move those lines into the matching `compose.devices.yml`
(they are already there in this repository).

## Boot: every service comes back by itself

A station that is expected to run unattended must survive "the machine came
back but the broker did not". Two traps caused exactly that on the Pi:

1. **No RTC.** The Pi wakes up with the time of its last shutdown (observed:
   39 hours behind). Anything that validates a certificate at start-up dies
   immediately and — with `restart: unless-stopped` — is not retried: mosquitto
   with TLS exited 255 and stayed down.
2. **Slow USB enumeration.** A mapped device must exist when the container is
   CREATED, so direwolf exited with `Audio input device 0 error code -19` and
   never came back.

`warnflux-boot-up.sh` fixes both. It waits for a plausible clock and for every
device named by a stack's `compose.devices.yml`, then brings the stacks up in
dependency order (broker → radio → clock discipline → station). It is
**idempotent**, so the same script runs from a timer as the self-healing pass:

```sh
install -m 0755 deploy/warnflux-boot-up.sh /usr/local/sbin/
install -m 0644 deploy/systemd/warnflux-boot-up.service \
               deploy/systemd/warnflux-boot-up.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now warnflux-boot-up.service warnflux-boot-up.timer
systemctl enable systemd-timesyncd        # early clock; the guard STOPS it later
```

Which supersedes the older `warnflux-stack.service`, `aprs-direwolf.service` and
`warnflux-stack-reconcile.timer` — disable them so two units do not race for the
same stacks. The clock guard no longer *disables* the host NTP client, it only
stops it (see `docs/time-sync.md`): it must run again at every boot.

Verifying a reboot:

```sh
sudo reboot
# after it comes back:
systemctl is-active warnflux-boot-up.service warnflux-boot-up.timer
docker ps --format '{{.Names}} {{.Status}}'      # all four stacks up
curl -sk -o /dev/null -w '%{http_code}\n' https://<domain>/healthz
journalctl -u warnflux-boot-up.service -b --no-pager
```

## Host information on the panel (optional mount)

The dashboard's System card shows the kernel, the architecture, the processor
model, the host uptime and its load average — all read from `/proc`, which is
the **host's** even inside a container. The distribution name is the exception:
inside a container `/etc/os-release` describes the *image*, so the row is left
out unless the host's file is mounted for the panel:

```yaml
services:
  warnflux:
    volumes:
      - /etc/os-release:/host/os-release:ro
```

That is the only line involved; without it the card simply omits the "Operating
system" row instead of showing the image's distribution as if it were the host's.

## kernel modules for the running kernel

`warnflux-usb-modules.service` (install it the same way) installs
`linux-modules-extra-$(uname -r)` when the USB serial/audio modules are missing,
loads them and re-triggers udev. That is what makes a bare new kernel
(`linux-virtual` does not depend on `linux-modules-extra`) self-healing instead
of needing a manual `apt install` after every kernel upgrade.

## Verifying

```sh
systemctl is-active warnflux-stack.service aprs-direwolf.service
docker ps --format '{{.Names}} {{.Status}}'
ls -l /dev/ttyUSB0 /dev/serial/by-id/        # present when the hardware is in
curl -sk -o /dev/null -w '%{http_code}\n' https://<domain>/healthz
```

And the real proof: unplug the AIOC/modem (or simply reboot with it absent), let
the station start, and read the `Hardware` section of the dashboard — with the
device back, the reconcile timer recreates the container within ~2 minutes.
