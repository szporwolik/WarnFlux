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
| `warnflux-stack-up.sh` | Waits up to 120 s for the device, then `docker compose up -d` **with** the overlay when it exists, **without** it when it does not. |
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
