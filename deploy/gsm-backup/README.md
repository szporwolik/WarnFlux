# GSM backup uplink — internet from the modem, SMS untouched

The station's modem (Huawei E173) is not only an SMS device: it is also the
cheapest possible insurance against the wired uplink dying. This directory turns
the SIM's data connection into a **backup default route** that takes over when
Ethernet stops working, while the panel keeps sending and receiving SMS on the
same modem at the same time.

## Why both work at once

The E173 exposes three serial interfaces, and **their roles rotate across a USB
reset** — that is the single most expensive thing to learn about this stick.
Measured on the production stick (2026-11-10, Orange SIM):

| Interface | Stable path | What it answered |
|---|---|---|
| `if00` | `…-if00-port0` → `/dev/ttyUSB0` | PPP negotiated an address (`local IP 10.155.53.209`); after a later power cycle it went silent |
| `if03` | `…-if03-port0` → `/dev/ttyUSB1` | silent — never answers `AT` |
| `if04` | `…-if04-port0` → `/dev/ttyUSB2` | `AT`, `ATE0`, `AT+CMGF=1`, `AT+CPIN?` → the station's SMS session |

So the split is real but **not fixed**: one interface carries the station's
AT/SMS session and another carries the packet call. Two processes on the SAME
tty destroy the session (measured), which is why the mapping and the two
configurations have to be kept in step:

```yaml
    devices:
      - /dev/serial/by-id/usb-HUAWEI_Technology_HUAWEI_Mobile-if00-port0:/dev/modem-0
      - /dev/serial/by-id/usb-HUAWEI_Technology_HUAWEI_Mobile-if03-port0:/dev/modem-3
      - /dev/serial/by-id/usb-HUAWEI_Technology_HUAWEI_Mobile-if04-port0:/dev/modem-4
```
```yaml
gsm:
  # Everything except the interface the backup dials.
  device: /dev/modem-4,/dev/modem-3
```

After any replug or USB reset: read the station's `gsm: modem ready
device=…` line, run the two probes below, and repoint `gsm.device` and
`warnflux-gsm.peers` at the interfaces that actually answered. A pppd run on a
port the station holds logs `Receive serial link is not 8-bit clean`; a silent
port logs `Connect script failed` — both mean "wrong interface", never "bad
modem".

## Failover is more than a route

`warnflux-gsm-backup.service` keeps the packet link up permanently, so the
kernel can switch the moment the wired route disappears (cable, switch, DHCP).
That is not enough for the case that actually happens here — **the ISP is down
while the cable stays up**: the wired default route never goes away, keeps its
lower metric, and the station silently loses the internet with ppp0 sitting
idle.

`warnflux-net-watchdog.timer` covers it. Every 30 s it probes the WIRED path
(`ping -I eth0` at three public addresses) and

* after three failed probes, raises the cellular default to **metric 50**, so
  it outranks the wired metric 100 and every new connection leaves through the
  modem;
* after three good probes, deletes exactly that route — pppd's own metric-700
  route stays, Ethernet wins again and the station stops paying for data.

Two verdicts in a row are required on both edges, so one lost ICMP packet never
moves the station. All transitions land in the journal tagged `warnflux-net`,
and the current state is readable in `/run/warnflux-net-watchdog/mode`.

## How the routing behaves

* Ethernet keeps the default route with **metric 100**.
* The backup adds its own default with **metric 700**
  (`defaultroute-metric` in the peer file, re-asserted by the `ip-up` script).
* The kernel always prefers the lower metric, so nothing changes while the wire
  works. When the wired route disappears — cable pulled, DHCP lease lost, ISP
  gateway dead — the modem route takes over immediately and silently.
* While the wire is dead, the watchdog raises a second cellular default with
  **metric 50** (see "Failover is more than a route"); it is deleted as soon as
  the wire answers again, so the station never stays on the expensive link.
  The routing table is the source of truth: the stored mode only says `primary`
  once the forced route is really gone, and a failed add is never announced as a
  failover — the next 30-second pass retries either way.
* DNS: NetworkManager owns `/etc/resolv.conf` and names only the wired gateway.
  The `ip-up` script appends the operator's resolver as a **marked fallback
  block**, and `ip-down` removes exactly that block again. Without it a domain
  lookup during an outage would hang until the wired resolver timed out.
  Both hooks call `/usr/local/sbin/warnflux-gsm-dns`, which never replaces
  `/etc/resolv.conf`: when systemd-resolved is running it asks `resolvectl` for a
  per-link server instead, and otherwise it rewrites the file **through** the
  path, so a `resolv.conf` that is a symlink stays a symlink and keeps receiving
  the manager's own updates. (`mv` over the path turned the symlink into a plain
  file, and every later resolver update then landed in a file nobody read.)
  The operator's resolvers answer on the mobile network only (measured:
  `194.204.159.1` replies over `ppp0` and stays silent over `eth0`), so they are
  a fallback entry and the wired resolver has to survive pppd's own
  `ip-up.d/0000usepeerdns`, which rewrites the file with the PPP resolvers
  alone. The wired entries are taken from the CURRENT file (so a new DHCP lease
  is respected); pppd's pre-connect copy is used only when the current file has
  nothing but the operator's own addresses, which is the state right after a
  connect.
* **Per-session DNS record.** The ip-up hook writes what the session learned to
  `/run/warnflux-gsm/<iface>.dns` and ip-down removes exactly that file. pppd's
  `/etc/ppp/resolv.conf` is shared by every PPP profile on the host (a VPN, a
  second modem) and is never read when the fallback block is rebuilt.
  Order follows the working link: while the wire is dead the resolvers that only
  answer on-net must lead, because a first entry that cannot be reached costs a
  timeout on every lookup; the recovery puts the wired one back on top.
  The watchdog calls `warnflux-gsm-dns ensure <iface> <cellular|wired>` on every
  30-second pass: NetworkManager regenerates `/etc/resolv.conf` whenever the
  wired network changes and silently takes the block with it, so the entry is
  rebuilt from the list pppd recorded in `/etc/ppp/resolv.conf`. `ensure` is a
  no-op when the block is present and already in the right order.
* **Only our own PPP profile is touched.** `ip-up.d`/`ip-down.d` are global, so
  both hooks first require `ipparam warnflux-gsm` (sixth argument, set in the
  peer file). A VPN or a second modem on `ppp1` therefore neither gets a route
  pushed nor loses the backup link's resolver block, and the backup link keeps
  working when it is assigned `ppp1` itself.

## Install

```sh
sudo apt-get install -y ppp          # pppd + chat, if not present
sudo sh deploy/gsm-backup/install.sh # copies files, enables and starts the unit
sudo warnflux-gsm-status             # read-only report: unit, ppp0, routes, DNS
```

`install.sh` also installs `warnflux-net-watchdog.timer` (every 30 s) and copies
this file to `/etc/warnflux/gsm-backup.md`, which is what the units reference.

The PDP context (`APN internet`, what the SIM reports in `AT+CGDCONT`) is
already defined, so the script dials `*99***1#` directly. A contract that needs
another APN: put `AT+CGDCONT=1,"IP","<apn>"` line before the dial in
`/etc/ppp/warnflux-gsm.chat` and add `OK ATD*99***1#`.

## Verify

1. `sudo warnflux-gsm-status` — `ppp0` has an address, `journalctl` shows
   `local IP address …` and `remote IP address …`.
2. `ip route show default` — two lines: `… dev eth0 metric 100` and
   `… dev ppp0 metric 700`.
3. The SMS path is still alive: the panel's **GSM** page shows the modem
   connected and `warnflux` keeps logging its hourly
   `gsm: storage maintenance done`.
4. The watchdog: `systemctl list-timers warnflux-net-watchdog.timer` shows the
   next run, and `journalctl -t warnflux-net` stays quiet while the wire works.
   Pull the cable (or `sudo ip route del default via <gw>`) and within ~1.5 min
   the journal says "cellular default raised to metric 50" and
   `ip route get 1.1.1.1` answers `dev ppp0`; restore the route and it demotes
   itself again.
5. Restore the wired default when you simulated the outage by hand; with a real
   cable pull NetworkManager/DHCP does it, and the watchdog demotes the
   cellular route within ~1.5 min.

## Turning it off

```sh
sudo systemctl disable --now warnflux-gsm-backup.service
sudo systemctl disable --now warnflux-net-watchdog.timer
```

That is the whole rollback: the routes and the DNS block disappear with the
link, and the watchdog stops touching the routing. The installed files can stay (they do nothing while the unit is disabled),
or be removed by hand.

## Measured: what the data link needs (2026-11-10, Orange SIM)

`AT+CGATT?` answers **0** on a fresh boot — this modem never attaches to the
packet domain by itself, so the chat script attaches it by hand before dialling.
With that done the call goes through and IPCP grants a `10.x.x.x` address:

| Probe | Result |
|---|---|
| `AT+CGATT=1` | `OK`, then `+CGATT: 1` |
| `AT+CGACT=1,1` / `AT+CGPADDR=1` | `OK` / `+CGPADDR: 1,"10.8.9.63"` — a PDP context is granted |
| `ATD*99***1#` on a clean session | `CONNECT`, IPCP `local IP 10.155.53.209` |
| Traffic over ppp0 | `ping 1.1.1.1` 0% loss, `https://example.com` → 200 |
| Throughput | **~3.5 kB/s (≈28 kbit/s)** — EDGE (`AT^SYSINFO` sys_mode 3) |

Two things follow from the measurements:

* **Throughput is EDGE, not 3G.** The E173 is a 3G stick and Orange is shutting
  3G down, so the station falls back to 2G. Enough for SSH, MQTT, alert text
  and light HTTP; not enough to browse the panel or pull updates.
* **A killed session blocks the next dial.** After a `pppd` that was killed
  without its disconnect chat, every following `ATD*99***1#` answers
  `NO CARRIER` for several minutes — the network still holds the old PDP for
  this SIM. Dial again later; do not keep hammering it. This is also why the
  unit runs pppd with `nodetach` and a real `disconnect` script: a clean
  `ATH0` releases the session immediately.

## Known fragility (measured 2026-11-10, same evening)

Data **is** provisioned on the SIM — the PDP context is granted an address
(`AT+CGACT=1,1` → `+CGPADDR: 1,"10.8.9.63"`) and PPP carried real traffic
(`https://example.com` → 200, ~3.5 kB/s). What is not reliable is repeated
dialling:

* **One clean dial per modem boot.** A `pppd` killed without its disconnect
  chat leaves the session hanging and every following `ATD*99***1#` answers
  `NO CARRIER` for minutes. A USB power cycle (or `AT+CFUN=1,1`) clears it, and
  the next dial succeeds. Never dial twice on the same boot while testing.
* **NDIS is a dead end on this stick.** `AT^NDISDUP=1,1,"internet"` returns OK
  and the `wwan0` interface even completes a DHCP lease, but nothing is
  forwarded: the gateway does not answer, ICMP and TCP both time out. Do not
  build a second path on it.
* **Roles rotate on every USB reset**, so a supervisor that dials for data has
  to rediscover the AT port each time and must not take the one the station
  holds.
* Consequence: unattended operation needs a supervisor that power-cycles the
  modem before each dial attempt. `warnflux-gsm-backup.service` alone survives
  a dropped link, but not a modem that has latched into the `NO CARRIER` state.

## Testing the hooks without touching the host

`go test ./deploy/gsm-backup/` runs both hooks against a throwaway
`resolv.conf`, a fake `ip` and a fake `resolvectl`. The hooks take two
environment overrides used only there: `WARNFLUX_RESOLVCONF` (the file to edit)
`WARNFLUX_GSM_DNS` (the helper to call) and `WARNFLUX_NET_STATE_DIR` /
`WARNFLUX_GSM_STATE_DIR` (the watchdog's and the hooks' state). Neither is set in production — the
installed hooks use `/etc/resolv.conf` and `/usr/local/sbin/warnflux-gsm-dns`.

## Risks worth knowing

* **Data plan.** An always-on PDP context costs almost nothing while idle (LCP
  echoes every 30 s), but a fallen-back station runs its internet-backed sources
  over the SIM: GDDKiA KPD alone is ~1.9 MB per pull (15 min), so a long outage
  is real traffic. Cap it in the operator's panel if the plan is small.
* **IPv4 only.** The peer file negotiates `IP`; IPv6 over the modem is not
  configured.
* **One modem, two callers.** The two sessions must stay on different
  interfaces: whichever port `gsm: modem ready device=…` names belongs to the
  station and must never be dialled by hand or by `pppd`.
