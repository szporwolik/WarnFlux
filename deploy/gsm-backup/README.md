# GSM backup uplink — internet from the modem, SMS untouched

The station's modem (Huawei E173) is not only an SMS device: it is also the
cheapest possible insurance against the wired uplink dying. This directory turns
the SIM's data connection into a **backup default route** that takes over when
Ethernet stops working, while the panel keeps sending and receiving SMS on the
same modem at the same time.

## Why both work at once

The E173 exposes three serial interfaces:

| Interface | Stable path | Who uses it |
|---|---|---|
| `if00` | `…-if00-port0` → `/dev/ttyUSB0` | **WarnFlux** — AT/PCUI session: SMS receive and send (`/dev/modem` inside the container) |
| `if03` | `…-if03-port0` → `/dev/ttyUSB1` | nobody — the diagnostic port: it silently swallows `AT` (measured) |
| `if04` | `…-if04-port0` → `/dev/ttyUSB2` | **pppd** — the packet data interface: `ATD*99***1#` answers `CONNECT` |

The two sessions are on different interfaces, which is exactly why dialling for
data does not lock the station out of the SIM. (`AT^GETPORTMODE` on the modem
reports the split `MDM:0,NDIS:1,DIAG:2,PCUI:3,…`; measuring which node answers
what put the data pipe on **if04**, not on the `if03` the interface numbering
suggests — `if03` never answered a single byte.)

The data port also does not answer a bare `AT`: the chat script dials straight
away, because a leading `AT` only burns the 30-second timeout and ends in
`Connect script failed`.

## How the routing behaves

* Ethernet keeps the default route with **metric 100**.
* The backup adds its own default with **metric 700**
  (`defaultroute-metric` in the peer file, re-asserted by the `ip-up` script).
* The kernel always prefers the lower metric, so nothing changes while the wire
  works. When the wired route disappears — cable pulled, DHCP lease lost, ISP
  gateway dead — the modem route takes over immediately and silently.
* DNS: NetworkManager owns `/etc/resolv.conf` and names only the wired gateway.
  The `ip-up` script appends the operator's resolver as a **marked fallback
  block**, and `ip-down` removes exactly that block again. Without it a domain
  lookup during an outage would hang until the wired resolver timed out.

## Install

```sh
sudo apt-get install -y ppp          # pppd + chat, if not present
sudo sh deploy/gsm-backup/install.sh # copies files, enables and starts the unit
sudo warnflux-gsm-status             # read-only report: unit, ppp0, routes, DNS
```

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
4. The fallback really works: `ip route get 1.1.1.1` follows Ethernet today.
   Simulating an outage is done by pulling the cable (or
   `sudo ip route del default via <gw>` — restore it afterwards with
   `sudo systemctl restart NetworkManager`).

## Turning it off

```sh
sudo systemctl disable --now warnflux-gsm-backup.service
```

That is the whole rollback: the routes and the DNS block disappear with the
link. The installed files can stay (they do nothing while the unit is disabled),
or be removed by hand.

## Measured on the production modem (2026-11-06) — data is NOT provisioned yet

Everything above is wired and testable, but the operator side currently refuses
the packet call. The measurements, so nobody repeats them:

| Probe | Result |
|---|---|
| `AT+CPIN?` / `AT+CREG?` | `READY`, registered on `POL` (Play), `AT+CSQ` 27 |
| `AT+CGDCONT?` | CID 1, `IP`, APN `internet` — already defined by the SIM |
| `AT+CGATT?` | **0** → the modem does not attach to the packet domain by itself |
| `AT+CGATT=1` | `OK`, then `+CGATT: 1` |
| `AT+CGACT=1,1` | `OK`, `+CGPADDR: 1,"10.x.x.x"` — a PDP context **is** granted |
| `ATD*99***1#`, `ATD*99#`, `AT+CGDATA="PPP",1` | **`NO CARRIER`** every time, on APN `internet` and `play`, IPv4 |
| `wwan0` (NDIS, `cdc_ether`) | comes up, DHCP `DISCOVER` gets no `OFFER`; a static address from `+CGPADDR` pings nothing |

Conclusion: the SIM registers and a PDP context can be activated, but the data
call itself is refused — the classic signature of a **voice/SMS-only provision**
(or data blocked on the M2M tariff). Nothing in the station can change that:

1. ask the operator to enable data on the SIM (and confirm the APN), then
2. re-run the two dial probes above — `CONNECT` is the green light — and
3. `sudo sh deploy/gsm-backup/install.sh`.

The chat script is already the sequence those probes point to — the packet
domain is attached by hand (`AT+CGATT=1`) and then dialled, because this modem
never attaches by itself. Install it with `--no-start` until the operator
confirms data; a running unit on a data-less SIM only repeats `NO CARRIER` in
the journal every 30 seconds:

```sh
sudo sh deploy/gsm-backup/install.sh --no-start
# … ask the operator, confirm CONNECT by hand …
sudo systemctl start warnflux-gsm-backup.service
```

## Risks worth knowing

* **Data plan.** An always-on PDP context costs almost nothing while idle (LCP
  echoes every 30 s), but a fallen-back station runs its internet-backed sources
  over the SIM: GDDKiA KPD alone is ~1.9 MB per pull (15 min), so a long outage
  is real traffic. Cap it in the operator's panel if the plan is small.
* **IPv4 only.** The peer file negotiates `IP`; IPv6 over the modem is not
  configured.
* **One modem, two callers.** `install.sh` never touches the AT interface, but
  do not run `pppd call …` against `if00` by hand: that port belongs to the
  station.
