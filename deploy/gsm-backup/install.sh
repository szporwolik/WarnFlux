#!/bin/sh
# WarnFlux: install (or repair) the cellular backup uplink.
#
# Run as root on the station host. It is idempotent: running it twice changes
# nothing the second time.
#
#   sudo sh deploy/gsm-backup/install.sh            # install + start
#   sudo sh deploy/gsm-backup/install.sh --no-start # install only
#
# Rollback is in README.md ("Turning it off") and touches only the unit.
set -eu

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
start=1
[ "${1:-}" = "--no-start" ] && start=0

if [ "$(id -u)" -ne 0 ]; then
	echo "install.sh must run as root (it writes /etc and systemd units)" >&2
	exit 1
fi
if ! command -v /usr/sbin/pppd >/dev/null 2>&1; then
	echo "pppd is missing: apt-get install -y ppp" >&2
	exit 1
fi

install -m 0644 "$here/warnflux-gsm.chat" /etc/ppp/warnflux-gsm.chat
install -m 0644 "$here/warnflux-gsm-off.chat" /etc/ppp/warnflux-gsm-off.chat
install -m 0644 "$here/warnflux-gsm.peers" /etc/ppp/peers/warnflux-gsm
install -m 0755 "$here/warnflux-gsm-dns" /usr/local/sbin/warnflux-gsm-dns
install -m 0755 "$here/90-warnflux-gsm" /etc/ppp/ip-up.d/90-warnflux-gsm
install -m 0755 "$here/90-warnflux-gsm-down" /etc/ppp/ip-down.d/90-warnflux-gsm
install -m 0644 "$here/warnflux-gsm-backup.service" /etc/systemd/system/warnflux-gsm-backup.service
install -m 0755 "$here/warnflux-gsm-status.sh" /usr/local/sbin/warnflux-gsm-status
install -m 0755 "$here/warnflux-net-watchdog.sh" /usr/local/sbin/warnflux-net-watchdog
install -m 0644 "$here/warnflux-net-watchdog.service" /etc/systemd/system/warnflux-net-watchdog.service
install -m 0644 "$here/warnflux-net-watchdog.timer" /etc/systemd/system/warnflux-net-watchdog.timer
install -d -m 0755 /etc/warnflux
install -m 0644 "$here/README.md" /etc/warnflux/gsm-backup.md

systemctl daemon-reload
systemctl enable warnflux-gsm-backup.service >/dev/null
systemctl enable warnflux-net-watchdog.timer >/dev/null

if [ "$start" -eq 1 ]; then
	systemctl restart warnflux-gsm-backup.service
	systemctl start warnflux-net-watchdog.timer
	sleep 3
	systemctl --no-pager --lines=0 status warnflux-gsm-backup.service || true
fi

echo
echo "Installed. Verify with: sudo warnflux-gsm-status"
echo "The station's SMS session is untouched: it owns the AT interface (if00)."
echo "Both hooks act only for ipparam warnflux-gsm, so other PPP links are left alone."
echo "The watchdog probes the wired uplink every 30 s and only then raises the cellular route."
