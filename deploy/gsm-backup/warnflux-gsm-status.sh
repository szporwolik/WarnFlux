#!/bin/sh
# WarnFlux: read-only report of the cellular backup uplink.
# Run it before and after enabling the unit; it changes nothing.
set -u

echo "== unit =="
systemctl is-enabled warnflux-gsm-backup.service 2>/dev/null || echo "not installed"
systemctl is-active warnflux-gsm-backup.service 2>/dev/null || true

echo
echo "== link =="
if ip -brief address show ppp0 >/dev/null 2>&1; then
	ip -brief address show ppp0
else
	echo "ppp0 is down"
fi

echo
echo "== default routes (lower metric wins) =="
ip route show default

echo
echo "== resolvers =="
grep -E '^nameserver' /etc/resolv.conf || echo "none"

echo
echo "== modem ports =="
for dev in /dev/serial/by-id/usb-HUAWEI_Technology_HUAWEI_Mobile-if00-port0 \
	/dev/serial/by-id/usb-HUAWEI_Technology_HUAWEI_Mobile-if03-port0 \
	/dev/serial/by-id/usb-HUAWEI_Technology_HUAWEI_Mobile-if04-port0; do
	[ -e "$dev" ] && echo "present: $dev -> $(readlink -f "$dev")" || echo "missing: $dev"
done

echo
echo "== recent pppd log =="
journalctl -u warnflux-gsm-backup.service -n 12 --no-pager 2>/dev/null || echo "no journal yet"
