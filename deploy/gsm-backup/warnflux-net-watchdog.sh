#!/bin/sh
# WarnFlux: keep the station online across a wired-uplink failure.
#
# Two independent things can break "the internet":
#   1. the link itself — cable pulled, switch down. The kernel drops the wired
#      default route and the cellular route (metric 700) already wins.
#   2. the uplink — the ISP is down while the cable stays up. The wired route
#      never disappears, so the lower metric keeps winning and the station
#      quietly stops reaching the internet while ppp0 sits idle.
#
# Case 2 is what this watchdog exists for: probe the WIRED path on a fixed
# cadence, and while it is dead give the cellular link a route that outranks
# it. The moment the wire answers again, that route is taken back out and
# Ethernet wins by itself — the station never stays on the expensive link.
#
# State lives in /run (volatile on purpose): a reboot re-evaluates from
# scratch instead of trusting a stale decision.
set -u

PRIMARY_IFACE="${PRIMARY_IFACE:-eth0}"
PROBE_HOSTS="${PROBE_HOSTS:-1.1.1.1 8.8.8.8 9.9.9.9}"
CELLULAR_IFACE="${CELLULAR_IFACE:-ppp0}"
DNS_HELPER="${WARNFLUX_GSM_DNS:-/usr/local/sbin/warnflux-gsm-dns}"
# Outranks the wired metric (100) and nothing else: it exists only while the
# wire is dead, so it can never compete with a healthy Ethernet.
FORCED_METRIC="${FORCED_METRIC:-50}"
# Two probes, ~1 s each: a single lost packet must not move the station over.
PING_COUNT="${PING_COUNT:-2}"
PING_WAIT="${PING_WAIT:-2}"
# Consecutive verdicts needed before acting — flap protection. With a 30 s
# timer this is 90 s of real outage before the switch, and 90 s of recovery.
FAIL_THRESHOLD="${FAIL_THRESHOLD:-3}"
OK_THRESHOLD="${OK_THRESHOLD:-3}"

STATE_DIR="/run/warnflux-net-watchdog"
FAIL_FILE="$STATE_DIR/fail_streak"
OK_FILE="$STATE_DIR/ok_streak"
MODE_FILE="$STATE_DIR/mode"
mkdir -p "$STATE_DIR"

log() { logger -t warnflux-net -- "$*"; }

read_int() {
	[ -f "$1" ] && cat "$1" 2>/dev/null || echo 0
}

write_int() { echo "$1" >"$2"; }

wired_answers() {
	for host in $PROBE_HOSTS; do
		# -I pins the probe to the wire: a cellular route that already won
		# must never be mistaken for the wired path coming back.
		if ping -c "$PING_COUNT" -W "$PING_WAIT" -I "$PRIMARY_IFACE" "$host" >/dev/null 2>&1; then
			return 0
		fi
	done
	return 1
}

cellular_has_default() {
	ip route show default | grep -q "dev $CELLULAR_IFACE"
}

# Give the cellular link a route above the wired metric (idempotent).
#
# The resolver order follows the route. The operator's resolvers answer on the
# mobile network only, so while the wire is dead the wired one at the top of
# /etc/resolv.conf is a black hole: every lookup would wait out its timeout
# before falling through to a server that answers.
raise_cellular() {
	ip route replace default dev "$CELLULAR_IFACE" metric "$FORCED_METRIC" 2>/dev/null || true
	"$DNS_HELPER" prefer "$CELLULAR_IFACE" cellular 2>/dev/null || true
}

# Take back exactly the route this script added; pppd's own metric-700 route
# stays, so the backup is still one probe away from being used.
lower_cellular() {
	ip route del default dev "$CELLULAR_IFACE" metric "$FORCED_METRIC" 2>/dev/null || true
	"$DNS_HELPER" prefer "$CELLULAR_IFACE" wired 2>/dev/null || true
}

mode=$(cat "$MODE_FILE" 2>/dev/null || echo primary)

if wired_answers; then
	write_int 0 "$FAIL_FILE"
	write_int $(($(read_int "$OK_FILE") + 1)) "$OK_FILE"
	if [ "$mode" != "primary" ] && [ "$(read_int "$OK_FILE")" -ge "$OK_THRESHOLD" ]; then
		lower_cellular
		echo primary >"$MODE_FILE"
		log "wired uplink is back after $(read_int "$OK_FILE") checks — Ethernet wins again, cellular route demoted"
	fi
	exit 0
fi

write_int 0 "$OK_FILE"
write_int $(($(read_int "$FAIL_FILE") + 1)) "$FAIL_FILE"
if [ "$(read_int "$FAIL_FILE")" -lt "$FAIL_THRESHOLD" ]; then
	exit 0
fi

if ! cellular_has_default; then
	# The modem link is not up yet: say so once per outage instead of every
	# 30 seconds, and leave the routing untouched.
	if [ "$mode" != "no-cellular" ]; then
		echo no-cellular >"$MODE_FILE"
		log "wired uplink is down and the cellular link is not up — no backup route yet (check warnflux-gsm-backup.service)"
	fi
	exit 1
fi

raise_cellular
if [ "$mode" != "cellular" ]; then
	echo cellular >"$MODE_FILE"
	log "wired uplink is down after $(read_int "$FAIL_FILE") checks — cellular default raised to metric $FORCED_METRIC"
fi
exit 0
