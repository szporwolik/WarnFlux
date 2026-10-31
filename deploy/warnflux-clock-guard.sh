#!/bin/sh
# Keep exactly one clock disciplinarian on the host.
#
# The station owns the clock (time_sync): its discipline process runs in the
# warnflux-time container and steps the host clock through CAP_SYS_TIME. The
# host's own systemd-timesyncd must NOT run at the same time — two
# disciplinarians fight over one clock.
#
# But if that container dies (or hangs: no pass for longer than its interval),
# the host is left with a free-running clock and nobody notices until every
# published warning carries a wrong timestamp. This guard is the fallback: it
# watches the discipline process and hands the clock back to the host's own NTP
# client while the station cannot keep it.
#
# Called by warnflux-clock-guard.timer; safe to run by hand.
set -eu

CONTAINER="${WARNFLUX_CLOCK_CONTAINER:-warnflux-time}"
STATUS_FILE="${WARNFLUX_CLOCK_STATUS:-/srv/docker/warnflux/data/time-sync.json}"
# A pass is overdue once next_at is older than this. The grace covers a pass
# that takes a while (GPS acquisition) and a clock that is being corrected.
GRACE_MIN="${WARNFLUX_CLOCK_GRACE_MIN:-15}"

log() { echo "warnflux-clock-guard: $*"; }

container_running() {
	docker inspect --format '{{.State.Running}}' "$CONTAINER" 2>/dev/null | grep -qx true
}

# next_pass_epoch prints the epoch second of the next scheduled pass, or
# nothing when the status file is missing/unreadable (never ran, or the
# process cannot write it — both mean "nobody is disciplining the clock").
next_pass_epoch() {
	[ -r "$STATUS_FILE" ] || return 0
	next_at=$(sed -n 's/.*"next_at"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$STATUS_FILE" 2>/dev/null | head -n 1)
	[ -n "$next_at" ] || return 0
	date -d "$next_at" +%s 2>/dev/null || true
}

timesyncd_active() { systemctl is-active --quiet systemd-timesyncd; }

station_owns_clock() {
	container_running || { log "container $CONTAINER is not running"; return 1; }
	due=$(next_pass_epoch)
	[ -n "$due" ] || { log "no readable status file ($STATUS_FILE)"; return 1; }
	now=$(date +%s)
	if [ "$due" -lt $((now - GRACE_MIN * 60)) ]; then
		log "last scheduled pass is overdue (next_at $(date -d "@$due" '+%Y-%m-%d %H:%M:%S'))"
		return 1
	fi
	return 0
}

if station_owns_clock; then
	if timesyncd_active; then
		# STOP, never disable: the host client must run again at every boot,
		# because a station without an RTC wakes up with a stale clock and
		# every service that needs correct time (TLS certificates, message
		# timestamps) starts before the discipline process can step it. It is
		# stopped here only while the station keeps the clock itself.
		systemctl stop systemd-timesyncd
		log "the station disciplines the clock - host systemd-timesyncd stopped (still enabled for the next boot)"
	fi
	exit 0
fi

# The station cannot keep the clock: the host takes over. timesyncd is pointed
# at the ordinary NTP pool (deploy/systemd/timesyncd-warnflux-fallback.conf),
# never at the station's own server — while the station is gone that server is
# gone too.
if timesyncd_active; then
	exit 0
fi
systemctl enable systemd-timesyncd >/dev/null 2>&1 || true
systemctl start systemd-timesyncd
log "the station cannot keep the clock - host systemd-timesyncd enabled as fallback"
