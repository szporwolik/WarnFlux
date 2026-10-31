#!/bin/sh
# Bring every WarnFlux service back after a crash or a reboot — by itself.
#
# A station is expected to run unattended, so "the machine came back but the
# broker did not" is the worst failure mode: the panel looks alive while the
# radio, the broker and the clock are gone. Two things cause it, and this script
# handles both:
#
#   1. STALE CLOCK. The Pi has no RTC, so it wakes up with the time of its last
#      shutdown (observed: 39 hours behind). Anything that validates a
#      certificate at start-up dies immediately — mosquitto with TLS exited 255
#      and stayed down. We therefore wait for a plausible clock before starting
#      the stacks. The station's own discipline process fixes the clock within
#      seconds; the host client (systemd-timesyncd, kept ENABLED by
#      warnflux-clock-guard.sh) fixes it even earlier when the network is up.
#   2. SLOW USB ENUMERATION. A device mapped with `--device` must exist when the
#      container is CREATED, and direwolf exits ("Audio input device 0 error
#      code -19", "Terminating after audio input failure") when the sound card
#      is not there yet. We wait for the device paths before touching the stack.
#
# Order matters: broker -> radio -> clock discipline -> station, because each
# service connects to the previous one at start-up.
set -eu

DOCKER_ROOT="${WARNFLUX_DOCKER_ROOT:-/srv/docker}"
CLOCK_WAIT_SECONDS="${WARNFLUX_CLOCK_WAIT_SECONDS:-120}"
DEVICE_WAIT_SECONDS="${WARNFLUX_DEVICE_WAIT_SECONDS:-60}"
LOG_TAG="warnflux-boot-up"

log() { echo "$LOG_TAG: $*"; }

# wait_for_clock blocks until the system time is plausibly current. A station
# that boots in 1970 or with the time of its last shutdown must not start
# TLS-dependent services: they would fail once and never retry.
wait_for_clock() {
	waited=0
	while [ "$waited" -lt "$CLOCK_WAIT_SECONDS" ]; do
		# The host client (kept ENABLED by the guard) is the earliest source;
		# once it has stepped the clock, certificate-validating services are
		# safe to start.
		if [ "$(timedatectl show -p NTPSynchronized --value 2>/dev/null)" = "yes" ]; then
			log "clock synchronised after ${waited}s"
			return 0
		fi
		year=$(date +%Y)
		if [ "$year" -ge 2024 ] && [ "$waited" -ge 15 ]; then
			# Plausible enough: a station booting without an RTC is hours off,
			# never years. The timer retries anyway if a service still fails.
			log "clock plausible after ${waited}s (year $year, not yet NTP-synchronised)"
			return 0
		fi
		sleep 1
		waited=$((waited + 1))
	done
	log "clock still implausible after ${CLOCK_WAIT_SECONDS}s (year $(date +%Y)) - starting anyway"
	return 1
}

# wait_for_path blocks until a device path exists (it is resolved when a
# container is created, so starting too early fails permanently).
wait_for_path() {
	path="$1"
	waited=0
	while [ "$waited" -lt "$DEVICE_WAIT_SECONDS" ]; do
		if [ -e "$path" ]; then
			return 0
		fi
		sleep 1
		waited=$((waited + 1))
	done
	log "device $path missing after ${DEVICE_WAIT_SECONDS}s - that stack is skipped"
	return 1
}

# compose_up starts one stack, using its device overlay only when the overlay
# exists AND every path it maps is present (a missing device makes `up` fail
# with exit 255 and Docker never retries).
compose_up() {
	stack="$1"
	dir="$DOCKER_ROOT/$stack"
	[ -f "$dir/compose.yml" ] || dir="$dir"
	if [ -f "$DOCKER_ROOT/$stack/compose.yml" ]; then
		file="$DOCKER_ROOT/$stack/compose.yml"
	elif [ -f "$DOCKER_ROOT/$stack/compose.yaml" ]; then
		file="$DOCKER_ROOT/$stack/compose.yaml"
	else
		log "no compose file in $DOCKER_ROOT/$stack - skipped"
		return 0
	fi
	overlay="$DOCKER_ROOT/$stack/compose.devices.yml"
	if [ -f "$overlay" ]; then
		missing=$(sed -n 's#^ *- \(/dev/[^:]*\):.*#\1#p' "$overlay" | while read -r p; do
			[ -e "$p" ] || echo "$p"
		done)
		if [ -n "$missing" ]; then
			log "$stack: devices not present yet ($missing) - waiting"
			for p in $missing; do wait_for_path "$p" || break; done
			missing=$(sed -n 's#^ *- \(/dev/[^:]*\):.*#\1#p' "$overlay" | while read -r p; do
				[ -e "$p" ] || echo "$p"
			done)
		fi
		if [ -z "$missing" ]; then
			(cd "$DOCKER_ROOT/$stack" && docker compose -f "$file" -f "$overlay" up -d) || log "$stack: up with overlay failed"
			return 0
		fi
		log "$stack: starting WITHOUT the device overlay (missing: $missing)"
	fi
	(cd "$DOCKER_ROOT/$stack" && docker compose -f "$file" up -d) || log "$stack: up failed"
}

# restart_unhealthy restarts a station container that Docker reports as
# unhealthy. A restart policy covers exits, not hangs: a process that is alive
# but no longer answering keeps its container "running" forever, and nothing
# else in the system would notice. Names match the container_name values in the
# station's compose files.
restart_unhealthy() {
	for name in $(docker ps --filter health=unhealthy --format '{{.Names}}'); do
		case "$name" in
		mosquitto | direwolf | warnflux | warnflux-time)
			log "$name: unhealthy - restarting"
			docker restart "$name" >/dev/null || log "$name: restart failed"
			;;
		*)
			log "$name: unhealthy but not managed here - left alone"
			;;
		esac
	done
}

log "starting: waiting for a plausible clock"
wait_for_clock || true

# Broker first: the station's receiver, the radio's pub/sub and other hosts
# all connect to it at start-up.
compose_up mqtt
# Radio: needs the AIOC sound card (and its tty for PTT).
compose_up aprs
# Clock discipline: needs the GPS receiver; serves UDP 123 to the LAN.
compose_up warnflux-time
# The station itself.
compose_up warnflux

# Last: anything that came back but stopped answering (see restart_unhealthy).
restart_unhealthy

log "done"
