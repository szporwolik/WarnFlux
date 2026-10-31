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
#   3. RENUMBERED DEVICES. Waiting is not enough: the container keeps the node it
#      was created with, so after a re-enumeration it talks to a device that no
#      longer exists (ttyACM0 became ttyACM1, hidraw0 became hidraw1). Stable
#      names (by-id, the warnflux-ptt udev rule) keep the mapping valid, and the
#      fingerprint stored per stack recreates the container when the node behind
#      a mapped name has moved.
#   4. WEDGED CONTAINERS. A restart policy covers exits, not hangs; the pass ends
#      by restarting its own containers that Docker reports as unhealthy.
#
# Order matters: broker -> radio -> clock discipline -> station, because each
# service connects to the previous one at start-up.
set -eu

DOCKER_ROOT="${WARNFLUX_DOCKER_ROOT:-/srv/docker}"
CLOCK_WAIT_SECONDS="${WARNFLUX_CLOCK_WAIT_SECONDS:-120}"
DEVICE_WAIT_SECONDS="${WARNFLUX_DEVICE_WAIT_SECONDS:-60}"
# Where the device-mapping fingerprints live (see devices_fingerprint).
STATE_DIR="${WARNFLUX_STATE_DIR:-/var/lib/warnflux}"
# The AIOC cable's HID interface, addressed by a stable name instead of
# /dev/hidrawN (see deploy/udev/70-warnflux-ptt.rules).
PTT_LINK=/dev/warnflux-ptt
PTT_HID_ID="${WARNFLUX_PTT_HID_ID:-00001209:00007388}"
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

# ensure_ptt_link gives the AIOC HID interface a name that survives a
# re-enumeration. The udev rule (deploy/udev/70-warnflux-ptt.rules) is the
# canonical way to create it; this is the safety net for a host where the rule
# was never installed, so the device overlay does not depend on it.
#
# A dangling link counts as absent: udev repoints the link when the cable comes
# back, and if udev is not there, the old target is gone and we rebuild it.
ensure_ptt_link() {
	[ -e "$PTT_LINK" ] && return 0
	for hid in /sys/class/hidraw/hidraw*; do
		[ -e "$hid" ] || continue
		if grep -qF "$PTT_HID_ID" "$hid/device/uevent" 2>/dev/null; then
			node="/dev/$(basename "$hid")"
			if ln -sf "$node" "$PTT_LINK"; then
				log "created $PTT_LINK -> $node"
				return 0
			fi
		fi
	done
	log "$PTT_LINK: AIOC HID interface not found"
	return 1
}

# devices_fingerprint describes what the host paths in a device overlay resolve
# to RIGHT NOW: the node behind each (possibly stable) name plus its major:minor.
#
# A container keeps the device node it was CREATED with, so a re-enumeration
# silently leaves it talking to a node that no longer exists. Comparing the
# stored fingerprint with a fresh one is how the pass notices.
devices_fingerprint() {
	sed -n 's#^ *- \(/dev/[^:]*\):.*#\1#p' "$1" | while read -r path; do
		target=$(readlink -f "$path" 2>/dev/null || true)
		if [ -n "$target" ] && [ -e "$target" ]; then
			printf '%s=%s:%s\n' "$path" "$target" "$(stat -Lc '%t:%T' "$target" 2>/dev/null)"
		else
			printf '%s=missing\n' "$path"
		fi
	done | sort
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
			# Recreate only when a mapped device now resolves somewhere else,
			# never on a plain pass.
			extra=""
			state_file="$STATE_DIR/$stack.devices"
			if [ -n "$STATE_DIR" ] && [ -f "$state_file" ]; then
				if ! devices_fingerprint "$overlay" | cmp -s - "$state_file"; then
					log "$stack: a mapped device changed since the container was created - recreating"
					extra="--force-recreate"
				fi
			fi
			if (cd "$DOCKER_ROOT/$stack" && docker compose -f "$file" -f "$overlay" up -d $extra); then
				if [ -n "$STATE_DIR" ]; then
					devices_fingerprint "$overlay" >"$state_file" 2>/dev/null || true
				fi
			else
				log "$stack: up with overlay failed"
			fi
			return 0
		fi
		log "$stack: starting WITHOUT the device overlay (missing: $missing)"
	fi
	(cd "$DOCKER_ROOT/$stack" && docker compose -f "$file" up -d) || log "$stack: up failed"
}

# restart_unhealthy restarts a station container that Docker reports as
# unhealthy. A restart policy covers exits, not hangs: a process that is alive
# but no longer answering keeps its container "running" forever, and nothing
# else in the system would notice.
#
# Ownership is decided by the compose PROJECT label, not by the container name:
# when a name is momentarily taken during a recreate, compose falls back to a
# generated name (f36e42987682_warnflux) and a name-based rule would silently
# stop managing it.
restart_unhealthy() {
	for name in $(docker ps --filter health=unhealthy --format '{{.Names}}'); do
		project=$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "$name" 2>/dev/null || true)
		case "$project" in
		mqtt | aprs | warnflux | warnflux-time)
			log "$name: unhealthy (project $project) - restarting"
			docker restart "$name" >/dev/null || log "$name: restart failed"
			;;
		*)
			log "$name: unhealthy but not managed here - left alone"
			;;
		esac
	done
}

# record_boot appends one line per boot to the station's own history. The host
# journal on Raspberry Pi OS is volatile (Storage=volatile, to spare the SD
# card), so an unattended reboot leaves nothing behind to look at — and "did it
# come back by itself, and did the hardware complain?" is exactly what matters
# on a station nobody watches. The line records the boot id, how long the
# machine had been up when this pass ran, the firmware's throttle flags
# (undervoltage survives in them until the power is really cut) and how many
# dmesg lines look like a hardware event.
record_boot() {
	[ -n "$STATE_DIR" ] || return 0
	boot_id=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo unknown)
	seen=""
	[ -f "$STATE_DIR/boot_id" ] && seen=$(cat "$STATE_DIR/boot_id" 2>/dev/null || true)
	[ "$boot_id" = "$seen" ] && return 0
	throttled=$(vcgencmd get_throttled 2>/dev/null || echo "throttled=n/a")
	# Only real trouble: the panel's own dmesg mentions the watchdog and the
	# thermal governor at every boot without anything being wrong.
	alerts=$(dmesg 2>/dev/null | grep -icE "under-voltage|oops|kernel panic|BUG: |thermal.*(critical|throttl)|emergency" || true)
	history="$STATE_DIR/boots.log"
	printf '%s boot=%s uptime=%ss %s dmesg_alerts=%s\n' \
		"$(date -Is)" "$boot_id" "$(cut -d. -f1 /proc/uptime)" "$throttled" "$alerts" >>"$history"
	echo "$boot_id" >"$STATE_DIR/boot_id"
	# Keep the file readable: one line per boot, a few months is plenty.
	tail -n 200 "$history" >"$history.tmp" 2>/dev/null && mv "$history.tmp" "$history" 2>/dev/null || true
	log "boot recorded in $history ($throttled, dmesg alerts: $alerts)"
}

log "starting: waiting for a plausible clock"
wait_for_clock || true

# State for the device fingerprints and the boot history; without it the pass
# still starts stacks, it just cannot tell that a mapped node moved.
if ! mkdir -p "$STATE_DIR" 2>/dev/null; then
	log "cannot use $STATE_DIR for device fingerprints - recreations disabled"
	STATE_DIR=""
fi
record_boot || true

# Broker first: the station's receiver, the radio's pub/sub and other hosts
# all connect to it at start-up.
compose_up mqtt
# Radio: needs the AIOC sound card (and its tty for PTT).
ensure_ptt_link || true
compose_up aprs
# Clock discipline: needs the GPS receiver; serves UDP 123 to the LAN.
compose_up warnflux-time
# The station itself.
compose_up warnflux

# Last: anything that came back but stopped answering (see restart_unhealthy).
restart_unhealthy

log "done"
