#!/bin/sh
# stack-up starts a compose project with the optional device overlay.
#
# `devices:` is resolved at CREATE time: if the host path does not exist, Docker
# refuses to create the container (exit 255) and never retries — not even with
# `restart: unless-stopped`. So a station whose AIOC cable or GSM stick is
# unplugged at boot stays dark, together with the panel that could explain why.
#
# This script waits briefly for the device and then starts the stack either way:
# with the overlay when the device is there, without it when it is not (the
# station comes up, logs the missing device and — since 0.8.10 — lists it on the
# panel's health card). The reconcile timer recreates the container once the
# device appears, because a running container never gains a device node.
#
# usage: stack-up.sh <compose-dir> <device-path> [wait-seconds]
set -eu

dir=$1
device=$2
wait=${3:-120}

i=0
while [ ! -e "$device" ] && [ "$i" -lt "$wait" ]; do
	sleep 1
	i=$((i + 1))
done

cd "$dir"
if [ -e "$device" ]; then
	echo "stack-up: $device present — starting with the device overlay"
	exec docker compose -f compose.yml -f compose.devices.yml up -d
fi
echo "stack-up: $device missing — starting WITHOUT the device overlay (the station reports it in the panel)" >&2
exec docker compose -f compose.yml up -d
