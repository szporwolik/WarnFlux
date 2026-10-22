#!/bin/sh
# stack-reconcile keeps the containers' device mappings in step with the host.
#
# A container only has the device nodes it was CREATED with, so a stick plugged
# in after start is invisible until the container is recreated. This script
# compares "device present on the host" with "device mapped into the container"
# and recreates the service only when they disagree — no needless restarts.
#
# usage: stack-reconcile.sh <compose-dir> <device-path> <service>
set -eu

dir=$1
device=$2
service=$3

present=0
[ -e "$device" ] && present=1

mapped=0
if docker inspect -f '{{range .HostConfig.Devices}}{{.PathOnHost}} {{end}}' "$service" 2>/dev/null |
	grep -qF -- "$device"; then
	mapped=1
fi

[ "$present" = "$mapped" ] && exit 0

cd "$dir"
if [ "$present" = 1 ]; then
	echo "stack-reconcile: $device appeared — recreating $service with the device overlay"
	exec docker compose -f compose.yml -f compose.devices.yml up -d "$service"
fi
echo "stack-reconcile: $device vanished — recreating $service without the device overlay"
exec docker compose -f compose.yml up -d "$service"
