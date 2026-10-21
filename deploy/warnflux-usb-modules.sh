#!/bin/sh
# ensure-usb-modules keeps the USB serial/audio drivers available to the
# containers.
#
# A kernel whose linux-modules-extra package is missing ships no
# cdc_acm/usbserial/option/snd-usb-audio, so /dev/ttyUSB0 and the AIOC cable
# never appear and both containers fail at CREATE time (exit 255) — Docker does
# not retry create failures. A host on the linux-virtual metapackage does not
# pull linux-modules-extra, so a freshly installed kernel can arrive without it
# (exactly what happened on the production VM on 2026-10-21).
#
# Installing the package for the RUNNING kernel here makes every reboot heal
# itself without switching the whole host to linux-generic.
set -u

krel=$(uname -r)
missing=""
for m in cdc_acm snd-usb-audio option; do
	if ! modinfo -n "$m" >/dev/null 2>&1; then
		missing="$missing $m"
	fi
done

if [ -n "$missing" ]; then
	echo "ensure-usb-modules: missing$missing — installing linux-modules-extra-$krel"
	DEBIAN_FRONTEND=noninteractive apt-get install -y "linux-modules-extra-$krel" || echo "ensure-usb-modules: install failed" >&2
	depmod -a "$krel" || true
fi

modprobe cdc_acm || true
modprobe option || true
modprobe usbserial || true
modprobe snd_usb_audio || true
udevadm trigger || true
udevadm settle --timeout=20 || true
exit 0
