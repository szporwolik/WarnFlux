package config

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The station keeps ONE rule for time:
//
//   - storage and logs are UTC (SQLite stores unix milliseconds and
//     RFC3339 "Z" strings, log lines carry a "Z" timestamp);
//   - every human-facing rendering (panel, public pages, mails, notices)
//     uses the station's display zone — app.timezone, the host's zone
//     when that key is unset, UTC when nothing is detectable — and NAMES
//     that zone so a time is never ambiguous;
//   - the radio/mesh/SMS texts and the "WF-…" message id stay UTC and say
//     so (see the Help manual).
//
// The functions below implement the middle bullet: resolution, detection
// and the first-run pin.

// zoneinfoPaths are read in order when TZ does not name a zone.
var zoneinfoPaths = []string{"/etc/timezone", "/etc/localtime"}

// DetectTimezone returns the IANA name of the host's time zone: the TZ
// environment first (containers usually pin it), then /etc/timezone, then
// the /etc/localtime symlink into the zoneinfo database. An undetectable
// host reports "UTC" — an installation must never render a nameless time.
func DetectTimezone() string {
	if tz := strings.TrimSpace(os.Getenv("TZ")); tz != "" {
		if loc, err := time.LoadLocation(tz); err == nil {
			return loc.String()
		}
	}
	for _, path := range zoneinfoPaths {
		if name := zoneFromFile(path); name != "" {
			return name
		}
	}
	return "UTC"
}

// zoneFromFile reads one zoneinfo hint file: /etc/timezone holds the name
// outright, /etc/localtime is usually a symlink into /usr/share/zoneinfo.
func zoneFromFile(path string) string {
	if target, err := filepath.EvalSymlinks(path); err == nil && target != path {
		if name := zoneFromZoneinfoPath(target); name != "" {
			return name
		}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	name := strings.TrimSpace(string(raw))
	// A binary tzfile starts with "TZif"; only /etc/timezone is text.
	if name == "" || strings.HasPrefix(name, "TZif") || strings.ContainsAny(name, "\x00") {
		return ""
	}
	if loc, err := time.LoadLocation(name); err == nil {
		return loc.String()
	}
	return ""
}

// zoneFromZoneinfoPath extracts the IANA name from a zoneinfo path, e.g.
// "/usr/share/zoneinfo/Europe/Warsaw" -> "Europe/Warsaw".
func zoneFromZoneinfoPath(target string) string {
	const marker = "/zoneinfo/"
	i := strings.Index(target, marker)
	if i < 0 {
		return ""
	}
	name := target[i+len(marker):]
	if loc, err := time.LoadLocation(name); err == nil {
		return loc.String()
	}
	return ""
}

// Timezone resolves the station's display zone and its name: the
// configured IANA name when set, the detected host zone otherwise. The
// location never fails — an unusable name degrades to UTC, which is
// reported instead of silently showing a shifted wall clock.
func Timezone(configured string) (*time.Location, string) {
	name := strings.TrimSpace(configured)
	if name == "" {
		name = DetectTimezone()
	}
	if loc, err := time.LoadLocation(name); err == nil {
		return loc, name
	}
	return time.UTC, "UTC"
}

// PinTimezoneIfAbsent writes app.timezone into the configuration file
// when the key is absent, so the zone an installation displays is a
// decision recorded on disk instead of a property of whichever host it
// happens to start on (first run: the detected zone becomes explicit).
// It reports whether the file was written. An empty path, an existing
// key or an unwritable file leave the file alone.
func PinTimezoneIfAbsent(path, zone string) (bool, error) {
	if path == "" || strings.TrimSpace(zone) == "" {
		return false, nil
	}
	if _, ok := ScalarAt(path, "app.timezone"); ok {
		return false, nil
	}
	if err := UpdateFile(path, []ScalarEdit{{Path: "app.timezone", Value: StringScalar(zone)}}); err != nil {
		return false, err
	}
	return true, nil
}
