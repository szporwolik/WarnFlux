package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestTimezoneResolution pins the one rule the station follows: a
// configured IANA name wins, an unset key falls back to the host zone
// (detected, never nameless) and an unusable name degrades to UTC —
// reported as UTC instead of a silently shifted wall clock.
func TestTimezoneResolution(t *testing.T) {
	if loc, name := Timezone("Europe/Warsaw"); name != "Europe/Warsaw" || loc.String() != "Europe/Warsaw" {
		t.Errorf("configured zone = %q/%q, want Europe/Warsaw", name, loc)
	}
	if _, name := Timezone("Not/AZone"); name != "UTC" {
		t.Errorf("unusable zone name = %q, want UTC", name)
	}
	// Unset: the detected host zone, whatever it is (TZ or the host files).
	t.Setenv("TZ", "Europe/Warsaw")
	if loc, name := Timezone(""); name != "Europe/Warsaw" || loc.String() != "Europe/Warsaw" {
		t.Errorf("unset zone with TZ=Europe/Warsaw = %q/%q", name, loc)
	}
	if got := DetectTimezone(); got != "Europe/Warsaw" {
		t.Errorf("DetectTimezone with TZ = %q, want Europe/Warsaw", got)
	}
	// A host without any hint: UTC, never a nameless time.
	t.Setenv("TZ", "")
	zoneinfoPaths = nil
	t.Cleanup(func() { zoneinfoPaths = []string{"/etc/timezone", "/etc/localtime"} })
	if got := DetectTimezone(); got != "UTC" {
		t.Errorf("DetectTimezone without hints = %q, want UTC", got)
	}
}

// TestPinTimezoneIfAbsent pins the first-run decision: an unset key is
// written once so the displayed zone is recorded on disk; an existing key
// or an unwritable path is left alone.
func TestPinTimezoneIfAbsent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("app:\n  log_level: info\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	written, err := PinTimezoneIfAbsent(path, "Europe/Warsaw")
	if err != nil || !written {
		t.Fatalf("pin = (%v, %v), want written", written, err)
	}
	if got, ok := ScalarAt(path, "app.timezone"); !ok || got != "Europe/Warsaw" {
		t.Fatalf("app.timezone = %q (%v), want Europe/Warsaw", got, ok)
	}
	// Idempotent: a second run leaves the file alone.
	if written, err = PinTimezoneIfAbsent(path, "UTC"); err != nil || written {
		t.Fatalf("second pin = (%v, %v), want untouched", written, err)
	}
	if got, _ := ScalarAt(path, "app.timezone"); got != "Europe/Warsaw" {
		t.Errorf("an existing key was rewritten: %q", got)
	}
	// No file at all: nothing to do.
	if written, err := PinTimezoneIfAbsent("", "UTC"); err != nil || written {
		t.Errorf("empty path = (%v, %v), want no-op", written, err)
	}
	_ = time.UTC
}

// TestPinLocalZone pins the reported drift: the daily anchors (the beacon and
// the notice lines) are documented as "station local time" but were resolved
// with time.Now(), which follows the HOST zone — a distroless container has
// none, so "06:00" fired at 06:00 UTC (08:00 in Poland in summer). The
// station's zone must become the process's local zone.
func TestPinLocalZone(t *testing.T) {
	previous := time.Local
	t.Cleanup(func() { time.Local = previous })

	loc, name := PinLocalZone("Europe/Warsaw")
	if name != "Europe/Warsaw" || loc.String() != "Europe/Warsaw" {
		t.Fatalf("PinLocalZone = (%v, %q), want Europe/Warsaw", loc, name)
	}
	if time.Local != loc {
		t.Errorf("process local zone = %v, want the station zone %v", time.Local, loc)
	}
	// The reading the anchors are built from: it must be the station's
	// wall clock, whatever the host is (this test may run anywhere).
	if got, want := time.Now().Location().String(), loc.String(); got != want {
		t.Errorf("time.Now() zone = %q, want %q", got, want)
	}
	// The anchor itself, as the scheduler resolves it.
	anchor, err := time.Parse("15:04", "06:00")
	if err != nil {
		t.Fatalf("parse anchor: %v", err)
	}
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day(), anchor.Hour(), anchor.Minute(), 0, 0, now.Location())
	if next.Before(now) {
		next = next.AddDate(0, 0, 1)
	}
	if got := next.In(loc).Format("15:04"); got != "06:00" {
		t.Errorf("anchor resolves to %s station time, want 06:00", got)
	}
	// No configured zone keeps the detected host zone (the historical
	// behaviour) instead of failing.
	if detected, _ := PinLocalZone(""); detected == nil {
		t.Error("PinLocalZone(\"\") returned no zone")
	}
}
