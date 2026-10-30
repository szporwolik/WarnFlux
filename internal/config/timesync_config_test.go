package config

import (
	"strings"
	"testing"
	"time"
)

// TestTimeSyncAbsent: a station that never configured the clock discipline
// keeps working exactly as before — the feature is opt-in and costs nothing.
func TestTimeSyncAbsent(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, "app:\n  log_level: info\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.TimeSync.Enabled {
		t.Fatal("time_sync must stay off when the section is absent")
	}
}

// TestTimeSyncExplicit pins what an operator actually gets from the snippet in
// the docs: the block switches the feature on and the omitted keys keep their
// defaults.
func TestTimeSyncExplicit(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, `
app:
  log_level: info
time_sync:
  ntp_servers: ["pool.ntp.org", " "]
  gps_device: "/dev/ttyACM0"
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	ts := cfg.TimeSync
	if !ts.Enabled {
		t.Fatal("a present time_sync block must switch the feature on")
	}
	if len(ts.NTPServers) != 1 || ts.NTPServers[0] != "pool.ntp.org" {
		t.Fatalf("ntp_servers = %v, want the blank entry dropped", ts.NTPServers)
	}
	if ts.GPSDevice != "/dev/ttyACM0" {
		t.Fatalf("gps_device = %q", ts.GPSDevice)
	}
	if ts.Interval != TimeSyncDefaultInterval || ts.GPSBaud != TimeSyncDefaultGPSBaud ||
		ts.StepThreshold != TimeSyncDefaultStepThreshold || ts.ServeListen != TimeSyncDefaultListen {
		t.Fatalf("defaults = %+v", ts)
	}
	if !ts.Serve {
		t.Fatal("serving the time to the LAN must be on by default (off-grid is the point)")
	}
}

// TestTimeSyncValidation keeps a half-configured feature from starting a
// process that can never discipline anything.
func TestTimeSyncValidation(t *testing.T) {
	if _, err := Load(writeTempConfig(t, `
app:
  log_level: info
time_sync:
  enabled: true
`)); err == nil || !strings.Contains(err.Error(), "ntp_servers") {
		t.Fatalf("no source = %v, want a configuration error", err)
	}
	if _, err := Load(writeTempConfig(t, `
app:
  log_level: info
time_sync:
  interval: 10s
  ntp_servers: ["pool.ntp.org"]
`)); err == nil || !strings.Contains(err.Error(), "interval") {
		t.Fatalf("tiny interval = %v, want a configuration error", err)
	}
	// Switched off explicitly: an empty block must not be an error.
	cfg, err := Load(writeTempConfig(t, `
app:
  log_level: info
time_sync:
  enabled: false
`))
	if err != nil {
		t.Fatalf("disabled block: %v", err)
	}
	if cfg.TimeSync.Enabled {
		t.Fatal("enabled: false must switch the feature off")
	}
}

// TestTimeSyncIntervalAcceptsHours documents the unit the operator writes.
func TestTimeSyncIntervalAcceptsHours(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, `
app:
  log_level: info
time_sync:
  interval: 12h
  ntp_servers: ["pool.ntp.org"]
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.TimeSync.Interval != 12*time.Hour {
		t.Fatalf("interval = %s, want 12h", cfg.TimeSync.Interval)
	}
}
