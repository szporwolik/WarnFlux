package preflight

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type logSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *logSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func newTestMonitor(t *testing.T) (*Monitor, *logSink) {
	t.Helper()
	sink := &logSink{}
	m := New(slog.New(slog.NewTextHandler(sink, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return m, sink
}

// TestMissingDeviceIsReportedOnce pins the episode policy: a missing device is
// one WARN, not one per probe — the same rule the source plugins follow.
func TestMissingDeviceIsReportedOnce(t *testing.T) {
	m, logs := newTestMonitor(t)
	m.Add("gsm.modem", SeverityBad, DevicePath("/nonexistent/ttyUSB0"))

	for i := 0; i < 5; i++ {
		m.Probe()
	}
	active := m.Active()
	if len(active) != 1 {
		t.Fatalf("active = %d, want 1 finding", len(active))
	}
	if active[0].Key != "gsm.modem" || active[0].Severity != SeverityBad {
		t.Fatalf("finding = %+v", active[0])
	}
	if !strings.Contains(active[0].Detail, "/nonexistent/ttyUSB0") {
		t.Errorf("detail = %q, want the device path", active[0].Detail)
	}
	if n := strings.Count(logs.String(), "level=WARN"); n != 1 {
		t.Fatalf("warnings = %d, want exactly 1 for the whole episode:\n%s", n, logs.String())
	}
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("a missing device must never be fatal:\n%s", logs.String())
	}
}

// TestDeviceAppearingClearsTheFinding pins the other half: the probe keeps
// running, so a device plugged in after start clears its own finding and the
// operator reads one INFO about it.
func TestDeviceAppearingClearsTheFinding(t *testing.T) {
	dir := t.TempDir()
	device := filepath.Join(dir, "ttyUSB0")

	m, logs := newTestMonitor(t)
	m.Add("gsm.modem", SeverityBad, DevicePath(device))
	m.Probe()
	if len(m.Active()) != 1 {
		t.Fatalf("missing device not reported")
	}

	if err := os.WriteFile(device, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	m.Probe()
	if got := m.Active(); len(got) != 0 {
		t.Fatalf("finding survived the device appearing: %+v", got)
	}
	if !strings.Contains(logs.String(), "dependency available again") {
		t.Fatalf("recovery not logged:\n%s", logs.String())
	}
}

// TestActiveIsEmptyWithoutFindings pins the healthy state: no findings means no
// rows, and the panel shows "all present" instead of an empty section.
func TestActiveIsEmptyWithoutFindings(t *testing.T) {
	dir := t.TempDir()
	device := filepath.Join(dir, "ttyACM0")
	if err := os.WriteFile(device, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	m, _ := newTestMonitor(t)
	m.Add("meshtastic.device", SeverityBad, DevicePath(device))
	m.Probe()
	if got := m.Active(); len(got) != 0 {
		t.Fatalf("active = %+v, want none", got)
	}
}

// TestDevicePathsAcceptsAnyCandidate pins the Meshtastic case: the hub tries a
// comma-separated list, so the dependency is satisfied by any one path.
func TestDevicePathsAcceptsAnyCandidate(t *testing.T) {
	dir := t.TempDir()
	present := filepath.Join(dir, "ttyACM0")
	if err := os.WriteFile(present, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	p := DevicePaths(filepath.Join(dir, "ttyACM1") + " , " + present)
	if err := p(); err != nil {
		t.Fatalf("probe = %v, want nil (one candidate exists)", err)
	}
	if err := DevicePaths(filepath.Join(dir, "nope") + "," + filepath.Join(dir, "also-nope"))(); err == nil {
		t.Fatal("probe = nil, want an error when every candidate is missing")
	}
	if err := DevicePaths("")(); err == nil {
		t.Fatal("probe = nil, want an error when nothing is configured")
	}
}

// TestRunKeepsProbing pins the loop: Run picks up a device that appears after
// start without anyone restarting the station.
func TestRunKeepsProbing(t *testing.T) {
	dir := t.TempDir()
	device := filepath.Join(dir, "ttyUSB0")
	m, _ := newTestMonitor(t)
	m.interval = 5 * time.Millisecond
	m.Add("gsm.modem", SeverityBad, DevicePath(device))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()

	deadline := time.Now().Add(2 * time.Second)
	for len(m.Active()) == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if len(m.Active()) != 1 {
		t.Fatal("Run did not report the missing device")
	}
	if err := os.WriteFile(device, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for len(m.Active()) != 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if len(m.Active()) != 0 {
		t.Fatal("Run did not clear the finding after the device appeared")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after its context was cancelled")
	}
}

// TestNilProbeIsIgnored pins the wiring rule: a dependency that is not
// configured (no device path) is not a finding.
func TestNilProbeIsIgnored(t *testing.T) {
	m, _ := newTestMonitor(t)
	m.Add("gsm.modem", SeverityBad, nil)
	m.Probe()
	if got := m.Active(); len(got) != 0 {
		t.Fatalf("active = %+v, want none", got)
	}
	// A nil monitor is what the server sees before wiring: it must not panic
	// and must report nothing.
	var nilMon *Monitor
	if got := nilMon.Active(); got != nil {
		t.Fatalf("nil monitor reported %+v", got)
	}
}

// TestProbeErrorIsCarriedIntoTheFinding pins the detail text: the operator
// must see what exactly failed, not just "unavailable".
func TestProbeErrorIsCarriedIntoTheFinding(t *testing.T) {
	m, _ := newTestMonitor(t)
	m.Add("custom", SeverityWarn, func() error { return errors.New("dial tcp: connection refused") })
	m.Probe()
	active := m.Active()
	if len(active) != 1 || active[0].Severity != SeverityWarn {
		t.Fatalf("active = %+v", active)
	}
	if active[0].Detail != "dial tcp: connection refused" {
		t.Fatalf("detail = %q", active[0].Detail)
	}
	if active[0].Since.IsZero() {
		t.Fatal("since not stamped")
	}
}
