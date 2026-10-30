package timesync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeClock is the station clock the tests step. Guarded by a mutex: the
// daemon runs in its own goroutine while the test polls for the step.
type fakeClock struct {
	mu    sync.Mutex
	t     time.Time
	steps []time.Time
	err   error
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Set(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.t = t
	c.steps = append(c.steps, t)
	return nil
}

// Steps returns the applied steps.
func (c *fakeClock) Steps() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.steps...)
}

// fakeGPS replays NMEA lines; an empty list means the receiver stayed silent.
type fakeGPS struct {
	lines []string
	i     int
}

func (g *fakeGPS) ReadLine() (string, error) {
	if g.i >= len(g.lines) {
		return "", io.EOF
	}
	line := g.lines[g.i]
	g.i++
	return line, nil
}

func (g *fakeGPS) Close() error { return nil }

// rmc builds a valid RMC sentence for t, so the parser accepts what the test
// pretends the receiver said.
func rmc(t time.Time) string {
	body := fmt.Sprintf("GNRMC,%02d%02d%02d.00,A,5002.08,N,02013.04,E,0.03,0.0,%02d%02d%02d,,,A",
		t.Hour(), t.Minute(), t.Second(), t.Day(), int(t.Month()), t.Year()%100)
	var sum byte
	for i := 0; i < len(body); i++ {
		sum ^= body[i]
	}
	return fmt.Sprintf("$%s*%02X", body, sum)
}

func newTestDaemon(t *testing.T, cfg Config, clock Clock, gps *fakeGPS) *Daemon {
	t.Helper()
	d := NewDaemon(cfg, clock, nil)
	if gps != nil {
		d.openGPS = func(string, int) (SampleSource, error) { return gps, nil }
	} else {
		d.openGPS = func(string, int) (SampleSource, error) {
			t.Fatal("the GPS receiver must not be opened when NTP answers")
			return nil, errors.New("unreachable")
		}
	}
	return d
}

// TestPassPrefersNTP: the network source is tried first, and a successful
// answer steps the clock by exactly the measured offset.
func TestPassPrefersNTP(t *testing.T) {
	base := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: base}
	d := newTestDaemon(t, Config{
		NTPServers:    []string{"pool.ntp.org"},
		StepThreshold: 250 * time.Millisecond,
		GPSDevice:     "/dev/ttyACM0",
	}, clock, nil)
	d.probeNTP = func(context.Context, string) (time.Duration, time.Duration, error) {
		return 500 * time.Millisecond, 10 * time.Millisecond, nil
	}

	d.pass(context.Background())

	if len(clock.Steps()) != 1 {
		t.Fatalf("steps = %d, want 1", len(clock.Steps()))
	}
	if got := clock.Steps()[0].Sub(base); got != 500*time.Millisecond {
		t.Fatalf("stepped by %s, want 500ms", got)
	}
	st := d.Status()
	if st.Source != "ntp" || st.Detail != "pool.ntp.org" || !st.Stepped {
		t.Fatalf("status = %+v, want an ntp step", st)
	}
	if st.Stratum != 1 {
		t.Fatalf("stratum = %d, want 1 after a reference source", st.Stratum)
	}
}

// TestPassFallsBackToGPS is the off-grid case: the network is gone, the
// receiver answers, and the clock is still disciplined.
func TestPassFallsBackToGPS(t *testing.T) {
	base := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: base}
	gps := &fakeGPS{lines: []string{rmc(base.Add(2 * time.Second))}}
	gps.lines = append(gps.lines, rmc(base.Add(2*time.Second)), rmc(base.Add(2*time.Second)))
	d := newTestDaemon(t, Config{
		NTPServers:    []string{"pool.ntp.org"},
		StepThreshold: 250 * time.Millisecond,
		GPSDevice:     "/dev/ttyACM0",
	}, clock, gps)
	d.probeNTP = func(context.Context, string) (time.Duration, time.Duration, error) {
		return 0, 0, errors.New("network unreachable")
	}

	d.pass(context.Background())

	if len(clock.Steps()) != 1 {
		t.Fatalf("steps = %d, want 1", len(clock.Steps()))
	}
	if got := clock.Steps()[0].Sub(base); got != 2*time.Second {
		t.Fatalf("stepped by %s, want 2s", got)
	}
	st := d.Status()
	if st.Source != "gps" || !st.Stepped {
		t.Fatalf("status = %+v, want a gps step", st)
	}
	if st.Detail != "$GNRMC" {
		t.Fatalf("detail = %q, want the sentence that was used", st.Detail)
	}
}

// TestPassMedianFiltersOneBadSentence: a single sentence carrying a wild time
// must not move the clock — the median of the samples is used.
func TestPassMedianFiltersOneBadSentence(t *testing.T) {
	base := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: base}
	gps := &fakeGPS{lines: []string{
		rmc(base.Add(time.Hour)),
		rmc(base.Add(2 * time.Second)),
		rmc(base.Add(2 * time.Second)),
	}}
	d := newTestDaemon(t, Config{
		StepThreshold: 250 * time.Millisecond,
		GPSDevice:     "/dev/ttyACM0",
	}, clock, gps)

	d.pass(context.Background())

	if len(clock.Steps()) != 1 {
		t.Fatalf("steps = %d, want 1", len(clock.Steps()))
	}
	if got := clock.Steps()[0].Sub(base); got != 2*time.Second {
		t.Fatalf("stepped by %s, want the 2s median", got)
	}
}

// TestPassLeavesGoodClockAlone: below the threshold the clock is correct
// enough, and stepping it for a few milliseconds would only make timestamps
// jump.
func TestPassLeavesGoodClockAlone(t *testing.T) {
	base := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: base}
	d := newTestDaemon(t, Config{
		NTPServers:    []string{"pool.ntp.org"},
		StepThreshold: 250 * time.Millisecond,
	}, clock, nil)
	d.probeNTP = func(context.Context, string) (time.Duration, time.Duration, error) {
		return 5 * time.Millisecond, time.Millisecond, nil
	}

	d.pass(context.Background())

	if len(clock.Steps()) != 0 {
		t.Fatalf("steps = %d, want none below the threshold", len(clock.Steps()))
	}
	st := d.Status()
	if st.Stepped || st.Source != "ntp" {
		t.Fatalf("status = %+v, want a measurement without a step", st)
	}
	if st.LastError != "" {
		t.Fatalf("last_error = %q, want empty", st.LastError)
	}
}

// TestPassRejectsImplausibleGPSDate: a receiver without a fix reports its
// power-on date, and the station must never follow it.
func TestPassRejectsImplausibleGPSDate(t *testing.T) {
	base := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: base}
	old := time.Date(1999, 8, 22, 12, 0, 0, 0, time.UTC)
	gps := &fakeGPS{lines: []string{rmc(old), rmc(old), rmc(old)}}
	d := newTestDaemon(t, Config{GPSDevice: "/dev/ttyACM0"}, clock, gps)

	d.pass(context.Background())

	if len(clock.Steps()) != 0 {
		t.Fatalf("steps = %d, want none for an implausible date", len(clock.Steps()))
	}
	st := d.Status()
	if st.Source != "none" {
		t.Fatalf("status = %+v, want no source", st)
	}
	// The reason matters: an unplugged antenna must be reported as "no fix",
	// not as an absurd correction the operator cannot act on.
	if !strings.Contains(st.LastError, "implausible") {
		t.Fatalf("last_error = %q, want the implausible-date guard to fire", st.LastError)
	}
}

// TestPassRejectsHugeGPSOffset: a correction of weeks is a broken source, not
// a drifted clock.
func TestPassRejectsHugeGPSOffset(t *testing.T) {
	base := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: base}
	skewed := base.Add(40 * 24 * time.Hour)
	gps := &fakeGPS{lines: []string{rmc(skewed), rmc(skewed), rmc(skewed)}}
	d := newTestDaemon(t, Config{GPSDevice: "/dev/ttyACM0"}, clock, gps)

	d.pass(context.Background())

	if len(clock.Steps()) != 0 {
		t.Fatalf("steps = %d, want none", len(clock.Steps()))
	}
	if st := d.Status(); st.LastError == "" {
		t.Fatalf("status = %+v, want the safety limit reported", st)
	}
}

// TestPassReportsMissingCapability: without CAP_SYS_TIME the step fails, and
// the operator must see why instead of a silently wrong clock.
func TestPassReportsMissingCapability(t *testing.T) {
	base := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: base, err: os.ErrPermission}
	d := newTestDaemon(t, Config{NTPServers: []string{"pool.ntp.org"}}, clock, nil)
	d.probeNTP = func(context.Context, string) (time.Duration, time.Duration, error) {
		return time.Second, time.Millisecond, nil
	}

	d.pass(context.Background())

	st := d.Status()
	if st.LastError == "" {
		t.Fatal("a failed step must be reported")
	}
	if st.Stratum != 3 || st.Stepped {
		t.Fatalf("status = %+v, want a failed step at the default stratum", st)
	}
}

// TestRunWritesStatusAndServes: the daemon publishes what it did and answers
// the LAN while it runs.
func TestRunWritesStatusAndServes(t *testing.T) {
	dir := t.TempDir()
	statusFile := filepath.Join(dir, "time-sync.json")
	base := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: base}
	d := newTestDaemon(t, Config{
		NTPServers:    []string{"pool.ntp.org"},
		Interval:      time.Hour,
		StepThreshold: 250 * time.Millisecond,
		Serve:         true,
		ServeListen:   "127.0.0.1:0",
		StatusFile:    statusFile,
	}, clock, nil)
	d.probeNTP = func(context.Context, string) (time.Duration, time.Duration, error) {
		return 300 * time.Millisecond, time.Millisecond, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(statusFile); err == nil && len(clock.Steps()) == 1 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("status file %s was not written", statusFile)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v", err)
	}

	body, err := os.ReadFile(statusFile)
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	var st Status
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatalf("status is not JSON: %v", err)
	}
	if st.Source != "ntp" || !st.Stepped {
		t.Fatalf("status = %+v, want a recorded ntp step", st)
	}
	if st.NextAt.IsZero() {
		t.Fatal("next_at must be published so the operator knows when the next pass runs")
	}
}

// TestLoadStatusRoundTrip pins the contract with the panel: what the daemon
// publishes is what the operator sees, including the reason a pass failed.
func TestLoadStatusRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "time-sync.json")
	d := NewDaemon(Config{StatusFile: path}, nil, nil)
	want := Status{
		Source: "gps", Detail: "$GNRMC", OffsetMS: -12.5, Stepped: true,
		At: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC), Stratum: 1, GPSSats: 9,
	}
	d.writeStatus(want)

	got, err := LoadStatus(path)
	if err != nil {
		t.Fatalf("LoadStatus: %v", err)
	}
	if got.Source != want.Source || got.Detail != want.Detail || got.OffsetMS != want.OffsetMS ||
		got.Stepped != want.Stepped || !got.At.Equal(want.At) || got.Stratum != want.Stratum || got.GPSSats != want.GPSSats {
		t.Fatalf("status = %+v, want %+v", got, want)
	}
}

// TestLoadStatusMissingFile: a station that never ran the discipline has no
// status file, and the panel must be able to say so instead of crashing.
func TestLoadStatusMissingFile(t *testing.T) {
	if _, err := LoadStatus(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Fatal("a missing status file must be an error")
	}
}
