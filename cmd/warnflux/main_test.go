package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/weatherreport"
)

// TestHazardsFromMirrorSevereOnly pins the mesh-digest filter: the
// hourly channel drop keeps severe-and-above only (the mirror carries
// plenty of minor road-works noise), while the full projection keeps
// every active hazard for the on-demand /hazard command.
func TestHazardsFromMirrorSevereOnly(t *testing.T) {
	m := state.New()
	seed := func(sev, head string) {
		if err := m.AddOrUpdateActive("local", "warnflux/active/x/"+head, state.Hazard{
			EventKey: head, Source: "x", Severity: sev, Headline: head, Status: "active",
		}); err != nil {
			t.Fatal(err)
		}
	}
	seed("minor", "M1")
	seed("moderate", "M2")
	seed("severe", "S1")
	seed("extreme", "X1")
	seed("unknown", "U1")

	got := hazardsFromMirror(m, "severe")
	if len(got) != 2 || got[0].Headline != "X1" || got[1].Headline != "S1" {
		t.Fatalf("severe+ projection = %+v, want [X1 S1]", got)
	}
	if all := hazardsFromMirror(m, ""); len(all) != 5 {
		t.Fatalf("full projection = %d entries, want 5", len(all))
	}
}

func testResolverLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestHazardsForRadio pins the public /hazard command text: a header
// plus one Zulu-windowed line per hazard (the hourly digest form), and
// an explicit answer when nothing is active.
func TestHazardsForRadio(t *testing.T) {
	if got := hazardsForRadio(nil); got != "No active messages" {
		t.Fatalf("empty list = %q, want the no-hazards answer", got)
	}
	eff := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	exp := eff.Add(2 * time.Hour)
	got := hazardsForRadio([]meshtastic.ActiveHazard{
		{Headline: "Burza", Description: "Porywy", EffectiveAt: &eff, ExpiresAt: &exp},
		{Headline: "Mgla"},
	})
	want := "Active messages: 2\n2026-10-05 10:00Z-2026-10-05 12:00Z Burza — Porywy\n?-? Mgla"
	if got != want {
		t.Fatalf("hazardsForRadio = %q, want %q", got, want)
	}
}

// TestWeatherForRadioOffGrid pins the off-grid /weather path: the local
// hub cache (updated before the infrastructure filter and without any
// broker) is the primary APRS source — with an EMPTY mirror the radio
// aggregation still reports the RF-observed measurement.
func TestWeatherForRadioOffGrid(t *testing.T) {
	hub, err := aprs.NewHub(aprs.HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", Icon: "/j", GridSquare: "JO90WW",
		RadiusKM: aprs.DefaultRadiusKM, StationTTL: 30 * time.Minute,
		ExcludeInfrastructure: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	hub.Observe(aprs.ParseFeedLine("SP9WX>APRS,TCPIP*:!5056.25N/01952.50E_220/004g005t077r000p000P000h50b09900", time.Now()), "aprs-inet")

	mirror := state.New() // no broker loopback at all
	var rep weatherreport.Report
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rep = weatherForRadio(hub, mirror, time.Now())
		if rep.HasTemp {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !rep.HasTemp || rep.TempC != 25.0 {
		t.Fatalf("off-grid /weather = %+v, want temp 25 from the RF-observed WX report", rep)
	}
}

func TestResolveStoragePathExplicit(t *testing.T) {
	got := resolveStoragePath("./data/events.db", testResolverLogger())
	if got != "./data/events.db" {
		t.Errorf("explicit path changed: %q", got)
	}
}

func TestResolveStoragePathFallsBackNextToBinary(t *testing.T) {
	got := resolveStoragePath("", testResolverLogger())
	if !strings.HasSuffix(got, "warnflux.db") {
		t.Fatalf("fallback path = %q, want suffix warnflux.db", got)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	want := filepath.Join(filepath.Dir(exe), "warnflux.db")
	if got != want {
		t.Errorf("fallback = %q, want %q", got, want)
	}
}

func TestResolveVersionInjectedUnchanged(t *testing.T) {
	if got := resolveVersion("1.2.3"); got != "1.2.3" {
		t.Errorf("injected version changed: %q", got)
	}
}

func TestResolveVersionReadsWorkingDirFile(t *testing.T) {
	// The cwd is the package directory when tests run; a VERSION file next
	// to the repo root is not guaranteed, so test with a temp cwd.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "VERSION"), []byte("0.7.4\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })

	if got := resolveVersion("dev"); got != "0.7.4" {
		t.Errorf("dev build did not pick up VERSION file: %q", got)
	}
}

func TestResolveVersionIgnoresWhitespaceOnly(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "VERSION"), []byte("  \n "), 0o600); err != nil {
		t.Fatal(err)
	}
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })

	if got := resolveVersion("dev"); got != "dev" {
		t.Errorf("whitespace-only VERSION file should be ignored: %q", got)
	}
}

// TestCheckConfigValid runs the full -check-config path (config load +
// strict construction of plugins/actions/receivers/web) against a minimal
// configuration and expects success without any side effects.
func TestCheckConfigValid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
app:
  log_level: info
storage:
  driver: sqlite
  path: ` + filepath.Join(dir, "warnflux.db") + `
sources:
  - id: imgw-warnings
    type: imgw
    enabled: false
actions:
  - id: smtp-alerts
    type: smtp
    enabled: false
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(path, true); err != nil {
		t.Fatalf("check-config on a valid config: %v", err)
	}
}

// TestCheckConfigRejectsBadPluginConfig pins the strict decode: an unknown
// field inside an enabled plugin block must fail -check-config the same
// way it would fail a real startup.
func TestCheckConfigRejectsBadPluginConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := "sources:\n  - id: imgw-warnings\n    type: imgw\n    enabled: true\n    config:\n      wat: 1\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(path, true); err == nil {
		t.Fatal("check-config accepted an unknown plugin field")
	}
}

// TestPingForRadio pins the public /ping answer: version and uptime, in
// a form that still fits the smallest frame we speak (APRS, 67 chars),
// even after a very long uptime.
func TestPingForRadio(t *testing.T) {
	t.Cleanup(func() { processStart = time.Now() })
	for _, tc := range []struct {
		version string
		ago     time.Duration
		want    string
	}{
		{"0.7.3", 3*24*time.Hour + 4*time.Hour + 12*time.Minute, "Pong from WarnFlux 0.7.3, uptime 3d4h12m"},
		{"0.7.3", 4*time.Hour + 12*time.Minute, "Pong from WarnFlux 0.7.3, uptime 4h12m"},
		{"0.7.3", 12 * time.Minute, "Pong from WarnFlux 0.7.3, uptime 12m"},
		{"0.7.3", 30 * time.Second, "Pong from WarnFlux 0.7.3, uptime <1m"},
		{"0.7.3", 400 * 24 * time.Hour, "Pong from WarnFlux 0.7.3, uptime 400d0h0m"},
		{"", time.Minute, "Pong from WarnFlux, uptime 1m"},
	} {
		processStart = time.Now().Add(-tc.ago)
		got := pingForRadio(tc.version)
		if got != tc.want {
			t.Errorf("pingForRadio(%q, %s) = %q, want %q", tc.version, tc.ago, got, tc.want)
		}
		// The APRS frame is the tightest budget we speak (67 chars less
		// the ack suffix).
		if n := len([]rune(got)); n > aprs.MaxMessageText-aprs.AckSuffixLen {
			t.Errorf("pingForRadio(%q, %s) = %d runes — over the APRS frame", tc.version, tc.ago, n)
		}
	}

	// A release version with a commit suffix stays within the frame too.
	processStart = time.Now().Add(-(9*24*time.Hour + 23*time.Hour + 59*time.Minute))
	if got := pingForRadio("0.7.3-dev+abc1234"); len([]rune(got)) > aprs.MaxMessageText-aprs.AckSuffixLen {
		t.Errorf("ping reply = %q, %d runes — over the APRS frame", got, len([]rune(got)))
	}
}

// TestFormatUptime pins the shortest-round-form rendering.
func TestFormatUptime(t *testing.T) {
	for _, tc := range []struct {
		in   time.Duration
		want string
	}{
		{0, "<1m"},
		{59 * time.Second, "<1m"},
		{time.Minute, "1m"},
		{90 * time.Second, "2m"}, // rounds to the nearest minute
		{time.Hour + 30*time.Minute, "1h30m"},
		{25*time.Hour + time.Minute, "1d1h1m"},
	} {
		if got := formatUptime(tc.in); got != tc.want {
			t.Errorf("formatUptime(%s) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestHazardSummaryFromMirror pins the counters the periodic channel line
// reports: severe and above (the hazards that rang the devices) against
// everything below the alert bar, unknown included.
func TestHazardSummaryFromMirror(t *testing.T) {
	m := state.New()
	for _, sev := range []string{"minor", "moderate", "severe", "extreme", "unknown"} {
		if err := m.AddOrUpdateActive("local", "warnflux/active/x/"+sev, state.Hazard{
			EventKey: sev, Source: "x", Severity: sev, Headline: sev, Status: "active",
		}); err != nil {
			t.Fatal(err)
		}
	}
	got := hazardSummaryFromMirror(m)
	if got.Severe != 2 || got.Other != 3 || got.Total() != 5 {
		t.Fatalf("summary = %+v, want 2 severe / 3 other / 5 total", got)
	}
}

// TestPublicSite pins the link the mesh status line points at: a bare
// host (the usual configuration) gets the scheme, an explicit URL stays.
func TestPublicSite(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"sosna.sp9moa.pl", "https://sosna.sp9moa.pl"},
		{" sosna.sp9moa.pl/ ", "https://sosna.sp9moa.pl"},
		{"https://sosna.sp9moa.pl", "https://sosna.sp9moa.pl"},
		{"http://spok.example.com", "http://spok.example.com"},
		{"", ""},
	} {
		if got := publicSite(tc.in); got != tc.want {
			t.Errorf("publicSite(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// settleRotatedFiles returns the directory listing once the archived history
// has stopped shrinking, or the last listing when it did not settle within the
// grace period. The rotation library prunes in its own goroutine, so the count
// is only meaningful after that goroutine had a chance to run; a history that
// stays above the bound is returned as it is (the caller fails on it).
func settleRotatedFiles(t *testing.T, dir string, want int) []os.DirEntry {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read dir: %v", err)
		}
		if len(entries) <= want || time.Now().After(deadline) {
			return entries
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestLoggerRotatedFilesStayBounded pins the reported P2 for the log
// rotation: a long-running station must never accumulate an unbounded
// history of archived log files, not even when the configured backup count
// is zero (the zero value falls back to the shipped default instead of
// letting lumberjack keep everything).
func TestLoggerRotatedFilesStayBounded(t *testing.T) {
	for _, maxBackups := range []int{1, 2, 5, 0} {
		t.Run(fmt.Sprintf("backups-%d", maxBackups), func(t *testing.T) {
			dir := t.TempDir()
			app := config.Default().App
			app.LogFile = filepath.Join(dir, "warnflux.log")
			app.LogMaxSizeMB = 1
			app.LogMaxBackups = maxBackups

			// newLogger always tees to stdout; keep the megabytes of
			// synthetic lines out of the test output.
			devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer devNull.Close()
			saved := os.Stdout
			os.Stdout = devNull
			logger, closer, err := newLogger(app, io.Discard)
			os.Stdout = saved
			if err != nil {
				t.Fatalf("newLogger: %v", err)
			}
			// ~6 MB of log lines through a 1 MB rotation threshold: several
			// rotations, well past any allowed backup count.
			line := strings.Repeat("x", 900)
			for i := 0; i < 7000; i++ {
				logger.Info(line)
			}
			if err := closer.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			want := maxBackups
			if want <= 0 {
				want = config.Default().App.LogMaxBackups
			}
			// Lumberjack prunes the archived files in ITS OWN goroutine, so
			// right after the last rotation the directory can still hold the
			// files that are about to be removed. Wait (bounded) for the
			// history to settle instead of racing that goroutine: on a
			// machine with few usable cores the pruning had not been
			// scheduled yet and the count was read mid-flight (reported:
			// 4, 6 files with maxBackups=1). A bound that never holds is
			// still a failure, which is what this test is about.
			entries := settleRotatedFiles(t, dir, want+1)
			// The live file plus at most `want` archives.
			if len(entries) > want+1 {
				t.Fatalf("rotated log files = %d, want at most %d (%d archives + the live file)",
					len(entries), want+1, want)
			}
			var total int64
			for _, e := range entries {
				info, err := e.Info()
				if err != nil {
					t.Fatal(err)
				}
				total += info.Size()
			}
			// The whole history must fit in roughly the allowed copies.
			if limit := int64(want+1) * 2 << 20; total > limit {
				t.Fatalf("log history = %d bytes, want at most %d for %d copies", total, limit, want)
			}
		})
	}
}

// TestLoggerStampsUTC pins the log time policy: the log stream is read by
// machines and collectors, so every line carries a UTC RFC3339 stamp with
// "Z" — never the station's display zone — and that holds for both logger
// variants (with and without a log file).
func TestLoggerStampsUTC(t *testing.T) {
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = devnull
	defer func() { os.Stdout = old; devnull.Close() }()

	for _, name := range []string{"stdout only", "rotating file"} {
		app := config.App{}
		if name == "rotating file" {
			app.LogFile = filepath.Join(t.TempDir(), "warnflux.log")
		}
		var buf strings.Builder
		logger, closer, err := newLogger(app, &buf)
		if err != nil {
			t.Fatalf("%s: newLogger: %v", name, err)
		}
		if closer == nil {
			t.Fatalf("%s: newLogger returned a nil closer", name)
		}
		logger.Info("tick")
		_ = closer.Close()

		line := strings.TrimSpace(buf.String())
		if line == "" {
			t.Fatalf("%s: the captured ring buffer stayed empty", name)
		}
		if !strings.HasPrefix(line, "time=") || !strings.Contains(line, "Z level=INFO msg=tick") {
			t.Errorf("%s: log line = %q, want a UTC (Z) stamp", name, line)
		}
	}
}
