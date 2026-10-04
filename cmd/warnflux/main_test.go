package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/weatherreport"
)

func testResolverLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestHazardsForRadio pins the public /hazard command text: a header
// plus one Zulu-windowed line per hazard (the hourly digest form), and
// an explicit answer when nothing is active.
func TestHazardsForRadio(t *testing.T) {
	if got := hazardsForRadio(nil); got != "No active hazards" {
		t.Fatalf("empty list = %q, want the no-hazards answer", got)
	}
	eff := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	exp := eff.Add(2 * time.Hour)
	got := hazardsForRadio([]meshtastic.ActiveHazard{
		{Headline: "Burza", Description: "Porywy", EffectiveAt: &eff, ExpiresAt: &exp},
		{Headline: "Mgla"},
	})
	want := "Active hazards: 2\n2026-10-05 10:00Z-2026-10-05 12:00Z Burza — Porywy\n?-? Mgla"
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
