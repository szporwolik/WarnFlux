package config

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestUpdateFileRoundTrip pins the surgical YAML editor: values set
// through UpdateFile survive a reload, unrelated lines stay byte-identical
// (comments, alignment, blank lines), and a rolling backup is written.
func TestUpdateFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `# station configuration
app:
  log_level: info   # keep this comment
web:
  enabled: true
  title: "SOSNA"

  offline_mode: false
  auth:
    username: admin
    password: secret123
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	err := UpdateFile(path, []ScalarEdit{
		{Path: "web.force_local_tiles", Value: BoolScalar(true)},
		{Path: "web.header2", Value: StringScalar("Społeczny system")},
		{Path: "web.about", Value: StringScalar("# Info\n\ndruga linia")},
		{Path: "meshtastic.station_alerts", Value: BoolScalar(true)},
	})
	if err != nil {
		t.Fatalf("UpdateFile: %v", err)
	}

	// Backup written, no temp file left behind.
	if _, err := os.Stat(path + backupSuffix); err != nil {
		t.Fatalf("backup missing: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file left behind: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !cfg.Web.ForceLocalTiles {
		t.Error("web.force_local_tiles = false, want true")
	}
	if cfg.Web.Header2 != "Społeczny system" {
		t.Errorf("web.header2 = %q", cfg.Web.Header2)
	}
	if cfg.Web.About != "# Info\n\ndruga linia" {
		t.Errorf("web.about = %q", cfg.Web.About)
	}
	if !cfg.Meshtastic.StationAlerts {
		t.Error("meshtastic.station_alerts = false, want true (section auto-created)")
	}
	if !cfg.Meshtastic.ChannelAlerts || !cfg.Meshtastic.DMAlerts {
		t.Errorf("channel/dm defaults must stay ON: %+v", cfg.Meshtastic)
	}
	if cfg.Web.OfflineMode || cfg.Web.Title != "SOSNA" || cfg.App.LogLevel != "info" {
		t.Errorf("unrelated config changed: web=%+v app=%+v", cfg.Web, cfg.App)
	}

	data, _ := os.ReadFile(path)
	got := string(data)
	// Untouched lines stay byte-identical, comments and alignment included.
	for _, want := range []string{
		"# station configuration",
		"  log_level: info   # keep this comment",
		"  title: \"SOSNA\"",
		"\n\n  offline_mode: false",
		"  auth:\n    username: admin",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("untouched content %q lost:\n%s", want, got)
		}
	}
}

// TestUpdateFileByteExactness pins the no-reformat promise: one scalar
// change must alter exactly that line and nothing else.
func TestUpdateFileByteExactness(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	before := `app:
  log_level: debug          # debug | info | warn | error
  expiration_interval: 1m

web:
  enabled: true

  offline_mode: false
  auth:
    username: admin
    password: secret123
`
	if err := os.WriteFile(path, []byte(before), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := UpdateFile(path, []ScalarEdit{
		{Path: "web.offline_mode", Value: BoolScalar(true)},
	}); err != nil {
		t.Fatalf("UpdateFile: %v", err)
	}
	after, _ := os.ReadFile(path)
	linesBefore := strings.Split(before, "\n")
	linesAfter := strings.Split(string(after), "\n")
	if len(linesBefore) != len(linesAfter) {
		t.Fatalf("line count changed %d -> %d:\n%s", len(linesBefore), len(linesAfter), after)
	}
	changed := 0
	for i := range linesBefore {
		if linesBefore[i] != linesAfter[i] {
			changed++
			t.Logf("line %d: %q -> %q", i, linesBefore[i], linesAfter[i])
		}
	}
	if changed != 1 {
		t.Errorf("changed lines = %d, want exactly 1", changed)
	}
	if !strings.Contains(string(after), "  offline_mode: true") {
		t.Errorf("new value missing:\n%s", after)
	}
}

// TestUpdateFileRejectsNonMapping pins the safety guard: editing a
// document whose root is not a mapping must fail without touching the
// file.
func TestUpdateFileRejectsNonMapping(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	before := []byte("- just\n- a\n- list\n")
	if err := os.WriteFile(path, before, 0o600); err != nil {
		t.Fatal(err)
	}
	err := UpdateFile(path, []ScalarEdit{{Path: "web.x", Value: BoolScalar(true)}})
	if err == nil {
		t.Fatal("UpdateFile on a list document must fail")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Errorf("file modified despite the error:\n%s", after)
	}
}

// TestUpdateFileQuoting pins value quoting: reserved-looking strings are
// quoted so the reload reads the same value back.
func TestUpdateFileQuoting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	before := "web:\n  enabled: true\n  auth:\n    username: admin\n    password: secret123\n"
	if err := os.WriteFile(path, []byte(before), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := UpdateFile(path, []ScalarEdit{
		{Path: "web.tagline", Value: StringScalar("on: #tak")},
	}); err != nil {
		t.Fatalf("UpdateFile: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if cfg.Web.Tagline != "on: #tak" {
		t.Errorf("tagline = %q, want the quoted round-trip", cfg.Web.Tagline)
	}
}
