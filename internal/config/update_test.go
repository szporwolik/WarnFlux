package config

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestUpdateFileRoundTrip pins the YAML edit layer: values set through
// UpdateFile survive a reload, unrelated sections stay intact, comments
// survive and a rolling backup is written.
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

	err := UpdateFile(path, func(root *yaml.Node) error {
		if err := SetScalarPath(root, "web.force_local_tiles", BoolScalar(true)); err != nil {
			return err
		}
		if err := SetScalarPath(root, "web.header2", StringScalar("Społeczny system")); err != nil {
			return err
		}
		if err := SetScalarPath(root, "web.about", StringScalar("# Info\n\ndruga linia")); err != nil {
			return err
		}
		return SetScalarPath(root, "meshtastic.station_alerts", BoolScalar(true))
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
		t.Error("meshtastic.station_alerts = false, want true")
	}
	// Untouched state preserved.
	if cfg.Web.OfflineMode || cfg.Web.Title != "SOSNA" || cfg.App.LogLevel != "info" {
		t.Errorf("unrelated config changed: web=%+v app=%+v", cfg.Web, cfg.App)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"# station configuration", "# keep this comment"} {
		if !bytes.Contains(data, []byte(want)) {
			t.Errorf("comment %q lost:\n%s", want, data)
		}
	}
}

// TestSetScalarPathCreatesMissingSections pins section creation: a panel
// write into a not-yet-existing top-level section adds the mappings
// instead of failing.
func TestSetScalarPathCreatesMissingSections(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("web:\n  enabled: true\n  auth:\n    username: admin\n    password: secret123\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := UpdateFile(path, func(root *yaml.Node) error {
		return SetScalarPath(root, "meshtastic.station_alerts", BoolScalar(true))
	}); err != nil {
		t.Fatalf("UpdateFile: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !cfg.Meshtastic.StationAlerts {
		t.Error("station_alerts = false, want true")
	}
	if !cfg.Meshtastic.ChannelAlerts || !cfg.Meshtastic.DMAlerts {
		t.Errorf("channel/dm defaults must stay ON: %+v", cfg.Meshtastic)
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
	err := UpdateFile(path, func(root *yaml.Node) error {
		return nil
	})
	if err == nil {
		t.Fatal("UpdateFile on a list document must fail")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Errorf("file modified despite the error:\n%s", after)
	}
}

// TestSetScalarPathOverwrite pins value replacement on an existing key
// (the key keeps its position, only the value changes).
func TestSetScalarPathOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("web:\n  enabled: true\n  auth:\n    username: admin\n    password: secret123\n  offline_mode: false\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := UpdateFile(path, func(root *yaml.Node) error {
		return SetScalarPath(root, "web.offline_mode", BoolScalar(true))
	}); err != nil {
		t.Fatalf("UpdateFile: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !cfg.Web.OfflineMode {
		t.Error("offline_mode = false, want true after overwrite")
	}
}
