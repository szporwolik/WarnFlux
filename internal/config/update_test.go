package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
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
	if leftovers := tempLeftovers(t, dir); len(leftovers) != 0 {
		t.Fatalf("temporary files left behind: %v", leftovers)
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

// TestUpdateFileListIndexPath pins the list-entry path support: an operator
// edits ONE source instance's poll interval, and the rest of the file —
// comments, other instances, formatting — stays byte-identical.
func TestUpdateFileListIndexPath(t *testing.T) {
	const src = `app:
  log_level: info

sources:
  - id: weather-home
    type: openmeteo
    config:
      poll_interval: 30m # dev shares the budget
      request_timeout: 10s
  - id: planes
    type: adsb
    config:
      poll_interval: 3m
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := UpdateFile(path, []ScalarEdit{
		{Path: "sources[0].config.poll_interval", Value: DurationScalar(2 * time.Hour)},
	}); err != nil {
		t.Fatalf("UpdateFile: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Replace(src, "poll_interval: 30m # dev shares the budget", "poll_interval: 2h0m0s # dev shares the budget", 1)
	if string(got) != want {
		t.Fatalf("file after the edit:\n%s\nwant:\n%s", got, want)
	}

	// The second instance is untouched, and an out-of-range index is an
	// error rather than a silent no-op.
	if err := UpdateFile(path, []ScalarEdit{
		{Path: "sources[5].config.poll_interval", Value: DurationScalar(time.Minute)},
	}); err == nil {
		t.Fatal("an out-of-range list index was accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(got) {
		t.Fatalf("the failed edit changed the file:\n%s", after)
	}
}

// TestUpdateFileQuotesClockTime pins the anchor spelling: a plain 07:30
// is a sexagesimal number to YAML 1.1 readers, so the panel writes it the
// way the example config does.
func TestUpdateFileQuotesClockTime(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("meshtastic:\n  emcom_at: \"06:00\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := UpdateFile(path, []ScalarEdit{
		{Path: "meshtastic.emcom_at", Value: StringScalar("07:30")},
		{Path: "meshtastic.emcom_interval", Value: DurationScalar(0)},
	}); err != nil {
		t.Fatalf("UpdateFile: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "meshtastic:\n  emcom_at: \"07:30\"\n  emcom_interval: 0s\n"
	if string(got) != want {
		t.Fatalf("file:\n%s\nwant:\n%s", got, want)
	}
}

// tempLeftovers lists the temporary files a write cycle forgot in dir.
func tempLeftovers(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if name := e.Name(); strings.HasSuffix(name, backupSuffix) {
			continue // the rolling backup is expected
		} else if strings.Contains(name, ".tmp") {
			out = append(out, name)
		}
	}
	return out
}

// TestUpdateFileConcurrentFields pins the P2 report: two Config requests
// editing DIFFERENT fields of one file must both survive. The update cycle
// reads the file, patches it and renames a temporary over it, so without
// serialization each writer patched the bytes the other one had already
// replaced and one of the two edits silently disappeared; one shared
// "<path>.tmp" made the two renames collide on top of that.
func TestUpdateFileConcurrentFields(t *testing.T) {
	const writers, rounds = 8, 20
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("web:\n  enabled: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	last := make([]string, writers)
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				value := fmt.Sprintf("w%d-r%d", i, round)
				edit := []ScalarEdit{{Path: fmt.Sprintf("web.panel_%d", i), Value: StringScalar(value)}}
				if err := UpdateFile(path, edit); err != nil {
					errs <- fmt.Errorf("writer %d round %d: %w", i, round, err)
					return
				}
				last[i] = value
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent UpdateFile: %v", err)
	}
	if t.Failed() {
		return
	}

	var raw struct {
		Web map[string]string `yaml:"web"`
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		t.Fatalf("the file no longer parses: %v\n%s", err, data)
	}
	for i := 0; i < writers; i++ {
		if got := raw.Web[fmt.Sprintf("panel_%d", i)]; got != last[i] {
			t.Errorf("web.panel_%d = %q, want %q: the edit was lost", i, got, last[i])
		}
	}
	if leftovers := tempLeftovers(t, dir); len(leftovers) != 0 {
		t.Errorf("temporary files left behind: %v", leftovers)
	}
}

// TestUpdateFileConcurrentSameField pins the other half: writers racing
// for ONE field must leave a whole file holding one of the submitted
// values — never a truncated or half-written document.
func TestUpdateFileConcurrentSameField(t *testing.T) {
	const writers = 16
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("web:\n  title: \"seed\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	values := map[string]bool{}
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		value := fmt.Sprintf("v%03d", i)
		values[value] = true
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := UpdateFile(path, []ScalarEdit{{Path: "web.title", Value: StringScalar(value)}}); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent UpdateFile: %v", err)
	}
	if t.Failed() {
		return
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("the file no longer parses: %v", err)
	}
	if !values[cfg.Web.Title] {
		t.Errorf("web.title = %q, want one of the submitted values", cfg.Web.Title)
	}
	if leftovers := tempLeftovers(t, dir); len(leftovers) != 0 {
		t.Errorf("temporary files left behind: %v", leftovers)
	}
}
