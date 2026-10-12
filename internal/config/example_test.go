package config

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The example configuration is the only config documentation a new
// installation reads before copying it, so it is guarded by tests: it
// must keep loading, and a key added to the loader must not appear there
// only by accident.

// examplePath returns the example shipped at the repository root.
func examplePath(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "config.example.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("config.example.yaml: %v", err)
	}
	return path
}

// TestExampleConfigLoads proves the example is a working configuration:
// the loader parses it, applies defaults and accepts every value it
// names. A copy of this file is what a new operator starts from, so a
// value the loader would refuse is a documentation bug.
func TestExampleConfigLoads(t *testing.T) {
	path := examplePath(t)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("config.example.yaml does not load: %v", err)
	}
	// A few anchors, so an accidentally truncated file is noticed as
	// something more specific than "still parses".
	if cfg.App.Timezone == "" || cfg.Storage.Driver != "sqlite" {
		t.Fatalf("example lost its app/storage defaults: %+v", cfg.App)
	}
	if len(cfg.Sources) == 0 || len(cfg.IngestHTTP) == 0 {
		t.Fatalf("example lost its example instances: %d sources, %d ingest endpoints",
			len(cfg.Sources), len(cfg.IngestHTTP))
	}
	if cfg.APRS.Callsign == "" || cfg.APRS.RadiusKM <= 0 {
		t.Fatalf("example lost its APRS identity: %q radius %v", cfg.APRS.Callsign, cfg.APRS.RadiusKM)
	}
	// The actions live as commented blocks (uncomment to enable), so they
	// cannot be counted: check that the section and its types are there.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"actions:", "type: smtp", "type: discord", "type: meshtastic", "type: aprs-out", "type: sms"} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("example lacks an example of %q", want)
		}
	}
}

// TestExampleDocumentsEveryKey walks the loader's own field set and fails
// when a configuration key is nowhere in the example, so adding a key
// without documenting it breaks the build instead of surprising an
// operator. It is a coarse check on purpose (any mention counts, and the
// plugin-specific config blocks live in untyped nodes, so they are out of
// scope): it is here to catch a forgotten section, not to prove prose
// quality.
func TestExampleDocumentsEveryKey(t *testing.T) {
	raw, err := os.ReadFile(examplePath(t))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)

	var missing []string
	seen := map[reflect.Type]bool{}
	var walk func(rt reflect.Type)
	walk = func(rt reflect.Type) {
		for rt.Kind() == reflect.Pointer || rt.Kind() == reflect.Slice || rt.Kind() == reflect.Array {
			rt = rt.Elem()
		}
		if rt.Kind() != reflect.Struct || seen[rt] {
			return
		}
		seen[rt] = true
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			tag := strings.Split(f.Tag.Get("yaml"), ",")[0]
			if tag != "" && tag != "-" && !strings.Contains(text, tag) {
				missing = append(missing, tag)
			}
			walk(f.Type)
		}
	}
	walk(reflect.TypeOf(fileConfig{}))

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("config.example.yaml does not mention these configuration keys: %s",
			strings.Join(missing, ", "))
	}
}
