package config

import (
	"os"
	"path/filepath"
	"testing"
)

// FuzzLoad checks that arbitrary YAML input never panics the loader.
func FuzzLoad(f *testing.F) {
	f.Add([]byte("app:\n  log_level: info\n"))
	f.Add([]byte(""))
	f.Add([]byte("app:\n  log_level: 123\n"))
	f.Add([]byte("not: [valid"))
	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "fuzz.yaml")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Skipf("write temp config: %v", err)
		}
		_, _ = Load(path) // result may be an error; must not panic
	})
}
