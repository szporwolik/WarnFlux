package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const exampleConfig = `app:
  log_level: info
  expiration_interval: 1m
  change_retention: 24h
  notification_retention: 168h

storage:
  driver: sqlite
  path: ./warnflux.db

sources:
  - id: imgw-warnings
    type: imgw
    enabled: true
    runtime:
      restart: true
      shutdown_timeout: 10s
    config:
      poll_interval: 5m
      request_timeout: 10s

outputs:
  - id: mqtt-main
    type: mqtt
    enabled: true
    runtime:
      timeout: 10s
      failure_threshold: 5
    config:
      broker: tcp://localhost:1883
      topic_prefix: warnflux
      heartbeat_interval: 30s
`

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

// TestPluginIDValidation pins the durable identity format: plugin IDs (and
// especially output IDs, which are persisted journal consumer identities)
// must be lowercase slugs. Uppercase is rejected, not silently lowercased.
func TestPluginIDValidation(t *testing.T) {
	for _, id := range []string{"mqtt-main", "imgw-warnings", "a", "out_a.b-1", "trailing-"} {
		if _, err := Load(writeTempConfig(t, "sources:\n  - id: "+id+"\n    type: imgw\n")); err != nil {
			t.Errorf("id %q rejected: %v", id, err)
		}
	}
	for _, id := range []string{"", "Has Space", "-leading", "UPPER", "uni☃", strings.Repeat("x", 65)} {
		if _, err := Load(writeTempConfig(t, "sources:\n  - id: "+id+"\n    type: imgw\n")); err == nil {
			t.Errorf("id %q accepted, want rejection", id)
		}
	}
	// Outer whitespace is trimmed before validation.
	if _, err := Load(writeTempConfig(t, "outputs:\n  - id: '  out-a  '\n    type: mqtt\n    enabled: false\n")); err != nil {
		t.Errorf("trimmed id rejected: %v", err)
	}
}

func TestLoadFullConfig(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, exampleConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.App.LogLevel != "info" {
		t.Errorf("log level = %q", cfg.App.LogLevel)
	}
	if cfg.App.ExpirationInterval != time.Minute {
		t.Errorf("expiration_interval = %s", cfg.App.ExpirationInterval)
	}
	if cfg.App.ChangeRetention != 24*time.Hour {
		t.Errorf("change_retention = %s", cfg.App.ChangeRetention)
	}
	if cfg.App.EventRetention != 30*24*time.Hour {
		t.Errorf("event_retention = %s", cfg.App.EventRetention)
	}
	if cfg.App.NotificationRetention != 168*time.Hour {
		t.Errorf("notification_retention = %s", cfg.App.NotificationRetention)
	}
	if cfg.Storage.Driver != "sqlite" || cfg.Storage.Path != "./warnflux.db" {
		t.Errorf("storage = %+v", cfg.Storage)
	}
	if cfg.Web.Header1 != "WarnFlux" || cfg.Web.Header2 != "" {
		t.Errorf("web headers defaults = %q / %q, want WarnFlux / empty", cfg.Web.Header1, cfg.Web.Header2)
	}
	if len(cfg.Sources) != 1 || len(cfg.Outputs) != 1 {
		t.Fatalf("sources=%d outputs=%d", len(cfg.Sources), len(cfg.Outputs))
	}
	src := cfg.Sources[0]
	if src.ID != "imgw-warnings" || src.Type != "imgw" || !src.Enabled {
		t.Errorf("unexpected source: %+v", src)
	}
	if src.Runtime.Restart != true || src.Runtime.ShutdownTimeout != 10*time.Second {
		t.Errorf("source runtime: %+v", src.Runtime)
	}
	if src.Config == nil {
		t.Error("source config node missing")
	}
	out := cfg.Outputs[0]
	if out.ID != "mqtt-main" || !out.Enabled {
		t.Errorf("unexpected output: %+v", out)
	}
	if out.Runtime.Timeout != 10*time.Second || out.Runtime.FailureThreshold != 5 {
		t.Errorf("output runtime: %+v", out.Runtime)
	}
	if out.Config == nil {
		t.Error("output config node missing")
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, ""))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.App.LogLevel != "info" {
		t.Errorf("log level = %q", cfg.App.LogLevel)
	}
	if cfg.App.ExpirationInterval != time.Minute {
		t.Errorf("expiration_interval = %s", cfg.App.ExpirationInterval)
	}
	if cfg.App.ChangeRetention != 24*time.Hour {
		t.Errorf("change_retention = %s", cfg.App.ChangeRetention)
	}
	if cfg.App.EventRetention != 30*24*time.Hour {
		t.Errorf("event_retention = %s", cfg.App.EventRetention)
	}
	if cfg.App.NotificationRetention != 30*24*time.Hour {
		t.Errorf("notification_retention default = %s", cfg.App.NotificationRetention)
	}
	if cfg.Storage.Driver != "sqlite" || cfg.Storage.Path != "" {
		t.Errorf("storage defaults = %+v", cfg.Storage)
	}
}

func TestLoadPluginRuntimeDefaults(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, "sources:\n  - id: s\n    type: imgw\n    enabled: true\noutputs:\n  - id: o\n    type: mqtt\n    enabled: true\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Sources[0].Runtime; got.Restart != true || got.ShutdownTimeout != 10*time.Second {
		t.Errorf("source runtime defaults = %+v", got)
	}
	if got := cfg.Outputs[0].Runtime; got.Timeout != 10*time.Second || got.FailureThreshold != 5 {
		t.Errorf("output runtime defaults = %+v", got)
	}
}

func TestLoadPluginDuplicateIDRejected(t *testing.T) {
	_, err := Load(writeTempConfig(t, "sources:\n  - id: x\n    type: imgw\noutputs:\n  - id: x\n    type: mqtt\n"))
	if err == nil || !strings.Contains(err.Error(), "duplicate plugin id") {
		t.Errorf("error = %v, want duplicate plugin id", err)
	}
}

func TestLoadPluginMissingTypeRejected(t *testing.T) {
	if _, err := Load(writeTempConfig(t, "sources:\n  - id: x\n")); err == nil {
		t.Fatal("expected error for missing type, got nil")
	}
	if _, err := Load(writeTempConfig(t, "sources:\n  - type: imgw\n")); err == nil {
		t.Fatal("expected error for missing id, got nil")
	}
}

func TestLoadPluginInvalidRuntimeRejected(t *testing.T) {
	if _, err := Load(writeTempConfig(t, "sources:\n  - id: x\n    type: imgw\n    runtime:\n      shutdown_timeout: 0s\n")); err == nil {
		t.Fatal("expected error for shutdown_timeout 0s, got nil")
	}
	if _, err := Load(writeTempConfig(t, "outputs:\n  - id: x\n    type: mqtt\n    runtime:\n      failure_threshold: 0\n")); err == nil {
		t.Fatal("expected error for failure_threshold 0, got nil")
	}
}

func TestLoadRejectsTrailingDocument(t *testing.T) {
	_, err := Load(writeTempConfig(t, "app:\n  log_level: info\n---\nsomething:\n  unexpected: true\n"))
	if err == nil {
		t.Fatal("expected error for trailing YAML document, got nil")
	}
	if !strings.Contains(err.Error(), "multiple YAML documents") {
		t.Errorf("error should mention multiple documents, got: %v", err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

// TestLoadConfigSizeLimit pins the 1 MiB configuration file bound: a local
// administrator controls the file, but os.ReadFile must not accept
// arbitrary sizes by accident.
func TestLoadConfigSizeLimit(t *testing.T) {
	if _, err := Load(writeTempConfig(t, exampleConfig)); err != nil {
		t.Fatalf("small config rejected: %v", err)
	}

	// Exactly at the limit: the example config padded with YAML comments.
	pad := maxConfigFileBytes - len(exampleConfig) - 1
	if pad < 0 {
		t.Fatalf("example config already exceeds the limit (%d > %d)", len(exampleConfig), maxConfigFileBytes)
	}
	exact := exampleConfig + "\n" + strings.Repeat("#", pad)
	if len(exact) != maxConfigFileBytes {
		t.Fatalf("test setup: exact config is %d bytes, want %d", len(exact), maxConfigFileBytes)
	}
	if _, err := Load(writeTempConfig(t, exact)); err != nil {
		t.Fatalf("exact-limit config rejected: %v", err)
	}

	// One byte over: rejected with a clear error.
	_, err := Load(writeTempConfig(t, exact+"\n"))
	if err == nil {
		t.Fatal("config over the size limit must be rejected")
	}
	if !strings.Contains(err.Error(), "exceeds the maximum") {
		t.Errorf("error %q must explain the size limit", err)
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	if _, err := Load(writeTempConfig(t, "app: [unclosed\n")); err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

func TestLoadUnknownField(t *testing.T) {
	_, err := Load(writeTempConfig(t, "app:\n  verbosity: 3\n"))
	if err == nil {
		t.Fatal("expected error for unknown field, got nil")
	}
	if !strings.Contains(err.Error(), "verbosity") {
		t.Errorf("error should mention the unknown field, got: %v", err)
	}
}

func TestLoadInvalidLogLevel(t *testing.T) {
	if _, err := Load(writeTempConfig(t, "app:\n  log_level: loud\n")); err == nil {
		t.Fatal("expected error for invalid log level, got nil")
	}
}

func TestLoadInvalidStorageDriver(t *testing.T) {
	_, err := Load(writeTempConfig(t, "storage:\n  driver: postgres\n"))
	if err == nil {
		t.Fatal("expected error for unsupported storage driver, got nil")
	}
	if !strings.Contains(err.Error(), "storage.driver") {
		t.Errorf("error should mention storage.driver, got: %v", err)
	}
}

func TestLoadInvalidExpirationAndRetention(t *testing.T) {
	if _, err := Load(writeTempConfig(t, "app:\n  expiration_interval: 0s\n")); err == nil {
		t.Fatal("expected error for expiration_interval 0s, got nil")
	}
	if _, err := Load(writeTempConfig(t, "app:\n  change_retention: 0s\n")); err == nil {
		t.Fatal("expected error for change_retention 0s, got nil")
	}
}

func TestLoadLogRotation(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, "app:\n  log_file: /var/log/hr.log\n  log_max_size_mb: 25\n  log_max_backups: 3\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.App.LogFile != "/var/log/hr.log" || cfg.App.LogMaxSizeMB != 25 || cfg.App.LogMaxBackups != 3 {
		t.Errorf("log rotation = %+v", cfg.App)
	}
	if _, err := Load(writeTempConfig(t, "app:\n  log_max_size_mb: 0\n")); err == nil {
		t.Fatal("expected error for log_max_size_mb 0, got nil")
	}
}
