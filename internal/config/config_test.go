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

mqtt:
  enabled: true
  broker: tcp://localhost:1883
  client_id: warnflux
  username: ""
  password: ""
  topic_prefix: warnflux
  qos: 1
  ping_interval: 30s
`

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

func TestLoadFullConfig(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, exampleConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.App.LogLevel != "info" {
		t.Errorf("log level = %q, want %q", cfg.App.LogLevel, "info")
	}
	if !cfg.MQTT.Enabled {
		t.Error("MQTT should be enabled")
	}
	if cfg.MQTT.Broker != "tcp://localhost:1883" {
		t.Errorf("broker = %q, want %q", cfg.MQTT.Broker, "tcp://localhost:1883")
	}
	if cfg.MQTT.ClientID != "warnflux" {
		t.Errorf("client_id = %q, want %q", cfg.MQTT.ClientID, "warnflux")
	}
	if cfg.MQTT.Username != "" || cfg.MQTT.Password != "" {
		t.Error("credentials should be empty")
	}
	if cfg.MQTT.TopicPrefix != "warnflux" {
		t.Errorf("topic_prefix = %q, want %q", cfg.MQTT.TopicPrefix, "warnflux")
	}
	if cfg.MQTT.QoS != 1 {
		t.Errorf("qos = %d, want 1", cfg.MQTT.QoS)
	}
	if cfg.MQTT.PingInterval != 30*time.Second {
		t.Errorf("ping_interval = %s, want 30s", cfg.MQTT.PingInterval)
	}
}

func TestLoadDefaults(t *testing.T) {
	// An empty file is valid and yields all defaults.
	cfg, err := Load(writeTempConfig(t, ""))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.App.LogLevel != "info" {
		t.Errorf("log level = %q, want %q", cfg.App.LogLevel, "info")
	}
	if cfg.MQTT.Enabled {
		t.Error("MQTT should default to disabled")
	}
	if cfg.MQTT.ClientID != "warnflux" {
		t.Errorf("client_id = %q, want %q", cfg.MQTT.ClientID, "warnflux")
	}
	if cfg.MQTT.TopicPrefix != "warnflux" {
		t.Errorf("topic_prefix = %q, want %q", cfg.MQTT.TopicPrefix, "warnflux")
	}
	if cfg.MQTT.QoS != 1 {
		t.Errorf("qos = %d, want 1", cfg.MQTT.QoS)
	}
	if cfg.MQTT.PingInterval != 30*time.Second {
		t.Errorf("ping_interval = %s, want 30s", cfg.MQTT.PingInterval)
	}
}

func TestLoadExplicitZeroQoS(t *testing.T) {
	// An explicit qos: 0 must not be overridden by the default.
	cfg, err := Load(writeTempConfig(t, "mqtt:\n  enabled: true\n  broker: tcp://localhost:1883\n  qos: 0\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MQTT.QoS != 0 {
		t.Errorf("qos = %d, want 0", cfg.MQTT.QoS)
	}
}

func TestLoadDisabledMQTTIsValid(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, "app:\n  log_level: debug\nmqtt:\n  enabled: false\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.App.LogLevel != "debug" {
		t.Errorf("log level = %q, want %q", cfg.App.LogLevel, "debug")
	}
	if cfg.MQTT.Enabled {
		t.Error("MQTT should be disabled")
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("expected error for missing file, got nil")
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
	_, err := Load(writeTempConfig(t, "app:\n  log_level: loud\n"))
	if err == nil {
		t.Fatal("expected error for invalid log level, got nil")
	}
}

func TestLoadInvalidQoS(t *testing.T) {
	if _, err := Load(writeTempConfig(t, "mqtt:\n  qos: 7\n")); err == nil {
		t.Fatal("expected error for qos > 2, got nil")
	}
	if _, err := Load(writeTempConfig(t, "mqtt:\n  qos: -1\n")); err == nil {
		t.Fatal("expected error for negative qos, got nil")
	}
}

func TestLoadEnabledRequiresBroker(t *testing.T) {
	_, err := Load(writeTempConfig(t, "mqtt:\n  enabled: true\n"))
	if err == nil {
		t.Fatal("expected error when enabled without broker, got nil")
	}
	if !strings.Contains(err.Error(), "broker") {
		t.Errorf("error should mention the broker, got: %v", err)
	}
}

func TestLoadMissingValuesGetDefaults(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, "mqtt:\n  enabled: true\n  broker: ssl://broker.example.com:8883\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MQTT.ClientID != "warnflux" {
		t.Errorf("client_id = %q, want default", cfg.MQTT.ClientID)
	}
	if cfg.MQTT.TopicPrefix != "warnflux" {
		t.Errorf("topic_prefix = %q, want default", cfg.MQTT.TopicPrefix)
	}
}

func TestLoadPingIntervalParsed(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, "mqtt:\n  enabled: true\n  broker: tcp://localhost:1883\n  ping_interval: 45s\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MQTT.PingInterval != 45*time.Second {
		t.Errorf("ping_interval = %s, want 45s", cfg.MQTT.PingInterval)
	}
}

func TestLoadInvalidPingInterval(t *testing.T) {
	for _, interval := range []string{"0s", "-5s"} {
		_, err := Load(writeTempConfig(t, "mqtt:\n  ping_interval: "+interval+"\n"))
		if err == nil {
			t.Errorf("expected error for ping_interval %q, got nil", interval)
		}
	}
}

func TestLoadStorageConfig(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, "app:\n  expiration_interval: 45s\nstorage:\n  driver: sqlite\n  path: /data/warnflux.db\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.App.ExpirationInterval != 45*time.Second {
		t.Errorf("expiration_interval = %s, want 45s", cfg.App.ExpirationInterval)
	}
	if cfg.Storage.Driver != "sqlite" {
		t.Errorf("storage driver = %q, want %q", cfg.Storage.Driver, "sqlite")
	}
	if cfg.Storage.Path != "/data/warnflux.db" {
		t.Errorf("storage path = %q", cfg.Storage.Path)
	}
}

func TestLoadStorageDefaults(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, ""))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.App.ExpirationInterval != time.Minute {
		t.Errorf("expiration_interval = %s, want 1m", cfg.App.ExpirationInterval)
	}
	if cfg.Storage.Driver != "sqlite" {
		t.Errorf("storage driver = %q, want %q", cfg.Storage.Driver, "sqlite")
	}
	if cfg.Storage.Path != "warnflux.db" {
		t.Errorf("storage path = %q, want %q", cfg.Storage.Path, "warnflux.db")
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

func TestLoadInvalidExpirationInterval(t *testing.T) {
	if _, err := Load(writeTempConfig(t, "app:\n  expiration_interval: 0s\n")); err == nil {
		t.Fatal("expected error for expiration_interval 0s, got nil")
	}
}

func TestLoadLogRotationDefaults(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, ""))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.App.LogFile != "" {
		t.Errorf("log_file = %q, want empty (stdout only)", cfg.App.LogFile)
	}
	if cfg.App.LogMaxSizeMB != 10 {
		t.Errorf("log_max_size_mb = %d, want 10", cfg.App.LogMaxSizeMB)
	}
	if cfg.App.LogMaxBackups != 5 {
		t.Errorf("log_max_backups = %d, want 5", cfg.App.LogMaxBackups)
	}
}

func TestLoadLogRotationConfig(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, "app:\n  log_file: /var/log/warnflux.log\n  log_max_size_mb: 25\n  log_max_backups: 3\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.App.LogFile != "/var/log/warnflux.log" {
		t.Errorf("log_file = %q", cfg.App.LogFile)
	}
	if cfg.App.LogMaxSizeMB != 25 {
		t.Errorf("log_max_size_mb = %d, want 25", cfg.App.LogMaxSizeMB)
	}
	if cfg.App.LogMaxBackups != 3 {
		t.Errorf("log_max_backups = %d, want 3", cfg.App.LogMaxBackups)
	}
}

func TestLoadInvalidLogRotation(t *testing.T) {
	if _, err := Load(writeTempConfig(t, "app:\n  log_max_size_mb: 0\n")); err == nil {
		t.Fatal("expected error for log_max_size_mb 0, got nil")
	}
	if _, err := Load(writeTempConfig(t, "app:\n  log_max_backups: -1\n")); err == nil {
		t.Fatal("expected error for negative log_max_backups, got nil")
	}
}

const pluginConfig = `sources:
  - id: demo
    type: demo
    enabled: false
    runtime:
      restart: true
      startup_timeout: 15s
      shutdown_timeout: 10s
    config:
      interval: 30s
outputs:
  - id: mqtt-main
    type: mqtt
    enabled: true
    runtime:
      timeout: 10s
      failure_threshold: 5
    config:
      broker: tcp://localhost:1883
`

func TestLoadPluginConfig(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, pluginConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Sources) != 1 || len(cfg.Outputs) != 1 {
		t.Fatalf("sources=%d outputs=%d, want 1/1", len(cfg.Sources), len(cfg.Outputs))
	}
	src := cfg.Sources[0]
	if src.ID != "demo" || src.Type != "demo" || src.Enabled {
		t.Errorf("unexpected source: %+v", src)
	}
	if src.Runtime.Restart != true || src.Runtime.StartupTimeout != 15*time.Second || src.Runtime.ShutdownTimeout != 10*time.Second {
		t.Errorf("unexpected source runtime: %+v", src.Runtime)
	}
	if src.Config == nil {
		t.Error("source config node should be preserved")
	}
	out := cfg.Outputs[0]
	if out.ID != "mqtt-main" || out.Type != "mqtt" || !out.Enabled {
		t.Errorf("unexpected output: %+v", out)
	}
	if out.Runtime.Timeout != 10*time.Second || out.Runtime.FailureThreshold != 5 {
		t.Errorf("unexpected output runtime: %+v", out.Runtime)
	}
	if out.Config == nil {
		t.Error("output config node should be preserved")
	}
}

func TestLoadPluginRuntimeDefaults(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, "sources:\n  - id: s\n    type: demo\n    enabled: true\noutputs:\n  - id: o\n    type: mqtt\n    enabled: true\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Sources[0].Runtime; got.Restart != true || got.StartupTimeout != 15*time.Second || got.ShutdownTimeout != 10*time.Second {
		t.Errorf("source runtime defaults = %+v", got)
	}
	if got := cfg.Outputs[0].Runtime; got.Timeout != 10*time.Second || got.FailureThreshold != 5 {
		t.Errorf("output runtime defaults = %+v", got)
	}
}

func TestLoadPluginDuplicateIDRejected(t *testing.T) {
	_, err := Load(writeTempConfig(t, "sources:\n  - id: x\n    type: demo\noutputs:\n  - id: x\n    type: mqtt\n"))
	if err == nil || !strings.Contains(err.Error(), "duplicate plugin id") {
		t.Errorf("error = %v, want duplicate plugin id", err)
	}
}

func TestLoadPluginMissingTypeRejected(t *testing.T) {
	if _, err := Load(writeTempConfig(t, "sources:\n  - id: x\n")); err == nil {
		t.Fatal("expected error for missing type, got nil")
	}
	if _, err := Load(writeTempConfig(t, "sources:\n  - type: demo\n")); err == nil {
		t.Fatal("expected error for missing id, got nil")
	}
}

func TestLoadPluginInvalidRuntimeRejected(t *testing.T) {
	if _, err := Load(writeTempConfig(t, "sources:\n  - id: x\n    type: demo\n    runtime:\n      startup_timeout: 0s\n")); err == nil {
		t.Fatal("expected error for startup_timeout 0s, got nil")
	}
	if _, err := Load(writeTempConfig(t, "outputs:\n  - id: x\n    type: mqtt\n    runtime:\n      failure_threshold: 0\n")); err == nil {
		t.Fatal("expected error for failure_threshold 0, got nil")
	}
}
