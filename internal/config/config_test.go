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
