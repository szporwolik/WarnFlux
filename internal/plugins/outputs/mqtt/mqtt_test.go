package mqtt

import (
	"testing"

	"gopkg.in/yaml.v3"

	"warnflux/internal/plugin"
)

func decodeConfig(t *testing.T, yamlText string) *yaml.Node {
	t.Helper()
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(yamlText), &node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &node
}

func TestNewValidation(t *testing.T) {
	p, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nqos: 1\n"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if p.Name() != Type {
		t.Errorf("Name = %q, want %q", p.Name(), Type)
	}

	// Missing broker is a startup (configuration) error.
	if _, err := New(nil); err == nil {
		t.Fatal("missing broker must be rejected")
	}
	if _, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nqos: 7\n")); err == nil {
		t.Fatal("qos > 2 must be rejected")
	}
	if _, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nbogus: 1\n")); err == nil {
		t.Fatal("unknown config key must be rejected")
	}
}

func TestConfigDefaults(t *testing.T) {
	p, err := New(decodeConfig(t, "broker: tcp://localhost:1883\n"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	out := p.(*Output)
	if out.cfg.ClientID != "warnflux-events" {
		t.Errorf("client_id = %q, want default", out.cfg.ClientID)
	}
	if out.cfg.TopicPrefix != "warnflux" {
		t.Errorf("topic_prefix = %q, want default", out.cfg.TopicPrefix)
	}
}

func TestRegister(t *testing.T) {
	reg := plugin.NewRegistry()
	if err := Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := Register(reg); err == nil {
		t.Fatal("double registration must fail")
	}
}
