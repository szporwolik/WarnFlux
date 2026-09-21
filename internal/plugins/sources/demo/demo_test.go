package demo

import (
	"context"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

func decodeConfig(t *testing.T, yamlText string) *yaml.Node {
	t.Helper()
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(yamlText), &node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &node
}

func TestNewDefaultsAndValidation(t *testing.T) {
	p, err := New(nil)
	if err != nil {
		t.Fatalf("New(nil): %v", err)
	}
	if p.Name() != Type {
		t.Errorf("Name = %q, want %q", p.Name(), Type)
	}

	if _, err := New(decodeConfig(t, "interval: -5s")); err == nil {
		t.Fatal("negative interval must be rejected")
	}
	if _, err := New(decodeConfig(t, "unknown_key: 1")); err == nil {
		t.Fatal("unknown config key must be rejected")
	}
}

func TestRunEmitsSyntheticEvents(t *testing.T) {
	p, err := New(decodeConfig(t, "interval: 10ms"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var events []core.HazardEvent
	emit := plugin.EmitterFunc(func(_ context.Context, event core.HazardEvent) error {
		events = append(events, event)
		return nil
	})

	if err := p.Run(ctx, emit); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("emitted %d events, want at least 2", len(events))
	}
	for _, e := range events {
		if e.Source != Type || e.Event != "Drill" || e.SourceID == "" {
			t.Errorf("unexpected event: %+v", e)
		}
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
