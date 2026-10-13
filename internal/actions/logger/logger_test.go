package logger_test

import (
	"context"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/actions/logger"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

func cfgNode(level string) *yaml.Node {
	if level == "" {
		return nil
	}
	var n yaml.Node
	if err := yaml.Unmarshal([]byte("level: "+level), &n); err != nil {
		panic(err)
	}
	return &n
}

func TestFactoryConfigDecode(t *testing.T) {
	p, err := logger.New(cfgNode("warn"))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	if p.Name() == "" {
		t.Error("name empty")
	}
}

func TestFactoryDefaultLevel(t *testing.T) {
	if _, err := logger.New(nil); err != nil {
		t.Fatalf("default config rejected: %v", err)
	}
}

func TestFactoryRejectsBadLevel(t *testing.T) {
	if _, err := logger.New(cfgNode("verbose")); err == nil {
		t.Fatal("want error for invalid level")
	}
}

func TestExecuteAndClose(t *testing.T) {
	p, err := logger.New(cfgNode("info"))
	if err != nil {
		t.Fatal(err)
	}
	req := action.ActionRequest{
		ID:        "r1",
		CreatedAt: time.Now(),
		Event: dispatch.Event{
			Kind:       dispatch.EventHazardTransition,
			ReceivedAt: time.Now(),
			Origin:     dispatch.Origin{Type: "mqtt", ReceiverID: "local"},
			Hazard: &dispatch.HazardTransition{
				Type:   dispatch.TransitionNew,
				Key:    "imgw-meteo:123",
				Source: "imgw-meteo",
				Hazard: dispatch.Hazard{Severity: "extreme", Headline: "Wind"},
			},
		},
	}
	if err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// Generic MQTT events log without assuming any payload format.
	req2 := action.ActionRequest{
		ID: "r2",
		Event: dispatch.Event{
			Kind:   dispatch.EventMQTTMessage,
			Origin: dispatch.Origin{Type: "mqtt", ReceiverID: "remote"},
			MQTT:   &dispatch.MQTTMessage{Topic: "club/alarm/door", Payload: []byte{0x01}},
		},
	}
	if err := p.Execute(context.Background(), req2); err != nil {
		t.Fatalf("Execute generic: %v", err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestSharedDestination pins the capability the routing engine keys on: the
// logged line names the action, never the group, so two groups following the
// same sources must log the same alert once (reported P2).
func TestSharedDestination(t *testing.T) {
	var p action.Plugin
	built, err := logger.New(cfgNode("info"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p = built
	shared, ok := p.(action.SharedDestination)
	if !ok {
		t.Fatal("the logger action does not report a shared destination")
	}
	if !shared.SharedDestination() {
		t.Error("SharedDestination() = false, want true (one log line per alert)")
	}
}
