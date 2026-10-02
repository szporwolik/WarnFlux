package meshtastic

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

// stubSender records every transmission the action hands to the hub.
type stubSender struct {
	contacts []string
	channels []int
	texts    []string
	err      error
}

func (s *stubSender) SendContactMessage(_ context.Context, addr, text, operator string) error {
	s.contacts = append(s.contacts, addr)
	s.texts = append(s.texts, text)
	return s.err
}

func (s *stubSender) SendChannelText(_ context.Context, idx int, text, operator string) error {
	s.channels = append(s.channels, idx)
	return s.err
}

func hazardReq(nodeIDs []string, headline string) action.ActionRequest {
	return action.ActionRequest{
		ID:        "k",
		CreatedAt: time.Now(),
		Event: dispatch.Event{
			Kind: dispatch.EventHazardTransition,
			Hazard: &dispatch.HazardTransition{
				Hazard: dispatch.Hazard{Event: "Storm", Severity: "severe", Headline: headline},
			},
		},
		MeshNodeIDs: nodeIDs,
	}
}

// TestExecuteDMsEveryMember pins the per-user delivery: one direct
// message per routed node ID, none when the group has no registered
// nodes (channel broadcast fallback).
func TestExecuteDMsEveryMember(t *testing.T) {
	stub := &stubSender{}
	a := &Action{cfg: Config{Prefix: "SOSNA"}, hub: stub}

	if err := a.Execute(context.Background(), hazardReq([]string{"a0a85934", "b0b85934"}, "Big storm coming")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(stub.contacts) != 2 || stub.contacts[0] != "a0a85934" || stub.contacts[1] != "b0b85934" {
		t.Fatalf("contacts = %v, want both node IDs", stub.contacts)
	}
	if len(stub.channels) != 0 {
		t.Fatalf("broadcasts = %v, want none when node IDs exist", stub.channels)
	}
	for i, text := range stub.texts {
		if !strings.HasPrefix(text, "SOSNA SEVERE") {
			t.Fatalf("message %d = %q, want the SOSNA SEVERE prefix", i, text)
		}
	}

	// No node IDs and no fallback channel: nothing is transmitted.
	stub2 := &stubSender{}
	a2 := &Action{cfg: Config{Prefix: "SOSNA"}, hub: stub2}
	if err := a2.Execute(context.Background(), hazardReq(nil, "Flood alert")); err != nil {
		t.Fatalf("Execute skip: %v", err)
	}
	if len(stub2.channels) != 0 || len(stub2.contacts) != 0 {
		t.Fatalf("skip = contacts %v broadcasts %v, want none", stub2.contacts, stub2.channels)
	}

	// No node IDs with a fallback channel: one broadcast on that channel.
	stub3 := &stubSender{}
	a3 := &Action{cfg: Config{Prefix: "SOSNA", Channel: 1}, hub: stub3}
	if err := a3.Execute(context.Background(), hazardReq(nil, "Flood alert")); err != nil {
		t.Fatalf("Execute fallback: %v", err)
	}
	if len(stub3.channels) != 1 || stub3.channels[0] != 1 || len(stub3.contacts) != 0 {
		t.Fatalf("fallback = contacts %v broadcasts %v, want one broadcast on channel 1", stub3.contacts, stub3.channels)
	}
}

// TestExecuteFailurePropagates pins error handling and the empty-headline
// fallback to the event name.
func TestExecuteFailurePropagates(t *testing.T) {
	stub := &stubSender{err: errors.New("device gone")}
	a := &Action{cfg: Config{Prefix: "SOSNA"}, hub: stub}
	err := a.Execute(context.Background(), hazardReq([]string{"a0a85934"}, "x"))
	if err == nil || !strings.Contains(err.Error(), "a0a85934") {
		t.Fatalf("Execute = %v, want an error naming the node", err)
	}

	stub2 := &stubSender{}
	a2 := &Action{cfg: Config{Prefix: "SOSNA"}, hub: stub2}
	if err := a2.Execute(context.Background(), hazardReq([]string{"a0a85934"}, "")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(stub2.texts) != 1 || !strings.HasPrefix(stub2.texts[0], "SOSNA SEVERE Storm") {
		t.Fatalf("text = %v, want the event-name fallback", stub2.texts)
	}
}

// TestExecuteNonHazardNoop pins that non-hazard events are skipped.
func TestExecuteNonHazardNoop(t *testing.T) {
	stub := &stubSender{}
	a := &Action{cfg: Config{}, hub: stub}
	if err := a.Execute(context.Background(), action.ActionRequest{Event: dispatch.Event{Kind: dispatch.EventMQTTMessage}}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(stub.contacts) != 0 || len(stub.channels) != 0 {
		t.Fatalf("non-hazard event transmitted: %v %v", stub.contacts, stub.channels)
	}
}
