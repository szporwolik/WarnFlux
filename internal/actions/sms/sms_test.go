package sms

import (
	"context"
	"strings"
	"sync"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

// fakeSender is an in-memory sender seam.
type fakeSender struct {
	enabled bool

	mu   sync.Mutex
	sent [][2]string // number, text
}

func (f *fakeSender) Enabled() bool { return f.enabled }

func (f *fakeSender) Send(_ context.Context, number, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, [2]string{number, text})
	return nil
}

func (f *fakeSender) list() [][2]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][2]string, len(f.sent))
	copy(out, f.sent)
	return out
}

func configNode(t *testing.T, m map[string]any) *yaml.Node {
	t.Helper()
	var node yaml.Node
	if err := node.Encode(m); err != nil {
		t.Fatal(err)
	}
	return &node
}

func hazardReq() action.ActionRequest {
	return action.ActionRequest{
		App: action.AppInfo{Header1: "SOSNA"},
		Event: dispatch.Event{
			Kind: dispatch.EventHazardTransition,
			Hazard: &dispatch.HazardTransition{
				Hazard: dispatch.Hazard{
					EventKey:  "imgw-meteo:1",
					Severity:  "severe",
					Event:     "Burza z gradem",
					Headline:  "Ostrzeżenie dla powiatu krakowskiego",
					Latitude:  ptr(50.06),
					Longitude: ptr(19.94),
				},
			},
		},
	}
}

func ptr(f float64) *float64 { return &f }

func TestNewValidation(t *testing.T) {
	hub := &fakeSender{enabled: true}

	if _, err := New(configNode(t, map[string]any{}), hub); err == nil {
		t.Error("empty phones accepted")
	}
	if _, err := New(configNode(t, map[string]any{"phones": []string{"+48600111222", "abc"}}), hub); err == nil {
		t.Error("invalid phone accepted")
	}
	if _, err := New(configNode(t, map[string]any{"phones": []string{"+48600111222"}}), nil); err == nil {
		t.Error("nil hub accepted")
	}
	if _, err := New(configNode(t, map[string]any{"phones": []string{"+48600111222"}}), &fakeSender{enabled: false}); err == nil {
		t.Error("disabled hub accepted")
	}
	p, err := New(configNode(t, map[string]any{
		"phones": []string{"+48600111222", "509558155"},
		"prefix": "WarnFlux",
	}), hub)
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if p.Name() != Type {
		t.Errorf("Name() = %q, want %q", p.Name(), Type)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestExecuteSendsToAllRecipients(t *testing.T) {
	hub := &fakeSender{enabled: true}
	p, err := New(configNode(t, map[string]any{
		"phones": []string{"+48600111222"},
		"prefix": "WarnFlux",
	}), hub)
	if err != nil {
		t.Fatal(err)
	}
	req := hazardReq()
	req.Phones = []string{"509558155"}
	req.PhoneLangs = []string{"pl"}
	if err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	sent := hub.list()
	if len(sent) != 2 {
		t.Fatalf("sent = %d messages, want 2: %v", len(sent), sent)
	}
	if sent[0][0] != "+48600111222" || sent[1][0] != "509558155" {
		t.Errorf("recipients = %v", sent)
	}
	if !strings.HasPrefix(sent[0][1], "WarnFlux SEVERE") {
		t.Errorf("message missing prefix/severity: %q", sent[0][1])
	}
}

func TestExecuteDedupsByNumberKey(t *testing.T) {
	hub := &fakeSender{enabled: true}
	p, err := New(configNode(t, map[string]any{
		"phones": []string{"+48600111222"},
	}), hub)
	if err != nil {
		t.Fatal(err)
	}
	req := hazardReq()
	req.Phones = []string{"+48 600 111 222", "0048600111222"} // same number, different formats
	if err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sent := hub.list(); len(sent) != 1 {
		t.Fatalf("sent = %d messages, want 1 (dedup by digits): %v", len(sent), sent)
	}
}

func TestTextBoundedAndTransliterated(t *testing.T) {
	hub := &fakeSender{enabled: true}
	p, err := New(configNode(t, map[string]any{
		"phones": []string{"+48600111222"},
		"prefix": "WarnFlux",
	}), hub)
	if err != nil {
		t.Fatal(err)
	}
	a := p.(*Action)
	req := hazardReq()
	req.Event.Hazard.Hazard.Headline = "Żółć ąę " + strings.Repeat("x", 200)
	text := a.textFor(context.Background(), req, "pl")
	if n := len([]rune(text)); n > maxSMSChars {
		t.Fatalf("text length = %d, want <= %d", n, maxSMSChars)
	}
	if strings.ContainsAny(text, "Żółćąę") {
		t.Fatalf("text not transliterated: %q", text)
	}
	if !strings.HasPrefix(text, "WarnFlux ") {
		t.Fatalf("text missing the prefix: %q", text)
	}
	if !strings.HasPrefix(text, "WarnFlux SEVERE") && !strings.HasPrefix(text, "WarnFlux POWAZNE") {
		t.Fatalf("text missing the severity: %q", text)
	}
}
