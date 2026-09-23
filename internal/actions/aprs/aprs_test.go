package aprs

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

type fakeTransmitter struct {
	name  string
	ready bool
	sent  [][2]string
}

func (f *fakeTransmitter) Name() string { return f.name }
func (f *fakeTransmitter) Ready() bool  { return f.ready }
func (f *fakeTransmitter) Send(_ context.Context, to, text string) error {
	f.sent = append(f.sent, [2]string{to, text})
	return nil
}

func testHub(t *testing.T) *aprs.Hub {
	t.Helper()
	hub, err := aprs.NewHub(aprs.HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		GridSquare: "JO90WW",
		RadiusKM:   60,
		StationTTL: 30 * time.Minute,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func configNode(t *testing.T, m map[string]any) *yaml.Node {
	t.Helper()
	var node yaml.Node
	if err := node.Encode(m); err != nil {
		t.Fatal(err)
	}
	return &node
}

func TestNewValidation(t *testing.T) {
	hub := testHub(t)

	if _, err := New(configNode(t, map[string]any{}), hub); err == nil {
		t.Error("empty callsigns accepted")
	}
	if _, err := New(configNode(t, map[string]any{"callsigns": []string{"SP9XYZ", "BAD CALL"}}), hub); err == nil {
		t.Error("invalid callsign accepted")
	}
	if _, err := New(configNode(t, map[string]any{"callsigns": []string{"sp9xyz"}}), nil); err == nil {
		t.Error("nil hub accepted")
	}

	p, err := New(configNode(t, map[string]any{
		"callsigns": []string{"sp9xyz-7", "SR9KR"},
		"prefix":    "WarnFlux",
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
	hub := testHub(t)
	tx := &fakeTransmitter{name: "aprs-inet", ready: true}
	hub.AddTransmitter("aprs-inet", tx)

	p, err := New(configNode(t, map[string]any{
		"callsigns": []string{"SP9XYZ", "SR9KR"},
		"prefix":    "WarnFlux",
	}), hub)
	if err != nil {
		t.Fatal(err)
	}

	req := action.ActionRequest{
		App: action.AppInfo{Header1: "SOSNA"},
		Event: dispatch.Event{
			Kind: dispatch.EventHazardTransition,
			Hazard: &dispatch.HazardTransition{
				Hazard: dispatch.Hazard{
					Severity: "severe",
					Event:    "Burza z gradem",
					Headline: "Ostrzeżenie dla powiatu krakowskiego",
				},
			},
		},
	}
	if err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(tx.sent) != 2 {
		t.Fatalf("sent = %d messages, want 2", len(tx.sent))
	}
	if tx.sent[0][0] != "SP9XYZ" || tx.sent[1][0] != "SR9KR" {
		t.Errorf("recipients = %v", tx.sent)
	}
	text := tx.sent[0][1]
	if !strings.HasPrefix(text, "WarnFlux SEVERE Burza z gradem") {
		t.Errorf("message = %q", text)
	}
	if len(text) > aprs.MaxMessageText {
		t.Errorf("message is %d bytes, over the %d limit", len(text), aprs.MaxMessageText)
	}
}

func TestExecuteSendsToGroupMemberCallsigns(t *testing.T) {
	hub := testHub(t)
	tx := &fakeTransmitter{name: "aprs-inet", ready: true}
	hub.AddTransmitter("aprs-inet", tx)

	p, err := New(configNode(t, map[string]any{
		"callsigns": []string{"SR9KR"},
	}), hub)
	if err != nil {
		t.Fatal(err)
	}

	// A member callsign duplicates the configured one and must be deduped.
	req := action.ActionRequest{
		Event: dispatch.Event{
			Kind: dispatch.EventHazardTransition,
			Hazard: &dispatch.HazardTransition{
				Hazard: dispatch.Hazard{Severity: "severe", Event: "Burza", Headline: "x"},
			},
		},
		APRSCallsigns: []string{"sp9moa-16", "SR9KR"},
	}
	if err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(tx.sent) != 2 {
		t.Fatalf("sent = %d messages, want 2 (dedup of SR9KR)", len(tx.sent))
	}
	to := []string{tx.sent[0][0], tx.sent[1][0]}
	if to[0] != "SR9KR" || to[1] != "SP9MOA-16" {
		t.Errorf("recipients = %v, want [SR9KR SP9MOA-16]", to)
	}
}

func TestExecuteNoTransmitter(t *testing.T) {
	hub := testHub(t)
	p, err := New(configNode(t, map[string]any{
		"callsigns": []string{"SP9XYZ"},
	}), hub)
	if err != nil {
		t.Fatal(err)
	}
	err = p.Execute(context.Background(), action.ActionRequest{})
	if err == nil {
		t.Fatal("Execute with no transmitter must fail")
	}
	if !strings.Contains(err.Error(), "1 of 1 messages failed") {
		t.Errorf("error = %v", err)
	}
}
