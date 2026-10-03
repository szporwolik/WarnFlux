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
// message per routed node ID, and the configured group channel carries
// every alert as the base delivery — with or without registered nodes.
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
		t.Fatalf("broadcasts = %v, want none without a configured channel", stub.channels)
	}
	for i, text := range stub.texts {
		if !strings.HasPrefix(text, "SOSNA SEVERE") {
			t.Fatalf("message %d = %q, want the SOSNA SEVERE prefix", i, text)
		}
	}

	// No channel and no node IDs: nothing is transmitted.
	stubSkip := &stubSender{}
	aSkip := &Action{cfg: Config{Prefix: "SOSNA"}, hub: stubSkip}
	if err := aSkip.Execute(context.Background(), hazardReq(nil, "Flood alert")); err != nil {
		t.Fatalf("Execute skip: %v", err)
	}
	if len(stubSkip.channels) != 0 || len(stubSkip.contacts) != 0 {
		t.Fatalf("skip = contacts %v broadcasts %v, want none", stubSkip.contacts, stubSkip.channels)
	}

	// Channel configured: the broadcast goes out even when the group
	// has no registered node IDs.
	stub2 := &stubSender{}
	a2 := &Action{cfg: Config{Prefix: "SOSNA", Channel: 1}, hub: stub2}
	if err := a2.Execute(context.Background(), hazardReq(nil, "Flood alert")); err != nil {
		t.Fatalf("Execute fallback: %v", err)
	}
	if len(stub2.channels) != 1 || stub2.channels[0] != 1 || len(stub2.contacts) != 0 {
		t.Fatalf("fallback = contacts %v broadcasts %v, want one broadcast on channel 1", stub2.contacts, stub2.channels)
	}

	// Channel configured AND node IDs present: the broadcast is the
	// base delivery and the direct messages go out on top.
	stub3 := &stubSender{}
	a3 := &Action{cfg: Config{Prefix: "SOSNA", Channel: 2}, hub: stub3}
	if err := a3.Execute(context.Background(), hazardReq([]string{"a0a85934"}, "Storm alert")); err != nil {
		t.Fatalf("Execute channel+DM: %v", err)
	}
	if len(stub3.channels) != 1 || stub3.channels[0] != 2 {
		t.Fatalf("broadcasts = %v, want one on channel 2", stub3.channels)
	}
	if len(stub3.contacts) != 1 || stub3.contacts[0] != "a0a85934" {
		t.Fatalf("contacts = %v, want the node DM on top of the broadcast", stub3.contacts)
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

// fullReq builds one hazard request with every text-relevant field set.
// lat/lon are only attached when at least one is non-zero, so a zero
// pair means "no coordinates" (areas may still carry a location).
func fullReq(headline, event, desc string, lat, lon float64, areas []string) action.ActionRequest {
	var latP, lonP *float64
	if lat != 0 || lon != 0 {
		latP, lonP = &lat, &lon
	}
	return action.ActionRequest{
		ID:        "k",
		CreatedAt: time.Now(),
		Event: dispatch.Event{
			Kind: dispatch.EventHazardTransition,
			Hazard: &dispatch.HazardTransition{
				Hazard: dispatch.Hazard{
					Event: event, Severity: "severe", Headline: headline,
					Description: desc, Areas: areas,
					Latitude: latP, Longitude: lonP,
				},
			},
		},
	}
}

// TestTextForLocationRidesAlong pins the enriched message: coordinates,
// event type and description squeeze into the channel limit behind the
// title (APRS normalization transliterates diacritics).
func TestTextForLocationRidesAlong(t *testing.T) {
	a := &Action{cfg: Config{Prefix: "SOSNA"}}
	got := a.textFor(context.Background(), fullReq("Powódź na Rabie", "Flood", "Unikaj brzegów rzeki.", 49.985, 20.065, nil))
	if len([]rune(got)) > maxMeshMessageChars {
		t.Fatalf("text = %q, %d runes — over the channel limit", got, len([]rune(got)))
	}
	for _, want := range []string{"SOSNA SEVERE Powodz na Rabie", "49.985,20.065", "Flood", "Unikaj brzegow rzeki"} {
		if !strings.Contains(got, want) {
			t.Errorf("text = %q, missing %q", got, want)
		}
	}
}

// TestTextForTitleAndLocationSurvive pins the worst case: a gigantic
// headline gives up space so the LOCATION and the prefix/severity always
// survive — the title is trimmed, never the fixed parts.
func TestTextForTitleAndLocationSurvive(t *testing.T) {
	a := &Action{cfg: Config{Prefix: "SOSNA"}}
	huge := strings.Repeat("uwaga ", 80)
	got := a.textFor(context.Background(), fullReq(huge, "Flood", "", 49.985, 20.065, nil))
	if len([]rune(got)) > maxMeshMessageChars {
		t.Fatalf("text = %q, %d runes — over the channel limit", got, len([]rune(got)))
	}
	if !strings.HasPrefix(got, "SOSNA SEVERE ") {
		t.Errorf("text = %q, prefix/severity must survive", got)
	}
	if !strings.HasSuffix(got, "49.985,20.065") {
		t.Errorf("text = %q, the location must survive whole", got)
	}
	if !strings.Contains(got, "uwaga") {
		t.Errorf("text = %q, the trimmed title must still be present", got)
	}
}

// TestTextForHardCutNoLocation pins the no-location worst case: the
// text is cut hard at the limit with the prefix and severity intact.
func TestTextForHardCutNoLocation(t *testing.T) {
	a := &Action{cfg: Config{Prefix: "SOSNA"}}
	got := a.textFor(context.Background(), fullReq(strings.Repeat("abcdefghij", 30), "", "", 0, 0, nil))
	if len([]rune(got)) != maxMeshMessageChars {
		t.Fatalf("text = %d runes, want the hard cut at %d", len([]rune(got)), maxMeshMessageChars)
	}
	if !strings.HasPrefix(got, "SOSNA SEVERE ") {
		t.Errorf("text = %q, prefix/severity must survive the hard cut", got)
	}
}

// TestTextForDescriptionTail pins the leftover budget: the description
// fills the remaining space and a cut is marked with "...".
func TestTextForDescriptionTail(t *testing.T) {
	a := &Action{cfg: Config{Prefix: "SOSNA"}}
	got := a.textFor(context.Background(), fullReq("Krótki tytuł", "", strings.Repeat("opis ", 80), 0, 0, nil))
	if len([]rune(got)) > maxMeshMessageChars {
		t.Fatalf("text = %q, %d runes — over the channel limit", got, len([]rune(got)))
	}
	if !strings.HasSuffix(got, "...") {
		t.Errorf("text = %q, a cut description must end with ...", got)
	}
	if !strings.Contains(got, "Krotki tytul") || !strings.Contains(got, "opis") {
		t.Errorf("text = %q, the title and the description tail must be present", got)
	}
}
