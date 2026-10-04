package meshtastic

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

// stubSender records every transmission the action hands to the hub and
// keeps the durable progress ledger: every successful send marks the
// exact VERSIONED entry (publisher|event key|change id|recipient|
// channel) as done. Pre-seeding the map simulates the progress rows a
// previous attempt recorded.
type stubSender struct {
	contacts []string
	channels []int
	texts    []string
	err      error
	// ledgerErr makes both progress calls fail (unreadable ledger).
	ledgerErr error
	// progress keys: "publisher|eventKey|changeID|recipient|channel".
	progress map[string]bool
}

func (s *stubSender) SendContactMessage(_ context.Context, addr, text, operator string) error {
	if s.err != nil {
		return s.err
	}
	s.contacts = append(s.contacts, addr)
	s.texts = append(s.texts, text)
	return nil
}

func (s *stubSender) SendChannelText(_ context.Context, idx int, text, operator string) error {
	if s.err != nil {
		return s.err
	}
	s.channels = append(s.channels, idx)
	return nil
}

func progressKey(publisher, eventKey string, changeID int64, recipient string, channel int) string {
	return fmt.Sprintf("%s|%s|%d|%s|%d", publisher, eventKey, changeID, recipient, channel)
}

func (s *stubSender) MeshActionProgressDone(_ context.Context, publisher, eventKey string, changeID int64, recipient string, channel int) (bool, error) {
	if s.ledgerErr != nil {
		return false, s.ledgerErr
	}
	return s.progress[progressKey(publisher, eventKey, changeID, recipient, channel)], nil
}

func (s *stubSender) RecordMeshActionProgress(_ context.Context, publisher, eventKey string, changeID int64, recipient string, channel int) error {
	if s.ledgerErr != nil {
		return s.ledgerErr
	}
	if s.progress == nil {
		s.progress = map[string]bool{}
	}
	s.progress[progressKey(publisher, eventKey, changeID, recipient, channel)] = true
	return nil
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

// versionedReq is hazardReq with the message VERSION stamped (publisher
// + change id): the durable progress ledger only activates for these.
func versionedReq(nodeIDs []string, headline string, changeID int64) action.ActionRequest {
	req := hazardReq(nodeIDs, headline)
	req.Event.Hazard.Publisher = "publisher-1"
	req.Event.Hazard.ChangeID = changeID
	return req
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

// TestExecuteResumesUnfinishedSends pins the P1 fix: the whole group
// shares one bounded call deadline, so a large group is cut mid-list —
// but the durable progress ledger lets the next attempt skip every
// completed transmission and finish only the remaining ones. Across the
// attempts every recipient is reached exactly once and the broadcast
// goes out exactly once.
func TestExecuteResumesUnfinishedSends(t *testing.T) {
	stub := &stubSender{}
	a := &Action{cfg: Config{Prefix: "SOSNA", Channel: 1, TxInterval: 40 * time.Millisecond}, hub: stub}
	req := versionedReq([]string{"a0a85934", "b0b85934", "c0c85934"}, "Big storm coming", 1)

	// Attempt 1: four paced transmissions (broadcast + 3 DMs) need at
	// least 120 ms; the deadline grants 70 ms — the call fails
	// mid-list no matter how the scheduler interleaves.
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	defer cancel()
	if err := a.Execute(ctx, req); err == nil {
		t.Fatalf("attempt 1 = nil, want the deadline to cut the group")
	}
	if len(stub.channels) != 1 || stub.channels[0] != 1 {
		t.Fatalf("attempt 1 broadcasts = %v, want exactly one on channel 1", stub.channels)
	}

	// Attempt 2 resumes the SAME request: the ledger skips the
	// broadcast and every recipient attempt 1 already completed; only
	// the unfinished sends go out.
	if err := a.Execute(context.Background(), req); err != nil {
		t.Fatalf("attempt 2: %v", err)
	}
	if len(stub.channels) != 1 {
		t.Fatalf("broadcasts after resume = %v, want exactly one on channel 1", stub.channels)
	}
	seen := map[string]int{}
	for _, id := range stub.contacts {
		seen[id]++
	}
	if len(stub.contacts) != 3 || seen["a0a85934"] != 1 || seen["b0b85934"] != 1 || seen["c0c85934"] != 1 {
		t.Fatalf("contacts after resume = %v (%v), want each of a/b/c exactly once",
			stub.contacts, seen)
	}
}

// TestExecuteSkipsLedgerCompleted pins the progress reads themselves: a
// retry of an already partially-delivered group transmits only the
// entries without a successful ledger row.
func TestExecuteSkipsLedgerCompleted(t *testing.T) {
	stub := &stubSender{}
	a := &Action{cfg: Config{Prefix: "SOSNA", Channel: 2}, hub: stub}
	req := versionedReq([]string{"a0a85934"}, "Flood alert", 1)

	// A previous attempt delivered the broadcast and recipient a.
	if err := a.Execute(context.Background(), req); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	// Reset the call log, keep the ledger (the durable state).
	stub.contacts, stub.texts, stub.channels = nil, nil, nil

	// The full group retry: broadcast and a are skipped, only b goes out.
	if err := a.Execute(context.Background(), versionedReq([]string{"a0a85934", "b0b85934"}, "Flood alert", 1)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if len(stub.channels) != 0 {
		t.Fatalf("broadcasts on retry = %v, want none (already broadcast)", stub.channels)
	}
	if len(stub.contacts) != 1 || stub.contacts[0] != "b0b85934" {
		t.Fatalf("contacts on retry = %v, want only the unfinished b0b85934", stub.contacts)
	}
}

// TestExecuteNewVersionSameTextResends pins the reported P1: the resume
// ledger is keyed by the message VERSION, never by the text — a new
// alert whose text matches an already-delivered message still transmits
// (the old text-based ledger skipped it as "already sent").
func TestExecuteNewVersionSameTextResends(t *testing.T) {
	stub := &stubSender{}
	a := &Action{cfg: Config{Prefix: "SOSNA", Channel: 1}, hub: stub}
	ids := []string{"a0a85934"}

	// Version 1 goes out and lands in the ledger.
	if err := a.Execute(context.Background(), versionedReq(ids, "Big storm coming", 1)); err != nil {
		t.Fatalf("v1: %v", err)
	}
	if len(stub.contacts) != 1 {
		t.Fatalf("v1 contacts = %v, want one transmission", stub.contacts)
	}

	// The SAME text with a NEW version must transmit again.
	if err := a.Execute(context.Background(), versionedReq(ids, "Big storm coming", 2)); err != nil {
		t.Fatalf("v2: %v", err)
	}
	if len(stub.contacts) != 2 || stub.contacts[1] != "a0a85934" {
		t.Fatalf("v2 contacts = %v, want the recipient transmitted again (new version)", stub.contacts)
	}
	if len(stub.channels) != 2 {
		t.Fatalf("broadcasts = %v, want one per version", stub.channels)
	}

	// A retry of version 2 itself still resumes: nothing re-sent.
	if err := a.Execute(context.Background(), versionedReq(ids, "Big storm coming", 2)); err != nil {
		t.Fatalf("v2 retry: %v", err)
	}
	if len(stub.contacts) != 2 || len(stub.channels) != 2 {
		t.Fatalf("after v2 retry = contacts %v broadcasts %v, want no repeats", stub.contacts, stub.channels)
	}
}

// TestExecuteLedgerFailureFailsOpen pins the at-least-once behavior: an
// unreadable progress ledger must never suppress a transmission — the
// alert goes out again instead of being skipped by an unavailable store.
func TestExecuteLedgerFailureFailsOpen(t *testing.T) {
	stub := &stubSender{ledgerErr: errors.New("db down")}
	a := &Action{cfg: Config{Prefix: "SOSNA", Channel: 1}, hub: stub}
	if err := a.Execute(context.Background(), versionedReq([]string{"a0a85934"}, "x", 1)); err != nil {
		t.Fatalf("Execute with broken ledger: %v", err)
	}
	if len(stub.channels) != 1 || len(stub.contacts) != 1 {
		t.Fatalf("transmissions = broadcast %v contacts %v, want both delivered (fail-open)",
			stub.channels, stub.contacts)
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
