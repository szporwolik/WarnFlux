package meshtastic

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	mesh "github.com/szporwolik/WarnFlux/internal/meshtastic"
)

// stubSender records every transmission the action hands to the hub and
// keeps the durable progress ledger with the SAME semantics as the hub:
// a versioned send marks the exact entry (publisher|event key|change
// id|recipient|channel) as done only when the transmission succeeded —
// the tests revoke entries to simulate a later TxFailed.
type stubSender struct {
	contacts []string
	channels []int
	texts    []string
	err      error
	// ledgerErr makes the progress queries fail (unreadable ledger).
	ledgerErr error
	// progress keys: "publisher|eventKey|changeID|recipient|channel".
	progress map[string]bool
	// progs records the progress references of the versioned sends.
	progs []mesh.ProgressRef
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
	// The attempt is recorded even when the device rejects it: the
	// resume tests assert failed broadcasts were attempted.
	s.channels = append(s.channels, idx)
	s.texts = append(s.texts, text)
	if s.err != nil {
		return s.err
	}
	return nil
}

func progressKey(publisher, eventKey string, changeID int64, recipient string, channel int) string {
	return fmt.Sprintf("%s|%s|%d|%s|%d", publisher, eventKey, changeID, recipient, channel)
}

// recordProgress mirrors the hub's echo stamp: the transmission result
// arrives shortly after the send.
func (s *stubSender) recordProgress(publisher, eventKey string, changeID int64, recipient string, channel int) {
	if s.ledgerErr != nil {
		return
	}
	if s.progress == nil {
		s.progress = map[string]bool{}
	}
	s.progress[progressKey(publisher, eventKey, changeID, recipient, channel)] = true
}

// revokeProgress simulates the hub's TxFailed: the failed transmission
// becomes retryable again.
func (s *stubSender) revokeProgress(publisher, eventKey string, changeID int64, recipient string, channel int) {
	delete(s.progress, progressKey(publisher, eventKey, changeID, recipient, channel))
}

func (s *stubSender) SendContactMessageVersioned(_ context.Context, addr, text, operator string, prog mesh.ProgressRef) error {
	if err := s.SendContactMessage(nil, addr, text, operator); err != nil {
		return err
	}
	s.progs = append(s.progs, prog)
	s.recordProgress(prog.Publisher, prog.EventKey, prog.ChangeID, addr, 0)
	return nil
}

func (s *stubSender) SendChannelTextVersioned(_ context.Context, idx int, text, operator string, prog mesh.ProgressRef) error {
	if err := s.SendChannelText(nil, idx, text, operator); err != nil {
		return err
	}
	s.progs = append(s.progs, prog)
	s.recordProgress(prog.Publisher, prog.EventKey, prog.ChangeID, "", idx)
	return nil
}

func (s *stubSender) MeshActionProgressDone(_ context.Context, publisher, eventKey string, changeID int64, recipient string, channel int) (bool, error) {
	if s.ledgerErr != nil {
		return false, s.ledgerErr
	}
	return s.progress[progressKey(publisher, eventKey, changeID, recipient, channel)], nil
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
// + change id) and a routed request id ("key/action") so the durable
// progress ledger activates.
func versionedReq(nodeIDs []string, headline string, changeID int64) action.ActionRequest {
	req := hazardReq(nodeIDs, headline)
	req.ID = "k/mesh-main"
	req.Event.Hazard.Publisher = "publisher-1"
	req.Event.Hazard.ChangeID = changeID
	return req
}

// TestExecuteDMsDisabledChannelOnly pins the current operator decision:
// hazard notifications go out ONLY as the emcom channel broadcast — the
// direct-message loop to registered node IDs is disabled (direct message
// REPLIES stay in the hub).
func TestExecuteDMsDisabledChannelOnly(t *testing.T) {
	stub := &stubSender{}
	a := &Action{cfg: Config{Prefix: "SOSNA"}, hub: stub}

	if err := a.Execute(context.Background(), hazardReq([]string{"a0a85934", "b0b85934"}, "Big storm coming")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(stub.contacts) != 0 {
		t.Fatalf("contacts = %v, want none (direct hazard messages are disabled)", stub.contacts)
	}
	if len(stub.channels) != 0 {
		t.Fatalf("broadcasts = %v, want none without a configured channel", stub.channels)
	}

	// Channel configured: the broadcast goes out even when the group
	// has registered node IDs — and the node IDs stay silent.
	stub2 := &stubSender{}
	a2 := &Action{cfg: Config{Prefix: "SOSNA", Channel: 1}, hub: stub2}
	if err := a2.Execute(context.Background(), hazardReq([]string{"a0a85934"}, "Flood alert")); err != nil {
		t.Fatalf("Execute broadcast: %v", err)
	}
	if len(stub2.channels) != 1 || stub2.channels[0] != 1 {
		t.Fatalf("broadcasts = %v, want one on channel 1", stub2.channels)
	}
	if len(stub2.contacts) != 0 {
		t.Fatalf("contacts = %v, want none (direct hazard messages are disabled)", stub2.contacts)
	}
	if len(stub2.texts) != 1 || !strings.HasPrefix(stub2.texts[0], "SOSNA SEVERE") {
		t.Fatalf("text = %v, want the SOSNA SEVERE prefix", stub2.texts)
	}
}

// TestExecuteFailurePropagates pins error handling and the empty-headline
// fallback to the event name.
func TestExecuteFailurePropagates(t *testing.T) {
	stub := &stubSender{err: errors.New("device gone")}
	a := &Action{cfg: Config{Prefix: "SOSNA", Channel: 1}, hub: stub}
	err := a.Execute(context.Background(), hazardReq([]string{"a0a85934"}, "x"))
	if err == nil || !strings.Contains(err.Error(), "device gone") {
		t.Fatalf("Execute = %v, want the device error", err)
	}

	stub2 := &stubSender{}
	a2 := &Action{cfg: Config{Prefix: "SOSNA", Channel: 1}, hub: stub2}
	if err := a2.Execute(context.Background(), hazardReq([]string{"a0a85934"}, "")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(stub2.texts) != 1 || !strings.HasPrefix(stub2.texts[0], "SOSNA SEVERE Storm") {
		t.Fatalf("text = %v, want the event-name fallback", stub2.texts)
	}
}

// TestExecuteResumesUnfinishedBroadcast pins the resume semantics on
// the single remaining transmission path (the channel broadcast): a
// failed first attempt leaves no ledger row, so the retry transmits
// again — and once recorded, further retries skip it.
func TestExecuteResumesUnfinishedBroadcast(t *testing.T) {
	stub := &stubSender{err: errors.New("device gone")}
	a := &Action{cfg: Config{Prefix: "SOSNA", Channel: 1}, hub: stub}
	req := versionedReq([]string{"a0a85934"}, "Big storm coming", 1)

	// Attempt 1: the device fails — nothing lands in the ledger.
	if err := a.Execute(context.Background(), req); err == nil {
		t.Fatalf("attempt 1 = nil, want the device error")
	}
	if len(stub.channels) != 1 {
		t.Fatalf("attempt 1 broadcasts = %v, want one attempt on channel 1", stub.channels)
	}

	// Attempt 2 with a working device: the broadcast goes out again and
	// its progress row lands.
	stub.err = nil
	if err := a.Execute(context.Background(), req); err != nil {
		t.Fatalf("attempt 2: %v", err)
	}
	if len(stub.channels) != 2 {
		t.Fatalf("broadcasts after retry = %v, want the failed broadcast retransmitted", stub.channels)
	}
	if len(stub.contacts) != 0 {
		t.Fatalf("contacts = %v, want none (direct messages disabled)", stub.contacts)
	}

	// Attempt 3: the ledger skips the already-delivered broadcast.
	if err := a.Execute(context.Background(), req); err != nil {
		t.Fatalf("attempt 3: %v", err)
	}
	if len(stub.channels) != 2 {
		t.Fatalf("broadcasts after recorded retry = %v, want no repeat", stub.channels)
	}
}

// TestExecuteSkipsLedgerCompleted pins the progress reads themselves: a
// retry of an already-broadcast version transmits nothing new.
func TestExecuteSkipsLedgerCompleted(t *testing.T) {
	stub := &stubSender{}
	a := &Action{cfg: Config{Prefix: "SOSNA", Channel: 2}, hub: stub}
	req := versionedReq([]string{"a0a85934"}, "Flood alert", 1)

	// A previous attempt delivered the broadcast.
	if err := a.Execute(context.Background(), req); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	// Reset the call log, keep the ledger (the durable state).
	stub.contacts, stub.texts, stub.channels = nil, nil, nil

	// The full group retry: the broadcast is skipped, nothing goes out.
	if err := a.Execute(context.Background(), versionedReq([]string{"a0a85934", "b0b85934"}, "Flood alert", 1)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if len(stub.channels) != 0 {
		t.Fatalf("broadcasts on retry = %v, want none (already broadcast)", stub.channels)
	}
	if len(stub.contacts) != 0 {
		t.Fatalf("contacts on retry = %v, want none (direct messages disabled)", stub.contacts)
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
	if len(stub.channels) != 1 {
		t.Fatalf("v1 broadcasts = %v, want one transmission", stub.channels)
	}

	// The SAME text with a NEW version must transmit again.
	if err := a.Execute(context.Background(), versionedReq(ids, "Big storm coming", 2)); err != nil {
		t.Fatalf("v2: %v", err)
	}
	if len(stub.channels) != 2 {
		t.Fatalf("v2 broadcasts = %v, want one per version", stub.channels)
	}

	// A retry of version 2 itself still resumes: nothing re-sent.
	if err := a.Execute(context.Background(), versionedReq(ids, "Big storm coming", 2)); err != nil {
		t.Fatalf("v2 retry: %v", err)
	}
	if len(stub.channels) != 2 || len(stub.contacts) != 0 {
		t.Fatalf("after v2 retry = broadcasts %v contacts %v, want no repeats and no DMs",
			stub.channels, stub.contacts)
	}
}

// TestExecuteResendsAfterFailedTransmission pins the P1: the progress
// follows the TRANSMISSION OUTCOME, not the accept handoff. A broadcast
// that was recorded but later reported failed by the radio (TxFailed
// revokes the entry) must be retried.
func TestExecuteResendsAfterFailedTransmission(t *testing.T) {
	stub := &stubSender{}
	a := &Action{cfg: Config{Prefix: "SOSNA", Channel: 1}, hub: stub}
	req := versionedReq([]string{"a0a85934"}, "Big storm coming", 1)

	// The first attempt transmits and the device echo stamps progress.
	if err := a.Execute(context.Background(), req); err != nil {
		t.Fatalf("attempt 1: %v", err)
	}
	if len(stub.channels) != 1 {
		t.Fatalf("attempt 1 broadcasts = %v, want one", stub.channels)
	}

	// The modem later reports the broadcast failed: the hub revokes it.
	stub.revokeProgress("publisher-1", req.Event.Hazard.Key, 1, "", 1)

	// The group retry must send the failed broadcast again.
	if err := a.Execute(context.Background(), req); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if len(stub.channels) != 2 {
		t.Fatalf("broadcasts after failed tx = %v, want the broadcast retransmitted", stub.channels)
	}
	if len(stub.contacts) != 0 {
		t.Fatalf("contacts = %v, want none (direct messages disabled)", stub.contacts)
	}

	// The retry's own echo stamps the progress again: a further retry skips.
	if err := a.Execute(context.Background(), req); err != nil {
		t.Fatalf("retry 2: %v", err)
	}
	if len(stub.channels) != 2 {
		t.Fatalf("after re-stamped retry = broadcasts %v, want no repeats", stub.channels)
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
	if len(stub.channels) != 1 || len(stub.contacts) != 0 {
		t.Fatalf("transmissions = broadcast %v contacts %v, want the broadcast delivered and no DMs",
			stub.channels, stub.contacts)
	}
}

// TestExecutePassesJobIdentityToVersionedSends pins the P2 plumbing: the
// action hands the concrete job identity (group + dedup key stamped onto
// the request by the worker) to every versioned send, so the hub's async
// failure marker and re-arm scope to THIS job and never to another
// group sharing the action.
func TestExecutePassesJobIdentityToVersionedSends(t *testing.T) {
	stub := &stubSender{}
	a := &Action{cfg: Config{Prefix: "SOSNA", Channel: 1}, hub: stub}
	req := versionedReq([]string{"a0a85934"}, "x", 3)
	req.JobGroupID = 42
	req.JobDedupKey = "c:42"
	if err := a.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(stub.progs) != 1 {
		t.Fatalf("versioned sends = %d, want 1 (the channel broadcast only)", len(stub.progs))
	}
	prog := stub.progs[0]
	if prog.ActionID != "mesh-main" || prog.GroupID != 42 || prog.DedupKey != "c:42" ||
		prog.Publisher != "publisher-1" || prog.ChangeID != 3 {
		t.Fatalf("broadcast prog = %+v, want the job identity (group 42, dedup c:42) + version", prog)
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
					EventKey: "imgw-meteo:1",
					Event:    event, Severity: "severe", Headline: headline,
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
	// The stable message ID closes every transmission.
	if !strings.HasSuffix(got, " ["+core.MessageID("imgw-meteo:1")+"]") {
		t.Errorf("text = %q, the message ID must close the message", got)
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
	// The location survives whole; the ID closes the message after it.
	if !strings.HasSuffix(got, "49.985,20.065 ["+core.MessageID("imgw-meteo:1")+"]") {
		t.Errorf("text = %q, the location and the message ID must survive", got)
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
	if !strings.HasSuffix(got, " ["+core.MessageID("imgw-meteo:1")+"]") {
		t.Errorf("text = %q, the message ID must survive the hard cut", got)
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
	// The description fills the leftover budget; the ID closes the text.
	if !strings.HasSuffix(got, " ["+core.MessageID("imgw-meteo:1")+"]") {
		t.Errorf("text = %q, the message ID must close the message", got)
	}
	if !strings.Contains(got, "Krotki tytul") || !strings.Contains(got, "opis") {
		t.Errorf("text = %q, the title and the description tail must be present", got)
	}
}
