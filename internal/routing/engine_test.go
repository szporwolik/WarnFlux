package routing

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

type fakeStore struct {
	mu      sync.Mutex
	rules   []storage.GroupRouting
	bcc     map[int64][]string
	claimed map[string]bool // group|action|dedupKey -> already delivered
	err     error
}

func (f *fakeStore) ListGroupRoutings() ([]storage.GroupRouting, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]storage.GroupRouting, len(f.rules))
	for i, r := range f.rules {
		out[i] = r
		out[i].Actions = append([]storage.ChannelAssignment(nil), r.Actions...)
	}
	return out, f.err
}

func (f *fakeStore) GroupRecipientEmails(groupID int64) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.bcc[groupID]...), f.err
}

func (f *fakeStore) ClaimActionFire(groupID int64, actionID, eventKey, dedupKey string, at time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimed == nil {
		f.claimed = map[string]bool{}
	}
	key := fmt.Sprintf("%d|%s|%s", groupID, actionID, dedupKey)
	if f.claimed[key] {
		return false, f.err
	}
	f.claimed[key] = true
	return true, f.err
}

// setActionSeverity mutates one cached rule's action threshold.
func (f *fakeStore) setActionSeverity(groupID int64, actionID, severity string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.rules {
		if f.rules[i].GroupID != groupID {
			continue
		}
		for j := range f.rules[i].Actions {
			if f.rules[i].Actions[j].ID == actionID {
				f.rules[i].Actions[j].MinSeverity = severity
				return
			}
		}
	}
}

// asn builds one matrix cell (any source, action ID + threshold).
func asn(id, severity string) storage.ChannelAssignment {
	return storage.ChannelAssignment{ID: id, MinSeverity: severity}
}

// asnSrc builds one matrix cell with an explicit source (input plugin).
func asnSrc(source, id, severity string) storage.ChannelAssignment {
	return storage.ChannelAssignment{Source: source, ID: id, MinSeverity: severity}
}

type fakeActions struct {
	mu   sync.Mutex
	got  map[string][]string // actionID -> event keys
	bccs [][]string          // one Bcc list per submission, in order
	err  error
}

func (f *fakeActions) Submit(id string, req action.ActionRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.got == nil {
		f.got = map[string][]string{}
	}
	f.got[id] = append(f.got[id], req.Event.Hazard.Key)
	f.bccs = append(f.bccs, append([]string(nil), req.Bcc...))
	return f.err
}

// eventSeq gives every synthetic event a unique journal ChangeID, so the
// engine's delivery ledger sees them as distinct transitions (real
// publishers stamp each /events message with a unique change_id).
var eventSeq atomic.Int64

func hazardEvent(severity string, typ dispatch.TransitionType) dispatch.Event {
	return hazardEventFrom("imgw", severity, typ)
}

// hazardEventFrom builds one synthetic hazard transition from the given
// input plugin (source) at the given severity.
func hazardEventFrom(source, severity string, typ dispatch.TransitionType) dispatch.Event {
	return dispatch.Event{
		Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{
			Type:     typ,
			Key:      source + ":1",
			ChangeID: eventSeq.Add(1),
			Hazard: dispatch.Hazard{
				EventKey: source + ":1",
				Source:   source,
				SourceID: "1",
				Event:    "Storm",
				Severity: severity,
			},
		},
	}
}

// startEngine runs the engine against a test channel and returns the feed
// plus the engine for stats assertions.
func startEngine(t *testing.T, store RuleStore, acts ActionSubmitter) (*Engine, chan<- dispatch.Event) {
	t.Helper()
	e := New(store, acts, slog.New(slog.DiscardHandler), action.AppInfo{})
	events := make(chan dispatch.Event, 8)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.Run(ctx, events)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return e, events
}

func TestEngineSeverityThresholdAndFanOut(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("log", "severe")}},
		{GroupID: 2, Name: "rsp", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
		{GroupID: 3, Name: "silent"},
	}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts)

	// severe matches spok + rsp; moderate matches rsp only; unknown
	// matches rsp only: 2+1+1 action firings.
	feed <- hazardEvent("severe", dispatch.TransitionNew)
	feed <- hazardEvent("moderate", dispatch.TransitionNew)
	feed <- hazardEvent("unknown", dispatch.TransitionNew)

	waitFor(t, func() bool {
		acts.mu.Lock()
		defer acts.mu.Unlock()
		return len(acts.got["log"]) == 4
	}, "log action fired 4 times")

	stats := e.Stats()
	if stats.EventsSeen != 3 || stats.RulesMatched != 4 || stats.ActionsFired != 4 {
		t.Errorf("stats = %+v, want 3 seen, 4 matched, 4 actions", stats)
	}
}

// TestEngineDuplicateDeliveryFiresOnce pins the durable deduplication: the
// same transition (e.g. a retained message replayed after a restart) must
// fire each (group, action) exactly once.
func TestEngineDuplicateDeliveryFiresOnce(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
		{GroupID: 2, Name: "rsp", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
	}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts)

	ev := hazardEvent("severe", dispatch.TransitionNew)
	feed <- ev
	feed <- ev // identical replay

	waitFor(t, func() bool {
		s := e.Stats()
		return s.EventsSeen == 2 && s.ActionsDeduped == 2
	}, "two groups claimed once, two replays deduplicated")

	acts.mu.Lock()
	defer acts.mu.Unlock()
	if got := len(acts.got["log"]); got != 2 {
		t.Errorf("log fired %d times, want 2 (one per group, no replay)", got)
	}
	stats := e.Stats()
	if stats.ActionsFired != 2 || stats.RulesMatched != 2 {
		t.Errorf("stats = %+v, want 2 fired, 2 matched", stats)
	}
}

// TestEngineRoutingMatrix pins the per-action matrix semantics: one event
// fires only the actions whose own threshold it satisfies.
func TestEngineRoutingMatrix(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok",
			Actions: []storage.ChannelAssignment{
				asn("log", "moderate"),
				asn("sms", "severe"),
			}},
	}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts)

	feed <- hazardEvent("moderate", dispatch.TransitionNew)
	waitFor(t, func() bool {
		acts.mu.Lock()
		defer acts.mu.Unlock()
		return len(acts.got["log"]) == 1
	}, "log action fired once")

	// moderate satisfies only log: sms must not fire.
	acts.mu.Lock()
	smsCalls := len(acts.got["sms"])
	acts.mu.Unlock()
	if smsCalls != 0 {
		t.Fatalf("moderate fired sms %d times, want 0", smsCalls)
	}

	feed <- hazardEvent("severe", dispatch.TransitionNew)
	waitFor(t, func() bool {
		acts.mu.Lock()
		defer acts.mu.Unlock()
		return len(acts.got["sms"]) == 1
	}, "sms action fired once")

	if s := e.Stats(); s.EventsSeen != 2 || s.ActionsFired != 3 {
		t.Errorf("stats = %+v, want 2 seen, 3 actions", s)
	}
}

// TestEngineRoutingMatrixSource pins the per-cell matrix semantics: a
// cell routes only events from its own input plugin, the empty-source
// cell is the any-source fallback, and a source-specific cell shadows the
// fallback for the same action.
func TestEngineRoutingMatrixSource(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok",
			Actions: []storage.ChannelAssignment{
				asn("log", "severe"),                    // any source, severe
				asnSrc("imgw-meteo", "log", "moderate"), // imgw-meteo specific: shadows the fallback
				asnSrc("rso", "sms", "moderate"),        // rso only
			}},
	}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts)

	// imgw-meteo moderate: only the specific log cell fires; the
	// any-source log cell (severe) is shadowed and sms (rso) must not.
	feed <- hazardEventFrom("imgw-meteo", "moderate", dispatch.TransitionNew)
	waitFor(t, func() bool {
		acts.mu.Lock()
		defer acts.mu.Unlock()
		return len(acts.got["log"]) == 1
	}, "imgw-meteo cell fired log")
	acts.mu.Lock()
	logCalls := len(acts.got["log"])
	smsCalls := len(acts.got["sms"])
	acts.mu.Unlock()
	if smsCalls != 0 {
		t.Fatalf("imgw-meteo event fired sms %d times, want 0", smsCalls)
	}
	if logCalls != 1 {
		t.Fatalf("imgw-meteo event fired log %d times, want 1", logCalls)
	}

	// rso severe: sms fires from its rso cell; log has no rso cell, so
	// the any-source fallback (severe) applies.
	feed <- hazardEventFrom("rso", "severe", dispatch.TransitionNew)
	waitFor(t, func() bool {
		acts.mu.Lock()
		defer acts.mu.Unlock()
		return len(acts.got["sms"]) == 1
	}, "rso cell fired sms")
	acts.mu.Lock()
	logCalls = len(acts.got["log"])
	smsCalls = len(acts.got["sms"])
	acts.mu.Unlock()
	if logCalls != 2 {
		t.Errorf("rso severe event: log fired %d times, want 2 (fallback)", logCalls)
	}
	if smsCalls != 1 {
		t.Errorf("rso severe event: sms fired %d times, want 1", smsCalls)
	}

	// imgw-hydro minor: no specific cell and the fallback needs severe,
	// so nothing may fire.
	feed <- hazardEventFrom("imgw-hydro", "minor", dispatch.TransitionNew)
	time.Sleep(60 * time.Millisecond)
	acts.mu.Lock()
	logCalls = len(acts.got["log"])
	smsCalls = len(acts.got["sms"])
	acts.mu.Unlock()
	if logCalls != 2 || smsCalls != 1 {
		t.Errorf("imgw-hydro minor event fired (log %d, sms %d), want no new firings", logCalls, smsCalls)
	}

	if s := e.Stats(); s.EventsSeen != 3 || s.ActionsFired != 3 || s.RulesMatched != 2 {
		t.Errorf("stats = %+v, want 3 seen, 3 fired, 2 matched", s)
	}
}

func TestEngineNonHazardSkipped(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
	}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts)

	feed <- dispatch.Event{Kind: dispatch.EventMQTTMessage, MQTT: &dispatch.MQTTMessage{Topic: "x"}}
	time.Sleep(50 * time.Millisecond)

	if s := e.Stats(); s.EventsSeen != 0 {
		t.Errorf("EventsSeen = %d, want 0 (mqtt_message is not routed)", s.EventsSeen)
	}
	acts.mu.Lock()
	n := len(acts.got)
	acts.mu.Unlock()
	if n != 0 {
		t.Errorf("actions fired %d times for a raw MQTT message", n)
	}
}

func TestEngineUnrankedSeverityMatchesOnlyPermissive(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "strict", Actions: []storage.ChannelAssignment{asn("log", "minor")}},
		{GroupID: 2, Name: "permissive", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
	}}
	acts := &fakeActions{}
	_, feed := startEngine(t, store, acts)

	// "orange" is not a canonical severity: only the permissive group may
	// receive it.
	feed <- hazardEvent("orange", dispatch.TransitionNew)

	waitFor(t, func() bool {
		acts.mu.Lock()
		defer acts.mu.Unlock()
		return len(acts.got["log"]) == 1
	}, "permissive action fired once")
}

func TestEnginePassesGroupRecipientsAsBcc(t *testing.T) {
	store := &fakeStore{
		rules: []storage.GroupRouting{
			{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("smtp", "unknown")}},
			{GroupID: 2, Name: "rsp", Actions: []storage.ChannelAssignment{asn("smtp", "unknown")}},
		},
		bcc: map[int64][]string{
			1: {"a@example.com", "b@example.com"},
			2: {},
		},
	}
	acts := &fakeActions{}
	_, feed := startEngine(t, store, acts)

	feed <- hazardEvent("severe", dispatch.TransitionNew)

	waitFor(t, func() bool {
		acts.mu.Lock()
		defer acts.mu.Unlock()
		return len(acts.got["smtp"]) == 2
	}, "both groups' actions fired")

	acts.mu.Lock()
	bccs := make([][]string, len(acts.bccs))
	for i := range acts.bccs {
		bccs[i] = append([]string(nil), acts.bccs[i]...)
	}
	acts.mu.Unlock()

	var memberEmails []string
	for _, b := range bccs {
		memberEmails = append(memberEmails, b...)
	}
	if len(memberEmails) != 2 || memberEmails[0] != "a@example.com" || memberEmails[1] != "b@example.com" {
		t.Errorf("Bcc across submissions = %v, want [a@example.com b@example.com]", memberEmails)
	}
}

func TestEngineRuleReload(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("log", "severe")}},
	}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts)

	feed <- hazardEvent("unknown", dispatch.TransitionNew)
	time.Sleep(50 * time.Millisecond)

	// Lower the action's threshold; an explicit reload must pick it up.
	store.setActionSeverity(1, "log", "unknown")
	e.refresh()
	feed <- hazardEvent("unknown", dispatch.TransitionNew)

	waitFor(t, func() bool {
		acts.mu.Lock()
		defer acts.mu.Unlock()
		return len(acts.got["log"]) == 1
	}, "action fired after reload")
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
