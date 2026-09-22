package routing

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

type fakeStore struct {
	mu    sync.Mutex
	rules []storage.GroupRouting
	bcc   map[int64][]string
	err   error
}

func (f *fakeStore) ListGroupRoutings() ([]storage.GroupRouting, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]storage.GroupRouting(nil), f.rules...), f.err
}

func (f *fakeStore) GroupRecipientEmails(groupID int64) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.bcc[groupID]...), f.err
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

// asn builds one matrix assignment (channel ID + threshold).
func asn(id, severity string) storage.ChannelAssignment {
	return storage.ChannelAssignment{ID: id, MinSeverity: severity}
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

type fakeOutputs struct {
	mu  sync.Mutex
	got []outCall
}

type outCall struct {
	group string
	ids   []string
	sev   string
}

func (f *fakeOutputs) SubmitRule(_ context.Context, outputIDs []string, change core.EventChange, ref plugin.RuleRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, outCall{group: ref.GroupName, ids: outputIDs, sev: change.Event.Severity})
	return nil
}

func hazardEvent(severity string, typ dispatch.TransitionType) dispatch.Event {
	return dispatch.Event{
		Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{
			Type: typ,
			Key:  "imgw:1",
			Hazard: dispatch.Hazard{
				EventKey: "imgw:1",
				Source:   "imgw",
				SourceID: "1",
				Event:    "Storm",
				Severity: severity,
			},
		},
	}
}

// startEngine runs the engine against a test channel and returns the feed
// plus the engine for stats assertions.
func startEngine(t *testing.T, store RuleStore, acts ActionSubmitter, outs RuleOutputRouter) (*Engine, chan<- dispatch.Event) {
	t.Helper()
	e := New(store, acts, outs, slog.New(slog.DiscardHandler), action.AppInfo{})
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
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("log", "severe")}, Outputs: []storage.ChannelAssignment{asn("mqtt", "severe")}},
		{GroupID: 2, Name: "rsp", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
		{GroupID: 3, Name: "silent"},
	}}
	acts := &fakeActions{}
	outs := &fakeOutputs{}
	e, feed := startEngine(t, store, acts, outs)

	// severe matches spok (both channels) + rsp (action only); moderate
	// matches rsp only; unknown matches rsp only: 2+1+1 action firings.
	feed <- hazardEvent("severe", dispatch.TransitionNew)
	feed <- hazardEvent("moderate", dispatch.TransitionNew)
	feed <- hazardEvent("unknown", dispatch.TransitionNew)

	waitFor(t, func() bool {
		acts.mu.Lock()
		defer acts.mu.Unlock()
		return len(acts.got["log"]) == 4
	}, "log action fired 4 times")

	outs.mu.Lock()
	var severeGroups []string
	for _, c := range outs.got {
		if c.sev == "severe" {
			severeGroups = append(severeGroups, c.group)
		}
	}
	outs.mu.Unlock()
	if len(severeGroups) != 1 || severeGroups[0] != "spok" {
		t.Errorf("output deliveries for severe = %v, want exactly [spok]", severeGroups)
	}

	stats := e.Stats()
	if stats.EventsSeen != 3 || stats.RulesMatched != 4 || stats.ActionsFired != 4 || stats.OutputRounds != 1 {
		t.Errorf("stats = %+v, want 3 seen, 4 matched, 4 actions, 1 output round", stats)
	}
}

// TestEngineRoutingMatrix pins the per-channel matrix semantics: one event
// fires only the channels whose own threshold it satisfies.
func TestEngineRoutingMatrix(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok",
			Actions: []storage.ChannelAssignment{
				asn("log", "moderate"),
				asn("sms", "severe"),
			},
			Outputs: []storage.ChannelAssignment{
				asn("mqtt", "severe"),
			}},
	}}
	acts := &fakeActions{}
	outs := &fakeOutputs{}
	e, feed := startEngine(t, store, acts, outs)

	feed <- hazardEvent("moderate", dispatch.TransitionNew)
	waitFor(t, func() bool {
		acts.mu.Lock()
		defer acts.mu.Unlock()
		return len(acts.got["log"]) == 1
	}, "log action fired once")

	// moderate satisfies only log: no sms action, no mqtt round.
	acts.mu.Lock()
	smsCalls := len(acts.got["sms"])
	acts.mu.Unlock()
	outs.mu.Lock()
	rounds := len(outs.got)
	outs.mu.Unlock()
	if smsCalls != 0 || rounds != 0 {
		t.Fatalf("moderate fired sms %d times and %d output rounds, want 0/0", smsCalls, rounds)
	}

	feed <- hazardEvent("severe", dispatch.TransitionNew)
	waitFor(t, func() bool {
		acts.mu.Lock()
		defer acts.mu.Unlock()
		return len(acts.got["sms"]) == 1
	}, "sms action fired once")
	waitFor(t, func() bool {
		outs.mu.Lock()
		defer outs.mu.Unlock()
		return len(outs.got) == 1
	}, "one output round")

	outs.mu.Lock()
	ids := append([]string(nil), outs.got[0].ids...)
	outs.mu.Unlock()
	if len(ids) != 1 || ids[0] != "mqtt" {
		t.Errorf("output round ids = %v, want [mqtt]", ids)
	}
	if s := e.Stats(); s.EventsSeen != 2 {
		t.Errorf("EventsSeen = %d, want 2", s.EventsSeen)
	}
}

func TestEngineChangeTypeProjection(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Outputs: []storage.ChannelAssignment{asn("mqtt", "unknown")}},
	}}
	outs := &fakeOutputs{}
	e, feed := startEngine(t, store, &fakeActions{}, outs)

	feed <- hazardEvent("extreme", dispatch.TransitionCancelled)

	waitFor(t, func() bool {
		outs.mu.Lock()
		defer outs.mu.Unlock()
		return len(outs.got) == 1
	}, "one output round")

	outs.mu.Lock()
	got := outs.got[0]
	outs.mu.Unlock()
	if got.sev != "extreme" {
		t.Errorf("delivered severity = %q, want extreme", got.sev)
	}
	if e.Stats().EventsSeen != 1 {
		t.Errorf("EventsSeen = %d, want 1", e.Stats().EventsSeen)
	}
}

func TestEngineNonHazardSkipped(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
	}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts, &fakeOutputs{})

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
	_, feed := startEngine(t, store, acts, &fakeOutputs{})

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
	_, feed := startEngine(t, store, acts, &fakeOutputs{})

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
	e, feed := startEngine(t, store, acts, &fakeOutputs{})

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
