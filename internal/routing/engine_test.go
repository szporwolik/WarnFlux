package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/metrics"
	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/trail"
)

type fakeStore struct {
	mu           sync.Mutex
	rules        []storage.GroupRouting
	bcc          map[int64][]string
	aprsBcc      map[int64][]string
	discordBcc   map[int64][]string
	jobs         map[string]storage.DeliveryStatus // group|action|dedupKey -> status
	payloads     map[string][]byte                 // group|action|dedupKey -> JSON payload
	payloadOrder []string                          // insertion order of payload keys
	enqueueErr   error                             // returned by EnqueueDelivery
	recipientErr error                             // returned by the recipient lookups
	err          error
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
	if f.recipientErr != nil {
		return nil, f.recipientErr
	}
	return append([]string(nil), f.bcc[groupID]...), f.err
}

func (f *fakeStore) GroupRecipientAPRS(groupID int64) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recipientErr != nil {
		return nil, f.recipientErr
	}
	return append([]string(nil), f.aprsBcc[groupID]...), f.err
}

func (f *fakeStore) GroupRecipientDiscord(groupID int64) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recipientErr != nil {
		return nil, f.recipientErr
	}
	return append([]string(nil), f.discordBcc[groupID]...), f.err
}

func (f *fakeStore) EnqueueDelivery(ctx context.Context, job storage.DeliveryJob) (storage.DeliveryStatus, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.jobs == nil {
		f.jobs = map[string]storage.DeliveryStatus{}
	}
	if f.payloads == nil {
		f.payloads = map[string][]byte{}
	}
	if f.enqueueErr != nil {
		return 0, false, f.enqueueErr
	}
	key := fmt.Sprintf("%d|%s|%s", job.GroupID, job.ActionID, job.DedupKey)
	if st, ok := f.jobs[key]; ok {
		if st == storage.DeliveryFailed {
			// Terminal failure: the replay re-arms the job.
			f.jobs[key] = storage.DeliverySaved
			f.payloads[key] = job.Payload
			f.payloadOrder = append(f.payloadOrder, key)
			return storage.DeliverySaved, true, f.err
		}
		return st, false, f.err
	}
	f.jobs[key] = storage.DeliverySaved
	f.payloads[key] = job.Payload
	f.payloadOrder = append(f.payloadOrder, key)
	return storage.DeliverySaved, true, f.err
}

// payloadCount reports how many queued durable jobs belong to one action.
func (f *fakeStore) payloadCount(actionID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for k := range f.payloads {
		if strings.Contains(k, "|"+actionID+"|") {
			n++
		}
	}
	return n
}

// payloadsFor returns the JSON payloads queued for one action in
// insertion order (deduplicated replays appear once).
func (f *fakeStore) payloadsFor(actionID string) [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out [][]byte
	for _, k := range f.payloadOrder {
		if strings.Contains(k, "|"+actionID+"|") {
			out = append(out, f.payloads[k])
		}
	}
	return out
}

// failJob marks one (group, action, dedupKey) job as terminally failed.
func (f *fakeStore) failJob(groupID int64, actionID, dedupKey string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.jobs == nil {
		f.jobs = map[string]storage.DeliveryStatus{}
	}
	f.jobs[fmt.Sprintf("%d|%s|%s", groupID, actionID, dedupKey)] = storage.DeliveryFailed
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
	mu       sync.Mutex
	got      map[string][]string // actionID -> event keys
	bccs     [][]string          // one Bcc list per submission, in order
	aprss    [][]string          // one APRSCallsigns list per submission, in order
	discords [][]string          // one DiscordHandles list per submission, in order
	err      error
}

func (f *fakeActions) Submit(id string, req action.ActionRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.got == nil {
		f.got = map[string][]string{}
	}
	f.got[id] = append(f.got[id], req.Event.Hazard.Key)
	f.bccs = append(f.bccs, append([]string(nil), req.Bcc...))
	f.aprss = append(f.aprss, append([]string(nil), req.APRSCallsigns...))
	f.discords = append(f.discords, append([]string(nil), req.DiscordHandles...))
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
	e := New(store, acts, slog.New(slog.DiscardHandler), action.AppInfo{}, nil, nil)
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

	waitFor(t, func() bool { return store.payloadCount("log") == 4 }, "log action fired 4 times")

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
	if got := store.payloadCount("log"); got != 2 {
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
	waitFor(t, func() bool { return store.payloadCount("log") == 1 }, "log action fired once")

	// moderate satisfies only log: sms must not fire.
	if smsCalls := store.payloadCount("sms"); smsCalls != 0 {
		t.Fatalf("moderate fired sms %d times, want 0", smsCalls)
	}

	feed <- hazardEvent("severe", dispatch.TransitionNew)
	waitFor(t, func() bool { return store.payloadCount("sms") == 1 }, "sms action fired once")

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
	waitFor(t, func() bool { return store.payloadCount("log") == 1 }, "imgw-meteo cell fired log")
	logCalls := store.payloadCount("log")
	smsCalls := store.payloadCount("sms")
	if smsCalls != 0 {
		t.Fatalf("imgw-meteo event fired sms %d times, want 0", smsCalls)
	}
	if logCalls != 1 {
		t.Fatalf("imgw-meteo event fired log %d times, want 1", logCalls)
	}

	// rso severe: sms fires from its rso cell; log has no rso cell, so
	// the any-source fallback (severe) applies.
	feed <- hazardEventFrom("rso", "severe", dispatch.TransitionNew)
	waitFor(t, func() bool { return store.payloadCount("sms") == 1 }, "rso cell fired sms")
	logCalls = store.payloadCount("log")
	smsCalls = store.payloadCount("sms")
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
	logCalls = store.payloadCount("log")
	smsCalls = store.payloadCount("sms")
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
	store.mu.Lock()
	n := len(store.payloads)
	store.mu.Unlock()
	if n != 0 {
		t.Errorf("actions fired %d times for a raw MQTT message", n)
	}
}

// TestEngineTerminalTransitionsDoNotFire pins the notification semantics:
// cancelled and expired transitions only retire the dashboard view — they
// must never fire actions, even with a permissive threshold. Updated (and
// new) transitions still notify.
func TestEngineTerminalTransitionsDoNotFire(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
	}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts)

	feed <- hazardEventFrom("imgw", "severe", dispatch.TransitionCancelled)
	feed <- hazardEventFrom("imgw", "severe", dispatch.TransitionExpired)
	feed <- hazardEventFrom("imgw", "severe", dispatch.TransitionUpdated)

	waitFor(t, func() bool { return store.payloadCount("log") == 1 }, "only the updated transition fired")

	s := e.Stats()
	if s.EventsSeen != 3 || s.TransitionsSkipped != 2 || s.ActionsFired != 1 {
		t.Errorf("stats = %+v, want 3 seen, 2 skipped, 1 fired", s)
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

	waitFor(t, func() bool { return store.payloadCount("log") == 1 }, "permissive action fired once")
}

// TestEnginePassesGroupDiscordHandles pins the Discord part of the
// recipient plumbing: the engine hands each group's subscribed members'
// handles to the action request.
func TestEnginePassesGroupDiscordHandles(t *testing.T) {
	store := &fakeStore{
		rules: []storage.GroupRouting{
			{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("discord", "unknown")}},
		},
		discordBcc: map[int64][]string{
			1: {"alice#1234", "@bob"},
		},
	}
	acts := &fakeActions{}
	_, feed := startEngine(t, store, acts)

	feed <- hazardEvent("severe", dispatch.TransitionNew)

	waitFor(t, func() bool { return store.payloadCount("discord") == 1 }, "discord action fired")

	var handles []string
	for _, p := range store.payloadsFor("discord") {
		var req action.ActionRequest
		if err := json.Unmarshal(p, &req); err != nil {
			t.Fatalf("payload decode: %v", err)
		}
		handles = append(handles, req.DiscordHandles...)
	}
	if len(handles) != 2 || handles[0] != "alice#1234" || handles[1] != "@bob" {
		t.Errorf("discord handles = %v, want [alice#1234 @bob]", handles)
	}
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

	waitFor(t, func() bool { return store.payloadCount("smtp") == 2 }, "both groups' actions fired")

	var memberEmails []string
	for _, p := range store.payloadsFor("smtp") {
		var req action.ActionRequest
		if err := json.Unmarshal(p, &req); err != nil {
			t.Fatalf("payload decode: %v", err)
		}
		memberEmails = append(memberEmails, req.Bcc...)
	}
	if len(memberEmails) != 2 || memberEmails[0] != "a@example.com" || memberEmails[1] != "b@example.com" {
		t.Errorf("Bcc across submissions = %v, want [a@example.com b@example.com]", memberEmails)
	}
}

func TestEnginePassesGroupAPRSCallsigns(t *testing.T) {
	store := &fakeStore{
		rules: []storage.GroupRouting{
			{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("aprs", "unknown")}},
		},
		aprsBcc: map[int64][]string{
			1: {"SP9MOA-16", "SR9KR"},
		},
	}
	acts := &fakeActions{}
	_, feed := startEngine(t, store, acts)

	feed <- hazardEvent("severe", dispatch.TransitionNew)

	waitFor(t, func() bool { return store.payloadCount("aprs") == 1 }, "aprs action fired")

	var callsigns []string
	for _, p := range store.payloadsFor("aprs") {
		var req action.ActionRequest
		if err := json.Unmarshal(p, &req); err != nil {
			t.Fatalf("payload decode: %v", err)
		}
		callsigns = append(callsigns, req.APRSCallsigns...)
	}
	if len(callsigns) != 2 || callsigns[0] != "SP9MOA-16" || callsigns[1] != "SR9KR" {
		t.Errorf("APRSCallsigns = %v, want [SP9MOA-16 SR9KR]", callsigns)
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

	waitFor(t, func() bool { return store.payloadCount("log") == 1 }, "action fired after reload")
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	// 5s: the full suite (and the race run after it) executes packages in
	// parallel on loaded machines, so a 2s budget occasionally starves
	// innocent goroutines.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestEngineTrailRecording pins the audit trail: delivery, threshold
// skip, terminal skip and duplicate dedup all leave visible steps.
func TestEngineTrailRecording(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok",
			Actions: []storage.ChannelAssignment{
				asnSrc("imgw", "smtp-alerts", "moderate"),
				asn("log", "severe"),
			}},
	}}
	acts := &fakeActions{}
	rec := trail.NewRecorder(10)
	met := metrics.New()
	e := New(store, acts, slog.New(slog.DiscardHandler), action.AppInfo{}, rec, met)
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

	// moderate from imgw: smtp-alerts fires (≥ moderate), log skipped
	// (needs severe).
	first := hazardEvent("moderate", dispatch.TransitionNew)
	events <- first
	waitFor(t, func() bool {
		tr, _ := rec.Get("imgw:1")
		for _, s := range tr.Steps {
			if s.Kind == trail.StepSubmitted {
				return true
			}
		}
		return false
	}, "submitted step")

	tr, _ := rec.Get("imgw:1")
	var kinds []string
	for _, s := range tr.Steps {
		kinds = append(kinds, string(s.Kind))
	}
	joined := strings.Join(kinds, ",")
	if !strings.Contains(joined, "matched,route,submitted") {
		t.Fatalf("trail kinds = %v, want matched/route/submitted present", kinds)
	}
	if !strings.Contains(joined, "skipped") {
		t.Fatalf("trail kinds = %v, want a below-threshold skip for action log", kinds)
	}
	if tr.Outcome != trail.OutcomeSubmitted {
		t.Errorf("outcome = %q, want submitted", tr.Outcome)
	}

	// Duplicate replay of the same key: dedup skip on the same trail.
	events <- first
	waitFor(t, func() bool {
		tr, _ := rec.Get("imgw:1")
		for _, s := range tr.Steps {
			if s.Kind == trail.StepSkipped && strings.Contains(s.Text, "already queued or delivered") {
				return true
			}
		}
		return false
	}, "duplicate skip step")

	// Metrics: log skipped on both passes (threshold), smtp-alerts deduped
	// on the replay.
	out := met.Render()
	for _, want := range []string{
		`warnflux_notifications_total{action="log",result="skipped"} 2`,
		`warnflux_notifications_total{action="smtp-alerts",result="deduped"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q:\n%s", want, out)
		}
	}

	// Terminal transition: cancelled hazards record a skip and never
	// start the notification machine.
	cancelled := hazardEvent("severe", dispatch.TransitionCancelled)
	cancelled.Hazard.Key = "imgw:2"
	cancelled.Hazard.ChangeID++
	events <- cancelled
	waitFor(t, func() bool {
		tr, ok := rec.Get("imgw:2")
		return ok && tr.Outcome == trail.OutcomeSkipped
	}, "cancelled trail skipped")
	tr2, _ := rec.Get("imgw:2")
	if !strings.Contains(tr2.Steps[len(tr2.Steps)-1].Text, "cancelled") {
		t.Errorf("cancelled trail steps = %+v", tr2.Steps)
	}

	// Below-threshold everywhere: skipped outcome with a threshold step.
	moderate := hazardEvent("minor", dispatch.TransitionNew)
	moderate.Hazard.Key = "imgw:3"
	moderate.Hazard.ChangeID += 2
	events <- moderate
	waitFor(t, func() bool {
		tr, ok := rec.Get("imgw:3")
		if !ok {
			return false
		}
		for _, s := range tr.Steps {
			if s.Kind == trail.StepSkipped {
				return true
			}
		}
		return false
	}, "minor skip step")
}

// Stats returns a snapshot of the engine counters. It is test
// instrumentation only (asserting the counters in rule-engine tests);
// production monitoring would expose it through a health endpoint when
// needed.
func (e *Engine) Stats() EngineStats {
	return EngineStats{
		EventsSeen:         e.eventsSeen.Load(),
		TransitionsSkipped: e.transitionsSkipped.Load(),
		RulesMatched:       e.rulesMatched.Load(),
		ActionsFired:       e.actionsFired.Load(),
		ActionsFailed:      e.actionsFailed.Load(),
		ActionsDeduped:     e.actionsDeduped.Load(),
		RefreshFailures:    e.RefreshFailures(),
		SnapshotAge:        e.SnapshotAge(),
	}
}

// EngineStats is a point-in-time snapshot of the engine counters.
type EngineStats struct {
	EventsSeen         int64
	TransitionsSkipped int64 // cancelled/expired: dashboard-only, never notify
	RulesMatched       int64
	ActionsFired       int64
	ActionsFailed      int64
	ActionsDeduped     int64
	// RefreshFailures counts reloads that failed (rule list or any
	// recipient-channel lookup); the previous snapshot stays installed.
	RefreshFailures int64
	// SnapshotAge is how long ago the routing snapshot was last
	// refreshed successfully (0 = never).
	SnapshotAge time.Duration
}

// TestEngineRejectedSubmissionRetries pins the ledger-failure fallback:
// when the job cannot be persisted, the engine falls back to the
// in-memory submission path; when the action also rejects the request,
// the transition is counted failed and a replay retries it.
func TestEngineRejectedSubmissionRetries(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
	}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts)

	store.mu.Lock()
	store.enqueueErr = errors.New("db busy")
	store.mu.Unlock()
	acts.mu.Lock()
	acts.err = errors.New("queue full")
	acts.mu.Unlock()
	ev := hazardEvent("severe", dispatch.TransitionNew)
	feed <- ev
	waitFor(t, func() bool { return e.Stats().ActionsFailed == 1 }, "first submission rejected")

	// The ledger recovers and the queue drains; the same transition
	// replays and must be queued for execution.
	store.mu.Lock()
	store.enqueueErr = nil
	store.mu.Unlock()
	acts.mu.Lock()
	acts.err = nil
	acts.mu.Unlock()
	feed <- ev
	waitFor(t, func() bool { return e.Stats().ActionsFired == 1 }, "replay re-fired after rejection")

	// The failed fallback submission must not have deduplicated anything.
	if d := e.Stats().ActionsDeduped; d != 0 {
		t.Errorf("deduped = %d, want 0 (rejection must not deduplicate)", d)
	}
}

// TestEngineTerminalFailureRearms pins the burned-budget contract: a job
// whose execution exhausted all attempts is terminally failed, and a
// replay of the transition re-arms it with a fresh budget instead of
// deduplicating it away.
func TestEngineTerminalFailureRearms(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
	}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts)

	ev := hazardEvent("severe", dispatch.TransitionNew)
	store.failJob(1, "log", fireDedupKey(ev))
	feed <- ev
	waitFor(t, func() bool { return e.Stats().ActionsFired == 1 }, "terminal failure re-armed and queued")

	// The re-armed job is now pending: replays deduplicate.
	feed <- ev
	waitFor(t, func() bool { return e.Stats().ActionsDeduped == 1 }, "re-armed job deduplicated")
}

// TestEnginePayloadPersisted pins the durable-payload contract: the job
// handed to the queue carries the event and every recipient channel, so
// a worker can execute it after a restart.
func TestEnginePayloadPersisted(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
	}, bcc: map[int64][]string{1: {"a@example.net"}},
		aprsBcc:    map[int64][]string{1: {"SP9SPM-1"}},
		discordBcc: map[int64][]string{1: {"ops#1234"}}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts)

	ev := hazardEvent("severe", dispatch.TransitionNew)
	feed <- ev
	waitFor(t, func() bool { return e.Stats().ActionsFired == 1 }, "job queued")

	store.mu.Lock()
	payload := store.payloads[fmt.Sprintf("1|log|%s", fireDedupKey(ev))]
	store.mu.Unlock()
	if len(payload) == 0 {
		t.Fatal("job persisted without payload")
	}
	var req action.ActionRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		t.Fatalf("payload decode: %v", err)
	}
	if req.Event.Hazard == nil || req.Event.Hazard.Key != ev.Hazard.Key {
		t.Errorf("payload event = %+v, want hazard key %q", req.Event.Hazard, ev.Hazard.Key)
	}
	if len(req.Bcc) != 1 || req.Bcc[0] != "a@example.net" {
		t.Errorf("payload Bcc = %v, want [a@example.net]", req.Bcc)
	}
	if len(req.APRSCallsigns) != 1 || req.APRSCallsigns[0] != "SP9SPM-1" {
		t.Errorf("payload APRSCallsigns = %v, want [SP9SPM-1]", req.APRSCallsigns)
	}
	if len(req.DiscordHandles) != 1 || req.DiscordHandles[0] != "ops#1234" {
		t.Errorf("payload DiscordHandles = %v, want [ops#1234]", req.DiscordHandles)
	}
}

// TestEngineLedgerFallback pins the never-suppress contract: when the
// ledger fails, the engine still submits in memory, and once the ledger
// heals the replay is queued durably (at-least-once across the failure).
func TestEngineLedgerFallback(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
	}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts)

	store.mu.Lock()
	store.enqueueErr = errors.New("db busy")
	store.mu.Unlock()
	ev := hazardEvent("severe", dispatch.TransitionNew)
	feed <- ev
	waitFor(t, func() bool { return e.Stats().ActionsFired == 1 }, "in-memory fallback executed")

	// The ledger heals: the replay is queued durably (nothing was
	// recorded during the failure, so it fires again — at-least-once).
	store.mu.Lock()
	store.enqueueErr = nil
	store.mu.Unlock()
	feed <- ev
	waitFor(t, func() bool { return e.Stats().ActionsFired == 2 }, "replay queued durably after heal")

	feed <- ev
	waitFor(t, func() bool { return e.Stats().ActionsDeduped == 1 }, "durable job deduplicated")
}

// TestEnginePublisherIsolation pins the publisher-aware dedup key:
// independent publishers producing identical source/key/changeID tuples
// both fire; replays of the SAME publisher deduplicate together.
func TestEnginePublisherIsolation(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
	}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts)

	mk := func(publisher string) dispatch.Event {
		ev := hazardEvent("severe", dispatch.TransitionNew)
		ev.Hazard.Publisher = publisher
		return ev
	}
	evA, evB := mk("pub-a"), mk("pub-b")
	feed <- evA
	feed <- evA // same publisher, identical transition: deduplicated
	feed <- evB // identical identity, independent publisher: fires

	waitFor(t, func() bool {
		s := e.Stats()
		return s.ActionsFired == 2 && s.ActionsDeduped == 1
	}, "publishers isolated, replays deduplicated")

	if got := store.payloadCount("log"); got != 2 {
		t.Errorf("log fired %d times, want 2 (one per publisher)", got)
	}
}

// TestRefreshFailureKeepsSnapshot pins the all-or-nothing refresh: a
// recipient-channel lookup failure must NOT install the new rules (and
// must not wipe working recipient data) — the previous snapshot keeps
// serving until the next successful refresh.
func TestRefreshFailureKeepsSnapshot(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
	}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts)
	waitFor(t, e.Ready, "initial rules loaded")

	// The initial snapshot routes to log.
	feed <- hazardEvent("severe", dispatch.TransitionNew)
	waitFor(t, func() bool { return e.Stats().ActionsFired == 1 }, "initial delivery")

	// New rules want sms, but the recipient lookup breaks: the refresh
	// must fail and keep the OLD snapshot (log) in service.
	store.mu.Lock()
	store.rules = []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("sms", "unknown")}},
	}
	store.recipientErr = errors.New("directory unavailable")
	store.mu.Unlock()
	e.refresh()
	if got := e.RefreshFailures(); got != 1 {
		t.Fatalf("refresh failures = %d, want 1", got)
	}
	feed <- hazardEvent("severe", dispatch.TransitionNew)
	waitFor(t, func() bool { return e.Stats().ActionsFired == 2 }, "old snapshot still delivered")
	if n := store.payloadCount("sms"); n != 0 {
		t.Fatalf("sms queued %d times, want 0 (failed refresh must not install)", n)
	}

	// Healed directory: the next refresh installs the new snapshot.
	store.mu.Lock()
	store.recipientErr = nil
	store.mu.Unlock()
	e.refresh()
	if age := e.SnapshotAge(); age < 0 {
		t.Fatalf("snapshot age = %v, want non-negative", age)
	}
	feed <- hazardEvent("severe", dispatch.TransitionNew)
	waitFor(t, func() bool { return e.Stats().ActionsFired == 3 }, "new snapshot delivered")
	if n := store.payloadCount("sms"); n != 1 {
		t.Fatalf("sms queued %d times after healed refresh, want 1", n)
	}
}

// fakeInbox is a test double for the durable dispatch inbox.
type fakeInbox struct {
	mu      sync.Mutex
	pending []storage.InboxItem
	acked   []int64
	err     error
}

func (f *fakeInbox) PendingInboxEvents(_ context.Context, limit int) ([]storage.InboxItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return append([]storage.InboxItem(nil), f.pending...), nil
}

func (f *fakeInbox) AckInboxEvent(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.acked = append(f.acked, id)
	return nil
}

// TestEngineInboxRecoveryAndAck pins the durable acceptance loop: live
// events carrying an inbox ID are acknowledged after evaluation, and
// pending inbox rows (crash / full queue) re-enter the engine on
// recovery and are acknowledged too.
func TestEngineInboxRecoveryAndAck(t *testing.T) {
	store := &fakeStore{rules: []storage.GroupRouting{
		{GroupID: 1, Name: "spok", Actions: []storage.ChannelAssignment{asn("log", "unknown")}},
	}}
	acts := &fakeActions{}
	e, feed := startEngine(t, store, acts)
	waitFor(t, e.Ready, "rules loaded")

	in := &fakeInbox{}
	e.SetInbox(in)

	// Live path: the event carries its inbox ID and is acked afterwards.
	live := hazardEvent("severe", dispatch.TransitionNew)
	live.InboxID = 42
	feed <- live
	waitFor(t, func() bool {
		in.mu.Lock()
		defer in.mu.Unlock()
		return len(in.acked) == 1 && in.acked[0] == 42
	}, "live inbox event acked")
	if got := e.Stats().ActionsFired; got != 1 {
		t.Fatalf("actions fired = %d, want 1", got)
	}

	// Recovery path: a pending row (crash before evaluation) re-enters.
	rec := hazardEvent("severe", dispatch.TransitionNew)
	rec.InboxID = 7
	in.mu.Lock()
	in.pending = []storage.InboxItem{{ID: 7, Event: rec}}
	in.mu.Unlock()
	e.recoverInbox(context.Background())
	waitFor(t, func() bool {
		in.mu.Lock()
		defer in.mu.Unlock()
		return len(in.acked) == 2 && in.acked[1] == 7
	}, "recovered inbox event acked")
	if got := e.Stats().ActionsFired; got != 2 {
		t.Fatalf("actions fired = %d, want 2 after recovery", got)
	}
}
