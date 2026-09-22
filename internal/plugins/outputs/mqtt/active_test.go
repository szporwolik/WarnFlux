package mqtt

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// ---- fake MQTT transport ----

type fakeToken struct {
	err  error
	done chan struct{}
}

func newFakeToken(err error) *fakeToken {
	// Completed tokens (success or error): paho tokens finish immediately
	// unless explicitly gated by the test (gated tokens are constructed
	// directly with &fakeToken{...}).
	t := &fakeToken{err: err, done: make(chan struct{})}
	close(t.done)
	return t
}

func (t *fakeToken) Wait() bool            { <-t.done; return t.err == nil }
func (t *fakeToken) Done() <-chan struct{} { return t.done }
func (t *fakeToken) Error() error          { return t.err }
func (t *fakeToken) WaitTimeout(d time.Duration) bool {
	select {
	case <-t.done:
		return t.err == nil
	case <-time.After(d):
		return false
	}
}

type fakePublish struct {
	topic    string
	qos      byte
	retained bool
	payload  []byte
}

type fakeClient struct {
	mu         sync.Mutex
	connected  bool
	connectErr error
	publishErr func(topic string) error
	publishes  []fakePublish
	nextGate   *fakeToken
	options    *paho.ClientOptions
}

func (c *fakeClient) IsConnectionOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

func (c *fakeClient) Connect() paho.Token {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connectErr != nil {
		return newFakeToken(c.connectErr)
	}
	c.connected = true
	return newFakeToken(nil)
}

func (c *fakeClient) Disconnect(uint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = false
}

func (c *fakeClient) Publish(topic string, qos byte, retained bool, payload any) paho.Token {
	b, _ := payload.([]byte)
	c.mu.Lock()
	c.publishes = append(c.publishes, fakePublish{topic: topic, qos: qos, retained: retained, payload: append([]byte(nil), b...)})
	var gate *fakeToken
	if c.nextGate != nil {
		gate = c.nextGate
		c.nextGate = nil
	}
	errFn := c.publishErr
	c.mu.Unlock()
	if gate != nil {
		return gate
	}
	if errFn != nil {
		if err := errFn(topic); err != nil {
			return newFakeToken(err)
		}
	}
	return newFakeToken(nil)
}

func (c *fakeClient) OptionsReader() paho.ClientOptionsReader {
	if c.options == nil {
		c.options = paho.NewClientOptions()
	}
	return paho.NewOptionsReader(c.options)
}

func (c *fakeClient) snapshot() []fakePublish {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]fakePublish, len(c.publishes))
	copy(out, c.publishes)
	return out
}

func (c *fakeClient) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.publishes)
}

func (c *fakeClient) setPublishErr(fn func(topic string) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.publishErr = fn
}

func (c *fakeClient) gateNext(g *fakeToken) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextGate = g
}

func newTestOutput(fc *fakeClient) *Output {
	return &Output{
		cfg:            Config{TopicPrefix: "warnflux"},
		qos:            1,
		client:         fc,
		activeCache:    make(map[string]activeCacheEntry),
		pendingDeletes: make(map[string]activeDeleteEntry),
	}
}

func activeEvent() core.HazardEvent {
	e := core.HazardEvent{
		Source:   "meteoalarm",
		SourceID: "warning-123",
		Category: "met",
		Event:    "Rain",
		Severity: "orange",
		Headline: "Heavy rain expected",
		Status:   core.StatusActive,
	}
	e.Normalize()
	return e
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

// TestActiveIDAndTopicMapping: the final topic level is always 64 lowercase
// hex characters derived from the event key — raw keys never leak into the
// topic and MQTT wildcards are impossible.
func TestActiveIDAndTopicMapping(t *testing.T) {
	known := fmt.Sprintf("%x", sha256.Sum256([]byte("meteoalarm:warning-123")))
	if got := activeID("meteoalarm:warning-123"); got != known {
		t.Errorf("activeID = %q, want the sha256 hex of the key", got)
	}
	cases := []string{
		"meteoalarm:warning-123",
		"a/b/c",
		"warn+ing#x",
		"zażółć gęślą jaźń / u#n+i",
		strings.Repeat("x", 500),
		"line\nbreak?q=1&ok=",
	}
	o := newTestOutput(&fakeClient{})
	for _, key := range cases {
		id := activeID(key)
		if len(id) != 64 {
			t.Errorf("key %q: id length = %d, want 64", key, len(id))
		}
		for _, r := range id {
			if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
				t.Errorf("key %q: id %q contains non-hex character", key, id)
			}
		}
		if activeID(key) != id {
			t.Errorf("key %q: id is not deterministic", key)
		}
		topic := o.activeTopic("meteoalarm", key)
		want := "warnflux/active/meteoalarm/" + id
		if topic != want {
			t.Errorf("key %q: topic = %q, want %q", key, topic, want)
		}
		if strings.ContainsAny(topic, "+#") || strings.Contains(topic, " ") {
			t.Errorf("key %q: unsafe topic %q", key, topic)
		}
	}
	if activeID("a") == activeID("b") {
		t.Error("distinct keys must hash to distinct ids (by construction of sha256)")
	}
}

// TestActivePayloadUsesSharedWireHazardEvent: the active_hazard wrapper
// reuses the exact /events hazard wire mapping — no second hazard JSON.
func TestActivePayloadUsesSharedWireHazardEvent(t *testing.T) {
	o := newTestOutput(&fakeClient{})
	ev := activeEvent()
	payload, err := o.activePayload(ev)
	if err != nil {
		t.Fatalf("activePayload: %v", err)
	}
	var out wireActiveHazard
	if err := json.Unmarshal(payload, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.SchemaVersion != 1 || out.Type != "active_hazard" || out.EventKey != ev.Key() {
		t.Errorf("wrapper = %+v, want schema 1, type active_hazard, event_key %q", out, ev.Key())
	}
	eventsBlock, err := json.Marshal(toWireEvent(core.EventChange{ID: 7, Type: core.ChangeNew, Event: ev}).Event)
	if err != nil {
		t.Fatal(err)
	}
	activeBlock, err := json.Marshal(out.Event)
	if err != nil {
		t.Fatal(err)
	}
	if string(eventsBlock) != string(activeBlock) {
		t.Errorf("active event block differs from /events event block:\nactive: %s\nevents: %s", activeBlock, eventsBlock)
	}
}

// TestHandlePublishesEventsThenActiveRetained: NEW active publishes /events
// (retain=false) first, then the retained active payload, on the stable
// sha256 topic.
func TestHandlePublishesEventsThenActiveRetained(t *testing.T) {
	fc := &fakeClient{}
	o := newTestOutput(fc)
	ev := activeEvent()
	if err := o.Handle(context.Background(), core.EventChange{ID: 1, Type: core.ChangeNew, Event: ev}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	pubs := fc.snapshot()
	if len(pubs) != 2 {
		t.Fatalf("publishes = %d, want 2 (events + active)", len(pubs))
	}
	if pubs[0].topic != "warnflux/events" || pubs[0].retained {
		t.Errorf("first publish = %+v, want non-retained /events", pubs[0])
	}
	wantTopic := o.activeTopic(ev.Source, ev.Key())
	if pubs[1].topic != wantTopic || !pubs[1].retained {
		t.Errorf("second publish = %+v, want retained %s", pubs[1], wantTopic)
	}
	var w wireActiveHazard
	if err := json.Unmarshal(pubs[1].payload, &w); err != nil || w.EventKey != ev.Key() {
		t.Errorf("active payload: %v", err)
	}
	o.activeMu.Lock()
	entry, ok := o.activeCache[ev.Key()]
	o.activeMu.Unlock()
	if !ok || entry.topic != wantTopic {
		t.Errorf("desired cache entry = %+v (ok=%v), want %s", entry, ok, wantTopic)
	}
}

// TestHandleUpdateReplacesRetainedPayload: UPDATED active keeps the SAME
// topic (identity derives from the key, not content) with a new payload.
func TestHandleUpdateReplacesRetainedPayload(t *testing.T) {
	fc := &fakeClient{}
	o := newTestOutput(fc)
	v1 := activeEvent()
	v2 := activeEvent()
	v2.Headline = "Updated headline"

	if err := o.Handle(context.Background(), core.EventChange{ID: 1, Type: core.ChangeNew, Event: v1}); err != nil {
		t.Fatalf("Handle v1: %v", err)
	}
	if err := o.Handle(context.Background(), core.EventChange{ID: 2, Type: core.ChangeUpdated, Event: v2}); err != nil {
		t.Fatalf("Handle v2: %v", err)
	}
	pubs := fc.snapshot()
	if len(pubs) != 4 {
		t.Fatalf("publishes = %d, want 4", len(pubs))
	}
	if pubs[1].topic != pubs[3].topic {
		t.Errorf("active topic changed on update: %q vs %q", pubs[1].topic, pubs[3].topic)
	}
	if string(pubs[1].payload) == string(pubs[3].payload) {
		t.Error("active payload did not change on update")
	}
}

// TestHandleCancelAndExpireDeleteRetained: cancelled/expired events publish
// a zero-length retained payload on the same topic and remove the desired
// cache entry.
func TestHandleCancelAndExpireDeleteRetained(t *testing.T) {
	for name, status := range map[string]core.EventStatus{
		"cancelled": core.StatusCancelled,
		"expired":   core.StatusExpired,
	} {
		t.Run(name, func(t *testing.T) {
			fc := &fakeClient{}
			o := newTestOutput(fc)
			ev := activeEvent()
			if err := o.Handle(context.Background(), core.EventChange{ID: 1, Type: core.ChangeNew, Event: ev}); err != nil {
				t.Fatalf("Handle active: %v", err)
			}
			ev.Status = status
			if err := o.Handle(context.Background(), core.EventChange{ID: 2, Type: core.ChangeUpdated, Event: ev}); err != nil {
				t.Fatalf("Handle %s: %v", name, err)
			}
			pubs := fc.snapshot()
			if len(pubs) != 4 {
				t.Fatalf("publishes = %d, want 4", len(pubs))
			}
			if pubs[3].topic != pubs[1].topic || !pubs[3].retained {
				t.Errorf("deletion publish = %+v, want retained on the same active topic", pubs[3])
			}
			if len(pubs[3].payload) != 0 {
				t.Errorf("deletion payload = %d bytes, want zero (retained-topic removal)", len(pubs[3].payload))
			}
			o.activeMu.Lock()
			_, ok := o.activeCache[ev.Key()]
			o.activeMu.Unlock()
			if ok {
				t.Errorf("%s event still in the desired active cache", name)
			}
		})
	}
}

// TestHandleActivePublishFailureReturnsError: a failed /active publish must
// make Handle fail (journal ACK stays pending upstream); a failed /events
// publish stops before the active step.
func TestHandleActivePublishFailureReturnsError(t *testing.T) {
	// /active fails after /events succeeded.
	fc := &fakeClient{}
	o := newTestOutput(fc)
	fc.setPublishErr(func(topic string) error {
		if strings.HasPrefix(topic, "warnflux/active") {
			return errors.New("broker refused retained publish")
		}
		return nil
	})
	ev := activeEvent()
	err := o.Handle(context.Background(), core.EventChange{ID: 1, Type: core.ChangeNew, Event: ev})
	if err == nil || !strings.Contains(err.Error(), "active state") {
		t.Fatalf("Handle = %v, want active-state error", err)
	}
	if got := fc.count(); got != 2 {
		t.Errorf("publishes = %d, want 2 (events succeeded, active attempted)", got)
	}

	// /events fails: the active view is not touched at all.
	fc2 := &fakeClient{}
	o2 := newTestOutput(fc2)
	fc2.setPublishErr(func(topic string) error {
		if topic == "warnflux/events" {
			return errors.New("broker down")
		}
		return nil
	})
	if err := o2.Handle(context.Background(), core.EventChange{ID: 1, Type: core.ChangeNew, Event: ev}); err == nil {
		t.Fatal("Handle = nil, want events publish error")
	}
	if got := fc2.count(); got != 1 {
		t.Errorf("publishes = %d, want 1 (no active attempt after /events failure)", got)
	}
}

// TestSeedActiveStateIsLocalAndPopulatesCache: startup seeding registers
// the desired state LOCALLY — no network publish happens, so seeding can
// never serialize on a slow broker. The cache entry survives for the later
// rehydration pass.
func TestSeedActiveStateIsLocalAndPopulatesCache(t *testing.T) {
	fc := &fakeClient{}
	o := newTestOutput(fc)
	ev := activeEvent()
	if err := o.SeedActiveState(ev); err != nil {
		t.Fatalf("SeedActiveState: %v", err)
	}
	if got := fc.count(); got != 0 {
		t.Fatalf("seeding performed %d network publishes, want 0 (local registration only)", got)
	}
	o.activeMu.Lock()
	entry, ok := o.activeCache[ev.Key()]
	o.activeMu.Unlock()
	if !ok || entry.topic != o.activeTopic(ev.Source, ev.Key()) || len(entry.payload) == 0 {
		t.Errorf("desired cache entry = %+v (ok=%v), want a populated entry without any network I/O", entry, ok)
	}

	// Non-active events are ignored (startup seeding is active-only).
	ev.Status = core.StatusCancelled
	if err := o.SeedActiveState(ev); err != nil {
		t.Errorf("seeding a cancelled event: %v", err)
	}
}

// TestClientOptionsCarryOnConnectHandler pins the paho registration order:
// paho.NewClient copies the ClientOptions struct by value (c.options = *o),
// so the on-connect hook MUST already be set when the options leave
// newClientOptions. Simulate the copy and verify the hook survives it.
func TestClientOptionsCarryOnConnectHandler(t *testing.T) {
	called := make(chan struct{}, 1)
	opts := newClientOptions(
		Config{Broker: "tcp://localhost:1883", ClientID: "test", TopicPrefix: "warnflux"},
		1,
		[]byte(`{}`),
		func(paho.Client) { called <- struct{}{} },
	)
	// What paho.NewClient does: c.options = *o (struct COPY). The handler
	// must already be present before that copy.
	copied := *opts
	if copied.OnConnect == nil {
		t.Fatal("on-connect handler missing BEFORE the paho options copy — reconnect rehydration would be silently lost")
	}
	copied.OnConnect(nil)
	select {
	case <-called:
	default:
		t.Fatal("installed on-connect handler was not invoked")
	}
}

// TestRehydrateOnConnectRepublishesCache: a broker reconnect republishes
// every desired active entry as a retained message without any new provider
// update.
func TestRehydrateOnConnectRepublishesCache(t *testing.T) {
	fc := &fakeClient{}
	o := newTestOutput(fc)
	events := []core.HazardEvent{activeEvent(), activeEvent(), activeEvent()}
	events[1].SourceID = "warning-124"
	events[2].SourceID = "warning-125"
	for i, ev := range events {
		if err := o.Handle(context.Background(), core.EventChange{ID: int64(i + 1), Type: core.ChangeNew, Event: ev}); err != nil {
			t.Fatalf("Handle %d: %v", i, err)
		}
	}
	before := fc.count() // 3× (events + active)
	o.rehydrateOnConnect()
	waitFor(t, 2*time.Second, func() bool { return fc.count() == before+3 })
	for _, p := range fc.snapshot()[before:] {
		if !p.retained || !strings.HasPrefix(p.topic, "warnflux/active/") {
			t.Errorf("rehydration publish = %+v, want retained active topic", p)
		}
	}
}

// TestRehydrateConvergesAfterConcurrentUpdate: if Handle updates an entry
// while a rehydration pass is republishing an older snapshot, the pass
// detects the changed generation and republishes the NEW value — the final
// retained state converges to the newest desired state.
func TestRehydrateConvergesAfterConcurrentUpdate(t *testing.T) {
	fc := &fakeClient{}
	o := newTestOutput(fc)
	v1 := activeEvent()
	v2 := activeEvent()
	v2.Headline = "Newer headline"

	// Seed the desired cache with v1 directly (no publish).
	payload, err := o.activePayload(v1)
	if err != nil {
		t.Fatal(err)
	}
	o.activeMu.Lock()
	o.activeSeq++
	o.activeCache[v1.Key()] = activeCacheEntry{key: v1.Key(), topic: o.activeTopic(v1.Source, v1.Key()), payload: payload, seq: o.activeSeq}
	o.activeMu.Unlock()

	// Gate the FIRST rehydration publish.
	gate := &fakeToken{done: make(chan struct{})}
	fc.gateNext(gate)
	o.rehydrateOnConnect()
	waitFor(t, 2*time.Second, func() bool { return fc.count() == 1 }) // rehydration is inside publish v1

	// Concurrent Handle-style update: cache and publish v2 (not gated).
	if err := o.updateActiveState(context.Background(), v2); err != nil {
		t.Fatalf("updateActiveState: %v", err)
	}
	close(gate.done)

	// The pass notices the generation change and republishes v2.
	waitFor(t, 2*time.Second, func() bool { return fc.count() >= 3 })
	pubs := fc.snapshot()
	last := pubs[len(pubs)-1]
	if !strings.HasPrefix(last.topic, "warnflux/active/") || !last.retained {
		t.Fatalf("last publish = %+v, want retained active topic", last)
	}
	var w wireActiveHazard
	if err := json.Unmarshal(last.payload, &w); err != nil {
		t.Fatal(err)
	}
	if w.Event.Headline != "Newer headline" {
		t.Errorf("final retained headline = %q, want the newer value", w.Event.Headline)
	}
}

// rehydrateDeleteRace runs the deterministic race for a lifecycle deletion
// (cancelled or expired) happening while a rehydration pass is blocked
// inside the stale ACTIVE publish. The final broker-equivalent state for
// the topic must be ABSENT (last publish: retained, zero-length payload).
func rehydrateDeleteRace(t *testing.T, status core.EventStatus) {
	t.Helper()
	fc := &fakeClient{}
	o := newTestOutput(fc)
	ev := activeEvent()

	// Seed the desired cache with the ACTIVE payload directly (no publish).
	if err := o.SeedActiveState(ev); err != nil {
		t.Fatal(err)
	}

	// Gate the FIRST rehydration publish (the stale ACTIVE publish).
	gate := &fakeToken{done: make(chan struct{})}
	fc.gateNext(gate)
	o.rehydrateOnConnect()
	waitFor(t, 2*time.Second, func() bool { return fc.count() == 1 }) // stale ACTIVE publish is in flight

	// While the stale publish is blocked, the event is cancelled/expired:
	// cache removes it and the retained DELETE succeeds.
	ev.Status = status
	if err := o.updateActiveState(context.Background(), ev); err != nil {
		t.Fatalf("updateActiveState(%s): %v", status, err)
	}
	if got := fc.count(); got != 2 {
		t.Fatalf("publishes after deletion = %d, want 2 (stale ACTIVE blocked + DELETE)", got)
	}

	// Release the stale ACTIVE publish; the rehydration pass notices the
	// entry is GONE and publishes the retained DELETE again.
	close(gate.done)
	waitFor(t, 2*time.Second, func() bool { return fc.count() >= 3 })

	pubs := fc.snapshot()
	last := pubs[len(pubs)-1]
	if !last.retained || !strings.HasPrefix(last.topic, "warnflux/active/") {
		t.Fatalf("last publish = %+v, want the retained DELETE on the active topic", last)
	}
	if len(last.payload) != 0 {
		t.Errorf("final retained payload = %d bytes, want zero (hazard must not be resurrected)", len(last.payload))
	}
	o.activeMu.Lock()
	_, ok := o.activeCache[ev.Key()]
	o.activeMu.Unlock()
	if ok {
		t.Errorf("%s hazard still in the desired cache", status)
	}
}

// TestRehydrateCancelDuringPublish: a cancel racing a stale rehydration
// publish must end with the retained topic DELETED, never resurrected.
func TestRehydrateCancelDuringPublish(t *testing.T) {
	rehydrateDeleteRace(t, core.StatusCancelled)
}

// TestRehydrateExpireDuringPublish: same race for expiry.
func TestRehydrateExpireDuringPublish(t *testing.T) {
	rehydrateDeleteRace(t, core.StatusExpired)
}

// TestRehydrateCorrectiveDeleteFailurePersistsAndRetries covers the exact
// remaining edge-case: the corrective retained DELETE triggered by a stale
// rehydrate publish FAILS. The pending deletion must be remembered, and a
// later reconnect must retry it until the broker confirms the topic is
// gone.
func TestRehydrateCorrectiveDeleteFailurePersistsAndRetries(t *testing.T) {
	for name, status := range map[string]core.EventStatus{
		"cancelled": core.StatusCancelled,
		"expired":   core.StatusExpired,
	} {
		t.Run(name, func(t *testing.T) {
			fc := &fakeClient{}
			o := newTestOutput(fc)
			ev := activeEvent()
			if err := o.SeedActiveState(ev); err != nil {
				t.Fatal(err)
			}

			// Gate the first rehydrate publish (the stale ACTIVE).
			gate := &fakeToken{done: make(chan struct{})}
			fc.gateNext(gate)
			o.rehydrateOnConnect()
			waitFor(t, 2*time.Second, func() bool { return fc.count() == 1 })

			// The event is cancelled/expired while the stale publish is
			// blocked: the NORMAL retained DELETE succeeds and clears the
			// pending registration immediately.
			ev.Status = status
			if err := o.updateActiveState(context.Background(), ev); err != nil {
				t.Fatalf("updateActiveState(%s): %v", name, err)
			}
			if got := fc.count(); got != 2 {
				t.Fatalf("publishes = %d, want 2 (stale ACTIVE blocked + successful DELETE)", got)
			}
			o.activeMu.Lock()
			_, pending := o.pendingDeletes[ev.Key()]
			o.activeMu.Unlock()
			if pending {
				t.Fatal("pending delete must be cleared after a successful normal DELETE")
			}

			// Force the CORRECTIVE delete (triggered by releasing the
			// stale publish) to FAIL.
			fc.setPublishErr(func(topic string) error {
				if strings.HasPrefix(topic, "warnflux/active/") {
					return errors.New("delete failed")
				}
				return nil
			})
			close(gate.done)
			// The corrective delete is attempted and fails: the pending
			// registration persists for later recovery.
			waitFor(t, 2*time.Second, func() bool {
				o.activeMu.Lock()
				_, ok := o.pendingDeletes[ev.Key()]
				o.activeMu.Unlock()
				return ok && fc.count() >= 3
			})

			// A later reconnect with working MQTT retries the delete and
			// clears the registration.
			fc.setPublishErr(nil)
			o.rehydrateOnConnect()
			waitFor(t, 2*time.Second, func() bool {
				o.activeMu.Lock()
				_, ok := o.pendingDeletes[ev.Key()]
				o.activeMu.Unlock()
				return !ok
			})
			pubs := fc.snapshot()
			last := pubs[len(pubs)-1]
			if !last.retained || len(last.payload) != 0 || last.topic != o.activeTopic(ev.Source, ev.Key()) {
				t.Errorf("final publish = %+v, want the retained zero-length delete on the active topic", last)
			}
		})
	}
}

// TestReactivationInvalidatesPendingDelete: a legitimate reactivation after
// a failed cancel must remove the stale pending delete and win the final
// retained state — a later rehydrate must never delete the reactivated
// hazard.
func TestReactivationInvalidatesPendingDelete(t *testing.T) {
	fc := &fakeClient{}
	o := newTestOutput(fc)
	ev := activeEvent()
	if err := o.Handle(context.Background(), core.EventChange{ID: 1, Type: core.ChangeNew, Event: ev}); err != nil {
		t.Fatalf("Handle active: %v", err)
	}

	// Cancel with a failing DELETE: Handle errors, the pending delete
	// stays registered.
	fc.setPublishErr(func(topic string) error {
		if strings.HasPrefix(topic, "warnflux/active/") {
			return errors.New("delete failed")
		}
		return nil
	})
	cancelled := ev
	cancelled.Status = core.StatusCancelled
	if err := o.Handle(context.Background(), core.EventChange{ID: 2, Type: core.ChangeUpdated, Event: cancelled}); err == nil {
		t.Fatal("Handle(cancel) = nil, want an error while the DELETE fails")
	}
	o.activeMu.Lock()
	_, inCache := o.activeCache[ev.Key()]
	_, pending := o.pendingDeletes[ev.Key()]
	o.activeMu.Unlock()
	if inCache || !pending {
		t.Fatalf("after failed cancel: inCache=%v pending=%v, want absent + pending", inCache, pending)
	}

	// Reactivation removes the pending delete and publishes ACTIVE.
	fc.setPublishErr(nil)
	reactivated := ev
	reactivated.Headline = "reactivated"
	if err := o.Handle(context.Background(), core.EventChange{ID: 3, Type: core.ChangeUpdated, Event: reactivated}); err != nil {
		t.Fatalf("Handle reactivation: %v", err)
	}
	o.activeMu.Lock()
	_, inCache = o.activeCache[ev.Key()]
	_, pending = o.pendingDeletes[ev.Key()]
	o.activeMu.Unlock()
	if !inCache || pending {
		t.Fatalf("after reactivation: inCache=%v pending=%v, want active + no pending delete", inCache, pending)
	}

	// A later rehydrate must republish ACTIVE, never delete it.
	before := fc.count()
	o.rehydrateOnConnect()
	waitFor(t, 2*time.Second, func() bool { return fc.count() >= before+1 })
	for _, p := range fc.snapshot()[before:] {
		if p.topic == o.activeTopic(ev.Source, ev.Key()) && len(p.payload) == 0 {
			t.Error("rehydration deleted a reactivated hazard")
		}
	}
}

// TestHandleDeleteFailurePreservesAckSafety: a failed retained DELETE on
// the normal cancel path keeps Handle failing (journal stays unacknowledged
// upstream) while the pending delete remains registered for later recovery.
func TestHandleDeleteFailurePreservesAckSafety(t *testing.T) {
	fc := &fakeClient{}
	o := newTestOutput(fc)
	ev := activeEvent()
	if err := o.Handle(context.Background(), core.EventChange{ID: 1, Type: core.ChangeNew, Event: ev}); err != nil {
		t.Fatalf("Handle active: %v", err)
	}

	fc.setPublishErr(func(topic string) error {
		if strings.HasPrefix(topic, "warnflux/active/") {
			return errors.New("delete failed")
		}
		return nil
	})
	cancelled := ev
	cancelled.Status = core.StatusCancelled
	if err := o.Handle(context.Background(), core.EventChange{ID: 2, Type: core.ChangeUpdated, Event: cancelled}); err == nil {
		t.Fatal("Handle(cancel) = nil, want an error (journal must stay unacked)")
	}
	o.activeMu.Lock()
	_, pending := o.pendingDeletes[ev.Key()]
	o.activeMu.Unlock()
	if !pending {
		t.Fatal("pending delete must persist after the failed DELETE")
	}

	// Journal redelivery retry: /events may duplicate (at-least-once),
	// the DELETE now succeeds and clears the registration.
	fc.setPublishErr(nil)
	if err := o.Handle(context.Background(), core.EventChange{ID: 3, Type: core.ChangeUpdated, Event: cancelled}); err != nil {
		t.Fatalf("retry Handle(cancel): %v", err)
	}
	o.activeMu.Lock()
	_, pending = o.pendingDeletes[ev.Key()]
	o.activeMu.Unlock()
	if pending {
		t.Error("pending delete must be cleared after the successful retry")
	}
}
