package web

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
)

// recordingPublisher is a raw publisher that records the calls, in order,
// and can hold the next one in flight.
type recordingPublisher struct {
	mu       sync.Mutex
	calls    []string
	entering []string
	hold     chan struct{}
	held     chan struct{}
	heldOnce sync.Once
	failed   bool
}

// holdNext makes the next publication wait; the returned channel is
// closed while it waits and the release function lets it — and every
// later publication — through.
func (p *recordingPublisher) holdNext() (<-chan struct{}, func()) {
	hold := make(chan struct{})
	started := make(chan struct{})
	p.mu.Lock()
	p.hold, p.held = hold, started
	p.mu.Unlock()
	return started, func() {
		p.mu.Lock()
		p.hold = nil
		p.mu.Unlock()
		close(hold)
	}
}

func (p *recordingPublisher) enter(name string) error {
	p.mu.Lock()
	p.entering = append(p.entering, name)
	hold, held := p.hold, p.held
	p.mu.Unlock()
	if hold != nil {
		if held != nil {
			p.heldOnce.Do(func() { close(held) })
		}
		<-hold
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failed {
		return errors.New("broker down")
	}
	p.calls = append(p.calls, name)
	return nil
}

func (p *recordingPublisher) PublishActive(source string, h state.Hazard) error {
	return p.enter("active:" + h.EventKey)
}

func (p *recordingPublisher) ExpireActive(source, eventKey string) error {
	return p.enter("retire:" + eventKey)
}

func (p *recordingPublisher) PublishRaw(suffix string, retained bool, payload []byte) error {
	return p.enter("raw:" + suffix)
}

func (p *recordingPublisher) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

// fakeLedger is the durable version ledger of the tests: it remembers the
// newest version recorded for one event key, exactly like the SQLite
// lifecycle ledger does for local documents.
type fakeLedger struct {
	mu       sync.Mutex
	versions map[string]int64
	lookups  int
}

func newFakeLedger() *fakeLedger {
	return &fakeLedger{versions: map[string]int64{}}
}

// record stores a version under the publisher the gate resolves for local
// documents (the instance row).
func (l *fakeLedger) record(eventKey string, version int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if version > l.versions[eventKey] {
		l.versions[eventKey] = version
	}
}

func (l *fakeLedger) LifecycleSuperseded(ctx context.Context, publisher, eventKey string, changeID int64) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lookups++
	version, ok := l.versions[eventKey]
	if !ok {
		return false, nil
	}
	return version > changeID, nil
}

func (l *fakeLedger) lookupCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lookups
}

// enteredSnapshot lists the publications that REACHED the publisher,
// whether or not they finished.
func (p *recordingPublisher) enteredSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.entering...)
}

// TestBrokerGateDropsSupersededPublications pins the version rule: a
// publication built from an older version of the message must not reach
// the broker once a newer one did (reported P1: a delayed resync publish
// resurrected a communication the panel had already retired).
func TestBrokerGateDropsSupersededPublications(t *testing.T) {
	pub := &recordingPublisher{}
	g := newBrokerGate(pub, nil)
	h := state.Hazard{EventKey: "compose:1", Source: composeSource, Status: "active"}

	// The expiry writes version 7 and retires the document.
	if err := g.retireActive(composeSource, h.EventKey, 7); err != nil {
		t.Fatalf("retire: %v", err)
	}
	// The resync's copy was read at version 5: it loses.
	if err := g.publishActive(composeSource, h, 5); err != nil {
		t.Fatalf("stale publish: %v", err)
	}
	if got := pub.snapshot(); len(got) != 1 || got[0] != "retire:compose:1" {
		t.Fatalf("calls = %v, want only the retirement", got)
	}
	// The current version may be republished (an idempotent resync), and a
	// newer one as well.
	if err := g.publishActive(composeSource, h, 7); err != nil {
		t.Fatalf("current publish: %v", err)
	}
	if err := g.publishActive(composeSource, h, 8); err != nil {
		t.Fatalf("newer publish: %v", err)
	}
	if err := g.publishActive(composeSource, h, 6); err != nil {
		t.Fatalf("superseded publish: %v", err)
	}
	want := []string{"retire:compose:1", "active:compose:1", "active:compose:1"}
	if got := pub.snapshot(); len(got) != len(want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
	// Documents are gated one by one: another message is unaffected.
	other := state.Hazard{EventKey: "compose:2", Source: composeSource}
	if err := g.publishActive(composeSource, other, 1); err != nil {
		t.Fatalf("other message: %v", err)
	}
	if err := g.publishInfo("info/emcom/emcom/sp9moa/emcom", []byte("{}"), 1); err != nil {
		t.Fatalf("info document: %v", err)
	}
}

// TestBrokerGateSerializesOneMessage pins the other half: two
// publications of the SAME message never overlap, so the broker cannot
// end up with a state neither of them meant.
func TestBrokerGateSerializesOneMessage(t *testing.T) {
	pub := &recordingPublisher{}
	started, release := pub.holdNext()
	g := newBrokerGate(pub, nil)
	h := state.Hazard{EventKey: "compose:9", Source: composeSource}

	first := make(chan struct{})
	go func() {
		defer close(first)
		_ = g.publishActive(composeSource, h, 1)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the first publication never reached the broker")
	}

	second := make(chan struct{})
	go func() {
		defer close(second)
		_ = g.retireActive(composeSource, h.EventKey, 2)
	}()
	// The second call must wait for the first one to come back.
	select {
	case <-second:
		t.Fatal("a second publication of the same message ran concurrently")
	case <-time.After(100 * time.Millisecond):
	}
	if got := pub.enteredSnapshot(); len(got) != 1 {
		t.Fatalf("publications in flight = %v, want only the first one", got)
	}
	release()
	<-first
	<-second
	want := []string{"active:compose:9", "retire:compose:9"}
	if got := pub.snapshot(); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("calls = %v, want %v", got, want)
	}
}

// TestBrokerGateKeepsUnversionedPublications pins the escape hatch: a
// publication without a version identity (an installation without local
// storage) is serialized but never dropped, and it never raises the bar
// for a later versioned one.
func TestBrokerGateKeepsUnversionedPublications(t *testing.T) {
	pub := &recordingPublisher{}
	g := newBrokerGate(pub, nil)
	h := state.Hazard{EventKey: "compose:3", Source: composeSource}

	if err := g.retireActive(composeSource, h.EventKey, 9); err != nil {
		t.Fatal(err)
	}
	// Unversioned: never dropped ...
	if err := g.publishActive(composeSource, h, 0); err != nil {
		t.Fatal(err)
	}
	// ... and it does not raise the bar: an older versioned publication is
	// still superseded by the retirement.
	if err := g.publishActive(composeSource, h, 4); err != nil {
		t.Fatal(err)
	}
	// A newer version goes through.
	if err := g.publishActive(composeSource, h, 10); err != nil {
		t.Fatal(err)
	}
	want := []string{"retire:compose:3", "active:compose:3", "active:compose:3"}
	if got := pub.snapshot(); len(got) != len(want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
}

// TestBrokerGateWithoutPublisher pins the degraded mode: a station with
// no broker reports the missing publisher instead of panicking.
func TestBrokerGateWithoutPublisher(t *testing.T) {
	g := newBrokerGate(nil, nil)
	if err := g.publishActive(composeSource, state.Hazard{EventKey: "compose:4"}, 1); !errors.Is(err, errNoBroker) {
		t.Fatalf("err = %v, want errNoBroker", err)
	}
}

// TestBrokerGateKeepsTheVersionBarUnderTablePressure pins the reported
// P2: the table used to be replaced wholesale once it grew past its cap,
// which threw away the remembered versions — a retirement of version 2
// then lost to a publication of version 1. Pressure on the table must not
// touch a bar, and the table must still stay bounded. It runs in both
// modes: memory-only (the bar itself is kept) and ledger-backed (the bar
// may be evicted, because the ledger still answers for it).
func TestBrokerGateKeepsTheVersionBarUnderTablePressure(t *testing.T) {
	for _, mode := range []string{"memory", "ledger"} {
		t.Run(mode, func(t *testing.T) {
			testBrokerGateKeepsTheVersionBarUnderTablePressure(t, mode)
		})
	}
}

func testBrokerGateKeepsTheVersionBarUnderTablePressure(t *testing.T, mode string) {
	pub := &recordingPublisher{}
	var ledger *fakeLedger
	var g *brokerGate
	if mode == "ledger" {
		ledger = newFakeLedger()
		g = newBrokerGate(pub, ledger)
		ledger.record("compose:1", 2)
	} else {
		g = newBrokerGate(pub, nil)
	}
	h := state.Hazard{EventKey: "compose:1", Source: composeSource}

	// The message is retired at version 2.
	if err := g.retireActive(composeSource, h.EventKey, 2); err != nil {
		t.Fatal(err)
	}
	// Years of unversioned documents fill the table past its cap.
	for i := 0; i < gateKeyCap+64; i++ {
		suffix := fmt.Sprintf("info/emcom/emcom/net-%d/emcom", i)
		if err := g.publishInfo(suffix, []byte("{}"), 0); err != nil {
			t.Fatal(err)
		}
	}
	// The delayed publication of the retired version loses ...
	if err := g.publishActive(composeSource, h, 1); err != nil {
		t.Fatal(err)
	}
	// ... while the idempotent resync of the current version goes out.
	if err := g.publishActive(composeSource, h, 2); err != nil {
		t.Fatal(err)
	}
	want := []string{"retire:compose:1", "active:compose:1"}
	var calls []string
	for _, call := range pub.snapshot() {
		if strings.Contains(call, "compose:1") {
			calls = append(calls, call)
		}
	}
	if len(calls) != len(want) || calls[0] != want[0] || calls[1] != want[1] {
		t.Fatalf("calls for the message = %v, want %v (the retired version must not come back)", calls, want)
	}
	// The idle pool stays bounded even though the bars do not.
	g.mu.Lock()
	keys := len(g.keys)
	g.mu.Unlock()
	if keys > gateKeyCap {
		t.Fatalf("the gate kept %d keys, want at most %d", keys, gateKeyCap)
	}
}

// TestBrokerGateKeepsTheLockOfAnInFlightPublication pins the other half
// of the report: pressure on the table must not hand an in-flight
// publication's message a second lock, which would let two publications
// of it overlap — and the second one could then retire what the first was
// still publishing.
func TestBrokerGateKeepsTheLockOfAnInFlightPublication(t *testing.T) {
	pub := &recordingPublisher{}
	started, release := pub.holdNext()
	g := newBrokerGate(pub, nil)
	h := state.Hazard{EventKey: "compose:7", Source: composeSource}

	first := make(chan struct{})
	go func() {
		defer close(first)
		_ = g.publishActive(composeSource, h, 1)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the first publication never reached the broker")
	}

	// Table pressure while the publication is in flight. The publisher is
	// held, so the pressure is applied through the gate's own table —
	// exactly the path that used to replace it wholesale.
	for i := 0; i < gateKeyCap+64; i++ {
		g.release(g.key(fmt.Sprintf("info/emcom/emcom/pressure-%d/emcom", i)))
	}

	// A publication of the SAME message arriving now must meet the one in
	// flight on its lock instead of running beside it.
	second := make(chan struct{})
	go func() {
		defer close(second)
		_ = g.retireActive(composeSource, h.EventKey, 2)
	}()
	time.Sleep(50 * time.Millisecond) // let it reach the gate

	select {
	case <-second:
		t.Fatal("the second publication ran while the first one was in flight")
	default:
	}
	if got := pub.enteredSnapshot(); len(got) != 1 {
		t.Fatalf("publications in flight = %v, want only the first one", got)
	}
	release()
	<-first
	<-second
	want := []string{"active:compose:7", "retire:compose:7"}
	if got := pub.snapshot(); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("calls = %v, want %v", got, want)
	}
}

// TestBrokerGateBoundsTheVersionedTable pins the reported P2: entries that
// remember a version used to be kept forever, so a station publishing
// messages for years grew without bound (and every insertion at capacity
// scanned the whole table). With the durable ledger the gate may evict
// them, and the ledger still keeps a delayed publication of an older
// version out — the ordering rule must not depend on the table surviving.
func TestBrokerGateBoundsTheVersionedTable(t *testing.T) {
	pub := &recordingPublisher{}
	ledger := newFakeLedger()
	g := newBrokerGate(pub, ledger)

	// Long-running publication and retirement: every message carries its
	// own version, so nothing here is trivially evictable.
	over := gateKeyCap + gateEvictBatch
	for i := 0; i < over; i++ {
		key := fmt.Sprintf("compose:%d", i)
		h := state.Hazard{EventKey: key, Source: composeSource}
		ledger.record(key, 3)
		if err := g.retireActive(composeSource, key, 3); err != nil {
			t.Fatalf("retire %s: %v", key, err)
		}
		if err := g.publishActive(composeSource, h, 3); err != nil {
			t.Fatalf("publish %s: %v", key, err)
		}
	}

	g.mu.Lock()
	keys := len(g.keys)
	g.mu.Unlock()
	if keys > gateKeyCap {
		t.Fatalf("gate kept %d keys, want at most %d", keys, gateKeyCap)
	}
	if keys >= over {
		t.Fatalf("gate kept every one of the %d documents: memory is not bounded", over)
	}
	// The durable ledger is consulted once per (re)created bar, never per
	// insertion into a full table.
	if n := ledger.lookupCount(); n > over*2+64 {
		t.Errorf("durable version lookups = %d, want proportional to the %d documents", n, over)
	}

	// The oldest document's bar was evicted; a delayed publication built
	// from an older version still loses against the ledger.
	before := len(pub.snapshot())
	if err := g.publishActive(composeSource, state.Hazard{EventKey: "compose:0", Source: composeSource}, 2); err != nil {
		t.Fatal(err)
	}
	if got := len(pub.snapshot()); got != before {
		t.Fatalf("a superseded publication reached the broker: %v", pub.snapshot()[before:])
	}
	// Its current version may still be published (an idempotent resync).
	if err := g.publishActive(composeSource, state.Hazard{EventKey: "compose:0", Source: composeSource}, 3); err != nil {
		t.Fatal(err)
	}
	if got := len(pub.snapshot()); got != before+1 {
		t.Fatalf("the current version was dropped: %v", pub.snapshot()[before:])
	}
}

// TestBrokerGateUsesTheDurableLedger pins the wiring of the fallback: the
// production store must satisfy the ledger interface, and a publication
// arriving with no in-memory bar (an evicted entry, or a fresh process)
// must still lose to the version the store recorded — otherwise a delayed
// publication could resurrect a retired alarm.
func TestBrokerGateUsesTheDurableLedger(t *testing.T) {
	store, _, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ledger := lifecycleLedger(store)
	if ledger == nil {
		t.Fatal("the store does not satisfy gateLedger: the durable fallback would never engage")
	}
	instance, err := store.InstanceID(context.Background())
	if err != nil {
		t.Fatalf("instance id: %v", err)
	}
	// The panel retires "compose:1" at version 9 (the ledger records it
	// under the instance identity, which the empty publisher resolves).
	if err := store.RecordLifecycle(context.Background(), instance, "compose:1", 9, "expired"); err != nil {
		t.Fatalf("record lifecycle: %v", err)
	}

	pub := &recordingPublisher{}
	g := newBrokerGate(pub, ledger)
	h := state.Hazard{EventKey: "compose:1", Source: composeSource}
	if err := g.publishActive(composeSource, h, 5); err != nil {
		t.Fatal(err)
	}
	if got := pub.snapshot(); len(got) != 0 {
		t.Fatalf("a superseded publication reached the broker: %v", got)
	}
	// The current version still goes out (idempotent resync).
	if err := g.publishActive(composeSource, h, 9); err != nil {
		t.Fatal(err)
	}
	if got := pub.snapshot(); len(got) != 1 {
		t.Fatalf("calls = %v, want the current version only", got)
	}
}
