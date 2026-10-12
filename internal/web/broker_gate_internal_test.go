package web

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
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
	g := newBrokerGate(pub)
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
	g := newBrokerGate(pub)
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
	g := newBrokerGate(pub)
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
	g := newBrokerGate(nil)
	if err := g.publishActive(composeSource, state.Hazard{EventKey: "compose:4"}, 1); !errors.Is(err, errNoBroker) {
		t.Fatalf("err = %v, want errNoBroker", err)
	}
}
