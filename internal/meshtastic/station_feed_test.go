package meshtastic

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// recordingStationSink plays the broker: it records the station-feed
// operations it was asked to perform, can refuse them, and can hold one call
// open so a test can order two of them.
type recordingStationSink struct {
	mu       sync.Mutex
	ops      []string
	attempts int
	failWith error
	hold     bool
	entered  chan struct{}
	release_ chan struct{}
}

func newRecordingStationSink() *recordingStationSink {
	return &recordingStationSink{}
}

func (s *recordingStationSink) sink(_ context.Context, topic string, retained bool, payload []byte) error {
	op := "publish:" + topic
	if retained && payload == nil {
		op = "remove:" + topic
	}
	s.mu.Lock()
	hold := s.hold
	s.hold = false
	entered, release := s.entered, s.release_
	err := s.failWith
	s.attempts++
	s.mu.Unlock()
	if hold {
		close(entered)
		<-release
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.ops = append(s.ops, op)
	s.mu.Unlock()
	return nil
}

func (s *recordingStationSink) setFail(err error) {
	s.mu.Lock()
	s.failWith = err
	s.mu.Unlock()
}

// holdNext makes the NEXT call stop inside the sink until release() runs.
func (s *recordingStationSink) holdNext() {
	s.mu.Lock()
	s.hold = true
	s.entered = make(chan struct{})
	s.release_ = make(chan struct{})
	s.mu.Unlock()
}

func (s *recordingStationSink) release() {
	s.mu.Lock()
	ch := s.release_
	s.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// attempts counts every call, including the ones the broker refused.
func (s *recordingStationSink) attemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func (s *recordingStationSink) operations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ops...)
}

func (s *recordingStationSink) removals() []string {
	var out []string
	for _, op := range s.operations() {
		if len(op) > 8 && op[:7] == "remove:" {
			out = append(out, op)
		}
	}
	return out
}

// newStationFeedHub builds a hub with the station feed attached (no session).
func newStationFeedHub(t *testing.T, sink *recordingStationSink) *Hub {
	t.Helper()
	h, err := NewHub(Config{NodeTTL: time.Hour}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	if sink != nil {
		h.SetStationSink(sink.sink)
	}
	return h
}

// quietFor makes one node look unheard for d — and its last removal attempt
// just as old, so the retry pace cannot hide the retry from the test.
func quietFor(h *Hub, id string, d time.Duration) {
	h.mu.Lock()
	if n := h.nodes[id]; n != nil {
		n.LastSeen = time.Now().Add(-d)
	}
	if tb := h.tombstones[id]; tb != nil {
		tb.quietSince = time.Now().Add(-d)
		tb.tried = time.Now().Add(-d)
	}
	h.mu.Unlock()
}

// heardNow makes one node look active again (the packet path does this from
// the radio).
func heardNow(h *Hub, id string, telemetry *Telemetry) {
	h.mu.Lock()
	if n := h.nodes[id]; n != nil {
		n.LastSeen = time.Now()
		n.Telemetry = telemetry
	}
	h.mu.Unlock()
}

// removalConfirmed reports whether the broker already took the removal.
func removalConfirmed(h *Hub, id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	tb := h.tombstones[id]
	return tb != nil && tb.confirmed
}

// removalPending reports whether the broker still owes the removal.
func removalPending(h *Hub, id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	tb := h.tombstones[id]
	return tb != nil && !tb.confirmed
}

// TestStationRemovalIsRetriedAfterABrokerOutage pins the first ticket case: the
// removal is recorded only once the BROKER CONFIRMED it, so an attempt the
// broker refused is repeated by a later pass instead of leaving a retained
// document behind forever.
func TestStationRemovalIsRetriedAfterABrokerOutage(t *testing.T) {
	sink := newRecordingStationSink()
	sink.setFail(errors.New("broker unreachable"))
	h := newStationFeedHub(t, sink)
	h.SeedNode("aaaa0001", "visitor", "VIS", 0, 0, time.Now().Add(-2*time.Hour), nil)

	h.expireNodes()
	if got := sink.attemptCount(); got != 1 {
		t.Fatalf("attempts = %d, want the removal to be tried", got)
	}
	if removalConfirmed(h, "aaaa0001") {
		t.Fatal("a removal the broker refused was recorded as done")
	}

	// The broker comes back: the next pass repeats it.
	sink.setFail(nil)
	quietFor(h, "aaaa0001", 2*time.Hour)
	h.expireNodes()
	if got := sink.attemptCount(); got != 2 {
		t.Fatalf("attempts = %d, want the retry", got)
	}
	if got := sink.removals(); len(got) != 1 {
		t.Fatalf("removals = %v, want the removal to have landed", got)
	}
	if !removalConfirmed(h, "aaaa0001") {
		t.Fatal("the confirmed removal was not recorded")
	}

	// A confirmed removal is never repeated while the node stays quiet.
	quietFor(h, "aaaa0001", 2*time.Hour)
	h.expireNodes()
	if got := sink.attemptCount(); got != 2 {
		t.Fatalf("attempts = %d, want no further attempts", got)
	}
}

// TestStationRemovalFollowsANodeThatComesBack pins the second ticket case:
// active -> quiet -> active -> quiet must remove the document TWICE, because a
// node that spoke again published its document again.
func TestStationRemovalFollowsANodeThatComesBack(t *testing.T) {
	sink := newRecordingStationSink()
	h := newStationFeedHub(t, sink)
	h.SeedNode("bbbb0002", "roamer", "ROA", 0, 0, time.Now(), nil)

	// Active: nothing to remove.
	h.expireNodes()
	if got := sink.removals(); len(got) != 0 {
		t.Fatalf("removals = %v, want none while the node is heard", got)
	}

	// Quiet: the document goes away.
	quietFor(h, "bbbb0002", 2*time.Hour)
	h.expireNodes()
	if got := sink.removals(); len(got) != 1 {
		t.Fatalf("removals = %v, want the removal", got)
	}
	if !removalConfirmed(h, "bbbb0002") {
		t.Fatal("the removal was not recorded")
	}

	// Back on the air: the removal state is void, the node is active again.
	heardNow(h, "bbbb0002", &Telemetry{Voltage: 4.1})
	h.expireNodes()
	if removalConfirmed(h, "bbbb0002") {
		t.Fatal("a node heard again is still marked as removed")
	}
	if got := sink.removals(); len(got) != 1 {
		t.Fatalf("removals = %v, want none for an active node", got)
	}

	// Quiet once more: the document is removed again (it was published again
	// when the node came back).
	quietFor(h, "bbbb0002", 2*time.Hour)
	h.expireNodes()
	if got := sink.removals(); len(got) != 2 {
		t.Fatalf("removals = %v, want the second removal", got)
	}
}

// TestStationFeedOrdersRemovalAndPositionPublish pins the ordering rule: a
// removal already on its way to the broker and a position publish for the same
// node never interleave, so the document of a node that just spoke is the one
// the broker keeps.
func TestStationFeedOrdersRemovalAndPositionPublish(t *testing.T) {
	sink := newRecordingStationSink()
	h := newStationFeedHub(t, sink)
	h.SeedNode("cccc0003", "busy", "BUS", 0, 0, time.Now().Add(-2*time.Hour), nil)

	sink.holdNext()
	removed := make(chan struct{})
	go func() {
		h.expireNodes()
		close(removed)
	}()
	select {
	case <-sink.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the removal never reached the broker")
	}

	// The node speaks while that removal is still in flight.
	heardNow(h, "cccc0003", &Telemetry{Voltage: 4.2})
	published := make(chan struct{})
	go func() {
		h.mu.Lock()
		n := *h.nodes["cccc0003"]
		h.mu.Unlock()
		h.publishStation(&n)
		close(published)
	}()
	// Either the publish is waiting for the removal (expected) or it got
	// through — the order recorded at the broker decides, below.
	select {
	case <-published:
	case <-time.After(300 * time.Millisecond):
	}
	sink.release()
	<-removed
	<-published

	ops := sink.operations()
	if len(ops) != 2 || ops[0] != "remove:meshtastic/stations/cccc0003" || ops[1] != "publish:meshtastic/stations/cccc0003" {
		t.Fatalf("broker operations = %v, want the removal first and the position publish last", ops)
	}
}

func inDirectory(h *Hub, id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nodes[id] != nil
}

// TestStationRemovalOutlivesTheDirectory pins the first ticket case: a node
// restored from the broker (an obsolete retained document) whose local entry is
// already past nodeDirRetention. The directory drops it, and the broker must
// still receive the removal — the pending work cannot live in a map that no
// longer holds the node.
func TestStationRemovalOutlivesTheDirectory(t *testing.T) {
	sink := newRecordingStationSink()
	h := newStationFeedHub(t, sink)
	h.SeedNode("dddd0004", "ghost", "GHO", 0, 0, time.Now().Add(-(nodeDirRetention + 24*time.Hour)), nil)

	h.expireNodes()

	if inDirectory(h, "dddd0004") {
		t.Fatal("a node unheard for over the retention stayed in the directory")
	}
	if got := sink.removals(); len(got) != 1 {
		t.Fatalf("removals = %v, want the removal of the dropped node's document", got)
	}
	if !removalConfirmed(h, "dddd0004") {
		t.Fatal("the removal was not recorded as confirmed")
	}
}

// TestStationRemovalSurvivesTheRetentionBoundary pins the second ticket case:
// the removal fails during a broker outage, and the node then ages past
// nodeDirRetention. The pending work must survive the directory prune and still
// reach the broker once it is back.
func TestStationRemovalSurvivesTheRetentionBoundary(t *testing.T) {
	sink := newRecordingStationSink()
	sink.setFail(errors.New("broker unreachable"))
	h := newStationFeedHub(t, sink)
	h.SeedNode("eeee0005", "leaver", "LEA", 0, 0, time.Now().Add(-2*time.Hour), nil)

	h.expireNodes()
	if got := sink.attemptCount(); got != 1 {
		t.Fatalf("attempts = %d, want the failed removal", got)
	}

	// Time passes with the broker still down: the node is now past the
	// retention as well, so the directory drops it.
	quietFor(h, "eeee0005", nodeDirRetention+time.Hour)
	h.expireNodes()
	if inDirectory(h, "eeee0005") {
		t.Fatal("the directory should have dropped the node")
	}
	if !removalPending(h, "eeee0005") {
		t.Fatal("the pending removal was lost with the directory entry")
	}

	// The broker comes back: the removal is still owed and finally lands.
	sink.setFail(nil)
	quietFor(h, "eeee0005", nodeDirRetention+time.Hour)
	h.expireNodes()
	if got := sink.removals(); len(got) != 1 {
		t.Fatalf("removals = %v, want the removal after the recovery", got)
	}
	if !removalConfirmed(h, "eeee0005") {
		t.Fatal("the removal was not confirmed")
	}
}

// TestStationRemovalSurvivesARestart pins the acceptance requirement: pending
// work is persisted with the directory, so a station that stops with an
// unconfirmed removal (the broker was down) still removes the document after it
// starts again.
func TestStationRemovalSurvivesARestart(t *testing.T) {
	store := &memNodeStore{}
	sink := newRecordingStationSink()
	sink.setFail(errors.New("broker unreachable"))
	h := newStationFeedHub(t, sink)
	h.SetNodeStore(store)
	h.SeedNode("ffff0006", "drifter", "DRI", 0, 0, time.Now().Add(-(nodeDirRetention + 24*time.Hour)), nil)

	h.expireNodes() // the attempt fails, the node leaves the directory
	if inDirectory(h, "ffff0006") {
		t.Fatal("the node should have been dropped")
	}
	// Force the directory save (the hub throttles it to once per 30s).
	h.mu.Lock()
	h.lastPersist = time.Time{}
	h.mu.Unlock()
	h.persistNodes()
	waitFor(t, "the pending removal to be persisted", func() bool {
		rows, err := store.LoadMeshtasticNodes(context.Background())
		if err != nil {
			return false
		}
		for _, row := range rows {
			if row.ID == "ffff0006" && row.PendingRemoval {
				return true
			}
		}
		return false
	})

	// A NEW hub on the same store, with a broker that works: the removal is
	// owed again and lands.
	back := newRecordingStationSink()
	restarted := newStationFeedHub(t, back)
	restarted.SetNodeStore(store)
	restarted.restoreNodes(context.Background())
	if !removalPending(restarted, "ffff0006") {
		t.Fatal("the pending removal did not survive the restart")
	}
	restarted.expireNodes()
	if got := back.removals(); len(got) != 1 {
		t.Fatalf("removals = %v, want the removal after the restart", got)
	}
	if !removalConfirmed(restarted, "ffff0006") {
		t.Fatal("the removal was not confirmed after the restart")
	}
}
