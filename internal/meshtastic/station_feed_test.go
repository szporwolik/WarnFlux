package meshtastic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	pb "github.com/kabili207/meshtastic-go/core/proto"

	"github.com/szporwolik/WarnFlux/internal/storage"
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

// newStationFeedHub builds a hub with the station feed attached and its worker
// running (no radio session): the feed publishes on its own goroutine, exactly
// as it does next to a live session.
func newStationFeedHub(t *testing.T, sink *recordingStationSink) *Hub {
	t.Helper()
	h, err := NewHub(Config{NodeTTL: time.Hour}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	if sink != nil {
		h.SetStationSink(sink.sink)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go h.runStationFeed(ctx)
	return h
}

// TestStationFeedWithoutAQueuePublishesInline pins the fallback for a hub that
// was assembled without the constructor (a bare literal: an embedding, or a
// test): there is no queue to hand the work to, so the operations have to go
// out on the calling goroutine instead of being dropped.
func TestStationFeedWithoutAQueuePublishesInline(t *testing.T) {
	sink := newRecordingStationSink()
	h := &Hub{cfg: Config{NodeTTL: time.Minute}, nodes: map[string]*Node{
		"aaaa0009": {ID: "aaaa0009", Name: "visitor", Short: "VIS", LastSeen: time.Now().Add(-2 * time.Minute)},
	}}
	h.SetStationSink(sink.sink)
	if h.feedQueue != nil {
		t.Fatal("this hub was built without a queue on purpose")
	}
	h.mu.Lock()
	n := h.nodes["aaaa0009"]
	h.mu.Unlock()
	h.publishStation(n)
	// No waitFor here on purpose: with no worker both operations are already
	// done by the time the calls return.
	h.expireNodes()
	ops := sink.operations()
	if len(ops) != 2 {
		t.Fatalf("operations = %v, want a document and then a removal", ops)
	}
	if ops[0] != "publish:meshtastic/stations/aaaa0009" || ops[1] != "remove:meshtastic/stations/aaaa0009" {
		t.Fatalf("operations = %v, want publish then remove for the same node", ops)
	}
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
	waitFor(t, "the removal attempt", func() bool { return sink.attemptCount() == 1 })
	if removalConfirmed(h, "aaaa0001") {
		t.Fatal("a removal the broker refused was recorded as done")
	}

	// The broker comes back: the next pass repeats it.
	sink.setFail(nil)
	quietFor(h, "aaaa0001", 2*time.Hour)
	h.expireNodes()
	waitFor(t, "the retried removal", func() bool { return len(sink.removals()) == 1 })
	if got := sink.attemptCount(); got < 2 {
		t.Fatalf("attempts = %d, want the retry", got)
	}
	if !removalConfirmed(h, "aaaa0001") {
		t.Fatal("the confirmed removal was not recorded")
	}

	// A confirmed removal is never repeated while the node stays quiet.
	quietFor(h, "aaaa0001", 2*time.Hour)
	h.expireNodes()
	time.Sleep(50 * time.Millisecond) // the worker has nothing left to do
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
	waitFor(t, "the removal", func() bool { return len(sink.removals()) == 1 })
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
	waitFor(t, "the second removal", func() bool { return len(sink.removals()) == 2 })
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
	// Both operations are handed to the feed worker; wait until it published
	// them (it was parked in the removal).
	waitFor(t, "both operations at the broker", func() bool { return len(sink.operations()) == 2 })

	ops := sink.operations()
	if len(ops) != 2 || ops[0] != "remove:meshtastic/stations/cccc0003" || ops[1] != "publish:meshtastic/stations/cccc0003" {
		t.Fatalf("broker operations = %v, want the removal first and the position publish last", ops)
	}
}

// oldNodeID builds a valid 8-hex node id for a batch of historical nodes
// (SeedNode ignores anything that is not exactly 8 hex digits).
func oldNodeID(n int) string { return fmt.Sprintf("%08x", 0x10000000+n) }

func directorySize(h *Hub) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.nodes)
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
	waitFor(t, "the removal of the dropped node's document", func() bool { return len(sink.removals()) == 1 })
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
	waitFor(t, "the failed removal", func() bool { return sink.attemptCount() == 1 })

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
	waitFor(t, "the removal after the recovery", func() bool { return len(sink.removals()) == 1 })
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
	waitFor(t, "the removal after the restart", func() bool { return len(back.removals()) == 1 })
	if !removalConfirmed(restarted, "ffff0006") {
		t.Fatal("the removal was not confirmed after the restart")
	}
}

// TestStationRemovalDoesNotOutliveTheNodesReturn pins the acceptance case of the
// ticket: a removal that never reached the broker, a node that came back (its
// document published again), a directory save, and a restart. The pending flag
// is still set when the save happens — the node was quiet only moments before —
// so the restored removal must NOT be applied to the fresh activity: the
// document of an active node is never removed.
func TestStationRemovalDoesNotOutliveTheNodesReturn(t *testing.T) {
	store := &memNodeStore{}
	down := newRecordingStationSink()
	down.setFail(errors.New("broker unreachable"))
	h := newStationFeedHub(t, down)
	h.SetNodeStore(store)
	h.SeedNode("aaaa0007", "returner", "RET", 0, 0, time.Now().Add(-2*time.Hour), nil)

	// Quiet: the removal is queued and fails (the broker is down).
	h.expireNodes()
	if !removalPending(h, "aaaa0007") {
		t.Fatal("the removal was not queued")
	}

	// The node is heard again — that packet published its document again — and
	// the directory is saved BEFORE the next expire pass could void the entry
	// (the tick persists right after expiring, and a packet can land in
	// between).
	heardNow(h, "aaaa0007", &Telemetry{Voltage: 4.2})
	h.mu.Lock()
	h.lastPersist = time.Time{}
	h.mu.Unlock()
	h.persistNodes()
	waitFor(t, "the directory save", func() bool {
		rows, err := store.LoadMeshtasticNodes(context.Background())
		return err == nil && len(rows) > 0
	})

	// A restart on the same store, with a broker that works.
	back := newRecordingStationSink()
	restarted := newStationFeedHub(t, back)
	restarted.SetNodeStore(store)
	restarted.restoreNodes(context.Background())
	restarted.expireNodes()

	if got := back.operations(); len(got) != 0 {
		t.Fatalf("broker operations = %v, want none (the node is active, its document stays)", got)
	}
	if queued := len(restarted.feedQueue); queued != 0 {
		t.Fatalf("queued removals = %d, want none (nothing was owed)", queued)
	}
	if !inDirectory(restarted, "aaaa0007") {
		t.Error("the active node is missing from the directory")
	}
	if removalPending(restarted, "aaaa0007") || removalConfirmed(restarted, "aaaa0007") {
		t.Error("a removal was carried over from activity that came before the node's return")
	}
}

// TestStationRemovalBacklogIsBounded pins the bound: a broker that never
// takes the removals must not let the station grow without limit. Batches of
// quiet nodes keep arriving; the ledger stops at its bound and the rest WAIT
// (deferred, not lost) — and once the broker answers again, every waiting
// removal still goes out, because what waits is the node directory itself.
func TestStationRemovalBacklogIsBounded(t *testing.T) {
	sink := newRecordingStationSink()
	sink.setFail(errors.New("broker unreachable"))
	h := newStationFeedHub(t, sink)
	h.pendingRemovalsMax = 32

	const batches, perBatch = 6, 20
	seeded := 0
	for batch := 0; batch < batches; batch++ {
		for i := 0; i < perBatch; i++ {
			seeded++
			// Quiet, but inside retention: the node stays in the directory, so
			// its removal waits there instead of being lost.
			h.SeedNode(oldNodeID(seeded), "old", "", 0, 0, time.Now().Add(-2*time.Hour), nil)
		}
		h.expireNodes()
		pending, waiting, _ := h.StationRemovals()
		if pending > 32 {
			t.Fatalf("backlog = %d after batch %d, want at most the bound 32", pending, batch+1)
		}
		if pending+waiting != seeded {
			t.Fatalf("pending+waiting = %d+%d after batch %d, want all %d accounted for",
				pending, waiting, batch+1, seeded)
		}
	}

	pending, waiting, dropped := h.StationRemovals()
	if pending != 32 || waiting != seeded-32 || dropped != 0 {
		t.Fatalf("pending/waiting/dropped = %d/%d/%d, want 32/%d/0 (nothing abandoned)",
			pending, waiting, dropped, seeded-32)
	}
	snap := h.Snapshot()
	if snap.PendingRemovals != pending || snap.RemovalsWaiting != waiting || snap.RemovalsDropped != 0 {
		t.Fatalf("snapshot = %d/%d/%d, want the same numbers as the metric (%d/%d/0)",
			snap.PendingRemovals, snap.RemovalsWaiting, snap.RemovalsDropped, pending, waiting)
	}

	// The broker comes back: the ledger drains, and every deferred removal is
	// offered again — the waiting nodes are not forgotten.
	sink.setFail(nil)
	for pass := 0; pass < 40; pass++ {
		ageRemovalAttempts(h)
		h.expireNodes()
		if p, w, _ := h.StationRemovals(); p == 0 && w == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if p, w, d := h.StationRemovals(); p != 0 || w != 0 || d != 0 {
		t.Fatalf("after the broker returned: pending/waiting/dropped = %d/%d/%d, want 0/0/0", p, w, d)
	}
}

// TestStationRemovalAConfirmedOneIsNotQueuedAgain pins the fix for the counter
// the panel showed ("MQTT removals: 310"): a removal the broker CONFIRMED is a
// fact about the node's activity version, so it survives a restart and is never
// queued again while the node stays quiet — no repeated publishes, no repeated
// overflow counting.
func TestStationRemovalAConfirmedOneIsNotQueuedAgain(t *testing.T) {
	sink := newRecordingStationSink()
	h := newStationFeedHub(t, sink)
	store := &memNodeStore{}
	h.SetNodeStore(store)

	const nodes = 40
	for i := 1; i <= nodes; i++ {
		h.SeedNode(oldNodeID(i), "quiet", "", 0, 0, time.Now().Add(-2*time.Hour), nil)
	}
	h.expireNodes()
	waitFor(t, "every removal to reach the broker", func() bool { return len(sink.removals()) == nodes })

	// The next pass owes nothing: the documents are already gone.
	h.expireNodes()
	if got := len(sink.removals()); got != nodes {
		t.Fatalf("removals = %d after a second pass, want no new ones", got)
	}
	if p, w, d := h.StationRemovals(); p != 0 || w != 0 || d != 0 {
		t.Fatalf("pending/waiting/dropped = %d/%d/%d, want 0/0/0", p, w, d)
	}

	// A restart on the same store: the confirmed removals are remembered (the
	// row carries the version), so the station does NOT re-queue them.
	h.persistNodes()
	waitFor(t, "the directory save", func() bool {
		rows, err := store.LoadMeshtasticNodes(context.Background())
		return err == nil && len(rows) == nodes
	})
	restarted := newStationFeedHub(t, sink)
	restarted.SetNodeStore(store)
	restarted.restoreNodes(context.Background())
	restarted.expireNodes()
	time.Sleep(50 * time.Millisecond)
	if got := len(sink.removals()); got != nodes {
		t.Fatalf("removals = %d after a restart, want the confirmed ones NOT queued again", got)
	}
	if p, w, d := restarted.StationRemovals(); p != 0 || w != 0 || d != 0 {
		t.Fatalf("restarted pending/waiting/dropped = %d/%d/%d, want 0/0/0", p, w, d)
	}

	// A node heard again is a NEW version: its document is published again and
	// the next quiet period owes a fresh removal.
	heardNow(restarted, oldNodeID(1), nil)
	restarted.expireNodes()
	quietFor(restarted, oldNodeID(1), 2*time.Hour)
	restarted.expireNodes()
	waitFor(t, "the fresh removal of the node that came back", func() bool {
		return len(sink.removals()) == nodes+1
	})
}

// TestStationRemovalNeedsAStationFeed: an installation without a station feed
// never published a document, so it must not build a removal backlog for nodes
// that merely went quiet.
func TestStationRemovalNeedsAStationFeed(t *testing.T) {
	h := newStationFeedHub(t, nil)
	for i := 1; i <= 50; i++ {
		h.SeedNode(oldNodeID(i), "old", "", 0, 0, time.Now().Add(-(nodeDirRetention + time.Hour)), nil)
	}
	h.expireNodes()

	if pending, waiting, dropped := h.StationRemovals(); pending != 0 || waiting != 0 || dropped != 0 {
		t.Fatalf("backlog = %d/%d/%d without a station feed, want none", pending, waiting, dropped)
	}
	if got := directorySize(h); got != 0 {
		t.Fatalf("directory = %d nodes, want none past retention", got)
	}
}

// TestStationRemovalBacklogIsBoundedOnLoad pins the bounded READ: a database
// written with a longer backlog (an older station, or one that ran with a
// bigger bound) does not get loaded into memory in full — the excess is counted
// and reported.
func TestStationRemovalBacklogIsBoundedOnLoad(t *testing.T) {
	rows := make([]storage.MeshtasticNode, 0, maxPendingStationRemovals+25)
	for i := 0; i < maxPendingStationRemovals+25; i++ {
		rows = append(rows, storage.MeshtasticNode{
			ID:             oldNodeID(i),
			LastSeen:       time.Now().Add(-2 * time.Hour),
			PendingRemoval: true,
			PendingSince:   time.Now().Add(-2 * time.Hour),
		})
	}
	store := &memNodeStore{nodes: rows}
	h := newStationFeedHub(t, newRecordingStationSink())
	h.SetNodeStore(store)
	h.restoreNodes(context.Background())

	if pending, _, dropped := h.StationRemovals(); pending != maxPendingStationRemovals || dropped != 25 {
		t.Fatalf("restored backlog = %d/%d, want %d pending and 25 dropped",
			pending, dropped, maxPendingStationRemovals)
	}
}

// TestSlowStationFeedDoesNotBlockTheSession pins the acceptance case: a broker
// that keeps the connection open but never confirms publications must not stop
// the radio session. A whole batch of removals is queued while the publisher is
// parked, and the heartbeat/watchdog loop keeps running — it even keeps
// processing packets — and shutting the station down does not wait for the
// publisher either.
func TestSlowStationFeedDoesNotBlockTheSession(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	// A publisher that ignores the context entirely (exactly what the reported
	// broker does): it holds the call until the test lets it go.
	sink := func(_ context.Context, _ string, _ bool, _ []byte) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return nil
	}
	radio := newTestRadio(t, Config{Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour},
		testRadioOpts{
			setup: func(h *Hub) {
				h.SetStationSink(sink)
				// A batch worth queueing: every node is past the directory
				// retention, so one pass hands all of them to the feed.
				for i := 1; i <= 20; i++ {
					h.SeedNode(oldNodeID(i), "old", "", 0, 0,
						time.Now().Add(-(nodeDirRetention + time.Hour)), nil)
				}
			},
		})
	radio.waitConnected(t)
	t.Cleanup(func() { close(release) })

	// The pass returns while the broker is still parked.
	done := make(chan struct{})
	go func() {
		radio.hub.expireNodes()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("expireNodes waited for the broker")
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the feed worker never started publishing")
	}
	if queued := len(radio.hub.feedQueue); queued == 0 {
		t.Fatal("the batch was not handed to the feed worker")
	}

	// The session loop is alive: it still consumes packets from the radio.
	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xef010203,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_TELEMETRY_APP},
		},
	})
	waitFor(t, "the session to process a packet while the broker is parked", func() bool {
		for _, n := range radio.hub.Snapshot().Nodes {
			if n.ID == "ef010203" && len(n.Sends) > 0 {
				return true
			}
		}
		return false
	})
	if !radio.hub.Connected() {
		t.Fatal("the radio session died behind the slow broker")
	}

	// And the station shuts down without waiting for the publication.
	radio.stopHub(t)
}

// ageRemovalAttempts pretends the retry pace elapsed for every pending removal,
// so a test can drive the retry without waiting a minute.
func ageRemovalAttempts(h *Hub) {
	h.mu.Lock()
	for _, tb := range h.tombstones {
		if !tb.tried.IsZero() {
			tb.tried = time.Now().Add(-time.Hour)
		}
	}
	h.mu.Unlock()
}

// TestStationRemovalBacklogDoesNotChurn pins the acceptance case of the ticket:
// with more quiet nodes than the backlog holds, the passes that follow (no new
// data at all) must not keep re-queueing the same nodes — the refusals are
// remembered for the activity version they refer to, so the drop counter stops
// growing, the backlog stays put and the retry pace is respected instead of
// being re-armed by a re-created entry.
func TestStationRemovalBacklogDoesNotChurn(t *testing.T) {
	sink := newRecordingStationSink()
	sink.setFail(errors.New("broker unreachable"))
	h := newStationFeedHub(t, sink)

	// Quiet nodes that STAY in the directory: they are past the node TTL but
	// well inside the retention, which is exactly what keeps the next pass
	// seeing them.
	const extra = 100
	for i := 1; i <= maxPendingStationRemovals+extra; i++ {
		h.SeedNode(oldNodeID(i), "quiet", "", 0, 0, time.Now().Add(-2*time.Hour), nil)
	}

	h.expireNodes()
	pending, waiting, dropped := h.StationRemovals()
	if pending != maxPendingStationRemovals {
		t.Fatalf("backlog = %d, want the bound %d", pending, maxPendingStationRemovals)
	}
	if waiting != extra || dropped != 0 {
		t.Fatalf("waiting/dropped = %d/%d, want %d waiting and nothing dropped", waiting, dropped, extra)
	}
	// Wait until the WORKER worked off the batch: every pending removal was
	// handed to it, so the attempt count stops there.
	waitFor(t, "the batch to reach the broker", func() bool {
		return sink.attemptCount() == maxPendingStationRemovals
	})
	attempts := sink.attemptCount()

	// The next passes have no new data: nothing may be refused again, and the
	// pace must hold (no attempt may run before it elapsed).
	h.expireNodes()
	h.expireNodes()
	if pending, waiting, dropped = h.StationRemovals(); waiting != extra || dropped != 0 {
		t.Fatalf("waiting/dropped = %d/%d after two more passes, want %d waiting and nothing dropped",
			waiting, dropped, extra)
	}
	if pending != maxPendingStationRemovals {
		t.Fatalf("backlog = %d after two more passes, want the bound", pending)
	}
	time.Sleep(50 * time.Millisecond)
	if got := sink.attemptCount(); got != attempts {
		t.Fatalf("attempts = %d, want %d (the retry pace must hold)", got, attempts)
	}

	// Once the pace elapsed the backlog is retried — the SAME work, counted
	// once, and still no new refusals.
	ageRemovalAttempts(h)
	h.expireNodes()
	waitFor(t, "the retry after the pace", func() bool { return sink.attemptCount() > attempts })
	if _, waiting, dropped = h.StationRemovals(); waiting != extra || dropped != 0 {
		t.Fatalf("waiting/dropped = %d/%d after the retry, want %d waiting and nothing dropped",
			waiting, dropped, extra)
	}
}

// TestStationRemovalWaitingEndsWhenTheNodeReturns: a node due a removal that
// had no room WAITS in the directory; the moment it is heard again the
// document is fresh and nothing is owed — and once it goes quiet a second time
// the removal is offered again.
func TestStationRemovalWaitingEndsWhenTheNodeReturns(t *testing.T) {
	sink := newRecordingStationSink()
	sink.setFail(errors.New("broker unreachable"))
	h := newStationFeedHub(t, sink)
	h.pendingRemovalsMax = 4

	for i := 1; i <= 4; i++ {
		h.SeedNode(oldNodeID(i), "quiet", "", 0, 0, time.Now().Add(-2*time.Hour), nil)
	}
	waited := oldNodeID(5)
	h.SeedNode(waited, "quiet", "", 0, 0, time.Now().Add(-2*time.Hour), nil)
	h.expireNodes()
	if _, waiting, dropped := h.StationRemovals(); waiting != 1 || dropped != 0 {
		t.Fatalf("waiting/dropped = %d/%d, want the single waited removal and nothing dropped", waiting, dropped)
	}

	// The node is heard again: its document is fresh, so nothing is owed.
	heardNow(h, waited, &Telemetry{Voltage: 4.0})
	h.expireNodes()
	if _, waiting, _ := h.StationRemovals(); waiting != 0 {
		t.Fatalf("waiting = %d after the node returned, want none", waiting)
	}

	// It goes quiet a second time: the fresh version is owed (and still waits
	// for room, because the broker never confirmed the batch before it).
	quietFor(h, waited, 2*time.Hour)
	h.expireNodes()
	if _, waiting, dropped := h.StationRemovals(); waiting != 1 || dropped != 0 {
		t.Fatalf("waiting/dropped = %d/%d after the second quiet period, want 1/0", waiting, dropped)
	}
}
