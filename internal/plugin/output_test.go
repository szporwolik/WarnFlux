package plugin

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// memStore is an in-memory storage.EventStore used to drive output workers
// deterministically in tests.
type memStore struct {
	mu      sync.Mutex
	nextID  int64
	changes []storage.Change
	cursors map[string]int64
	events  map[string]*storage.StoredEvent
}

func newMemStore() *memStore {
	return &memStore{cursors: make(map[string]int64), events: make(map[string]*storage.StoredEvent)}
}

func (m *memStore) addChange(changeType core.ChangeType, event core.HazardEvent) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	id := m.nextID
	m.changes = append(m.changes, storage.Change{ID: id, ChangeType: changeType, Event: event})
	return id
}

func (m *memStore) Ingest(_ context.Context, event core.HazardEvent, _ string) (storage.Outcome, *storage.Change, error) {
	id := m.addChange(core.ChangeNew, event.Clone())
	return storage.OutcomeNew, &storage.Change{ID: id, ChangeType: core.ChangeNew, Event: event.Clone()}, nil
}

func (m *memStore) Expire(_ context.Context, _ time.Time) ([]storage.Change, error) {
	return nil, nil
}

func (m *memStore) PollChanges(_ context.Context, outputID string, limit int) ([]storage.Change, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cursor := m.cursors[outputID]
	var out []storage.Change
	for _, c := range m.changes {
		if c.ID > cursor && len(out) < limit {
			out = append(out, c)
		}
	}
	return out, nil
}

func (m *memStore) AckChanges(_ context.Context, outputID string, lastChangeID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lastChangeID > m.cursors[outputID] {
		m.cursors[outputID] = lastChangeID
	}
	return nil
}

func (m *memStore) SyncOutputs(_ context.Context, enabled []storage.OutputRef) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	enabledIDs := make(map[string]bool, len(enabled))
	for _, o := range enabled {
		enabledIDs[o.ID] = true
	}
	for id := range m.cursors {
		if !enabledIDs[id] {
			delete(m.cursors, id)
		}
	}
	for _, o := range enabled {
		if _, ok := m.cursors[o.ID]; !ok {
			m.cursors[o.ID] = 0
		}
	}
	return nil
}

func (m *memStore) CleanupChanges(_ context.Context, _ time.Time) (int64, error) { return 0, nil }
func (m *memStore) CleanupEvents(_ context.Context, _ time.Time) (int64, error)  { return 0, nil }
func (m *memStore) Get(_ context.Context, key string) (*storage.StoredEvent, error) {
	return nil, storage.ErrNotFound
}
func (m *memStore) Count(context.Context) (int, error) { return 0, nil }
func (m *memStore) PendingStats(context.Context) (int, time.Duration, error) {
	return 0, 0, nil
}
func (m *memStore) ListArchiveEvents(context.Context, time.Time, int, int) ([]storage.StoredEvent, error) {
	return nil, nil
}
func (m *memStore) CountArchiveEvents(context.Context, time.Time) (int, error) {
	return 0, nil
}
func (m *memStore) Close() error { return nil }

func (m *memStore) cursor(outputID string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cursors[outputID]
}

// recOutput records delivered changes; behavior driven by fields.
type recOutput struct {
	name string

	mu  sync.Mutex
	got []core.EventChange

	// handle, when set, overrides the default behavior entirely.
	handle func(ctx context.Context, change core.EventChange) error

	failFor int // -1 fails every call, N>0 fails the next N calls
	err     error
	panic   bool
	gate    chan struct{} // when non-nil, Handle blocks until closed (ignoring ctx)
	closed  bool

	invocations int
}

func (o *recOutput) Name() string { return o.name }

func (o *recOutput) Handle(ctx context.Context, change core.EventChange) error {
	o.mu.Lock()
	o.invocations++
	o.got = append(o.got, change)
	handler := o.handle
	fail := o.failFor != 0
	if fail && o.failFor > 0 {
		o.failFor--
	}
	panicMode := o.panic
	gate := o.gate
	o.mu.Unlock()

	if handler != nil {
		return handler(ctx, change)
	}
	if panicMode {
		panic("output boom")
	}
	if gate != nil {
		<-gate // intentionally ignores ctx
		return nil
	}
	if fail {
		return o.err
	}
	return nil
}

func (o *recOutput) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	return nil
}

func (o *recOutput) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.got)
}

func (o *recOutput) calls() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.invocations
}

func testOutputCfg(id string, timeout time.Duration, threshold int) config.Output {
	return config.Output{
		ID:      id,
		Type:    "test",
		Enabled: true,
		Runtime: config.OutputRuntime{
			Timeout:          timeout,
			FailureThreshold: threshold,
		},
	}
}

func sampleChange() core.EventChange {
	eff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lat, lon := 50.06, 19.94
	return core.EventChange{
		ID:   1,
		Type: core.ChangeNew,
		Event: core.HazardEvent{
			Source: "test", SourceID: "1", Event: "E",
			EffectiveAt: &eff, Latitude: &lat, Longitude: &lon,
			Areas: []string{"DE-NW"}, Status: core.StatusActive,
		},
	}
}

func TestOutputWorkerDeliversAndAcks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	store := newMemStore()
	id := store.addChange(core.ChangeNew, sampleChange().Event)

	out := &recOutput{name: "test"}
	tracker := newStatusTracker("out", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("out", time.Second, 3), out, store, testLogger(), tracker)
	w.pollInterval = 5 * time.Millisecond
	go w.run(ctx)

	waitFor(t, 2*time.Second, func() bool { return out.count() == 1 })
	if got := tracker.snapshot().State; got != StateRunning {
		t.Errorf("state = %v, want running", got)
	}
	if got := store.cursor("out"); got != id {
		t.Errorf("cursor = %d, want %d (acked only after success)", got, id)
	}

	cancel()
	select {
	case <-w.done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop")
	}
	if !out.closed {
		t.Error("Closer interface should have been invoked on shutdown")
	}
}

func TestOutputWorkerPanicRecovered(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemStore()
	store.addChange(core.ChangeNew, sampleChange().Event)

	out := &recOutput{name: "test", panic: true}
	tracker := newStatusTracker("out", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("out", time.Second, 3), out, store, testLogger(), tracker)
	w.pollInterval = 5 * time.Millisecond
	go w.run(ctx)

	waitFor(t, 2*time.Second, func() bool { return tracker.failures() >= 1 })
	st := tracker.snapshot()
	if !strings.Contains(st.LastError, "boom") {
		t.Errorf("LastError = %q, want panic value", st.LastError)
	}
	if got := store.cursor("out"); got != 0 {
		t.Errorf("cursor = %d, want 0 (nothing acked after panic)", got)
	}
}

func TestOutputWorkerSuspendsAfterThreshold(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemStore()
	store.addChange(core.ChangeNew, sampleChange().Event)
	store.addChange(core.ChangeNew, sampleChange().Event)

	out := &recOutput{name: "test", err: errors.New("destination down"), failFor: -1}
	tracker := newStatusTracker("out", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("out", time.Second, 2), out, store, testLogger(), tracker)
	w.pollInterval = 5 * time.Millisecond
	w.recoveryInterval = time.Hour // no recovery probes in this test
	go w.run(ctx)

	waitFor(t, 2*time.Second, func() bool { return tracker.snapshot().State == StateSuspended })

	// A new change must not be hammered while suspended.
	store.addChange(core.ChangeNew, sampleChange().Event)
	time.Sleep(30 * time.Millisecond)
	if out.count() > 2 {
		t.Errorf("deliveries = %d, want 2 (no hammering while suspended)", out.count())
	}
}

func TestOutputWorkerRecoversAfterProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemStore()
	store.addChange(core.ChangeNew, sampleChange().Event)
	store.addChange(core.ChangeNew, sampleChange().Event)

	out := &recOutput{name: "test", err: errors.New("temporary"), failFor: 2}
	tracker := newStatusTracker("out", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("out", time.Second, 2), out, store, testLogger(), tracker)
	w.pollInterval = 5 * time.Millisecond
	w.recoveryInterval = 5 * time.Millisecond
	go w.run(ctx)

	waitFor(t, 2*time.Second, func() bool { return tracker.snapshot().State == StateSuspended })

	// A third change becomes the recovery probe; success resets the counter.
	store.addChange(core.ChangeNew, sampleChange().Event)
	waitFor(t, 2*time.Second, func() bool {
		st := tracker.snapshot()
		return st.State == StateRunning && st.ConsecutiveFailures == 0
	})
	if out.count() < 3 {
		t.Errorf("deliveries = %d, want at least 3 (probe included)", out.count())
	}
}

// TestOutputWorkerWedgeSingleFlight verifies the M7 invariant: a handler
// that ignores its context forever is never invoked again — at most ONE
// contributor-code call is ever in flight.
func TestOutputWorkerWedgeSingleFlight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemStore()
	store.addChange(core.ChangeNew, sampleChange().Event)

	gate := make(chan struct{})
	out := &recOutput{name: "wedged", gate: gate}
	tracker := newStatusTracker("wedged", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("wedged", 20*time.Millisecond, 1), out, store, testLogger(), tracker)
	w.pollInterval = 5 * time.Millisecond
	w.recoveryInterval = 5 * time.Millisecond
	go w.run(ctx)

	waitFor(t, 2*time.Second, func() bool { return out.calls() == 1 })
	waitFor(t, 2*time.Second, func() bool {
		st := tracker.snapshot()
		return st.ConsecutiveFailures >= 1 // timeout was counted
	})

	// More changes arrive; the wedged plugin must not be invoked again.
	store.addChange(core.ChangeNew, sampleChange().Event)
	store.addChange(core.ChangeNew, sampleChange().Event)
	time.Sleep(50 * time.Millisecond) // several recovery intervals
	if got := out.calls(); got != 1 {
		t.Errorf("invocations = %d, want exactly 1 while the old call is stuck", got)
	}

	// Once the stuck call returns (successfully), delivery resumes.
	close(gate)
	waitFor(t, 2*time.Second, func() bool { return out.calls() >= 2 })
}

func TestOutputWorkerOneBrokenOneHealthy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemStore()
	store.addChange(core.ChangeNew, sampleChange().Event)
	store.addChange(core.ChangeNew, sampleChange().Event)

	broken := &recOutput{name: "broken", err: errors.New("down"), failFor: -1}
	brokenTracker := newStatusTracker("broken", "test", KindOutput)
	brokenWorker := newOutputWorker(testOutputCfg("broken", time.Second, 10), broken, store, testLogger(), brokenTracker)
	brokenWorker.pollInterval = 5 * time.Millisecond
	go brokenWorker.run(ctx)

	healthy := &recOutput{name: "healthy"}
	healthyTracker := newStatusTracker("healthy", "test", KindOutput)
	healthyWorker := newOutputWorker(testOutputCfg("healthy", time.Second, 3), healthy, store, testLogger(), healthyTracker)
	healthyWorker.pollInterval = 5 * time.Millisecond
	go healthyWorker.run(ctx)

	waitFor(t, 2*time.Second, func() bool { return healthy.count() == 2 })
	if got := store.cursor("healthy"); got != 2 {
		t.Errorf("healthy cursor = %d, want 2", got)
	}
	if got := healthyTracker.snapshot().State; got != StateRunning {
		t.Errorf("healthy state = %v", got)
	}
	if got := store.cursor("broken"); got != 0 {
		t.Errorf("broken cursor = %d, want 0 (nothing acked)", got)
	}
}

// TestOutputDeepCopyIsolation verifies M8/M26: mutations performed by one
// output never affect what another output sees.
func TestOutputDeepCopyIsolation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemStore()
	change := sampleChange()
	store.addChange(change.Type, change.Event)

	mutator := &recOutput{name: "mutator"}
	mutator.handle = func(_ context.Context, c core.EventChange) error {
		c.Event.Areas[0] = "PL-MA"
		*c.Event.Latitude = 99
		*c.Event.EffectiveAt = time.Now()
		return nil
	}

	observer := &recOutput{name: "observer"}
	observed := make(chan core.EventChange, 1)
	observer.handle = func(_ context.Context, c core.EventChange) error {
		select {
		case observed <- c:
		default:
		}
		return nil
	}

	trackerA := newStatusTracker("mutator", "test", KindOutput)
	wA := newOutputWorker(testOutputCfg("mutator", time.Second, 3), mutator, store, testLogger(), trackerA)
	wA.pollInterval = 5 * time.Millisecond
	go wA.run(ctx)

	trackerB := newStatusTracker("observer", "test", KindOutput)
	wB := newOutputWorker(testOutputCfg("observer", time.Second, 3), observer, store, testLogger(), trackerB)
	wB.pollInterval = 5 * time.Millisecond
	go wB.run(ctx)

	select {
	case seen := <-observed:
		if seen.Event.Areas[0] != "DE-NW" {
			t.Errorf("observer saw mutated area %q", seen.Event.Areas[0])
		}
		if *seen.Event.Latitude != 50.06 {
			t.Errorf("observer saw mutated latitude %v", *seen.Event.Latitude)
		}
		if !seen.Event.EffectiveAt.Equal(*change.Event.EffectiveAt) {
			t.Errorf("observer saw mutated EffectiveAt %v", seen.Event.EffectiveAt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("observer never received the change")
	}
}

// statusOut records PublishStatus calls.
type statusOut struct {
	mu     sync.Mutex
	status []Status
	handle func(ctx context.Context, c core.EventChange) error
}

func (s *statusOut) Name() string { return "status" }

func (s *statusOut) Handle(ctx context.Context, c core.EventChange) error {
	if s.handle != nil {
		return s.handle(ctx, c)
	}
	return nil
}

func (s *statusOut) PublishStatus(_ context.Context, status Status) error {
	s.mu.Lock()
	s.status = append(s.status, status)
	s.mu.Unlock()
	return nil
}

func (s *statusOut) StatusInterval() time.Duration { return 20 * time.Millisecond }

func (s *statusOut) statusCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.status)
}

func TestOutputWorkerPublishesStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemStore()
	out := &statusOut{}
	tracker := newStatusTracker("status", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("status", time.Second, 3), out, store, testLogger(), tracker)
	w.pollInterval = 5 * time.Millisecond
	w.health = func(context.Context) Status {
		return Status{Version: "test", PendingChanges: 7}
	}
	go w.run(ctx)

	waitFor(t, 2*time.Second, func() bool { return out.statusCount() >= 1 })
	out.mu.Lock()
	first := out.status[0]
	out.mu.Unlock()
	if first.Version != "test" || first.PendingChanges != 7 {
		t.Errorf("status = %+v", first)
	}
}

// failingStatusOut always fails status publication but delivers events fine.
type failingStatusOut struct {
	statusOut
	err error
}

func (o *failingStatusOut) PublishStatus(_ context.Context, _ Status) error { return o.err }

// TestOutputWorkerStatusFailureDoesNotAffectDelivery pins the REVIEW
// requirement: status heartbeat health is auxiliary. Constant status
// failures must never suspend the plugin, reset its delivery counter or
// otherwise block hazard event delivery.
func TestOutputWorkerStatusFailureDoesNotAffectDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemStore()
	store.addChange(core.ChangeNew, sampleChange().Event)

	out := &failingStatusOut{err: errors.New("status broken")}
	tracker := newStatusTracker("out", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("out", time.Second, 2), out, store, testLogger(), tracker)
	w.pollInterval = 5 * time.Millisecond
	w.health = func(context.Context) Status { return Status{Version: "test"} }
	go w.run(ctx)

	// The event must be delivered and acknowledged despite the heartbeat
	// failing on every interval.
	waitFor(t, 2*time.Second, func() bool { return store.cursor("out") == 1 })
	if st := tracker.snapshot(); st.State != StateRunning || st.ConsecutiveFailures != 0 {
		t.Errorf("status failures affected delivery health: %+v", st)
	}
}

// gateStatusOut blocks PublishStatus until its gate is closed, ignoring
// ctx entirely — a context-contract violation.
type gateStatusOut struct {
	statusOut
	gate        chan struct{}
	invocations int
}

func (o *gateStatusOut) PublishStatus(_ context.Context, _ Status) error {
	o.mu.Lock()
	o.invocations++
	o.mu.Unlock()
	<-o.gate
	return nil
}

func (o *gateStatusOut) statusInvocations() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.invocations
}

// TestOutputWorkerStatusGenerationBounded pins that status GENERATION (the
// health snapshot, including its database queries) runs under the same
// bounded context as the callback: a stalled database can degrade the
// snapshot but can never hang hazard delivery.
func TestOutputWorkerStatusGenerationBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemStore()
	out := &statusOut{}
	tracker := newStatusTracker("out", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("out", 50*time.Millisecond, 3), out, store, testLogger(), tracker)
	w.pollInterval = 5 * time.Millisecond
	// Generation blocks until its bounded context expires — simulating a
	// stalled database inside health().
	w.health = func(ctx context.Context) Status {
		<-ctx.Done()
		return Status{Version: "test"}
	}
	go w.run(ctx)

	// Hazard delivery must continue while status generation stalls on
	// every heartbeat.
	store.addChange(core.ChangeNew, sampleChange().Event)
	waitFor(t, 2*time.Second, func() bool { return store.cursor("out") == 1 })
	if st := tracker.snapshot(); st.State != StateRunning {
		t.Errorf("state = %v, want running", st.State)
	}
}

// TestOutputWorkerOneFailurePerInvocation pins the accounting invariant:
// one Handle invocation counts as exactly ONE delivery failure, even when
// it times out and later returns an error.
func TestOutputWorkerOneFailurePerInvocation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemStore()
	store.addChange(core.ChangeNew, sampleChange().Event)

	// Every invocation sleeps past the timeout and returns an error.
	out := &recOutput{name: "slow"}
	out.handle = func(context.Context, core.EventChange) error {
		time.Sleep(100 * time.Millisecond)
		return errors.New("late failure")
	}
	tracker := newStatusTracker("out", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("out", 30*time.Millisecond, 5), out, store, testLogger(), tracker)
	w.pollInterval = 5 * time.Millisecond
	go w.run(ctx)

	// Let at least two attempts happen; each must count exactly one
	// failure (timeout) and never ACK.
	waitFor(t, 2*time.Second, func() bool { return out.calls() >= 2 })
	waitFor(t, 2*time.Second, func() bool { return tracker.snapshot().ConsecutiveFailures == out.calls() })
	st := tracker.snapshot()
	if got := st.ConsecutiveFailures; got != out.calls() {
		t.Errorf("failures = %d for %d invocations, want exactly one per invocation", got, out.calls())
	}
	if got := store.cursor("out"); got != 0 {
		t.Errorf("cursor = %d, want 0 (no ACK for timed-out delivery)", got)
	}
}

// TestOutputWorkerStatusHangDisablesStatus pins the HIGH requirement: a
// StatusPublisher that ignores its timeout has status publishing disabled
// for the instance lifetime, hazard delivery continues, exactly one stuck
// status invocation exists and no new one is ever started.
func TestOutputWorkerStatusHangDisablesStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newMemStore()
	out := &gateStatusOut{gate: make(chan struct{})}
	tracker := newStatusTracker("out", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("out", 50*time.Millisecond, 3), out, store, testLogger(), tracker)
	w.pollInterval = 5 * time.Millisecond
	w.health = func(context.Context) Status { return Status{Version: "test"} }
	go w.run(ctx)

	// The stuck callback violates its timeout → status publishing disabled.
	waitFor(t, 2*time.Second, func() bool { return !w.statusEnabled() })

	// Hazard delivery must continue: the change is delivered and acked.
	store.addChange(core.ChangeNew, sampleChange().Event)
	waitFor(t, 2*time.Second, func() bool { return store.cursor("out") == 1 })

	// Exactly one stuck status invocation; no repeated goroutine leak and
	// no delivery suspension.
	if got := out.statusInvocations(); got != 1 {
		t.Errorf("status invocations = %d, want exactly 1", got)
	}
	if st := tracker.snapshot(); st.State != StateRunning {
		t.Errorf("state = %v, want running (status hang must not suspend delivery)", st.State)
	}

	// Release the abandoned goroutine for a clean test exit.
	close(out.gate)
}
