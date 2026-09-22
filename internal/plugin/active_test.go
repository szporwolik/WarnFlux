package plugin

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/ingest"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
)

// activeOutput is a fake output implementing the optional active-state
// capabilities (ActiveStateSeeder + ActiveStateRehydrater) plus normal
// hazard delivery.
type activeOutput struct {
	mu         sync.Mutex
	synced     []core.HazardEvent
	seedCalls  int
	rehydrates int
	seedErr    error
	hangOnce   bool // the next SeedActiveState hangs forever
	handles    int
}

func (o *activeOutput) Name() string { return "active" }

func (o *activeOutput) Handle(context.Context, core.EventChange) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.handles++
	return nil
}

func (o *activeOutput) SeedActiveState(ev core.HazardEvent) error {
	o.mu.Lock()
	o.seedCalls++
	if o.hangOnce {
		o.hangOnce = false
		o.mu.Unlock()
		select {} // never returns, never records
	}
	o.synced = append(o.synced, ev)
	err := o.seedErr
	o.mu.Unlock()
	return err
}

func (o *activeOutput) RehydrateActiveState() {
	o.mu.Lock()
	o.rehydrates++
	o.mu.Unlock()
}

func (o *activeOutput) syncedKeys() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]string, len(o.synced))
	for i, e := range o.synced {
		out[i] = e.Key()
	}
	return out
}

func (o *activeOutput) syncedCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.synced)
}

func (o *activeOutput) seedCallCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.seedCalls
}

func (o *activeOutput) rehydrateCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.rehydrates
}

func (o *activeOutput) handleCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.handles
}

// activeManager builds a manager wired to a real SQLite store + ingester
// and one fake active-capable output.
func activeManager(t *testing.T, out OutputPlugin, timeout time.Duration) (*Manager, *sqlite.Store, *ingest.Ingester) {
	t.Helper()
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	ing := ingest.NewIngester(store, testLogger())
	reg := NewRegistry()
	if err := reg.RegisterOutput("active-out", func(_ *yaml.Node) (OutputPlugin, error) {
		return out, nil
	}); err != nil {
		t.Fatalf("register output: %v", err)
	}
	m, err := NewManager(reg, nil, []config.Output{{
		ID: "active", Type: "active-out", Enabled: true,
		Runtime: config.OutputRuntime{Timeout: timeout, FailureThreshold: 5},
	}}, ing.Ingest, ing.Expire, store, managerOpts(), testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	for _, w := range m.outputs {
		w.pollInterval = 5 * time.Millisecond
		w.recoveryInterval = 10 * time.Millisecond
	}
	return m, store, ing
}

// ackAll drains and acknowledges the whole journal for an output, leaving
// ZERO pending changes (the scenario where only startup reconciliation can
// rebuild the active view). The cursor advances per batch: PollChanges
// re-returns the same batch until AckChanges moves the cursor forward.
func ackAll(t *testing.T, store *sqlite.Store, outputID string) {
	t.Helper()
	ctx := context.Background()
	for {
		batch, err := store.PollChanges(ctx, outputID, 256)
		if err != nil {
			t.Fatalf("PollChanges: %v", err)
		}
		if len(batch) == 0 {
			return
		}
		if err := store.AckChanges(ctx, outputID, batch[len(batch)-1].ID); err != nil {
			t.Fatalf("AckChanges: %v", err)
		}
	}
}

// TestActiveStateStartupSyncFromSQLite: with the journal fully acknowledged,
// the startup reconciliation synchronizes exactly the events whose CURRENT
// state is active — cancelled and expired events are never synchronized, and
// no historical journal replay happens.
func TestActiveStateStartupSyncFromSQLite(t *testing.T) {
	out := &activeOutput{}
	m, store, ing := activeManager(t, out, time.Second)
	ctx := context.Background()

	a := eventFor("hazard-a")
	b := eventFor("hazard-b")
	if _, _, err := ing.Ingest(ctx, a); err != nil {
		t.Fatalf("ingest A: %v", err)
	}
	if _, _, err := ing.Ingest(ctx, b); err != nil {
		t.Fatalf("ingest B: %v", err)
	}
	c := eventFor("hazard-c")
	if _, _, err := ing.Ingest(ctx, c); err != nil {
		t.Fatalf("ingest C: %v", err)
	}
	c.Status = core.StatusCancelled
	if _, _, err := ing.Ingest(ctx, c); err != nil {
		t.Fatalf("cancel C: %v", err)
	}
	d := eventFor("hazard-d")
	d.ExpiresAt = timePtr(time.Now().Add(time.Minute))
	if _, _, err := ing.Ingest(ctx, d); err != nil {
		t.Fatalf("ingest D: %v", err)
	}
	if _, err := ing.Expire(ctx, time.Now().Add(2*time.Minute)); err != nil {
		t.Fatalf("expire: %v", err)
	}
	ackAll(t, store, "active")

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(runCtx)
	}()
	waitFor(t, 3*time.Second, func() bool { return out.syncedCount() == 2 })
	// Exactly one background rehydration pass is triggered after seeding.
	waitFor(t, 3*time.Second, func() bool { return out.rehydrateCount() >= 1 })

	keys := out.syncedKeys()
	if len(keys) != 2 {
		t.Fatalf("synchronized keys = %v, want exactly A and B", keys)
	}
	seen := map[string]bool{}
	for _, k := range keys {
		seen[k] = true
	}
	if !seen[a.Key()] || !seen[b.Key()] {
		t.Errorf("synchronized keys = %v, want %q and %q", keys, a.Key(), b.Key())
	}
	if seen[c.Key()] || seen[d.Key()] {
		t.Errorf("cancelled/expired events were synchronized: %v", keys)
	}
	time.Sleep(50 * time.Millisecond)
	if got := out.handleCount(); got != 0 {
		t.Errorf("journal deliveries = %d, want 0 (nothing pending; active state does not depend on journal replay)", got)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestActiveStateStartupSyncPaginated: more active events than one startup
// page are all synchronized exactly once via bounded batches.
func TestActiveStateStartupSyncPaginated(t *testing.T) {
	out := &activeOutput{}
	m, store, ing := activeManager(t, out, time.Second)
	ctx := context.Background()

	const total = 600
	for i := 0; i < total; i++ {
		if _, _, err := ing.Ingest(ctx, eventFor(fmt.Sprintf("bulk-%04d", i))); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}
	ackAll(t, store, "active")

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(runCtx)
	}()
	waitFor(t, 10*time.Second, func() bool { return out.syncedCount() == total })
	// The rehydration pass starts after seeding completes; it is a separate
	// background step, so it needs its own wait (especially under -race).
	waitFor(t, 10*time.Second, func() bool { return out.rehydrateCount() >= 1 })

	keys := out.syncedKeys()
	if len(keys) != total {
		t.Fatalf("synchronized = %d, want %d", len(keys), total)
	}
	unique := map[string]bool{}
	for _, k := range keys {
		if unique[k] {
			t.Errorf("duplicate synchronization for %q", k)
		}
		unique[k] = true
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestActiveStateSyncFailureDoesNotBlockHazards: every seed fails (broken
// output) — all active events are still attempted, hazard delivery then
// proceeds normally, and nothing is suspended or counted.
func TestActiveStateSyncFailureDoesNotBlockHazards(t *testing.T) {
	out := &activeOutput{seedErr: errors.New("broker unavailable")}
	m, _, ing := activeManager(t, out, time.Second)
	ctx := context.Background()

	a := eventFor("sync-a")
	b := eventFor("sync-b")
	if _, _, err := ing.Ingest(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ing.Ingest(ctx, b); err != nil {
		t.Fatal(err)
	}
	// One hazard change stays pending in the journal (not acknowledged).
	// It is also ACTIVE current state, so the startup sync synchronizes
	// all three events even though every publish fails.
	h := eventFor("sync-hazard")
	if _, _, err := ing.Ingest(ctx, h); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(runCtx)
	}()
	waitFor(t, 3*time.Second, func() bool { return out.syncedCount() == 3 })
	waitFor(t, 3*time.Second, func() bool { return out.handleCount() >= 1 })

	statuses := m.Statuses()
	if len(statuses) != 1 || statuses[0].ConsecutiveFailures != 0 || statuses[0].State == StateSuspended {
		t.Errorf("output status after sync failures = %+v, want running with zero failures", statuses)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestActiveStateSyncHangDoesNotBlockHazards: a SeedActiveState that
// ignores its context forever is abandoned after its timeout, the seeding
// capability is disabled for the rest of the startup pass (exactly ONE
// seed invocation ever happens — no per-event goroutine leak), and hazard
// delivery still runs.
func TestActiveStateSyncHangDoesNotBlockHazards(t *testing.T) {
	out := &activeOutput{hangOnce: true}
	m, _, ing := activeManager(t, out, 50*time.Millisecond)
	ctx := context.Background()

	a := eventFor("hang-a")
	b := eventFor("hang-b")
	if _, _, err := ing.Ingest(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ing.Ingest(ctx, b); err != nil {
		t.Fatal(err)
	}
	h := eventFor("hang-hazard")
	if _, _, err := ing.Ingest(ctx, h); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(runCtx)
	}()
	// The hung seed is abandoned and the capability disabled: exactly one
	// invocation ever starts, no further events are seeded.
	waitFor(t, 3*time.Second, func() bool { return out.seedCallCount() == 1 })
	// Hazard delivery proceeds regardless.
	waitFor(t, 3*time.Second, func() bool { return out.handleCount() >= 1 })
	time.Sleep(100 * time.Millisecond)
	if got := out.seedCallCount(); got != 1 {
		t.Errorf("seed invocations = %d, want exactly 1 (capability disabled after the timeout)", got)
	}
	if got := out.syncedCount(); got != 0 {
		t.Errorf("seeded events = %d, want 0 (the hung invocation never recorded)", got)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop with an abandoned active-state goroutine")
	}
}

// TestActiveStateSeedingDoesNotDelayHazards: hundreds of active events with
// an unreachable broker must NOT produce a per-event serial network wait
// before the durable journal worker becomes operational — the seed is local
// and the rehydration is one background pass.
func TestActiveStateSeedingDoesNotDelayHazards(t *testing.T) {
	out := &activeOutput{}
	m, _, ing := activeManager(t, out, 10*time.Second)
	ctx := context.Background()

	const total = 600
	for i := 0; i < total; i++ {
		if _, _, err := ing.Ingest(ctx, eventFor(fmt.Sprintf("fast-%04d", i))); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}
	// One hazard change stays pending so the journal has work to do.
	if _, _, err := ing.Ingest(ctx, eventFor("fast-hazard")); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(runCtx)
	}()
	// Strict bound: the journal worker becomes operational far below the
	// 600 × 10s serial-wait worst case.
	waitFor(t, 2*time.Second, func() bool { return out.handleCount() >= 1 })
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Errorf("hazard delivery took %v, startup active seeding must not delay it", elapsed)
	}
	// The still-pending hazard is also active current state, so seeding
	// covers total+1 events.
	waitFor(t, 3*time.Second, func() bool { return out.syncedCount() == total+1 })
	if got := out.rehydrateCount(); got < 1 {
		t.Errorf("rehydration triggered %d times, want at least one background pass", got)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

func timePtr(t time.Time) *time.Time { return &t }

// captureSource records the emitter it receives (to exercise the emitter
// capabilities outside a source plugin's normal flow).
type captureSource struct {
	mu   sync.Mutex
	emit Emitter
}

func (s *captureSource) Name() string { return "capture" }

func (s *captureSource) Run(ctx context.Context, emit Emitter) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()
	<-ctx.Done()
	return nil
}

func (s *captureSource) emitter() Emitter {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.emit
}

// TestSourceEmitterListSourceActiveEvents proves the optional
// SourceActiveEventReader capability end to end: the per-source emitter
// pages the authoritative SQLite active state and filters by source.
func TestSourceEmitterListSourceActiveEvents(t *testing.T) {
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	ing := ingest.NewIngester(store, testLogger())

	capture := &captureSource{}
	reg := NewRegistry()
	if err := reg.RegisterSource("capture", func(_ *yaml.Node) (SourcePlugin, error) {
		return capture, nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	m, err := NewManager(reg, []config.Source{{
		ID: "src", Type: "capture", Enabled: true,
		Runtime: config.SourceRuntime{Restart: true, ShutdownTimeout: 50 * time.Millisecond},
	}}, nil, ing.Ingest, ing.Expire, store, managerOpts(), testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	waitFor(t, 3*time.Second, func() bool { return capture.emitter() != nil })

	one := eventFor("cap-one")
	two := eventFor("cap-two")
	other := eventFor("cap-other")
	other.Source = "other-src"
	other.SourceID = "x"
	if _, _, err := ing.Ingest(ctx, one); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ing.Ingest(ctx, two); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ing.Ingest(ctx, other); err != nil {
		t.Fatal(err)
	}
	cancelled := eventFor("cap-cancelled")
	if _, _, err := ing.Ingest(ctx, cancelled); err != nil {
		t.Fatal(err)
	}
	cancelled.Status = core.StatusCancelled
	if _, _, err := ing.Ingest(ctx, cancelled); err != nil {
		t.Fatal(err)
	}

	reader, ok := capture.emitter().(SourceActiveEventReader)
	if !ok {
		t.Fatal("emitter does not implement SourceActiveEventReader")
	}
	got, err := reader.ListSourceActiveEvents(ctx, "demo")
	if err != nil {
		t.Fatalf("ListSourceActiveEvents: %v", err)
	}
	keys := map[string]bool{}
	for _, ev := range got {
		keys[ev.Key()] = true
		if ev.Source != "demo" {
			t.Errorf("event %q leaked from another source", ev.Key())
		}
	}
	if !keys[one.Key()] || !keys[two.Key()] {
		t.Errorf("keys = %v, want %q and %q", keys, one.Key(), two.Key())
	}
	if keys[cancelled.Key()] {
		t.Errorf("cancelled event %q listed as active", cancelled.Key())
	}
	if keys[other.Key()] {
		t.Errorf("event %q from another source leaked in", other.Key())
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}
