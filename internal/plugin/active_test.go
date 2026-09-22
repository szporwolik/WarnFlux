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

// activeOutput is a fake output implementing the optional
// ActiveStatePublisher capability plus normal hazard delivery.
type activeOutput struct {
	mu       sync.Mutex
	synced   []core.HazardEvent
	syncErr  error
	hangOnce bool // the next PublishActiveState hangs forever (ignores ctx)
	handles  int
}

func (o *activeOutput) Name() string { return "active" }

func (o *activeOutput) Handle(context.Context, core.EventChange) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.handles++
	return nil
}

func (o *activeOutput) PublishActiveState(_ context.Context, ev core.HazardEvent) error {
	o.mu.Lock()
	if o.hangOnce {
		o.hangOnce = false
		o.mu.Unlock()
		select {} // never returns, never records
	}
	o.synced = append(o.synced, ev)
	err := o.syncErr
	o.mu.Unlock()
	return err
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

// TestActiveStateSyncFailureDoesNotBlockHazards: every sync publish fails
// (broker down) — all active events are still attempted, hazard delivery
// then proceeds normally, and nothing is suspended or counted.
func TestActiveStateSyncFailureDoesNotBlockHazards(t *testing.T) {
	out := &activeOutput{syncErr: errors.New("broker unavailable")}
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

// TestActiveStateSyncHangDoesNotBlockHazards: a PublishActiveState that
// ignores its context forever is abandoned after its timeout; the remaining
// events still synchronize and hazard delivery still runs.
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
	// The hung call is abandoned; the remaining two active events (b and
	// the still-pending hazard) still synchronize.
	waitFor(t, 3*time.Second, func() bool { return out.syncedCount() == 2 })
	// Hazard delivery proceeds regardless.
	waitFor(t, 3*time.Second, func() bool { return out.handleCount() >= 1 })

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop with an abandoned active-state goroutine")
	}
}

func timePtr(t time.Time) *time.Time { return &t }
