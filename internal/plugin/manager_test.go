package plugin

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/ingest"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
)

// emitListSource emits the given events (in order) and then blocks until ctx
// is cancelled.
type emitListSource struct {
	events []core.HazardEvent
}

func (s *emitListSource) Name() string { return "emitlist" }

func (s *emitListSource) Run(ctx context.Context, emit Emitter) error {
	for _, e := range s.events {
		if err := emit.Emit(ctx, e); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return nil
}

type listOutput struct {
	mu  sync.Mutex
	got []core.EventChange
	err error
}

func (o *listOutput) Name() string { return "list" }

func (o *listOutput) Handle(_ context.Context, change core.EventChange) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		return o.err
	}
	o.got = append(o.got, change)
	return nil
}

func (o *listOutput) types() []core.ChangeType {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]core.ChangeType, len(o.got))
	for i, c := range o.got {
		out[i] = c.Type
	}
	return out
}

func (o *listOutput) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.got)
}

// buildManager wires a real SQLite store, registry and manager for E2E
// tests.
func buildManager(t *testing.T, source *emitListSource, outputs map[string]*listOutput, opts ManagerOptions) (*Manager, *sqlite.Store) {
	t.Helper()

	reg := NewRegistry()
	if err := reg.RegisterSource("emitlist", func(_ *yaml.Node) (SourcePlugin, error) {
		return source, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}
	for id, out := range outputs {
		typ := "list-" + id
		if err := reg.RegisterOutput(typ, func(_ *yaml.Node) (OutputPlugin, error) {
			return out, nil
		}); err != nil {
			t.Fatalf("register output %q: %v", id, err)
		}
	}

	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	ing := ingest.NewIngester(store, testLogger())

	var sourceCfgs []config.Source
	if source != nil {
		sourceCfgs = append(sourceCfgs, config.Source{
			ID: "src", Type: "emitlist", Enabled: true,
			Runtime: config.SourceRuntime{Restart: true, ShutdownTimeout: 50 * time.Millisecond},
		})
	}

	var outputCfgs []config.Output
	for id := range outputs {
		outputCfgs = append(outputCfgs, config.Output{
			ID: id, Type: "list-" + id, Enabled: true,
			Runtime: config.OutputRuntime{Timeout: time.Second, FailureThreshold: 5},
		})
	}

	m, err := NewManager(reg, sourceCfgs, outputCfgs, ing.Ingest, ing.Expire, store, opts, testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	for _, w := range m.outputs {
		w.pollInterval = 5 * time.Millisecond
		w.recoveryInterval = 10 * time.Millisecond
	}
	return m, store
}

func managerOpts() ManagerOptions {
	return ManagerOptions{
		ExpirationInterval: time.Minute,
		ChangeRetention:    time.Hour,
		Version:            "test",
	}
}

func eventFor(id string) core.HazardEvent {
	e := core.HazardEvent{
		Source:   "demo",
		SourceID: id,
		Event:    "Drill",
		Severity: "minor",
	}
	e.Normalize()
	return e
}

func TestManagerEndToEndOrdered(t *testing.T) {
	base := eventFor("001")
	updated := base.Clone()
	updated.Severity = "severe"
	cancelled := updated.Clone()
	cancelled.Status = core.StatusCancelled

	source := &emitListSource{events: []core.HazardEvent{base, updated, cancelled}}
	out := &listOutput{}
	m, store := buildManager(t, source, map[string]*listOutput{"out": out}, managerOpts())
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()

	// The single ordered ingest worker guarantees journal order:
	// new, updated, cancelled.
	waitFor(t, 3*time.Second, func() bool {
		got := out.types()
		return len(got) == 3 &&
			got[0] == core.ChangeNew && got[1] == core.ChangeUpdated && got[2] == core.ChangeCancelled
	})

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not shut down in time")
	}
	for _, st := range m.Statuses() {
		if st.State != StateStopped {
			t.Errorf("plugin %q state = %v, want stopped", st.ID, st.State)
		}
	}
}

// TestManagerAtLeastOnceAcrossRestart is covered by
// TestManagerRestartResumesPendingDelivery below.

// The previous test needs a shared DB path; use a real restart test instead.
func TestManagerRestartResumesPendingDelivery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")

	reg := NewRegistry()
	out := &listOutput{}
	if err := reg.RegisterOutput("list", func(_ *yaml.Node) (OutputPlugin, error) {
		return out, nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	store1, _, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	ing := ingest.NewIngester(store1, testLogger())
	event := eventFor("restart")
	if _, _, err := ing.Ingest(context.Background(), event); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	store1.Close()

	// Second manager on the same database delivers the pending change.
	store2, _, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer store2.Close()

	m2, err := NewManager(reg, nil,
		[]config.Output{{ID: "out", Type: "list", Enabled: true, Runtime: config.OutputRuntime{Timeout: time.Second, FailureThreshold: 5}}},
		ingest.NewIngester(store2, testLogger()).Ingest,
		ingest.NewIngester(store2, testLogger()).Expire,
		store2, managerOpts(), testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	m2.outputs[0].pollInterval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m2.Run(ctx)
	}()

	waitFor(t, 3*time.Second, func() bool { return out.count() == 1 })
	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

func TestManagerOneBrokenOutputDoesNotBlockHealthy(t *testing.T) {
	base := eventFor("002")
	source := &emitListSource{events: []core.HazardEvent{base}}
	broken := &listOutput{err: errors.New("down")}
	healthy := &listOutput{}

	reg := NewRegistry()
	if err := reg.RegisterSource("emitlist", func(_ *yaml.Node) (SourcePlugin, error) {
		return source, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}
	if err := reg.RegisterOutput("list", func(_ *yaml.Node) (OutputPlugin, error) {
		return &listOutput{}, nil
	}); err != nil {
		t.Fatalf("register output: %v", err)
	}

	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	ing := ingest.NewIngester(store, testLogger())

	// Use distinct plugin instances per output: patch after construction is
	// not possible, so register a factory choosing by call order.
	order := 0
	var orderMu sync.Mutex
	reg2 := NewRegistry()
	if err := reg2.RegisterSource("emitlist", func(_ *yaml.Node) (SourcePlugin, error) { return source, nil }); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg2.RegisterOutput("list", func(_ *yaml.Node) (OutputPlugin, error) {
		orderMu.Lock()
		defer orderMu.Unlock()
		order++
		if order == 1 {
			return broken, nil
		}
		return healthy, nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	m, err := NewManager(reg2,
		[]config.Source{{ID: "src", Type: "emitlist", Enabled: true, Runtime: config.SourceRuntime{Restart: true, ShutdownTimeout: 50 * time.Millisecond}}},
		[]config.Output{
			{ID: "broken", Type: "list", Enabled: true, Runtime: config.OutputRuntime{Timeout: time.Second, FailureThreshold: 10}},
			{ID: "healthy", Type: "list", Enabled: true, Runtime: config.OutputRuntime{Timeout: time.Second, FailureThreshold: 5}},
		},
		ing.Ingest, ing.Expire, store, managerOpts(), testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	for _, w := range m.outputs {
		w.pollInterval = 5 * time.Millisecond
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()

	waitFor(t, 3*time.Second, func() bool { return healthy.count() == 1 })
	if broken.count() != 0 {
		t.Errorf("broken output delivered %d changes, want 0", broken.count())
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestManagerExpirationReachesOutputs proves the M2 requirement end to end:
// an event that expires produces exactly one logical expired change that
// survives restart.
func TestManagerExpirationReachesOutputs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")

	reg := NewRegistry()
	out := &listOutput{}
	if err := reg.RegisterOutput("list", func(_ *yaml.Node) (OutputPlugin, error) {
		return out, nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	store, _, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ing := ingest.NewIngester(store, testLogger())

	past := time.Now().Add(-time.Hour)
	event := eventFor("expiry")
	event.ExpiresAt = &past
	if _, _, err := ing.Ingest(context.Background(), event); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	opts := managerOpts()
	opts.ExpirationInterval = 10 * time.Millisecond
	m, err := NewManager(reg, nil,
		[]config.Output{{ID: "out", Type: "list", Enabled: true, Runtime: config.OutputRuntime{Timeout: time.Second, FailureThreshold: 5}}},
		ing.Ingest, ing.Expire, store, opts, testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	m.outputs[0].pollInterval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()

	// The maintenance loop expires the event; the output receives exactly
	// the expired change (the earlier "new" change is also pending).
	waitFor(t, 5*time.Second, func() bool {
		types := out.types()
		for _, ct := range types {
			if ct == core.ChangeExpired {
				return true
			}
		}
		return false
	})

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
	store.Close()

	// Restart: the expired change was acked and must not be re-created
	// (expiration is idempotent); reopening must not add new changes.
	store2, _, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()
	changes, err := store2.Expire(context.Background(), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("Expire after restart: %v", err)
	}
	if len(changes) != 0 {
		t.Errorf("expiration after restart created %d changes, want 0", len(changes))
	}
}

func TestManagerShutdownWithPendingDeliveries(t *testing.T) {
	base := eventFor("003")
	source := &emitListSource{events: []core.HazardEvent{base}}

	reg := NewRegistry()
	if err := reg.RegisterSource("emitlist", func(_ *yaml.Node) (SourcePlugin, error) { return source, nil }); err != nil {
		t.Fatalf("register source: %v", err)
	}
	blocking := &recOutput{name: "blocked", gate: make(chan struct{})}
	if err := reg.RegisterOutput("blocked", func(_ *yaml.Node) (OutputPlugin, error) { return blocking, nil }); err != nil {
		t.Fatalf("register output: %v", err)
	}

	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	ing := ingest.NewIngester(store, testLogger())

	m, err := NewManager(reg,
		[]config.Source{{ID: "src", Type: "emitlist", Enabled: true, Runtime: config.SourceRuntime{Restart: true, ShutdownTimeout: 50 * time.Millisecond}}},
		[]config.Output{{ID: "blocked", Type: "blocked", Enabled: true, Runtime: config.OutputRuntime{Timeout: 100 * time.Millisecond, FailureThreshold: 5}}},
		ing.Ingest, ing.Expire, store, managerOpts(), testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	m.outputs[0].pollInterval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()

	// Let the delivery start (and wedge), then shut down: must not hang.
	waitFor(t, 3*time.Second, func() bool { return blocking.calls() >= 1 })
	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager hung on shutdown with a wedged output")
	}
	close(blocking.gate)
}

// mutatingSource emits one event and then mutates its own copy, proving the
// emitter boundary deep-copies.
type mutatingSource struct {
	event core.HazardEvent
	gate  chan struct{}
}

func (s *mutatingSource) Name() string { return "mutating" }

func (s *mutatingSource) Run(ctx context.Context, emit Emitter) error {
	if err := emit.Emit(ctx, s.event); err != nil {
		return err
	}
	// Mutate the source-owned copy after emission.
	s.event.Severity = "mutated"
	if len(s.event.Areas) > 0 {
		s.event.Areas[0] = "PL-MA"
	}
	if s.event.EffectiveAt != nil {
		*s.event.EffectiveAt = time.Now()
	}
	<-ctx.Done()
	return nil
}

// TestManagerEmitterDeepCopy verifies the source boundary: a source that
// mutates its event after Emit cannot affect the stored state (M8/M26).
func TestManagerEmitterDeepCopy(t *testing.T) {
	eff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	event := eventFor("copy")
	sourceEff := eff
	event.EffectiveAt = &sourceEff
	event.Areas = []string{"DE-NW"}

	source := &mutatingSource{event: event}

	reg := NewRegistry()
	if err := reg.RegisterSource("mutating", func(_ *yaml.Node) (SourcePlugin, error) { return source, nil }); err != nil {
		t.Fatalf("register: %v", err)
	}

	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	ing := ingest.NewIngester(store, testLogger())

	m, err := NewManager(reg,
		[]config.Source{{ID: "src", Type: "mutating", Enabled: true, Runtime: config.SourceRuntime{Restart: true, ShutdownTimeout: 50 * time.Millisecond}}},
		nil, ing.Ingest, ing.Expire, store, managerOpts(), testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()

	waitFor(t, 3*time.Second, func() bool {
		_, err := store.Get(ctx, event.Key())
		return err == nil
	})
	got, err := store.Get(ctx, event.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Severity != "minor" {
		t.Errorf("stored severity = %q, want original", got.Event.Severity)
	}
	if got.Event.Areas[0] != "DE-NW" {
		t.Errorf("stored areas = %v, want original", got.Event.Areas)
	}
	if !got.Event.EffectiveAt.Equal(eff) {
		t.Errorf("stored EffectiveAt = %v, want original %v", got.Event.EffectiveAt, eff)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestManagerEmitAcknowledgesDurablePersistence pins the durable Emit
// contract: Emit returns nil only after the event is durably persisted.
func TestManagerEmitAcknowledgesDurablePersistence(t *testing.T) {
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	ing := ingest.NewIngester(store, testLogger())
	m, err := NewManager(NewRegistry(), nil, nil, ing.Ingest, ing.Expire, store, managerOpts(), testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	waitFor(t, 2*time.Second, func() bool { return m.accepting.Load() })

	e := eventFor("durable")
	if err := m.emit(context.Background(), e); err != nil {
		t.Fatalf("emit: %v", err)
	}
	// nil Emit ⇒ the event is in SQLite, not merely in the RAM queue.
	got, err := store.Get(context.Background(), e.Key())
	if err != nil {
		t.Fatalf("Get after Emit nil: %v", err)
	}
	if got.Event.Status != core.StatusActive {
		t.Errorf("stored status = %v, want active", got.Event.Status)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestManagerEmitPropagatesIngestFailure: an ingest error is returned to
// the source — the source can never believe an event was persisted when it
// was not.
func TestManagerEmitPropagatesIngestFailure(t *testing.T) {
	boom := errors.New("boom")
	ingestFn := func(context.Context, core.HazardEvent) (ingest.Result, core.EventChange, error) {
		return 0, core.EventChange{}, boom
	}
	m, err := NewManager(NewRegistry(), nil, nil, ingestFn, nil, nil, managerOpts(), testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	waitFor(t, 2*time.Second, func() bool { return m.accepting.Load() })

	if err := m.emit(context.Background(), eventFor("fails")); !errors.Is(err, boom) {
		t.Fatalf("emit = %v, want the ingest error", err)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestManagerEmitDrainedOnShutdown: an event accepted into the queue before
// shutdown is always completed — the source gets the ingest outcome even
// when shutdown races the drain.
func TestManagerEmitDrainedOnShutdown(t *testing.T) {
	var called atomic.Int32
	release := make(chan struct{})
	ingestFn := func(context.Context, core.HazardEvent) (ingest.Result, core.EventChange, error) {
		called.Add(1)
		<-release
		return ingest.ResultNew, core.EventChange{}, nil
	}
	m, err := NewManager(NewRegistry(), nil, nil, ingestFn, nil, nil, managerOpts(), testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	waitFor(t, 2*time.Second, func() bool { return m.accepting.Load() })

	done := make(chan error, 1)
	go func() { done <- m.emit(context.Background(), eventFor("drain")) }()
	waitFor(t, 2*time.Second, func() bool { return called.Load() == 1 })

	// Shutdown while the ingest is mid-flight: the event must still be
	// completed with its outcome.
	cancel()
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("emit during shutdown = %v, want nil (drained and persisted)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("emit never completed during shutdown drain")
	}
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

func TestManagerUnknownPluginTypeEvenWhenDisabled(t *testing.T) {
	reg := NewRegistry()
	_, err := NewManager(reg,
		[]config.Source{{ID: "s", Type: "nope", Enabled: false}},
		nil, nil, nil, nil, managerOpts(), testLogger())
	if err == nil || !strings.Contains(err.Error(), "unknown source plugin type") {
		t.Errorf("error = %v, want unknown type even for disabled instance", err)
	}
}

// TestManagerEmitShutdownSemantics pins the shutdown contract: Emit never
// returns nil ("accepted") once shutdown begins or when the caller's
// context is cancelled — even when the queue is writable.
func TestManagerEmitShutdownSemantics(t *testing.T) {
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	m, err := NewManager(NewRegistry(), nil, nil, nil, nil, store, managerOpts(), testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	// Not running: the accepting flag is false.
	e := eventFor("shutdown")
	if err := m.emit(context.Background(), e); err != errShuttingDown {
		t.Fatalf("emit before Run = %v, want errShuttingDown", err)
	}

	// Accepting but caller context cancelled: cancellation wins over an
	// empty, writable queue.
	m.accepting.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.emit(ctx, e); err != context.Canceled {
		t.Fatalf("emit with cancelled ctx = %v, want context.Canceled", err)
	}
	if len(m.eventQueue) != 0 {
		t.Errorf("queue has %d events, want 0 (nothing accepted)", len(m.eventQueue))
	}
}
