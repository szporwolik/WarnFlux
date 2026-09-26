package plugin

import (
	"context"
	"errors"
	"fmt"
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
	// Under the race detector the previous connection's teardown can lag
	// briefly, so retry while the database reports a lock.
	var (
		store2   *sqlite.Store
		reopenOK bool
	)
	for attempt := 0; attempt < 40; attempt++ {
		store2, _, err = sqlite.Open(path)
		if err == nil {
			reopenOK = true
			break
		}
		if !strings.Contains(err.Error(), "locked") && !strings.Contains(err.Error(), "busy") {
			t.Fatalf("reopen: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !reopenOK {
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

// TestManagerEmitShutdownRaceStress is the adversarial durable-Emit
// shutdown race: many concurrent emitters race the shutdown drain. The
// invariants are: every Emit returns, and the number of nil results equals
// the number of events the ingest worker actually processed — no nil
// without persistence and no silent loss after claimed durable success.
// Run under -race in CI.
func TestManagerEmitShutdownRaceStress(t *testing.T) {
	for round := 0; round < 50; round++ {
		var persisted atomic.Int32
		ingestFn := func(_ context.Context, _ core.HazardEvent) (ingest.Result, core.EventChange, error) {
			persisted.Add(1)
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

		var nilEmits atomic.Int32
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for n := 0; ; n++ {
					select {
					case <-stop:
						return
					default:
					}
					if err := m.emit(context.Background(), eventFor(fmt.Sprintf("race-%d-%d", w, n))); err == nil {
						nilEmits.Add(1)
					}
				}
			}(w)
		}

		// Let a few events flow, then shut down while emits are in flight.
		waitFor(t, 2*time.Second, func() bool { return persisted.Load() > 0 })
		cancel()
		close(stop)
		wg.Wait()
		select {
		case <-runDone:
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d: manager did not stop", round)
		}

		// The durable contract: nil Emit ⇔ ingest processed the event.
		// Events failed by the shutdown drain complete with an error, so
		// they can never appear as nil.
		if got, want := nilEmits.Load(), persisted.Load(); got != want {
			t.Fatalf("round %d: nil Emits = %d, persisted events = %d — durable acknowledgment contract violated", round, got, want)
		}
	}
}

// infoOutput is a fake output implementing both hazard delivery and
// information publishing. failWith makes information publishes fail (hazard
// delivery stays unaffected); block, when set, blocks publishes until
// closed; hang makes PublishInformation ignore its context forever
// (select{}); mutate corrupts the received payload before storing it (to
// prove per-output payload isolation). Handle records every hazard change.
type infoOutput struct {
	mu       sync.Mutex
	messages []core.InformationMessage
	calls    int
	handles  int
	handled  []core.EventChange
	events   []string // ordered "info:<key>" / "handle:<source-id>" log
	failWith error
	block    chan struct{}
	hang     bool
	mutate   bool
}

func (o *infoOutput) Name() string { return "info" }

func (o *infoOutput) Handle(_ context.Context, change core.EventChange) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.handles++
	o.handled = append(o.handled, change)
	o.events = append(o.events, "handle:"+change.Event.SourceID)
	return nil
}

func (o *infoOutput) PublishInformation(_ context.Context, m core.InformationMessage) error {
	o.mu.Lock()
	o.calls++ // the invocation has started
	o.events = append(o.events, "info:"+m.Key)
	if o.hang {
		// Completely ignore cancellation: never return.
		o.mu.Unlock()
		select {}
	}
	if o.block != nil {
		ch := o.block
		o.mu.Unlock()
		<-ch
		o.mu.Lock()
	}
	m = m.Clone()
	if o.mutate {
		m.Payload = append(m.Payload, 'x')
	}
	o.messages = append(o.messages, m)
	err := o.failWith
	o.mu.Unlock()
	return err
}

func (o *infoOutput) infoCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls
}

func (o *infoOutput) handleCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.handles
}

// payloadsByKey returns the delivered payloads grouped by information key,
// in delivery order.
func (o *infoOutput) payloadsByKey() map[string][]string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[string][]string)
	for _, m := range o.messages {
		out[m.Key] = append(out[m.Key], string(m.Payload))
	}
	return out
}

// eventsSnapshot returns the ordered invocation log.
func (o *infoOutput) eventsSnapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.events...)
}

func (o *infoOutput) lastPayload() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.messages) == 0 {
		return nil
	}
	return o.messages[len(o.messages)-1].Payload
}

func (o *infoOutput) receivedProducers() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []string
	for _, m := range o.messages {
		out = append(out, m.ProducerID)
	}
	return out
}

func infoMessage(key string) core.InformationMessage {
	return core.InformationMessage{
		Source:      "openmeteo",
		Key:         key,
		Kind:        "weather",
		GeneratedAt: time.Now(),
		Payload:     []byte(`{"schema_version":1}`),
	}
}

func infoManager(t *testing.T, out OutputPlugin, ingestFn IngestFunc) *Manager {
	return infoManagerWithTimeout(t, out, ingestFn, 5*time.Second)
}

func infoManagerWithTimeout(t *testing.T, out OutputPlugin, ingestFn IngestFunc, timeout time.Duration) *Manager {
	t.Helper()
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	reg := NewRegistry()
	if err := reg.RegisterOutput("info-out", func(_ *yaml.Node) (OutputPlugin, error) {
		return out, nil
	}); err != nil {
		t.Fatalf("register output: %v", err)
	}
	m, err := NewManager(reg, nil, []config.Output{{
		ID: "info", Type: "info-out", Enabled: true,
		Runtime: config.OutputRuntime{Timeout: timeout, FailureThreshold: 5},
	}}, ingestFn, nil, store, managerOpts(), testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

// infoManagerFull is infoManagerWithTimeout with a real durable ingester
// and fast worker poll intervals, for tests where hazard changes must flow
// through the journal while information is being delivered.
func infoManagerFull(t *testing.T, out OutputPlugin, timeout time.Duration) (*Manager, *sqlite.Store, *ingest.Ingester) {
	t.Helper()
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	ing := ingest.NewIngester(store, testLogger())
	reg := NewRegistry()
	if err := reg.RegisterOutput("info-out", func(_ *yaml.Node) (OutputPlugin, error) {
		return out, nil
	}); err != nil {
		t.Fatalf("register output: %v", err)
	}
	m, err := NewManager(reg, nil, []config.Output{{
		ID: "info", Type: "info-out", Enabled: true,
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

// TestManagerEmitInformationForwards verifies the auxiliary information
// path: messages reach information-capable outputs with the ProducerID
// stamped by the manager, and never touch the hazard failure accounting.
func TestManagerEmitInformationForwards(t *testing.T) {
	out := &infoOutput{}
	m := infoManager(t, out, func(_ context.Context, _ core.HazardEvent) (ingest.Result, core.EventChange, error) {
		return ingest.ResultNew, core.EventChange{}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	waitFor(t, 2*time.Second, func() bool { return m.accepting.Load() })

	if err := m.emitInformation(context.Background(), "weather-home", infoMessage("home")); err != nil {
		t.Fatalf("emitInformation: %v", err)
	}
	// Delivery is asynchronous (the output worker owns the callbacks).
	waitFor(t, 2*time.Second, func() bool { return out.infoCount() == 1 })
	if got := out.receivedProducers(); len(got) != 1 || got[0] != "weather-home" {
		t.Errorf("received producers = %v, want [weather-home]", got)
	}

	// The hazard failure counter stays untouched by information traffic.
	statuses := m.Statuses()
	if len(statuses) != 1 || statuses[0].ConsecutiveFailures != 0 {
		t.Errorf("output status = %+v, want zero failures", statuses)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestManagerEmitInformationNoCapableOutput: without an information-capable
// output the source gets a clear error so misconfiguration is visible.
func TestManagerEmitInformationNoCapableOutput(t *testing.T) {
	out := &listOutput{}
	m := infoManager(t, out, nil)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	waitFor(t, 2*time.Second, func() bool { return m.accepting.Load() })

	err := m.emitInformation(context.Background(), "weather-home", infoMessage("home"))
	if err == nil || !strings.Contains(err.Error(), "no information-capable outputs") {
		t.Fatalf("emitInformation = %v, want no-capable-outputs error", err)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestManagerEmitInformationIsolation: an information publish failure is
// logged asynchronously and must not suspend hazard delivery or increment
// the hazard failure counter.
func TestManagerEmitInformationIsolation(t *testing.T) {
	failing := &infoOutput{failWith: errors.New("broker unavailable")}
	m := infoManager(t, failing, nil)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	waitFor(t, 2*time.Second, func() bool { return m.accepting.Load() })

	if err := m.emitInformation(context.Background(), "weather-home", infoMessage("home")); err != nil {
		t.Fatalf("emitInformation enqueue: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return failing.infoCount() == 1 })
	statuses := m.Statuses()
	if len(statuses) != 1 || statuses[0].ConsecutiveFailures != 0 {
		t.Errorf("information failure leaked into hazard accounting: %+v", statuses)
	}
	if statuses[0].State == StateSuspended {
		t.Error("information failure suspended hazard delivery")
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestManagerEmitInformationPayloadIsolation: two information outputs must
// each receive their own deep copy — mutating one payload cannot corrupt
// the other output's message.
func TestManagerEmitInformationPayloadIsolation(t *testing.T) {
	mutator := &infoOutput{mutate: true}
	observer := &infoOutput{}
	reg := NewRegistry()
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	for id, out := range map[string]OutputPlugin{"a": mutator, "b": observer} {
		if err := reg.RegisterOutput("iso-"+id, func(_ *yaml.Node) (OutputPlugin, error) {
			return out, nil
		}); err != nil {
			t.Fatalf("register output: %v", err)
		}
	}
	m, err := NewManager(reg, nil, []config.Output{
		{ID: "a", Type: "iso-a", Enabled: true, Runtime: config.OutputRuntime{Timeout: 5 * time.Second, FailureThreshold: 5}},
		{ID: "b", Type: "iso-b", Enabled: true, Runtime: config.OutputRuntime{Timeout: 5 * time.Second, FailureThreshold: 5}},
	}, nil, nil, store, managerOpts(), testLogger())
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

	msg := infoMessage("home")
	original := string(msg.Payload)
	if err := m.emitInformation(context.Background(), "weather-home", msg); err != nil {
		t.Fatalf("emitInformation: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return mutator.infoCount() == 1 && observer.infoCount() == 1 })
	if got := string(observer.lastPayload()); got != original {
		t.Errorf("observer payload was corrupted by the mutator output: %q", got)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestManagerEmitInformationCoalescesLatest: repeated snapshots for the
// same source+producer+key+kind replace the pending one — only the newest
// value is delivered.
func TestManagerEmitInformationCoalescesLatest(t *testing.T) {
	out := &infoOutput{}
	m := infoManagerWithTimeout(t, out, nil, 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	waitFor(t, 2*time.Second, func() bool { return m.accepting.Load() })

	// Block the worker inside the first delivery, then overwrite the
	// pending value twice.
	release := make(chan struct{})
	out.mu.Lock()
	out.block = release
	out.mu.Unlock()

	v1 := infoMessage("home")
	if err := m.emitInformation(context.Background(), "weather-home", v1); err != nil {
		t.Fatalf("emit v1: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		out.mu.Lock()
		defer out.mu.Unlock()
		return out.calls == 1
	}) // first delivery is now blocked inside the plugin

	v2 := infoMessage("home")
	v2.Payload = []byte(`{"schema_version":1,"v":2}`)
	v3 := infoMessage("home")
	v3.Payload = []byte(`{"schema_version":1,"v":3}`)
	if err := m.emitInformation(context.Background(), "weather-home", v2); err != nil {
		t.Fatalf("emit v2: %v", err)
	}
	if err := m.emitInformation(context.Background(), "weather-home", v3); err != nil {
		t.Fatalf("emit v3: %v", err)
	}
	close(release)

	// The worker delivers v3 (the coalesced latest), never v2.
	waitFor(t, 3*time.Second, func() bool { return out.infoCount() == 2 })
	if got := string(out.lastPayload()); got != `{"schema_version":1,"v":3}` {
		t.Errorf("delivered payload = %s, want the coalesced latest (v3)", got)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestManagerEmitInformationTimeoutDisables: a plugin callback that
// ignores its context permanently disables information publishing for that
// output — EmitInformation returns bounded and no further calls happen.
func TestManagerEmitInformationTimeoutDisables(t *testing.T) {
	out := &infoOutput{block: make(chan struct{})} // never released
	m := infoManagerWithTimeout(t, out, nil, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	waitFor(t, 2*time.Second, func() bool { return m.accepting.Load() })

	if err := m.emitInformation(context.Background(), "weather-home", infoMessage("home")); err != nil {
		t.Fatalf("emitInformation: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return out.infoCount() == 1 })
	// Give the worker time to hit the timeout and disable the capability.
	waitFor(t, 2*time.Second, func() bool {
		w := m.infoOuts[0]
		w.mu <- struct{}{}
		defer func() { <-w.mu }()
		return w.infoDisabled
	})
	// A later message is rejected — no second stuck goroutine is spawned.
	if err := m.emitInformation(context.Background(), "weather-home", infoMessage("home")); err == nil {
		t.Fatal("emitInformation must fail after the capability was disabled")
	}
	time.Sleep(100 * time.Millisecond)
	if out.infoCount() != 1 {
		t.Errorf("publish calls = %d, want exactly 1 (no repeated stuck calls)", out.infoCount())
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestManagerEmitInformationProducerSeparation: identical keys from
// different producer instances must not collide in the coalescing queue.
func TestManagerEmitInformationProducerSeparation(t *testing.T) {
	out := &infoOutput{}
	m := infoManager(t, out, nil)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	waitFor(t, 2*time.Second, func() bool { return m.accepting.Load() })

	if err := m.emitInformation(context.Background(), "weather-home-a", infoMessage("home")); err != nil {
		t.Fatalf("emit a: %v", err)
	}
	if err := m.emitInformation(context.Background(), "weather-home-b", infoMessage("home")); err != nil {
		t.Fatalf("emit b: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return out.infoCount() == 2 })
	producers := out.receivedProducers()
	if len(producers) != 2 || producers[0] == producers[1] {
		t.Errorf("producers = %v, want two distinct producer IDs", producers)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestManagerEmitInformationValidationAndShutdown: invalid messages and
// shutdown are rejected before any output is called.
func TestManagerEmitInformationValidationAndShutdown(t *testing.T) {
	out := &infoOutput{}
	m := infoManager(t, out, nil)

	// Before Run: shutdown rejection.
	if err := m.emitInformation(context.Background(), "weather-home", infoMessage("home")); err != errShuttingDown {
		t.Fatalf("emitInformation before Run = %v, want errShuttingDown", err)
	}

	m.accepting.Store(true)
	invalid := infoMessage("home")
	invalid.Payload = []byte(`{`)
	if err := m.emitInformation(context.Background(), "weather-home", invalid); err == nil || !strings.Contains(err.Error(), "invalid information message") {
		t.Fatalf("emitInformation invalid = %v, want validation error", err)
	}
	if got := out.infoCount(); got != 0 {
		t.Errorf("invalid message reached the output (%d calls)", got)
	}
}

// TestInfoTimeoutDoesNotBlockHazardDelivery is the adversarial hang case:
// PublishInformation ignores ctx forever (select{}). The information
// capability must disable on timeout, the worker must return to its main
// loop immediately, a durable hazard change must still be delivered and
// acknowledged, no second information call may ever start, and shutdown
// must stay bounded.
func TestInfoTimeoutDoesNotBlockHazardDelivery(t *testing.T) {
	out := &infoOutput{hang: true}
	m, store, ing := infoManagerFull(t, out, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	waitFor(t, 2*time.Second, func() bool { return m.accepting.Load() })

	if err := m.emitInformation(context.Background(), "weather-home", infoMessage("home")); err != nil {
		t.Fatalf("emitInformation: %v", err)
	}
	// The hung information callback has started...
	waitFor(t, 2*time.Second, func() bool { return out.infoCount() == 1 })
	// ...and the timeout has permanently disabled the capability.
	waitFor(t, 2*time.Second, func() bool {
		w := m.infoOuts[0]
		w.mu <- struct{}{}
		defer func() { <-w.mu }()
		return w.infoDisabled
	})

	// A durable hazard change is delivered and acknowledged while the
	// information goroutine is still hung: information must never block
	// hazard delivery.
	if _, _, err := ing.Ingest(ctx, eventFor("hang-hazard")); err != nil {
		t.Fatalf("ingest hazard: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return out.handleCount() >= 1 })
	// The SQLite cursor advanced past the delivered change (durable ACK).
	waitFor(t, 2*time.Second, func() bool {
		changes, err := store.PollChanges(ctx, "info", 8)
		return err == nil && len(changes) == 0
	})

	// Exactly one information invocation ever started; later messages are
	// rejected instead of starting a second stuck goroutine.
	if err := m.emitInformation(context.Background(), "weather-home", infoMessage("home")); err == nil {
		t.Fatal("emitInformation must fail after the capability was disabled")
	}
	time.Sleep(100 * time.Millisecond)
	if got := out.infoCount(); got != 1 {
		t.Errorf("information invocations = %d, want exactly 1", got)
	}

	// Shutdown stays bounded even with the abandoned goroutine alive.
	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop with an abandoned information goroutine")
	}
}

// TestInfoMultiKeyWakeupAllDelivered: three distinct information keys
// enqueued before the worker handles the first notification must all be
// delivered (no stranded pending state), and coalescing during the
// delivery must deliver only the latest value of each key.
func TestInfoMultiKeyWakeupAllDelivered(t *testing.T) {
	out := &infoOutput{}
	m := infoManagerWithTimeout(t, out, nil, 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	waitFor(t, 2*time.Second, func() bool { return m.accepting.Load() })

	// Block the first delivery inside the plugin to force coalescing.
	release := make(chan struct{})
	out.mu.Lock()
	out.block = release
	out.mu.Unlock()

	homeV1 := infoMessage("home")
	homeV1.Payload = []byte(`{"schema_version":1,"v":1}`)
	if err := m.emitInformation(context.Background(), "weather-home", homeV1); err != nil {
		t.Fatalf("emit home v1: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return out.infoCount() == 1 })

	cabinV1 := infoMessage("cabin")
	cabinV1.Payload = []byte(`{"schema_version":1,"key":"cabin","v":1}`)
	officeV1 := infoMessage("office")
	officeV1.Payload = []byte(`{"schema_version":1,"key":"office","v":1}`)
	homeV2 := infoMessage("home")
	homeV2.Payload = []byte(`{"schema_version":1,"v":2}`)
	homeV3 := infoMessage("home")
	homeV3.Payload = []byte(`{"schema_version":1,"v":3}`)
	for _, msg := range []core.InformationMessage{cabinV1, officeV1, homeV2, homeV3} {
		if err := m.emitInformation(context.Background(), "weather-home", msg); err != nil {
			t.Fatalf("emit %q: %v", msg.Key, err)
		}
	}
	close(release)

	// All three keys are eventually delivered: the in-flight home v1 plus
	// cabin, office and the coalesced home v3.
	waitFor(t, 3*time.Second, func() bool { return out.infoCount() == 4 })
	// The pending queue drains completely — nothing is stranded.
	waitFor(t, 2*time.Second, func() bool {
		w := m.infoOuts[0]
		w.mu <- struct{}{}
		n := len(w.infoPending)
		<-w.mu
		return n == 0
	})
	byKey := out.payloadsByKey()
	home := byKey["home"]
	if len(home) != 2 {
		t.Errorf("home delivered %d times, want 2 (in-flight v1 + coalesced latest)", len(home))
	}
	if home[len(home)-1] != `{"schema_version":1,"v":3}` {
		t.Errorf("latest home payload = %s, want v3", home[len(home)-1])
	}
	for _, p := range home {
		if p == `{"schema_version":1,"v":2}` {
			t.Error("home v2 was delivered; latest state must win")
		}
	}
	if got := byKey["cabin"]; len(got) != 1 || got[0] != `{"schema_version":1,"key":"cabin","v":1}` {
		t.Errorf("cabin payloads = %v, want exactly one v1", got)
	}
	if got := byKey["office"]; len(got) != 1 || got[0] != `{"schema_version":1,"key":"office","v":1}` {
		t.Errorf("office payloads = %v, want exactly one v1", got)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestInfoBacklogDoesNotStarveHazards: a pending information backlog must
// never postpone durable hazard delivery — the worker handles the hazard
// while information calls are still blocked.
func TestInfoBacklogDoesNotStarveHazards(t *testing.T) {
	out := &infoOutput{}
	m, _, ing := infoManagerFull(t, out, 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	waitFor(t, 2*time.Second, func() bool { return m.accepting.Load() })

	release := make(chan struct{})
	out.mu.Lock()
	out.block = release
	out.mu.Unlock()

	if err := m.emitInformation(context.Background(), "weather-home", infoMessage("home")); err != nil {
		t.Fatalf("emit home: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return out.infoCount() == 1 })

	// A backlog of further information messages sits pending behind the
	// blocked delivery.
	if err := m.emitInformation(context.Background(), "weather-home", infoMessage("office")); err != nil {
		t.Fatalf("emit office: %v", err)
	}
	if err := m.emitInformation(context.Background(), "weather-home", infoMessage("cabin")); err != nil {
		t.Fatalf("emit cabin: %v", err)
	}
	// Hold the block for several poll intervals so a poll tick is
	// guaranteed to be pending when the in-flight call is released.
	time.Sleep(50 * time.Millisecond)

	// The hazard is ingested while the information backlog is blocked
	// behind the in-flight delivery.
	if _, _, err := ing.Ingest(ctx, eventFor("prio-hazard")); err != nil {
		t.Fatalf("ingest hazard: %v", err)
	}

	// Release the in-flight information call. The worker loop re-checks
	// the hazard journal BEFORE any further backlog item, so the hazard
	// is delivered while the two backlog messages are still pending.
	close(release)
	waitFor(t, 2*time.Second, func() bool { return out.handleCount() >= 1 })

	// The remaining backlog still drains completely afterwards.
	waitFor(t, 3*time.Second, func() bool { return out.infoCount() == 3 })

	// Deterministic sequencing: the hazard was handled before the
	// information backlog fully drained (never starved to the end).
	ev := out.eventsSnapshot()
	hazardIdx := -1
	for i, e := range ev {
		if e == "handle:prio-hazard" {
			hazardIdx = i
			break
		}
	}
	if hazardIdx == -1 {
		t.Fatalf("hazard was never handled (events: %v)", ev)
	}
	if hazardIdx == len(ev)-1 {
		t.Errorf("hazard was handled only after the entire information backlog drained (events: %v)", ev)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestInfoQueueFullBehavior: the queue bound counts UNIQUE pending keys.
// A new key at capacity is rejected with an error, while updating an
// already-pending key (latest-state replacement) always succeeds.
func TestInfoQueueFullBehavior(t *testing.T) {
	out := &infoOutput{}
	m := infoManagerWithTimeout(t, out, nil, 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	waitFor(t, 2*time.Second, func() bool { return m.accepting.Load() })

	release := make(chan struct{})
	out.mu.Lock()
	out.block = release
	out.mu.Unlock()

	// The first key is popped into the blocked delivery; the queue then
	// holds up to maxInfoPending additional keys.
	if err := m.emitInformation(context.Background(), "weather-home", infoMessage("k0")); err != nil {
		t.Fatalf("emit k0: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return out.infoCount() == 1 })
	for i := 1; i <= maxInfoPending; i++ {
		if err := m.emitInformation(context.Background(), "weather-home", infoMessage(fmt.Sprintf("k%d", i))); err != nil {
			t.Fatalf("emit k%d (queue not yet full): %v", i, err)
		}
	}

	// A NEW key at capacity is rejected...
	if err := m.emitInformation(context.Background(), "weather-home", infoMessage("overflow")); err == nil || !strings.Contains(err.Error(), "full") {
		t.Fatalf("new key at capacity = %v, want a full-queue error", err)
	}
	// ...while an update of an already-pending key succeeds.
	updated := infoMessage("k1")
	updated.Payload = []byte(`{"schema_version":1,"updated":true}`)
	if err := m.emitInformation(context.Background(), "weather-home", updated); err != nil {
		t.Fatalf("replacing an existing pending key at capacity: %v", err)
	}

	close(release)
	waitFor(t, 10*time.Second, func() bool { return out.infoCount() == maxInfoPending+1 })
	byKey := out.payloadsByKey()
	if _, ok := byKey["overflow"]; ok {
		t.Error("the rejected overflow key was delivered")
	}
	if got := byKey["k1"]; len(got) != 1 || got[0] != `{"schema_version":1,"updated":true}` {
		t.Errorf("k1 payloads = %v, want exactly the updated latest value", got)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}
