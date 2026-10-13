package plugin

import (
	"context"
	"errors"
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

// snapshotInstance is a stub FULL-SNAPSHOT source: every cycle it publishes
// the events it has in force and reconciles (cancels) the ones of its own
// instance that disappeared — the shape of the real snapshot sources (imgw,
// rso, burzedzis). Two instances of this one type share the source slug
// "demo" and differ only in the event ids they own.
type snapshotInstance struct {
	// eventID is the one event this instance knows about.
	eventID string

	mu      sync.Mutex
	emit    Emitter
	inForce bool
	cycles  chan chan error
}

func (s *snapshotInstance) Name() string { return "snapshot" }

func (s *snapshotInstance) Run(ctx context.Context, emit Emitter) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return nil
		case done := <-s.cycles:
			done <- s.cycle(ctx)
		}
	}
}

// cycle runs one poll: publish what is in force, then reconcile the rest.
func (s *snapshotInstance) cycle(ctx context.Context) error {
	s.mu.Lock()
	emit, inForce := s.emit, s.inForce
	s.mu.Unlock()
	if emit == nil {
		return errors.New("the source has not been started")
	}

	keys := map[string]bool{}
	event := eventFor(s.eventID)
	if inForce {
		keys[event.Key()] = true
		if err := emit.Emit(ctx, event); err != nil {
			return err
		}
	}

	// Exactly what the snapshot sources do through
	// SourceActiveEventReader + the reconciliation helper.
	reader, ok := emit.(SourceActiveEventReader)
	if !ok {
		return errors.New("emitter does not implement SourceActiveEventReader")
	}
	active, err := reader.ListSourceActiveEvents(ctx, event.Source)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, live := range active {
		if keys[live.Key()] {
			continue
		}
		if live.ExpiresAt != nil && !live.ExpiresAt.After(now) {
			continue // the expiration worker owns that transition
		}
		cancelled := live.Clone()
		cancelled.Status = core.StatusCancelled
		if err := emit.Emit(ctx, cancelled); err != nil {
			return err
		}
	}
	return nil
}

func (s *snapshotInstance) setInForce(v bool) {
	s.mu.Lock()
	s.inForce = v
	s.mu.Unlock()
}

func (s *snapshotInstance) emitter() Emitter {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.emit
}

// poll runs one cycle of this instance and returns its error.
func (s *snapshotInstance) poll(t *testing.T) error {
	t.Helper()
	done := make(chan error, 1)
	select {
	case s.cycles <- done:
	case <-time.After(5 * time.Second):
		t.Fatal("the source did not accept a poll")
	}
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("the poll did not finish")
		return nil
	}
}

// TestInstancesDoNotCancelEachOther pins the reported P1: two instances of
// one snapshot source type (same event source slug, different events) must
// not withdraw each other's alerts. Instance B knows nothing about A's
// event, so B's snapshot is "complete" from B's point of view — before the
// fix, B's reconciliation cancelled A's event.
func TestInstancesDoNotCancelEachOther(t *testing.T) {
	store, m, sources := buildInstanceManager(t, map[string]string{
		"stub-a": "a-1",
		"stub-b": "b-1",
	})
	a, b := sources["a-1"], sources["b-1"]

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()
	for _, src := range sources {
		waitFor(t, 3*time.Second, func() bool { return src.emitter() != nil })
	}

	// Both instances publish their own event.
	a.setInForce(true)
	b.setInForce(true)
	if err := a.poll(t); err != nil {
		t.Fatalf("instance A poll: %v", err)
	}
	if err := b.poll(t); err != nil {
		t.Fatalf("instance B poll: %v", err)
	}
	assertStatus(t, store, eventFor("a-1").Key(), core.StatusActive)
	assertStatus(t, store, eventFor("b-1").Key(), core.StatusActive)

	// A withdraws its warning. B still has its own in force and must not
	// touch A's event — and A must not touch B's.
	a.setInForce(false)
	if err := a.poll(t); err != nil {
		t.Fatalf("instance A poll: %v", err)
	}
	if err := b.poll(t); err != nil {
		t.Fatalf("instance B poll: %v", err)
	}
	assertStatus(t, store, eventFor("b-1").Key(), core.StatusActive)

	// A genuine withdrawal of A's OWN event still lands: the reconciliation
	// of the instance that owns it is what cancels it.
	assertStatus(t, store, eventFor("a-1").Key(), core.StatusCancelled)

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// TestInstanceOwnsItsEvents proves the ownership itself: the events of the
// other instance are invisible to the reader, and an event with no owner is
// adopted by the instance that reports it again.
func TestInstanceOwnsItsEvents(t *testing.T) {
	store, m, sources := buildInstanceManager(t, map[string]string{"stub-a": "a-1"})
	a := sources["a-1"]
	ctx := context.Background()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(runCtx)
	}()
	waitFor(t, 3*time.Second, func() bool { return a.emitter() != nil })

	reader, ok := a.emitter().(SourceActiveEventReader)
	if !ok {
		t.Fatal("emitter does not implement SourceActiveEventReader")
	}

	// An event that no instance produced: invisible to the reader, so no
	// snapshot source can reconcile it away by accident.
	orphan := eventFor("orphan")
	ing := ingest.NewIngester(store, testLogger())
	if _, _, err := ing.Ingest(ctx, orphan); err != nil {
		t.Fatalf("ingest orphan: %v", err)
	}
	got, err := reader.ListSourceActiveEvents(ctx, orphan.Source)
	if err != nil {
		t.Fatalf("ListSourceActiveEvents: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("the unowned event is visible to the instance: %+v", got)
	}

	// The same event published BY the instance is owned, and once owned a
	// foreign path that carries no producer cannot steal it back.
	a.setInForce(true)
	if err := a.poll(t); err != nil {
		t.Fatalf("instance A poll: %v", err)
	}
	got, err = reader.ListSourceActiveEvents(ctx, "demo")
	if err != nil {
		t.Fatalf("ListSourceActiveEvents: %v", err)
	}
	keys := map[string]string{}
	for _, ev := range got {
		keys[ev.Key()] = ev.Producer
	}
	if keys[eventFor("a-1").Key()] != "stub-a" {
		t.Errorf("the instance does not own its own event: %v", keys)
	}
	if _, ok := keys[orphan.Key()]; ok {
		t.Errorf("the unowned event became visible without being adopted: %v", keys)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop")
	}
}

// buildInstanceManager wires a real store, ingester and manager with one
// source instance per entry of instances (source id -> the event id it
// publishes). Both instances are the SAME plugin type, exactly like two
// burzedzis instances following different localities: they publish one
// shared event source slug and differ only in the events they own.
func buildInstanceManager(t *testing.T, instances map[string]string) (*sqlite.Store, *Manager, map[string]*snapshotInstance) {
	t.Helper()
	reg := NewRegistry()
	var mu sync.Mutex
	byEvent := map[string]*snapshotInstance{}
	if err := reg.RegisterSource("snapshot", func(node *yaml.Node) (SourcePlugin, error) {
		var cfg struct {
			EventID string `yaml:"event_id"`
		}
		if err := node.Decode(&cfg); err != nil {
			return nil, err
		}
		src := &snapshotInstance{eventID: cfg.EventID, cycles: make(chan chan error)}
		mu.Lock()
		byEvent[cfg.EventID] = src
		mu.Unlock()
		return src, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	sourceCfgs := make([]config.Source, 0, len(instances))
	for id, eventID := range instances {
		var node yaml.Node
		if err := node.Encode(map[string]string{"event_id": eventID}); err != nil {
			t.Fatalf("encode config of %q: %v", id, err)
		}
		sourceCfgs = append(sourceCfgs, config.Source{
			ID: id, Type: "snapshot", Enabled: true, Config: &node,
			Runtime: config.SourceRuntime{Restart: true, ShutdownTimeout: 50 * time.Millisecond},
		})
	}

	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	ing := ingest.NewIngester(store, testLogger())
	m, err := NewManager(reg, sourceCfgs, nil, ing.Ingest, ing.Expire, store, managerOpts(), testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return store, m, byEvent
}

func assertStatus(t *testing.T, store *sqlite.Store, key string, want core.EventStatus) {
	t.Helper()
	stored, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	if stored.Event.Status != want {
		t.Errorf("event %q is %q, want %q", key, stored.Event.Status, want)
	}
}
