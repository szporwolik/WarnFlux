package plugin

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"warnflux/internal/config"
	"warnflux/internal/core"
	"warnflux/internal/ingest"
)

// emitSource emits count events and then blocks until ctx is cancelled.
type emitSource struct {
	count int
}

func (e *emitSource) Name() string { return "emit" }

func (e *emitSource) Run(ctx context.Context, emit Emitter) error {
	for i := 1; i <= e.count; i++ {
		event := core.HazardEvent{Source: "emit", SourceID: string(rune('0' + i)), Event: "E"}
		if err := emit.Emit(ctx, event); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return nil
}

func TestManagerUnknownPluginType(t *testing.T) {
	reg := NewRegistry()
	_, err := NewManager(reg,
		[]config.Source{{ID: "s", Type: "nope", Enabled: true}},
		nil, nil, testLogger())
	if err == nil || !strings.Contains(err.Error(), "unknown source plugin type") {
		t.Errorf("error = %v, want unknown type", err)
	}

	_, err = NewManager(reg, nil,
		[]config.Output{{ID: "o", Type: "nope", Enabled: true}},
		nil, testLogger())
	if err == nil || !strings.Contains(err.Error(), "unknown output plugin type") {
		t.Errorf("error = %v, want unknown type", err)
	}
}

func TestManagerInvalidPluginConfigIdentifiesInstance(t *testing.T) {
	reg := NewRegistry()
	bad := func(*yaml.Node) (SourcePlugin, error) { return nil, errors.New("bad interval") }
	if err := reg.RegisterSource("bad", bad); err != nil {
		t.Fatalf("register: %v", err)
	}

	_, err := NewManager(reg, []config.Source{{ID: "s1", Type: "bad", Enabled: true}}, nil, nil, testLogger())
	if err == nil {
		t.Fatal("expected config error, got nil")
	}
	for _, want := range []string{"s1", "bad", "bad interval"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %q", err, want)
		}
	}
}

func TestManagerDisabledPluginNotInstantiated(t *testing.T) {
	reg := NewRegistry()
	calls := 0
	factory := func(*yaml.Node) (SourcePlugin, error) {
		calls++
		return &emitSource{count: 1}, nil
	}
	if err := reg.RegisterSource("demo", factory); err != nil {
		t.Fatalf("register: %v", err)
	}

	m, err := NewManager(reg, []config.Source{{ID: "s1", Type: "demo", Enabled: false}}, nil, nil, testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if calls != 0 {
		t.Errorf("factory calls = %d, want 0 for disabled plugin", calls)
	}
	st := m.Statuses()
	if len(st) != 1 || st[0].State != StateDisabled {
		t.Errorf("statuses = %+v, want one disabled entry", st)
	}
}

func TestManagerEndToEndAndShutdown(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterSource("emit", func(*yaml.Node) (SourcePlugin, error) {
		return &emitSource{count: 2}, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	rec := &recOutput{name: "rec"}
	if err := reg.RegisterOutput("rec", func(*yaml.Node) (OutputPlugin, error) {
		return rec, nil
	}); err != nil {
		t.Fatalf("register output: %v", err)
	}

	var mu sync.Mutex
	ingested := 0
	ingestFn := func(_ context.Context, event core.HazardEvent) (ingest.Result, core.EventChange, error) {
		mu.Lock()
		defer mu.Unlock()
		ingested++
		if ingested == 1 {
			return ingest.ResultNew, core.EventChange{Type: core.ChangeNew, Event: event}, nil
		}
		return ingest.ResultDuplicate, core.EventChange{}, nil
	}

	srcCfg := testSourceCfg("src", true)
	srcCfg.Type = "emit"
	outCfg := testOutputCfg("out", time.Second, 3)
	outCfg.Type = "rec"

	m, err := NewManager(reg, []config.Source{srcCfg}, []config.Output{outCfg}, ingestFn, testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()

	// Both events reach ingestion; only the non-duplicate reaches outputs.
	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return ingested == 2 && rec.count() == 1
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

func TestManagerShutdownDrainsQueuedEvents(t *testing.T) {
	reg := NewRegistry()

	var mu sync.Mutex
	ingested := 0
	ingestFn := func(_ context.Context, _ core.HazardEvent) (ingest.Result, core.EventChange, error) {
		mu.Lock()
		defer mu.Unlock()
		ingested++
		return ingest.ResultDuplicate, core.EventChange{}, nil
	}

	m, err := NewManager(reg, nil, nil, ingestFn, testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	// Queue events before any worker starts; shutdown must drain them.
	for i := 0; i < 3; i++ {
		m.eventQueue <- core.HazardEvent{Source: "s", SourceID: string(rune('0' + i)), Event: "E"}
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		m.Run(ctx)
	}()

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not shut down in time")
	}

	mu.Lock()
	defer mu.Unlock()
	if ingested != 3 {
		t.Errorf("ingested = %d, want 3 (queue drained on shutdown)", ingested)
	}
}

func TestManagerEmitBackpressureWhenQueueFull(t *testing.T) {
	reg := NewRegistry()
	m, err := NewManager(reg, nil, nil, nil, testLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	m.emitWait = 20 * time.Millisecond

	ctx := context.Background()
	event := core.HazardEvent{Source: "s", SourceID: "1", Event: "E"}

	for i := 0; i < cap(m.eventQueue); i++ {
		if err := m.emit(ctx, event); err != nil {
			t.Fatalf("emit %d: %v", i, err)
		}
	}
	if err := m.emit(ctx, event); err == nil || !strings.Contains(err.Error(), "queue full") {
		t.Errorf("emit on full queue = %v, want queue full error", err)
	}
}
