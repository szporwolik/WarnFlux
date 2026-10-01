package plugin

import (
	"context"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/ingest"
)

// runCountingSource signals each start and each cancellation.
type runCountingSource struct {
	starts chan struct{}
	exits  chan struct{}
}

func (r *runCountingSource) Name() string { return "run-counting" }

func (r *runCountingSource) Run(ctx context.Context, _ Emitter) error {
	r.starts <- struct{}{}
	<-ctx.Done()
	r.exits <- struct{}{}
	return nil
}

// TestSupervisorSuspendResume proves the offline-mode switch: suspend
// cancels the run, blocks restarts and marks the source suspended; resume
// starts a fresh run; the root cancellation still shuts everything down.
func TestSupervisorSuspendResume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := &runCountingSource{starts: make(chan struct{}, 8), exits: make(chan struct{}, 8)}
	tracker := newStatusTracker("src", "test", KindSource)
	s := newSourceSupervisor(testSourceCfg("src", true), src, noopEmitter(), testLogger(), tracker)
	s.backoffBase = time.Millisecond
	s.backoffMax = 4 * time.Millisecond
	go s.run(ctx)

	// The first run starts and reaches Running.
	select {
	case <-src.starts:
	case <-time.After(2 * time.Second):
		t.Fatal("source never started")
	}
	waitFor(t, 2*time.Second, func() bool { return tracker.snapshot().State == StateRunning })

	// Suspend: the run is cancelled, no restart is scheduled, the state
	// reads suspended.
	s.Suspend()
	select {
	case <-src.exits:
	case <-time.After(2 * time.Second):
		t.Fatal("suspend did not cancel the running plugin")
	}
	waitFor(t, 2*time.Second, func() bool { return tracker.snapshot().State == StateSuspended })
	select {
	case <-src.starts:
		t.Fatal("suspended source restarted")
	case <-time.After(150 * time.Millisecond):
	}

	// Suspend is idempotent.
	s.Suspend()
	waitFor(t, 2*time.Second, func() bool { return tracker.snapshot().State == StateSuspended })

	// Resume: a fresh run starts.
	s.Resume()
	select {
	case <-src.starts:
	case <-time.After(2 * time.Second):
		t.Fatal("source never resumed")
	}
	waitFor(t, 2*time.Second, func() bool { return tracker.snapshot().State == StateRunning })

	cancel()
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not stop")
	}
	if got := tracker.snapshot().State; got != StateStopped {
		t.Errorf("state = %v, want stopped", got)
	}
}

// TestManagerSetOfflineOnlyInternetSources proves the manager suspends
// exactly the internet-classified sources; local sources stay untouched.
func TestManagerSetOfflineOnlyInternetSources(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterSource("net", func(*yaml.Node) (SourcePlugin, error) {
		return blockingSource{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterSource("radio", func(*yaml.Node) (SourcePlugin, error) {
		return blockingSource{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	reg.MarkSourceInternet("net")

	noopIngest := IngestFunc(func(context.Context, core.HazardEvent) (ingest.Result, core.EventChange, error) {
		return 0, core.EventChange{}, nil
	})
	noopExpire := ExpireFunc(func(context.Context, time.Time) ([]core.EventChange, error) {
		return nil, nil
	})

	m, err := NewManager(reg, []config.Source{
		{ID: "inet-feed", Type: "net", Enabled: true, Runtime: config.SourceRuntime{Restart: true, ShutdownTimeout: time.Second}},
		{ID: "kiss-radio", Type: "radio", Enabled: true, Runtime: config.SourceRuntime{Restart: true, ShutdownTimeout: time.Second}},
		{ID: "inet-off", Type: "net", Enabled: false, Runtime: config.SourceRuntime{Restart: true, ShutdownTimeout: time.Second}},
	}, nil, noopIngest, noopExpire, nil, ManagerOptions{}, testLogger())
	if err != nil {
		t.Fatal(err)
	}

	// Suspending before Run: the internet source is marked suspended and
	// will never start until resumed; the local and disabled sources are
	// untouched.
	m.SetOffline(true)
	st := map[string]PluginStatus{}
	for _, s := range m.Statuses() {
		st[s.ID] = s
	}
	if st["inet-feed"].State != StateSuspended {
		t.Errorf("inet-feed state = %v, want suspended", st["inet-feed"].State)
	}
	if st["kiss-radio"].State != StateStopped {
		t.Errorf("kiss-radio state = %v, want stopped (never started, not suspended)", st["kiss-radio"].State)
	}
	if st["inet-off"].State != StateDisabled {
		t.Errorf("inet-off state = %v, want disabled", st["inet-off"].State)
	}
	if !st["inet-feed"].Internet || st["kiss-radio"].Internet {
		t.Errorf("internet classification wrong: %+v", st)
	}
	if !m.Offline() {
		t.Error("manager offline flag not set")
	}

	// Idempotent.
	m.SetOffline(true)
	m.SetOffline(false)
	if m.Offline() {
		t.Error("manager offline flag not cleared")
	}
}
