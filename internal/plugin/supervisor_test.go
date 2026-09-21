package plugin

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/core"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func testSourceCfg(id string, restart bool) config.Source {
	return config.Source{
		ID:      id,
		Type:    "test",
		Enabled: true,
		Runtime: config.SourceRuntime{
			Restart:         restart,
			ShutdownTimeout: 50 * time.Millisecond,
		},
	}
}

func noopEmitter() Emitter {
	return EmitterFunc(func(context.Context, core.HazardEvent) error { return nil })
}

type blockingSource struct{}

func (blockingSource) Name() string { return "blocking" }

func (blockingSource) Run(ctx context.Context, _ Emitter) error {
	<-ctx.Done()
	return nil
}

type failingSource struct{}

func (failingSource) Name() string { return "failing" }

func (failingSource) Run(context.Context, Emitter) error {
	return errors.New("connection refused")
}

type panicSource struct{}

func (panicSource) Name() string { return "panic" }

func (panicSource) Run(context.Context, Emitter) error {
	panic("boom")
}

// gateSource fails the first fails runs, then blocks until gate is closed.
type gateSource struct {
	fails int
	gate  chan struct{}
}

func (g *gateSource) Name() string { return "gate" }

func (g *gateSource) Run(ctx context.Context, _ Emitter) error {
	if g.fails > 0 {
		g.fails--
		return errors.New("temporary failure")
	}
	select {
	case <-g.gate:
		return nil
	case <-ctx.Done():
		return nil
	}
}

func TestSupervisorPanicRecovered(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tracker := newStatusTracker("src", "test", KindSource)
	s := newSourceSupervisor(testSourceCfg("src", true), panicSource{}, noopEmitter(), testLogger(), tracker)
	s.backoffBase = time.Millisecond
	s.backoffMax = 4 * time.Millisecond

	go s.run(ctx)

	waitFor(t, 2*time.Second, func() bool { return tracker.failures() >= 1 })
	st := tracker.snapshot()
	if !strings.Contains(st.LastError, "boom") {
		t.Errorf("LastError = %q, want panic value", st.LastError)
	}
	if st.RestartCount == 0 {
		t.Error("panic should have triggered a restart")
	}

	cancel()
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not stop after cancellation")
	}
	if got := tracker.snapshot().State; got != StateStopped {
		t.Errorf("state = %v, want stopped", got)
	}
}

func TestSupervisorBackoffDelays(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tracker := newStatusTracker("src", "test", KindSource)
	s := newSourceSupervisor(testSourceCfg("src", true), failingSource{}, noopEmitter(), testLogger(), tracker)
	s.backoffBase = time.Millisecond
	s.backoffMax = 8 * time.Millisecond

	var mu sync.Mutex
	var delays []time.Duration
	s.waitBackoffFn = func(_ context.Context, d time.Duration) bool {
		mu.Lock()
		delays = append(delays, d)
		mu.Unlock()
		return true
	}

	go s.run(ctx)

	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(delays) >= 3
	})
	cancel()
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not stop")
	}

	mu.Lock()
	defer mu.Unlock()
	want := []time.Duration{time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond}
	if len(delays) < len(want) {
		t.Fatalf("delays = %v", delays)
	}
	for i, w := range want {
		if delays[i] != w {
			t.Errorf("delay[%d] = %v, want %v", i, delays[i], w)
		}
	}
}

func TestSupervisorFailureDoesNotAffectOtherSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	healthyTracker := newStatusTracker("healthy", "test", KindSource)
	healthy := newSourceSupervisor(testSourceCfg("healthy", true), blockingSource{}, noopEmitter(), testLogger(), healthyTracker)

	failingTracker := newStatusTracker("failing", "test", KindSource)
	failing := newSourceSupervisor(testSourceCfg("failing", true), failingSource{}, noopEmitter(), testLogger(), failingTracker)
	failing.backoffBase = time.Millisecond
	failing.backoffMax = 4 * time.Millisecond

	go healthy.run(ctx)
	go failing.run(ctx)

	waitFor(t, 2*time.Second, func() bool {
		return healthyTracker.snapshot().State == StateRunning && failingTracker.failures() >= 1
	})
	if got := healthyTracker.snapshot().State; got != StateRunning {
		t.Errorf("healthy source state = %v, want running despite failing source", got)
	}

	cancel()
	select {
	case <-healthy.done:
	case <-time.After(2 * time.Second):
		t.Fatal("healthy supervisor did not stop")
	}
	select {
	case <-failing.done:
	case <-time.After(2 * time.Second):
		t.Fatal("failing supervisor did not stop")
	}
}

func TestSupervisorStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	tracker := newStatusTracker("src", "test", KindSource)
	s := newSourceSupervisor(testSourceCfg("src", true), blockingSource{}, noopEmitter(), testLogger(), tracker)
	go s.run(ctx)

	waitFor(t, 2*time.Second, func() bool { return tracker.snapshot().State == StateRunning })

	cancel()
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not stop after cancellation")
	}
	if got := tracker.snapshot().State; got != StateStopped {
		t.Errorf("state = %v, want stopped", got)
	}
}

func TestSupervisorBackoffResetsAfterHealthyRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gate := make(chan struct{})
	src := &gateSource{fails: 1, gate: gate}
	tracker := newStatusTracker("src", "test", KindSource)
	s := newSourceSupervisor(testSourceCfg("src", true), src, noopEmitter(), testLogger(), tracker)
	s.backoffBase = time.Millisecond
	s.backoffMax = 4 * time.Millisecond
	s.backoffReset = 15 * time.Millisecond

	var mu sync.Mutex
	var delays []time.Duration
	s.waitBackoffFn = func(_ context.Context, d time.Duration) bool {
		mu.Lock()
		delays = append(delays, d)
		mu.Unlock()
		return true
	}

	go s.run(ctx)

	waitFor(t, 2*time.Second, func() bool { return tracker.snapshot().State == StateRunning })
	time.Sleep(25 * time.Millisecond) // let the reset timer fire
	close(gate)
	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(delays) >= 2
	})

	mu.Lock()
	d0, d1 := delays[0], delays[1]
	mu.Unlock()
	if d0 != time.Millisecond {
		t.Errorf("first delay = %v, want 1ms", d0)
	}
	if d1 != time.Millisecond {
		t.Errorf("delay after healthy reset = %v, want 1ms (backoff reset)", d1)
	}

	cancel()
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not stop")
	}
}

func TestSupervisorShutdownTimeoutIsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	tracker := newStatusTracker("stuck", "test", KindSource)
	cfg := testSourceCfg("stuck", true)
	cfg.Runtime.ShutdownTimeout = 20 * time.Millisecond
	s := newSourceSupervisor(cfg, &gateSource{gate: make(chan struct{})}, noopEmitter(), testLogger(), tracker)
	go s.run(ctx)

	waitFor(t, 2*time.Second, func() bool { return tracker.snapshot().State == StateRunning })

	start := time.Now()
	cancel()
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not stop; shutdown timeout not enforced")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("shutdown took %v; bounded timeout not honored", elapsed)
	}
}

func TestSupervisorNoRestartConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	tracker := newStatusTracker("once", "test", KindSource)
	s := newSourceSupervisor(testSourceCfg("once", false), failingSource{}, noopEmitter(), testLogger(), tracker)
	go s.run(ctx)

	waitFor(t, 2*time.Second, func() bool { return tracker.snapshot().State == StateStopped })
	if got := tracker.snapshot().RestartCount; got != 0 {
		t.Errorf("restart count = %d, want 0 with restart disabled", got)
	}
	cancel()
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not stop")
	}
}

// TestSupervisorNoRestartAfterCancellation verifies that cancelling during
// the backoff does not schedule another restart (M17).
func TestSupervisorNoRestartAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	tracker := newStatusTracker("src", "test", KindSource)
	s := newSourceSupervisor(testSourceCfg("src", true), failingSource{}, noopEmitter(), testLogger(), tracker)
	s.backoffBase = time.Millisecond

	release := make(chan struct{})
	waits := 0
	var mu sync.Mutex
	s.waitBackoffFn = func(_ context.Context, d time.Duration) bool {
		mu.Lock()
		waits++
		mu.Unlock()
		<-release // block inside the backoff until the test proceeds
		return false
	}

	go s.run(ctx)

	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return waits == 1
	})

	cancel()
	close(release)

	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not stop")
	}

	st := tracker.snapshot()
	if st.State != StateStopped {
		t.Errorf("state = %v, want stopped", st.State)
	}
	if st.RestartCount != 1 {
		t.Errorf("restart count = %d, want 1 (no extra restart after cancel)", st.RestartCount)
	}
}
