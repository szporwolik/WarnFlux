package plugin

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"warnflux/internal/config"
	"warnflux/internal/core"
)

// recOutput records delivered changes; behavior driven by fields.
type recOutput struct {
	name string

	mu  sync.Mutex
	got []core.EventChange

	failFor int // first N deliveries return an error
	err     error
	panic   bool
	gate    chan struct{} // when non-nil, Handle blocks until closed (ignoring ctx)
	closed  bool
}

func (o *recOutput) Name() string { return o.name }

func (o *recOutput) Handle(ctx context.Context, change core.EventChange) error {
	o.mu.Lock()
	o.got = append(o.got, change)
	// failFor: -1 fails every call, N>0 fails the next N calls, 0 never fails.
	fail := o.failFor != 0
	if fail && o.failFor > 0 {
		o.failFor--
	}
	panicMode := o.panic
	gate := o.gate
	o.mu.Unlock()

	if panicMode {
		panic("output boom")
	}
	if gate != nil {
		<-gate
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

func change() core.EventChange {
	return core.EventChange{
		Type:  core.ChangeNew,
		Event: core.HazardEvent{Source: "test", SourceID: "1", Event: "E"},
	}
}

func TestOutputWorkerDelivers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	out := &recOutput{name: "test"}
	tracker := newStatusTracker("out", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("out", time.Second, 3), out, testLogger(), tracker)
	go w.run(ctx)

	if err := w.submit(ctx, change()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return out.count() == 1 })
	if got := tracker.snapshot().State; got != StateRunning {
		t.Errorf("state = %v, want running", got)
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

	out := &recOutput{name: "test", panic: true}
	tracker := newStatusTracker("out", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("out", time.Second, 3), out, testLogger(), tracker)
	go w.run(ctx)

	if err := w.submit(ctx, change()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return tracker.failures() >= 1 })
	st := tracker.snapshot()
	if !strings.Contains(st.LastError, "boom") {
		t.Errorf("LastError = %q, want panic value", st.LastError)
	}
}

func TestOutputWorkerSuspendsAfterThreshold(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := &recOutput{name: "test", err: errors.New("destination down"), failFor: -1}
	tracker := newStatusTracker("out", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("out", time.Second, 2), out, testLogger(), tracker)
	w.recoveryInterval = time.Hour // no recovery probes in this test
	go w.run(ctx)

	if err := w.submit(ctx, change()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if err := w.submit(ctx, change()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return tracker.snapshot().State == StateSuspended })

	// While suspended, further changes queue up but are not delivered.
	if err := w.submit(ctx, change()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if err := w.submit(ctx, change()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if out.count() != 2 {
		t.Errorf("deliveries = %d, want 2 (no hammering while suspended)", out.count())
	}
}

func TestOutputWorkerRecoversAfterProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := &recOutput{name: "test", err: errors.New("temporary"), failFor: 2}
	tracker := newStatusTracker("out", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("out", time.Second, 2), out, testLogger(), tracker)
	w.recoveryInterval = 5 * time.Millisecond
	go w.run(ctx)

	if err := w.submit(ctx, change()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if err := w.submit(ctx, change()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return tracker.snapshot().State == StateSuspended })

	// A new change after suspension becomes the recovery probe; it
	// succeeds and resets the failure counter.
	if err := w.submit(ctx, change()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		st := tracker.snapshot()
		return st.State == StateRunning && st.ConsecutiveFailures == 0
	})
	if out.count() < 3 {
		t.Errorf("deliveries = %d, want at least 3 (probe included)", out.count())
	}
}

func TestOutputWorkerTimeoutSuspendsAndIsolates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slow := &recOutput{name: "slow", gate: make(chan struct{})}
	slowTracker := newStatusTracker("slow", "test", KindOutput)
	slowWorker := newOutputWorker(testOutputCfg("slow", 20*time.Millisecond, 1), slow, testLogger(), slowTracker)
	go slowWorker.run(ctx)

	fast := &recOutput{name: "fast"}
	fastTracker := newStatusTracker("fast", "test", KindOutput)
	fastWorker := newOutputWorker(testOutputCfg("fast", time.Second, 3), fast, testLogger(), fastTracker)
	go fastWorker.run(ctx)

	ch := change()
	if err := slowWorker.submit(ctx, ch); err != nil {
		t.Fatalf("slow submit: %v", err)
	}
	if err := fastWorker.submit(ctx, ch); err != nil {
		t.Fatalf("fast submit: %v", err)
	}

	// The fast output is delivered immediately; the slow one times out and
	// gets suspended without affecting the fast one.
	waitFor(t, 2*time.Second, func() bool { return fast.count() == 1 })
	if got := fastTracker.snapshot().State; got != StateRunning {
		t.Errorf("fast state = %v, want running", got)
	}
	waitFor(t, 2*time.Second, func() bool { return slowTracker.snapshot().State == StateSuspended })

	close(slow.gate) // release the stuck handler goroutine
}

func TestOutputQueueSaturationReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := &recOutput{name: "busy", gate: make(chan struct{})}
	tracker := newStatusTracker("busy", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("busy", time.Second, 5), out, testLogger(), tracker)
	w.queue = make(chan core.EventChange, 2)
	w.queueWait = 20 * time.Millisecond
	go w.run(ctx)

	// First change is consumed and blocks in Handle; two more fill the
	// queue; the fourth must fail with backpressure instead of dropping.
	if err := w.submit(ctx, change()); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	waitFor(t, time.Second, func() bool { return out.count() == 1 })
	if err := w.submit(ctx, change()); err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	if err := w.submit(ctx, change()); err != nil {
		t.Fatalf("submit 3: %v", err)
	}
	err := w.submit(ctx, change())
	if err == nil {
		t.Fatal("expected queue-full error, got nil")
	}
	if !strings.Contains(err.Error(), "queue full") {
		t.Errorf("error = %v, want queue full", err)
	}
	close(out.gate)
}

func TestOutputWorkerSubmitRespectsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	out := &recOutput{name: "test"}
	tracker := newStatusTracker("out", "test", KindOutput)
	w := newOutputWorker(testOutputCfg("out", time.Second, 3), out, testLogger(), tracker)
	w.queue = make(chan core.EventChange, 1)
	w.queueWait = time.Minute

	if err := w.submit(ctx, change()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	cancel()
	if err := w.submit(ctx, change()); !errors.Is(err, context.Canceled) {
		t.Errorf("submit after cancel = %v, want context.Canceled", err)
	}
}
