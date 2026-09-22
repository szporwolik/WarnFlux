package action

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// InstanceState is the health state of an action instance.
type InstanceState string

const (
	StateHealthy  InstanceState = "healthy"
	StateDegraded InstanceState = "degraded"
	StateDisabled InstanceState = "disabled"
)

// graceAfterTimeout is how long the worker waits after the per-call
// deadline for a plugin to return before declaring it hung. Plugins that
// respect context return essentially immediately after cancellation.
const graceAfterTimeout = 1 * time.Second

// Status is a point-in-time view of one action instance.
type Status struct {
	ID            string
	Type          string
	Enabled       bool
	State         InstanceState
	Reason        string
	QueueDepth    int
	QueueCapacity int
	Handled       int64
	Failures      int64
	LastSuccess   time.Time
	LastError     time.Time
	LastErrorText string
}

// Instance owns the queue, worker goroutine and health of one configured
// action instance.
//
// Isolation contract:
//   - every enabled instance owns a bounded queue and exactly one
//     sequential worker goroutine;
//   - Execute runs under a per-call timeout context and a panic guard;
//   - if a plugin ignores cancellation, its instance is marked disabled
//     for the rest of the process lifetime and the abandoned goroutine is
//     never re-invoked (at most one abandoned goroutine per instance);
//   - a full queue returns a clear Submit error instead of blocking;
//   - shutdown drains each queue within a bounded shutdown_timeout.
type Instance struct {
	id      string
	typ     string
	plugin  Plugin
	queue   chan ActionRequest
	callTO  time.Duration
	closeTO time.Duration
	logger  *slog.Logger

	mu     sync.Mutex
	status Status

	disabled atomic.Bool
	handled  atomic.Int64
	failures atomic.Int64

	wg    sync.WaitGroup
	start sync.Once
}

// NewInstance creates an action instance with its bounded queue. The
// worker is not started until Start is called.
func NewInstance(id, typ string, p Plugin, queueSize int, callTimeout, shutdownTimeout time.Duration, logger *slog.Logger) *Instance {
	return &Instance{
		id:      id,
		typ:     typ,
		plugin:  p,
		queue:   make(chan ActionRequest, queueSize),
		callTO:  callTimeout,
		closeTO: shutdownTimeout,
		logger:  logger,
		status: Status{
			ID:            id,
			Type:          typ,
			Enabled:       true,
			State:         StateHealthy,
			QueueCapacity: queueSize,
		},
	}
}

// Start launches the single worker goroutine. It is idempotent.
func (i *Instance) Start(ctx context.Context) {
	i.start.Do(func() {
		i.wg.Add(1)
		go i.run(ctx)
	})
}

// run is the worker loop: sequential Execute calls, drain on cancel.
func (i *Instance) run(ctx context.Context) {
	defer i.wg.Done()
	for {
		select {
		case <-ctx.Done():
			i.drainAndClose()
			return
		case req := <-i.queue:
			if i.disabled.Load() {
				// Hung plugin is disabled: never invoke it again.
				continue
			}
			i.handle(req)
		}
	}
}

// drainAndClose processes already-queued requests within the bounded
// shutdown_timeout, then closes the plugin with a bounded timeout.
// A broken plugin cannot extend shutdown beyond the configured bounds.
func (i *Instance) drainAndClose() {
	drainCtx, cancel := context.WithTimeout(context.Background(), i.closeTO)
	defer cancel()

	i.logger.Info("action draining", "action", i.id, "queued", len(i.queue))
drain:
	for {
		select {
		case <-drainCtx.Done():
			i.logger.Warn("action drain deadline reached, discarding remaining requests",
				"action", i.id, "queued", len(i.queue))
			break drain
		case req := <-i.queue:
			if i.disabled.Load() {
				continue
			}
			i.handle(req)
		default:
			break drain
		}
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), i.closeTO)
	defer closeCancel()
	done := make(chan struct{})
	go func() {
		defer func() {
			if p := recover(); p != nil {
				i.logger.Error("action Close panicked", "action", i.id, "panic", p)
			}
			close(done)
		}()
		_ = i.plugin.Close(closeCtx)
	}()
	select {
	case <-done:
	case <-closeCtx.Done():
		// At most one abandoned goroutine per broken instance, at shutdown only.
		i.logger.Warn("action Close ignored cancellation; abandoning", "action", i.id)
	}
}

// handle invokes one plugin call under the per-call timeout and a panic
// guard (a panicking plugin degrades the instance, never the process).
func (i *Instance) handle(req ActionRequest) {
	i.handled.Add(1)

	callCtx, cancel := context.WithTimeout(context.Background(), i.callTO)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				done <- fmt.Errorf("panic: %v", p)
			}
		}()
		done <- i.plugin.Execute(callCtx, req)
	}()

	select {
	case err := <-done:
		i.recordResult(err)
	case <-callCtx.Done():
		// Deadline hit. Give a context-respecting plugin a short grace
		// period to return; if it does not, it ignored cancellation and
		// is hung. In that case we disable it and abandon the single
		// in-flight goroutine.
		select {
		case err := <-done:
			i.recordResult(fmt.Errorf("callback exceeded %s: %w", i.callTO, err))
		case <-time.After(graceAfterTimeout):
			i.disable(fmt.Sprintf("callback exceeded %s and ignored cancellation", i.callTO))
		}
	}
}

func (i *Instance) recordResult(err error) {
	now := time.Now()
	i.mu.Lock()
	if err != nil {
		f := i.failures.Add(1)
		i.status.State = StateDegraded
		i.status.Failures = f
		i.status.LastError = now
		i.status.LastErrorText = err.Error()
	} else {
		i.failures.Store(0)
		i.status.State = StateHealthy
		i.status.Failures = 0
		i.status.LastSuccess = now
		i.status.LastErrorText = ""
	}
	i.mu.Unlock()

	if err != nil {
		i.logger.Error("action call failed", "action", i.id, "type", i.typ, "error", err)
	}
}

// disable permanently marks the instance disabled for the rest of the
// process lifetime. The abandoned goroutine is never re-invoked.
func (i *Instance) disable(reason string) {
	i.disabled.Store(true)
	i.mu.Lock()
	i.status.State = StateDisabled
	i.status.Reason = reason
	i.mu.Unlock()
	i.logger.Error("action disabled for the rest of the process lifetime",
		"action", i.id, "reason", reason)
}

// Status returns a point-in-time copy of the instance status.
func (i *Instance) Status() Status {
	i.mu.Lock()
	defer i.mu.Unlock()
	s := i.status
	s.QueueDepth = len(i.queue)
	s.Handled = i.handled.Load()
	if s.State != StateDisabled {
		s.Failures = i.failures.Load()
	}
	return s
}

// DisabledInstanceStatus returns the status for an action disabled in
// configuration (no queue, no worker).
func DisabledInstanceStatus(id, typ string, queueSize int) Status {
	return Status{
		ID:            id,
		Type:          typ,
		Enabled:       false,
		State:         StateDisabled,
		Reason:        "disabled in configuration",
		QueueCapacity: queueSize,
	}
}
