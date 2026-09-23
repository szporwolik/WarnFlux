package action

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/szporwolik/WarnFlux/internal/metrics"
	"github.com/szporwolik/WarnFlux/internal/trail"
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

// RetryBackoff is the fixed delay between failed attempts and the next
// one. It is deliberately simple: action traffic is low-volume and a
// sequential worker needs no exponential scheduler. Exported only so
// tests can shorten it; production code never mutates it.
var RetryBackoff = 5 * time.Second

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

	// trail is the optional per-alert audit recorder; retries is the
	// number of extra delivery attempts after the first failure.
	trail   *trail.Recorder
	retries int

	// metric cells (optional, nil-safe).
	metricDelivered func(int64)
	metricFailed    func(int64)
	metricRetry     func(int64)

	mu     sync.Mutex
	status Status

	disabled atomic.Bool
	handled  atomic.Int64
	failures atomic.Int64

	wg    sync.WaitGroup
	start sync.Once
}

// NewInstance creates an action instance with its bounded queue. The
// worker is not started until Start is called. trail and reg may be nil.
func NewInstance(id, typ string, p Plugin, queueSize int, callTimeout, shutdownTimeout time.Duration, logger *slog.Logger, trail *trail.Recorder, retries int, reg *metrics.Registry) *Instance {
	if retries < 0 {
		retries = 0
	}
	inst := &Instance{
		id:              id,
		typ:             typ,
		plugin:          p,
		queue:           make(chan ActionRequest, queueSize),
		callTO:          callTimeout,
		closeTO:         shutdownTimeout,
		logger:          logger,
		trail:           trail,
		retries:         retries,
		metricDelivered: func(int64) {},
		metricFailed:    func(int64) {},
		metricRetry:     func(int64) {},
		status: Status{
			ID:            id,
			Type:          typ,
			Enabled:       true,
			State:         StateHealthy,
			QueueCapacity: queueSize,
		},
	}
	if reg != nil {
		inst.metricDelivered = reg.Counter("warnflux_notifications_total",
			"Notification outcomes per action.", "action", id, "result", "delivered")
		inst.metricFailed = reg.Counter("warnflux_notifications_total",
			"Notification outcomes per action.", "action", id, "result", "failed")
		inst.metricRetry = reg.Counter("warnflux_notification_retry_total",
			"Delivery retry attempts per action.", "action", id)
	}
	return inst
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
// Failed calls are retried up to the configured retry count with a fixed
// backoff; every attempt and its result land in the audit trail.
func (i *Instance) handle(req ActionRequest) {
	i.handled.Add(1)

	// Audit trail anchor: only hazard transitions have an event key.
	key := ""
	if req.Event.Hazard != nil {
		key = req.Event.Hazard.Key
	}

	total := i.retries + 1
	for attempt := 0; ; attempt++ {
		err := i.executeOnce(req)
		if err == nil {
			i.metricDelivered(1)
			if attempt > 0 {
				i.trail.Add(key, trail.StepDelivered,
					fmt.Sprintf("delivered after %d retr%s", attempt, plural(attempt)), time.Now())
			} else {
				i.trail.Add(key, trail.StepDelivered, "delivered", time.Now())
			}
			i.trail.SetOutcome(key, trail.OutcomeDelivered)
			return
		}

		i.metricFailed(1)
		i.trail.Add(key, trail.StepFailed,
			fmt.Sprintf("attempt %d/%d failed: %v", attempt+1, total, err), time.Now())
		if i.disabled.Load() {
			// A hung plugin was disabled mid-attempt: never re-invoke it.
			i.trail.SetOutcome(key, trail.OutcomeFailed)
			return
		}
		if attempt < i.retries {
			i.metricRetry(1)
			i.trail.Add(key, trail.StepRetry,
				fmt.Sprintf("retrying in %s", RetryBackoff), time.Now())
			time.Sleep(RetryBackoff)
			continue
		}
		i.trail.SetOutcome(key, trail.OutcomeFailed)
		return
	}
}

// plural returns "y" for 1 and "ies" otherwise ("1 retry", "2 retries").
func plural(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}

// executeOnce runs a single plugin call under the per-call timeout and
// panic guard, updating the instance health status. It returns the
// plugin's error (or a timeout wrapper).
func (i *Instance) executeOnce(req ActionRequest) error {
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
		return err
	case <-callCtx.Done():
		// Deadline hit. Give a context-respecting plugin a short grace
		// period to return; if it does not, it ignored cancellation and
		// is hung. In that case we disable it and abandon the single
		// in-flight goroutine.
		select {
		case err := <-done:
			err = fmt.Errorf("callback exceeded %s: %w", i.callTO, err)
			i.recordResult(err)
			return err
		case <-time.After(graceAfterTimeout):
			i.disable(fmt.Sprintf("callback exceeded %s and ignored cancellation", i.callTO))
			return fmt.Errorf("callback exceeded %s and ignored cancellation", i.callTO)
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
