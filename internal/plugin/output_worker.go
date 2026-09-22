package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

const (
	defaultPollInterval     = time.Second
	defaultRecoveryInterval = 30 * time.Second
	pollBatchSize           = 32
	// maxInfoPending bounds the latest-state information queue per output
	// worker (coalesced by source+producer+key+kind).
	maxInfoPending = 64
)

// outputWorker delivers journaled changes to one output plugin with
// at-least-once semantics: it polls the durable journal, invokes the plugin
// and acknowledges only after a successful delivery. Each output progresses
// independently, so one broken destination cannot delay another.
type outputWorker struct {
	id      string
	kind    string
	plugin  OutputPlugin
	store   storage.EventStore
	logger  *slog.Logger
	tracker *statusTracker
	done    chan struct{}

	timeout          time.Duration
	threshold        int
	pollInterval     time.Duration
	recoveryInterval time.Duration

	// health optionally provides application status for StatusPublisher
	// plugins. It receives the bounded status context so database queries
	// inside status generation can never block hazard delivery indefinitely.
	health func(ctx context.Context) Status

	// mu serializes the small worker state below (channel semaphore).
	mu chan struct{}

	// suspended stops delivery attempts (except periodic recovery probes).
	suspended bool

	// nextProbeAt schedules the next recovery probe for a suspended
	// worker, so probes happen at recoveryInterval granularity rather than
	// on every poll tick.
	nextProbeAt time.Time

	// statusDisabled is set permanently once the plugin's status callback
	// violates its timeout. The heartbeat is auxiliary, so it is disabled
	// for the lifetime of this worker instead of being allowed to block
	// hazard delivery. At most ONE abandoned status goroutine can exist
	// per output because no new one is ever started afterwards.
	statusDisabled bool

	// Information state (only when the plugin implements
	// InformationPublisher): a bounded latest-value queue coalesced by
	// source+producer+key+kind, owned exclusively by this worker so every
	// callback into the plugin has exactly one runtime owner.
	infoPending  map[string]core.InformationMessage
	infoNotify   chan struct{}
	infoDisabled bool // permanently true after a contract-violating callback
}

func newOutputWorker(cfg config.Output, p OutputPlugin, store storage.EventStore, logger *slog.Logger, tracker *statusTracker) *outputWorker {
	w := &outputWorker{
		id:               cfg.ID,
		kind:             cfg.Type,
		plugin:           p,
		store:            store,
		logger:           logger,
		tracker:          tracker,
		done:             make(chan struct{}),
		timeout:          cfg.Runtime.Timeout,
		threshold:        cfg.Runtime.FailureThreshold,
		pollInterval:     defaultPollInterval,
		recoveryInterval: defaultRecoveryInterval,
		mu:               make(chan struct{}, 1),
	}
	if _, ok := p.(InformationPublisher); ok {
		w.infoPending = make(map[string]core.InformationMessage)
		w.infoNotify = make(chan struct{}, 1)
	}
	return w
}

// infoCapable reports whether this worker owns an information queue.
func (w *outputWorker) infoCapable() bool {
	return w.infoNotify != nil
}

// enqueueInfo coalesces a latest-state information message into the
// worker's bounded queue: a newer snapshot with the same
// source+producer+key+kind replaces the pending one. The message is
// deep-copied here so every output owns its payload independently.
func (w *outputWorker) enqueueInfo(message core.InformationMessage) error {
	if !w.infoCapable() {
		return fmt.Errorf("output %q does not support information messages", w.id)
	}
	message = message.Clone()
	key := infoKey(message)
	w.mu <- struct{}{}
	if w.infoDisabled {
		<-w.mu
		return fmt.Errorf("information capability of output %q is disabled (callback violated its timeout)", w.id)
	}
	if _, exists := w.infoPending[key]; !exists && len(w.infoPending) >= maxInfoPending {
		<-w.mu
		return fmt.Errorf("information queue of output %q is full (%d pending keys)", w.id, maxInfoPending)
	}
	w.infoPending[key] = message
	<-w.mu

	// Wake the worker; the notification is coalesced (never blocking).
	select {
	case w.infoNotify <- struct{}{}:
	default:
	}
	return nil
}

func infoKey(m core.InformationMessage) string {
	return m.Source + "\x00" + m.ProducerID + "\x00" + m.Key + "\x00" + m.Kind
}

// run polls the durable journal until ctx is cancelled. The worker is
// running (accepting deliveries) from the moment it starts.
func (w *outputWorker) run(ctx context.Context) {
	defer close(w.done)
	w.tracker.setState(StateRunning)

	poll := time.NewTicker(w.pollInterval)
	defer poll.Stop()

	publisher, isPublisher := w.plugin.(StatusPublisher)
	var nextStatusAt time.Time
	if isPublisher && publisher.StatusInterval() > 0 {
		nextStatusAt = time.Now().Add(publisher.StatusInterval())
	}

	for {
		// Hazard journal delivery has strict priority over information
		// snapshots: a pending poll tick is handled before any queued
		// information message.
		select {
		case <-ctx.Done():
			w.stop()
			return
		case <-poll.C:
			w.pollDeliveries(ctx)
			continue
		default:
		}

		// One status publication per interval, never concurrent with
		// Handle. Status publishing stops permanently for this output once
		// the callback violates its timeout (see publishStatus).
		if isPublisher && w.statusEnabled() && !nextStatusAt.IsZero() && time.Now().After(nextStatusAt) {
			nextStatusAt = time.Now().Add(publisher.StatusInterval())
			w.publishStatus(ctx, publisher)
			continue
		}

		// Wait for the next tick, an information snapshot or shutdown.
		select {
		case <-ctx.Done():
			w.stop()
			return
		case <-poll.C:
			w.pollDeliveries(ctx)
		case <-w.infoNotify:
			w.deliverPendingInfo(ctx)
		}
	}
}

// deliverPendingInfo pops ONE pending information message (latest state,
// coalesced) and invokes the plugin with a bounded per-call timeout and
// panic recovery. A callback that violates its timeout permanently
// disables this output's information capability — at most one abandoned
// information goroutine can ever exist per output. Information failures
// are logged only: they never touch hazard failure accounting.
func (w *outputWorker) deliverPendingInfo(ctx context.Context) {
	w.mu <- struct{}{}
	if w.infoDisabled {
		<-w.mu
		return
	}
	var message core.InformationMessage
	var found bool
	for key, m := range w.infoPending {
		message, found = m, true
		delete(w.infoPending, key)
		break
	}
	<-w.mu
	if !found {
		return
	}

	pub, ok := w.plugin.(InformationPublisher)
	if !ok {
		return
	}

	callCtx, cancel := context.WithTimeout(ctx, w.timeout)
	result := make(chan error, 1)
	go func() {
		result <- invokeInformation(pub, callCtx, message)
	}()

	select {
	case err := <-result:
		cancel()
		if err != nil {
			w.logger.Warn("information publish failed",
				"output_id", w.id,
				"source", message.Source, "producer_id", message.ProducerID,
				"key", message.Key, "kind", message.Kind, "error", err)
		}
	case <-callCtx.Done():
		// Context-contract violation: disable information publishing for
		// this output's lifetime. The abandoned goroutine is bounded — no
		// new information call is ever started.
		w.mu <- struct{}{}
		w.infoDisabled = true
		<-w.mu
		w.logger.Warn("information publish violated its timeout; information capability disabled for this output",
			"output_id", w.id, "timeout", w.timeout)
		select {
		case late := <-result:
			if late != nil {
				w.logger.Warn("information publish returned an error after its timeout",
					"output_id", w.id, "error", late)
			}
		case <-ctx.Done():
		}
		cancel()
	}
}

// invokeInformation runs an information publish under recover(), converting
// panics into errors with a stack trace.
func invokeInformation(p InformationPublisher, ctx context.Context, message core.InformationMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("plugin panic: %v\n%s", r, debug.Stack())
		}
	}()
	return p.PublishInformation(ctx, message)
}

// pollDeliveries fetches unacknowledged changes and delivers them serially
// in journal order. On the first failure it stops, keeping durable order
// and acknowledging only what was actually delivered.
func (w *outputWorker) pollDeliveries(ctx context.Context) {
	if w.isSuspended() && !w.probeDue() {
		return
	}

	changes, err := w.store.PollChanges(ctx, w.id, pollBatchSize)
	if err != nil {
		if ctx.Err() == nil {
			w.onFailure(fmt.Errorf("poll journal: %w", err))
		}
		return
	}

	for _, change := range changes {
		if !w.deliver(ctx, change) {
			return // keep the rest pending for the next attempt
		}
	}
}

// deliver invokes the plugin for one change and acknowledges it only after
// a successful delivery. The worker is serial, so at most one Handle call
// is ever in flight per plugin instance: a handler that ignores its context
// blocks this worker (never the application) and no further calls are made
// to the plugin until the stuck call returns or shutdown abandons it.
func (w *outputWorker) deliver(ctx context.Context, change storage.Change) bool {
	eventChange := core.EventChange{ID: change.ID, Type: change.ChangeType, Event: change.Event.Clone()}

	callCtx, cancel := context.WithTimeout(ctx, w.timeout)
	result := make(chan error, 1)
	go func() {
		result <- invokeOutput(w.plugin, callCtx, eventChange)
	}()

	var err error
	timedOut := false
	select {
	case err = <-result:
	case <-callCtx.Done():
		// ONE failure is recorded for this invocation. The late result is
		// absorbed: a late success is NOT trusted as timely delivery (no
		// ACK — the change stays pending for at-least-once redelivery)
		// and a late error is not counted a second time.
		timedOut = true
		w.onFailure(fmt.Errorf("timed out after %s", w.timeout))
		w.logger.Warn("output plugin timed out; waiting for the stuck call",
			"plugin_id", w.id, "plugin_type", w.kind, "timeout", w.timeout)
		select {
		case late := <-result:
			if late != nil {
				w.logger.Warn("output plugin returned an error after its timeout (already counted as one failure)",
					"plugin_id", w.id, "plugin_type", w.kind, "error", late)
			} else {
				w.logger.Warn("output plugin returned success after its timeout; not trusted as timely delivery, change stays pending",
					"plugin_id", w.id, "plugin_type", w.kind)
			}
		case <-ctx.Done():
			// Shutdown while the plugin is wedged: abandon the stuck
			// call. The goroutine is unkillable, but shutdown must not
			// hang; the change stays pending in the journal for the next
			// process.
			w.logger.Warn("abandoning stuck output call during shutdown",
				"plugin_id", w.id, "plugin_type", w.kind)
		}
	}
	cancel()

	if timedOut {
		// Already accounted: exactly one failure for one invocation.
		return false
	}
	if err != nil {
		w.onFailure(err)
		return false
	}
	if err := w.store.AckChanges(ctx, w.id, change.ID); err != nil {
		w.onFailure(fmt.Errorf("ack change %d: %w", change.ID, err))
		return false
	}
	w.onSuccess()
	return true
}

// publishStatus publishes one application status snapshot. Status health is
// auxiliary: failures are logged only and never count toward the
// delivery-failure threshold. A callback that ignores its timeout violates
// its context contract — status publishing is then disabled for this output
// instance permanently, and hazard delivery continues. The abandoned
// callback may still overlap later Handle calls; the StatusPublisher
// contract documents this.
func (w *outputWorker) publishStatus(ctx context.Context, publisher StatusPublisher) {
	if w.health == nil {
		return
	}

	callCtx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	// Status generation (including its database queries) shares the same
	// bounded context as the callback: a stalled database can make the
	// snapshot degraded, never hang hazard delivery.
	status := w.health(callCtx)
	result := make(chan error, 1)
	go func() {
		result <- invokeStatus(publisher, callCtx, status)
	}()
	select {
	case err := <-result:
		if err != nil {
			// Auxiliary: logged, never counted toward suspension.
			w.logger.Warn("output plugin status publish failed",
				"plugin_id", w.id, "plugin_type", w.kind, "error", err)
		}
	case <-callCtx.Done():
		// The callback violated its context contract. Disable status
		// publishing for this output instance and move on: hazard event
		// delivery must never be blocked by a broken heartbeat. The
		// abandoned goroutine is bounded — no further status call is ever
		// started for this worker.
		w.disableStatus()
		w.logger.Error("output plugin status publish violated its timeout; status publishing disabled for this output",
			"plugin_id", w.id, "plugin_type", w.kind, "timeout", w.timeout)
	}
}

// onFailure records the failure and suspends the plugin once the threshold
// is reached.
func (w *outputWorker) onFailure(err error) {
	suspended := w.tracker.failure(err, w.threshold, time.Now())
	w.logger.Error("output plugin failed",
		"plugin_id", w.id, "plugin_type", w.kind, "error", err)
	if suspended && !w.isSuspended() {
		w.setSuspended(true)
		w.logger.Warn("output plugin suspended",
			"plugin_id", w.id, "plugin_type", w.kind,
			"consecutive_failures", w.tracker.failures())
	}
}

// onSuccess resets the failure counter and reports recovery.
func (w *outputWorker) onSuccess() {
	wasSuspended := w.isSuspended()
	w.tracker.success(time.Now())
	if wasSuspended {
		w.setSuspended(false)
		w.logger.Info("output plugin recovered",
			"plugin_id", w.id, "plugin_type", w.kind)
	}
}

// stop marks the worker stopped and releases plugin resources, bounded by
// the configured timeout and protected by recover.
func (w *outputWorker) stop() {
	w.tracker.setState(StateStopping)
	if closer, ok := w.plugin.(Closer); ok {
		closeCtx, cancel := context.WithTimeout(context.Background(), w.timeout+5*time.Second)
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer func() { _ = recover() }()
			_ = closer.Close()
		}()
		select {
		case <-done:
		case <-closeCtx.Done():
			w.logger.Warn("output plugin failed to close cleanly",
				"plugin_id", w.id, "plugin_type", w.kind)
		}
		cancel()
	}
	w.tracker.setState(StateStopped)
}

func (w *outputWorker) setSuspended(v bool) {
	w.mu <- struct{}{}
	w.suspended = v
	if v {
		w.nextProbeAt = time.Now().Add(w.recoveryInterval)
	}
	<-w.mu
}

func (w *outputWorker) statusEnabled() bool {
	w.mu <- struct{}{}
	defer func() { <-w.mu }()
	return !w.statusDisabled
}

func (w *outputWorker) disableStatus() {
	w.mu <- struct{}{}
	w.statusDisabled = true
	<-w.mu
}

// probeDue reports whether a suspended worker should attempt a recovery
// probe now, rescheduling the next probe.
func (w *outputWorker) probeDue() bool {
	w.mu <- struct{}{}
	defer func() { <-w.mu }()
	now := time.Now()
	if now.Before(w.nextProbeAt) {
		return false
	}
	w.nextProbeAt = now.Add(w.recoveryInterval)
	return true
}

func (w *outputWorker) isSuspended() bool {
	w.mu <- struct{}{}
	defer func() { <-w.mu }()
	return w.suspended
}

// invokeOutput runs the plugin handler under recover(), converting panics
// into errors with a stack trace. The named return value lets the deferred
// recover modify the result.
func invokeOutput(p OutputPlugin, ctx context.Context, change core.EventChange) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("plugin panic: %v\n%s", r, debug.Stack())
		}
	}()
	return p.Handle(ctx, change)
}

// invokeStatus runs PublishStatus under recover().
func invokeStatus(p StatusPublisher, ctx context.Context, status Status) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("plugin panic: %v\n%s", r, debug.Stack())
		}
	}()
	return p.PublishStatus(ctx, status)
}
