package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"warnflux/internal/config"
	"warnflux/internal/core"
)

const (
	defaultOutputQueueSize   = 64
	outputQueueWait          = 5 * time.Second
	defaultRecoveryInterval  = 30 * time.Second
	outputShutdownGraceExtra = 5 * time.Second
)

// outputWorker delivers EventChange values to one output plugin through a
// dedicated bounded queue, with a per-call timeout, panic recovery and a
// lightweight circuit breaker. A slow or failing output never blocks other
// outputs or the core pipeline.
type outputWorker struct {
	id      string
	kind    string
	plugin  OutputPlugin
	timeout time.Duration
	logger  *slog.Logger
	tracker *statusTracker
	done    chan struct{}

	queue            chan core.EventChange
	threshold        int
	recoveryInterval time.Duration
	queueWait        time.Duration

	mu        chan struct{} // guards suspended
	suspended bool
}

func newOutputWorker(cfg config.Output, p OutputPlugin, logger *slog.Logger, tracker *statusTracker) *outputWorker {
	return &outputWorker{
		id:               cfg.ID,
		kind:             cfg.Type,
		plugin:           p,
		timeout:          cfg.Runtime.Timeout,
		logger:           logger,
		tracker:          tracker,
		done:             make(chan struct{}),
		queue:            make(chan core.EventChange, defaultOutputQueueSize),
		threshold:        cfg.Runtime.FailureThreshold,
		recoveryInterval: defaultRecoveryInterval,
		queueWait:        outputQueueWait,
		mu:               make(chan struct{}, 1),
	}
}

// run consumes the queue until ctx is cancelled. While suspended, the
// worker only attempts one delivery per recovery interval, so a permanently
// broken destination is not hammered.
func (w *outputWorker) run(ctx context.Context) {
	defer close(w.done)
	w.tracker.setState(StateStarting)

	for {
		if w.isSuspended() {
			select {
			case <-ctx.Done():
				w.stop(ctx)
				return
			case <-time.After(w.recoveryInterval):
			}
		}

		select {
		case <-ctx.Done():
			w.stop(ctx)
			return
		case change := <-w.queue:
			w.deliver(ctx, change)
		}
	}
}

// deliver invokes the plugin in an isolated goroutine with a bounded
// timeout. A plugin that ignores its context leaves a stuck goroutine
// behind (Go cannot kill goroutines); suspension stops feeding it, which
// keeps the number of stuck goroutines bounded by the failure threshold.
func (w *outputWorker) deliver(ctx context.Context, change core.EventChange) {
	callCtx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- invokeOutput(w.plugin, callCtx, change)
	}()

	select {
	case err := <-result:
		if err != nil {
			w.onFailure(err)
			return
		}
		w.onSuccess()
	case <-callCtx.Done():
		// Timed out; the plugin goroutine is abandoned (bounded by the
		// suspension mechanism).
		w.onFailure(fmt.Errorf("timed out after %s", w.timeout))
		w.logger.Warn("output plugin timed out; plugin suspended",
			"plugin_id", w.id, "plugin_type", w.kind)
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

// submit hands a change to the worker's bounded queue with backpressure
// instead of silently dropping it.
func (w *outputWorker) submit(ctx context.Context, change core.EventChange) error {
	select {
	case w.queue <- change:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(w.queueWait):
		return fmt.Errorf("output %q queue full (capacity %d)", w.id, cap(w.queue))
	}
}

// stop marks the worker stopped and releases plugin resources, bounded by
// the configured timeout and protected by recover.
func (w *outputWorker) stop(ctx context.Context) {
	w.tracker.setState(StateStopping)
	if closer, ok := w.plugin.(Closer); ok {
		closeCtx, cancel := context.WithTimeout(context.Background(), w.timeout+outputShutdownGraceExtra)
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
	<-w.mu
}

func (w *outputWorker) isSuspended() bool {
	w.mu <- struct{}{}
	defer func() { <-w.mu }()
	return w.suspended
}

// invokeOutput runs the plugin handler under recover(), converting panics
// into errors with a stack trace logged by the caller.
func invokeOutput(p OutputPlugin, ctx context.Context, change core.EventChange) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("plugin panic: %v\n%s", r, debug.Stack())
		}
	}()
	return p.Handle(ctx, change)
}
