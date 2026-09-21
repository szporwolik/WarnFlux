package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
)

// Supervisor defaults; tests in this package replace them via fields.
const (
	defaultBackoffBase  = time.Second
	defaultBackoffMax   = time.Minute
	defaultBackoffReset = time.Minute
)

// sourceSupervisor runs one source plugin instance under recover(), restarts
// it with bounded exponential backoff, and never lets its failures affect
// other plugins or the application.
type sourceSupervisor struct {
	id      string
	kind    string
	plugin  SourcePlugin
	runtime config.SourceRuntime
	emit    Emitter
	logger  *slog.Logger
	tracker *statusTracker
	done    chan struct{}

	// Injectable timing for fast, deterministic tests.
	backoffBase   time.Duration
	backoffMax    time.Duration
	backoffReset  time.Duration
	waitBackoffFn func(ctx context.Context, delay time.Duration) bool
}

func newSourceSupervisor(cfg config.Source, p SourcePlugin, emit Emitter, logger *slog.Logger, tracker *statusTracker) *sourceSupervisor {
	return &sourceSupervisor{
		id:            cfg.ID,
		kind:          cfg.Type,
		plugin:        p,
		runtime:       cfg.Runtime,
		emit:          emit,
		logger:        logger,
		tracker:       tracker,
		done:          make(chan struct{}),
		backoffBase:   defaultBackoffBase,
		backoffMax:    defaultBackoffMax,
		backoffReset:  defaultBackoffReset,
		waitBackoffFn: waitBackoff,
	}
}

// run executes the plugin until ctx is cancelled. The plugin is always
// invoked in its own goroutine under recover(), so a panic becomes a logged
// failure of this plugin only. There is no readiness contract: once the
// plugin goroutine has been launched, the source is considered running.
// A Run that returns while the application is still running is treated as
// an unexpected stop and restarted when restart=true (a polling source that
// completes one pass legitimately should return nil — restarting re-runs it).
func (s *sourceSupervisor) run(ctx context.Context) {
	defer close(s.done)

	s.tracker.markStarted(time.Now())
	attempt := 0

	for {
		s.tracker.setState(StateStarting)
		runCtx, cancel := context.WithCancel(ctx)
		runDone := make(chan runResult, 1)
		go func() {
			runDone <- invokeSource(s.plugin, runCtx, s.emit)
		}()

		// No readiness contract exists: the plugin is running.
		s.tracker.setState(StateRunning)
		s.tracker.success(time.Now())

		if !s.runtime.Restart {
			// Wait for a natural exit without restarting.
			select {
			case <-ctx.Done():
				s.shutdown(cancel, runDone)
				return
			case result := <-runDone:
				cancel()
				s.recordExit(result)
				s.tracker.setState(StateStopped)
				return
			}
		}

		// Restart enabled: wait for exit, reset backoff after a healthy
		// stretch, and restart with bounded backoff afterwards.
		_, shutdown := s.waitForExit(ctx, cancel, runDone, &attempt)
		if shutdown {
			return
		}
		// Never schedule another restart once the root context is cancelled.
		if ctx.Err() != nil {
			s.tracker.setState(StateStopped)
			return
		}
		next, ok := s.restart(ctx, attempt)
		if !ok {
			// The root context was cancelled during backoff: SourcePlugin.Run
			// must never be invoked again.
			return
		}
		attempt = next
	}
}

// waitForExit waits until the running plugin exits or the application shuts
// down. A plugin that runs healthily for backoffReset resets the backoff.
func (s *sourceSupervisor) waitForExit(ctx context.Context, cancel context.CancelFunc, runDone <-chan runResult, attempt *int) (bool, bool) {
	reset := time.NewTimer(s.backoffReset)
	defer reset.Stop()
	for {
		select {
		case <-ctx.Done():
			s.shutdown(cancel, runDone)
			return false, true
		case <-reset.C:
			// Healthy for a full reset period: start fresh next time.
			*attempt = 0
			s.tracker.success(time.Now())
		case result := <-runDone:
			cancel()
			s.recordExit(result)
			s.tracker.setState(StateStopped)
			return true, false
		}
	}
}

// restart waits out the bounded backoff and reports the next attempt index
// together with whether the supervisor may continue. It returns false once
// the root context is cancelled (including a cancellation that arrives
// DURING the backoff wait): after that, SourcePlugin.Run must never be
// invoked again.
func (s *sourceSupervisor) restart(ctx context.Context, attempt int) (int, bool) {
	if ctx.Err() != nil {
		s.tracker.setState(StateStopped)
		return attempt, false
	}
	delay := s.backoffDelay(attempt)
	next := attempt + 1
	s.tracker.restart()
	s.logger.Info("source plugin restarting",
		"plugin_id", s.id, "plugin_type", s.kind, "retry_in", delay)
	if !s.waitBackoffFn(ctx, delay) {
		s.tracker.setState(StateStopped)
		return next, false
	}
	return next, true
}

func (s *sourceSupervisor) backoffDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := s.backoffBase
	for i := 0; i < attempt && delay < s.backoffMax; i++ {
		delay *= 2
		if delay > s.backoffMax {
			delay = s.backoffMax
		}
	}
	return delay
}

// recordExit logs how the plugin run ended.
func (s *sourceSupervisor) recordExit(result runResult) {
	if result.panicStack != nil {
		s.tracker.failure(fmt.Errorf("plugin panic: %v", result.err), 0, time.Now())
		s.logger.Error("source plugin panic",
			"plugin_id", s.id, "plugin_type", s.kind,
			"panic", result.err, "stack", string(result.panicStack))
		return
	}
	if result.err != nil {
		s.tracker.failure(result.err, 0, time.Now())
		s.logger.Error("source plugin failed",
			"plugin_id", s.id, "plugin_type", s.kind, "error", result.err)
		return
	}
	s.tracker.success(time.Now())
	s.logger.Info("source plugin stopped",
		"plugin_id", s.id, "plugin_type", s.kind)
}

// shutdown cancels the plugin and waits (bounded) for it to finish, so a
// misbehaving plugin cannot freeze application shutdown.
func (s *sourceSupervisor) shutdown(cancel context.CancelFunc, runDone <-chan runResult) {
	s.tracker.setState(StateStopping)
	cancel()
	if !waitChannel(runDone, s.runtime.ShutdownTimeout) {
		s.logger.Warn("source plugin failed to stop cleanly",
			"plugin_id", s.id, "plugin_type", s.kind, "timeout", s.runtime.ShutdownTimeout)
	}
	s.tracker.setState(StateStopped)
}

// runResult captures the outcome of one plugin run.
type runResult struct {
	err        error
	panicStack []byte
}

// invokeSource runs the plugin under recover(), converting panics into
// runResult entries with a stack trace. The returned channel never blocks
// because it is buffered.
//
// The named return value is deliberate: deferred functions can only modify
// the function's result when it is named.
func invokeSource(p SourcePlugin, ctx context.Context, emit Emitter) (result runResult) {
	defer func() {
		if r := recover(); r != nil {
			result.err = fmt.Errorf("%v", r)
			result.panicStack = debug.Stack()
		}
	}()
	result.err = p.Run(ctx, emit)
	return result
}

func waitChannel(ch <-chan runResult, timeout time.Duration) bool {
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

// waitBackoff waits for delay or until ctx is cancelled.
func waitBackoff(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
