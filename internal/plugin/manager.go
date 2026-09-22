package plugin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/ingest"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// errShuttingDown is returned by Emit once the manager has started its
// shutdown sequence: the event was not accepted and the source may retry or
// drop it, but Emit's contract (nil = accepted) is preserved.
var errShuttingDown = errors.New("warnflux is shutting down; event not accepted")

const (
	defaultEventQueueSize = 256
	emitWaitTimeout       = 5 * time.Second
	drainTimeout          = 5 * time.Second
	groupShutdownGrace    = 5 * time.Second
)

// IngestFunc is the core ingestion entry point injected by the application.
type IngestFunc func(ctx context.Context, event core.HazardEvent) (ingest.Result, core.EventChange, error)

// ExpireFunc atomically expires stale events through the same durable
// journal used by ingestion.
type ExpireFunc func(ctx context.Context, now time.Time) ([]core.EventChange, error)

// ManagerOptions configures runtime maintenance.
type ManagerOptions struct {
	ExpirationInterval time.Duration
	ChangeRetention    time.Duration
	// EventRetention is how long cancelled/expired current-state records
	// are kept before cleanup; active events are never cleaned.
	EventRetention time.Duration
	Version        string
}

// Manager owns the plugin instances and the routing between sources, the
// core ingestion pipeline and outputs:
//
//	sources → bounded queue → ONE ordered ingest worker → SQLite event
//	state + durable change journal → independent per-output workers
//
// Because ingestion is a single worker and the journal assigns monotonic
// change IDs, transitions of the same event can never be reordered.
type Manager struct {
	logger   *slog.Logger
	statuses *StatusRegistry
	ingestFn IngestFunc
	expireFn ExpireFunc
	store    storage.EventStore
	opts     ManagerOptions

	eventQueue chan *queuedEvent
	emitWait   time.Duration

	sources   []*sourceSupervisor
	outputs   []*outputWorker
	startedAt time.Time

	// accepting is false once shutdown begins: Emit then rejects events
	// instead of accepting ownership it can no longer honor.
	accepting atomic.Bool
}

// NewManager builds the plugin instances from the configuration. Unknown
// plugin types and duplicate IDs are startup errors, detected even for
// disabled instances. Plugin-specific configuration is decoded and
// validated only when the plugin is enabled (factories may read secret
// files or initialize local resources). No goroutines are started here.
func NewManager(reg *Registry, sourceCfgs []config.Source, outputCfgs []config.Output, ingestFn IngestFunc, expireFn ExpireFunc, store storage.EventStore, opts ManagerOptions, logger *slog.Logger) (*Manager, error) {
	m := &Manager{
		logger:     logger,
		statuses:   newStatusRegistry(),
		ingestFn:   ingestFn,
		expireFn:   expireFn,
		store:      store,
		opts:       opts,
		eventQueue: make(chan *queuedEvent, defaultEventQueueSize),
		emitWait:   emitWaitTimeout,
	}

	for _, cfg := range sourceCfgs {
		tracker := m.statuses.add(cfg.ID, cfg.Type, KindSource)
		// Validate the type even when disabled: configuration typos must
		// fail at startup, not months later when the plugin is enabled.
		factory, err := reg.Source(cfg.Type)
		if err != nil {
			return nil, fmt.Errorf("source %q: %w", cfg.ID, err)
		}
		if !cfg.Enabled {
			tracker.setState(StateDisabled)
			continue
		}
		p, err := factory(cfg.Config)
		if err != nil {
			return nil, fmt.Errorf("source %q (type %q): invalid configuration: %w", cfg.ID, cfg.Type, err)
		}
		m.sources = append(m.sources, newSourceSupervisor(cfg, p, EmitterFunc(m.emit), logger, tracker))
	}

	for _, cfg := range outputCfgs {
		tracker := m.statuses.add(cfg.ID, cfg.Type, KindOutput)
		factory, err := reg.Output(cfg.Type)
		if err != nil {
			return nil, fmt.Errorf("output %q: %w", cfg.ID, err)
		}
		if !cfg.Enabled {
			tracker.setState(StateDisabled)
			continue
		}
		p, err := factory(cfg.Config)
		if err != nil {
			return nil, fmt.Errorf("output %q (type %q): invalid configuration: %w", cfg.ID, cfg.Type, err)
		}
		w := newOutputWorker(cfg, p, store, logger, tracker)
		w.health = m.health
		m.outputs = append(m.outputs, w)
	}
	return m, nil
}

// Statuses returns a snapshot of every configured plugin instance.
func (m *Manager) Statuses() []PluginStatus { return m.statuses.Snapshot() }

// Run starts all workers and blocks until ctx is cancelled, then shuts
// everything down with bounded timeouts:
//
//  1. stop sources (they stop emitting)
//  2. drain the already-queued events (bounded)
//  3. stop output workers and close plugin resources
//
// Run must be called exactly once per Manager.
func (m *Manager) Run(ctx context.Context) {
	m.startedAt = time.Now()
	m.accepting.Store(true)

	ingestCtl, cancelIngest := context.WithCancel(context.Background())
	procCtx, cancelProc := context.WithCancel(context.Background())
	outCtx, cancelOut := context.WithCancel(context.Background())
	defer cancelProc()

	var wgSources, wgIngest, wgOutputs, wgMaint sync.WaitGroup

	for _, w := range m.outputs {
		wgOutputs.Add(1)
		go func(w *outputWorker) {
			defer wgOutputs.Done()
			w.run(outCtx)
		}(w)
	}

	// ONE ordered ingestion worker: correctness over throughput.
	wgIngest.Add(1)
	go func() {
		defer wgIngest.Done()
		m.ingestLoop(ingestCtl, procCtx)
	}()

	for _, s := range m.sources {
		wgSources.Add(1)
		go func(s *sourceSupervisor) {
			defer wgSources.Done()
			s.run(ctx)
		}(s)
	}

	// Maintenance: expiration and journal retention.
	wgMaint.Add(1)
	go func() {
		defer wgMaint.Done()
		m.maintenance(ctx)
	}()

	<-ctx.Done()
	m.logger.Info("plugin manager stopping", "sources", len(m.sources), "outputs", len(m.outputs))

	// Stop accepting new events BEFORE stopping the sources: an Emit that
	// races with shutdown returns errShuttingDown instead of silently
	// taking ownership of an event the drain may no longer process.
	m.accepting.Store(false)

	// 1. Stop the sources so no new events enter the queue. Each source is
	// already bounded by its own shutdown timeout inside the supervisor.
	m.stopGroup(&wgSources, "sources", groupShutdownGrace)

	// 2. Stop ingestion after a bounded drain of already-queued events.
	cancelIngest()
	m.stopGroup(&wgIngest, "ingestion", drainTimeout+groupShutdownGrace)
	cancelProc()

	// 3. Stop maintenance.
	m.stopGroup(&wgMaint, "maintenance", groupShutdownGrace)

	// 4. Stop the output workers; plugin resources are closed with bounded
	// timeouts inside each worker.
	cancelOut()
	m.stopGroup(&wgOutputs, "outputs", maxOutputTimeout(m.outputs)+groupShutdownGrace)

	m.logger.Info("plugin manager stopped")
}

// EmitterFunc adapts a function to the Emitter interface.
type EmitterFunc func(ctx context.Context, event core.HazardEvent) error

// Emit implements Emitter.
func (f EmitterFunc) Emit(ctx context.Context, event core.HazardEvent) error { return f(ctx, event) }

// queuedEvent couples a normalized event with the channel that completes
// once ingestion has durably classified it.
type queuedEvent struct {
	event core.HazardEvent
	done  chan error // buffered 1; the ingest worker always completes it
}

// emit deep-copies the event (ownership transfers to the core) and queues
// it with bounded backpressure. It returns nil ONLY after the ingest worker
// has durably persisted and classified the event (durable Emit
// acknowledgment): a source that sees nil can rely on the event being in
// SQLite and the journal.
//
// A non-nil error means the event was NOT persisted; the source may retry
// (identity + fingerprint dedup make retries safe). During shutdown Emit
// returns errShuttingDown instead of accepting ownership it cannot honor.
func (m *Manager) emit(ctx context.Context, event core.HazardEvent) error {
	event = event.Clone()
	if !m.accepting.Load() {
		return errShuttingDown
	}
	// Check cancellation BEFORE selecting on the queue: when ctx is already
	// done, sending must never win over cancellation.
	if err := ctx.Err(); err != nil {
		return err
	}
	qe := &queuedEvent{event: event, done: make(chan error, 1)}
	select {
	case m.eventQueue <- qe:
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(m.emitWait):
		return fmt.Errorf("ingestion queue full (capacity %d)", cap(m.eventQueue))
	}
	// The core owns the event now. Caller cancellation after enqueue must
	// not create ambiguous ownership: wait for the durable ingest outcome.
	// Ingest is bounded by SQLite's busy_timeout, and shutdown completes
	// the drain with an error, so this cannot hang forever.
	return <-qe.done
}

// ingestLoop forwards queued events to the core. A single worker keeps
// same-event transitions strictly ordered. On shutdown it drains the queue
// for a bounded time; anything left over is completed with errShuttingDown
// so no source ever waits on an unanswered Emit.
func (m *Manager) ingestLoop(ctlCtx, procCtx context.Context) {
	for {
		select {
		case qe := <-m.eventQueue:
			m.processEvent(procCtx, qe)
		case <-ctlCtx.Done():
			timer := time.NewTimer(drainTimeout)
			defer timer.Stop()
			for len(m.eventQueue) > 0 {
				select {
				case qe := <-m.eventQueue:
					m.processEvent(procCtx, qe)
				case <-timer.C:
					m.failQueued()
					return
				}
			}
			m.failQueued() // no-op when the queue is already empty
			return
		}
	}
}

// processEvent runs one event through the core and completes its durable
// acknowledgment. Outputs poll the durable journal independently.
func (m *Manager) processEvent(ctx context.Context, qe *queuedEvent) {
	_, _, err := m.ingestFn(ctx, qe.event)
	if err != nil {
		m.logger.Error("ingest failed",
			"source", qe.event.Source, "source_id", qe.event.SourceID, "error", err)
	}
	qe.done <- err // buffered: never blocks
}

// failQueued completes every still-queued event with errShuttingDown.
func (m *Manager) failQueued() {
	for {
		select {
		case qe := <-m.eventQueue:
			qe.done <- errShuttingDown
		default:
			return
		}
	}
}

// maintenance expires stale events and conservatively cleans the journal.
func (m *Manager) maintenance(ctx context.Context) {
	interval := m.opts.ExpirationInterval
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			if m.expireFn != nil {
				if _, err := m.expireFn(ctx, now); err != nil && ctx.Err() == nil {
					m.logger.Warn("expiration check failed", "error", err)
				}
			}
			if m.store != nil && m.opts.ChangeRetention > 0 {
				if n, err := m.store.CleanupChanges(ctx, now.Add(-m.opts.ChangeRetention)); err != nil {
					if ctx.Err() == nil {
						m.logger.Warn("journal cleanup failed", "error", err)
					}
				} else if n > 0 {
					m.logger.Info("journal cleanup", "deleted_changes", n)
				}
			}
			if m.store != nil && m.opts.EventRetention > 0 {
				if n, err := m.store.CleanupEvents(ctx, now.Add(-m.opts.EventRetention)); err != nil {
					if ctx.Err() == nil {
						m.logger.Warn("event cleanup failed", "error", err)
					}
				} else if n > 0 {
					m.logger.Info("event cleanup", "deleted_events", n)
				}
			}
		}
	}
}

// health builds the application health snapshot for status-publishing
// outputs. The context bounds the database queries: status generation is
// auxiliary and must never block hazard delivery indefinitely.
func (m *Manager) health(ctx context.Context) Status {
	status := Status{
		Version: m.opts.Version,
		Uptime:  time.Since(m.startedAt),
	}
	all := m.statuses.Snapshot()
	for _, p := range all {
		if p.Kind == KindSource {
			status.Sources = append(status.Sources, p)
		} else {
			status.Outputs = append(status.Outputs, p)
		}
	}
	if m.store != nil {
		pending, oldest, err := m.store.PendingStats(ctx)
		if err != nil {
			// database_healthy means "the last status DB query succeeded",
			// not a full integrity check.
			status.DatabaseHealthy = false
			m.logger.Warn("pending stats unavailable", "error", err)
		} else {
			status.DatabaseHealthy = true
			status.PendingChanges = pending
			status.OldestPendingAge = oldest
		}
	}
	return status
}

// stopGroup waits for a worker group with a bounded timeout and logs when
// something fails to stop in time.
func (m *Manager) stopGroup(wg *sync.WaitGroup, name string, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		// The remaining goroutines belong to misbehaving plugins; the
		// process can still exit because Run returns.
		m.logger.Warn("plugin group did not stop in time", "group", name, "timeout", timeout)
	}
}

func maxOutputTimeout(outputs []*outputWorker) time.Duration {
	max := time.Duration(0)
	for _, w := range outputs {
		if w.timeout > max {
			max = w.timeout
		}
	}
	return max
}
