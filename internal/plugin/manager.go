package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/ingest"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

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
	Version            string
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

	eventQueue chan core.HazardEvent
	emitWait   time.Duration

	sources   []*sourceSupervisor
	outputs   []*outputWorker
	startedAt time.Time
}

// NewManager builds the plugin instances from the configuration. Unknown
// types, duplicate IDs and malformed plugin configs are startup errors —
// detected even for disabled instances, so typos cannot stay hidden. No
// goroutines are started here.
func NewManager(reg *Registry, sourceCfgs []config.Source, outputCfgs []config.Output, ingestFn IngestFunc, expireFn ExpireFunc, store storage.EventStore, opts ManagerOptions, logger *slog.Logger) (*Manager, error) {
	m := &Manager{
		logger:     logger,
		statuses:   newStatusRegistry(),
		ingestFn:   ingestFn,
		expireFn:   expireFn,
		store:      store,
		opts:       opts,
		eventQueue: make(chan core.HazardEvent, defaultEventQueueSize),
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

// emit deep-copies the event (ownership transfers to the core) and queues
// it. The queue is bounded: when full, the emitter applies backpressure
// with a bounded wait instead of silently dropping hazard events.
func (m *Manager) emit(ctx context.Context, event core.HazardEvent) error {
	event = event.Clone()
	select {
	case m.eventQueue <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(m.emitWait):
		return fmt.Errorf("ingestion queue full (capacity %d)", cap(m.eventQueue))
	}
}

// ingestLoop forwards queued events to the core. A single worker keeps
// same-event transitions strictly ordered. On shutdown it drains the queue
// for a bounded time.
func (m *Manager) ingestLoop(ctlCtx, procCtx context.Context) {
	for {
		select {
		case event := <-m.eventQueue:
			m.process(procCtx, event)
		case <-ctlCtx.Done():
			timer := time.NewTimer(drainTimeout)
			defer timer.Stop()
			for {
				select {
				case event := <-m.eventQueue:
					m.process(procCtx, event)
				case <-timer.C:
					return
				default:
					return
				}
			}
		}
	}
}

// process runs one event through the core. Outputs poll the durable journal
// independently; the manager only logs failures here.
func (m *Manager) process(ctx context.Context, event core.HazardEvent) {
	result, _, err := m.ingestFn(ctx, event)
	if err != nil {
		m.logger.Error("ingest failed",
			"source", event.Source, "source_id", event.SourceID, "error", err)
		return
	}
	if result == ingest.ResultDuplicate {
		return
	}
	// The journal now holds the change; outputs pick it up on their next
	// poll. No in-memory fanout happens here.
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
		}
	}
}

// health builds the application health snapshot for status-publishing
// outputs.
func (m *Manager) health() Status {
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
		pending, oldest, err := m.store.PendingStats(context.Background())
		if err != nil {
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
