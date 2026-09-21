package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"warnflux/internal/config"
	"warnflux/internal/core"
	"warnflux/internal/ingest"
)

const (
	defaultEventQueueSize = 256
	defaultIngestWorkers  = 2

	emitWaitTimeout    = 5 * time.Second
	drainTimeout       = 5 * time.Second
	groupShutdownGrace = 5 * time.Second
)

// IngestFunc is the core ingestion entry point injected by the application.
type IngestFunc func(ctx context.Context, event core.HazardEvent) (ingest.Result, core.EventChange, error)

// Manager owns the plugin instances and the routing between sources, the
// core ingestion pipeline and outputs:
//
//	sources → bounded queue → ingest workers → EventChange → output workers
//
// Each source runs under its own supervisor and each output under its own
// worker, so one plugin failure stays a local plugin failure.
type Manager struct {
	logger   *slog.Logger
	statuses *StatusRegistry
	ingestFn IngestFunc

	eventQueue    chan core.HazardEvent
	ingestWorkers int
	emitWait      time.Duration

	sources []*sourceSupervisor
	outputs []*outputWorker
}

// NewManager builds the enabled plugin instances from the configuration.
// Configuration problems (unknown type, malformed plugin config) are
// startup errors.
func NewManager(reg *Registry, sourceCfgs []config.Source, outputCfgs []config.Output, ingestFn IngestFunc, logger *slog.Logger) (*Manager, error) {
	m := &Manager{
		logger:        logger,
		statuses:      newStatusRegistry(),
		ingestFn:      ingestFn,
		eventQueue:    make(chan core.HazardEvent, defaultEventQueueSize),
		ingestWorkers: defaultIngestWorkers,
		emitWait:      emitWaitTimeout,
	}

	for _, cfg := range sourceCfgs {
		tracker := m.statuses.add(cfg.ID, cfg.Type, KindSource)
		if !cfg.Enabled {
			tracker.setState(StateDisabled)
			continue
		}
		factory, err := reg.Source(cfg.Type)
		if err != nil {
			return nil, fmt.Errorf("source %q: %w", cfg.ID, err)
		}
		p, err := factory(cfg.Config)
		if err != nil {
			return nil, fmt.Errorf("source %q (type %q): invalid configuration: %w", cfg.ID, cfg.Type, err)
		}
		m.sources = append(m.sources, newSourceSupervisor(cfg, p, EmitterFunc(m.emit), logger, tracker))
	}

	for _, cfg := range outputCfgs {
		tracker := m.statuses.add(cfg.ID, cfg.Type, KindOutput)
		if !cfg.Enabled {
			tracker.setState(StateDisabled)
			continue
		}
		factory, err := reg.Output(cfg.Type)
		if err != nil {
			return nil, fmt.Errorf("output %q: %w", cfg.ID, err)
		}
		p, err := factory(cfg.Config)
		if err != nil {
			return nil, fmt.Errorf("output %q (type %q): invalid configuration: %w", cfg.ID, cfg.Type, err)
		}
		m.outputs = append(m.outputs, newOutputWorker(cfg, p, logger, tracker))
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
	ingestCtl, cancelIngest := context.WithCancel(context.Background())
	procCtx, cancelProc := context.WithCancel(context.Background())
	outCtx, cancelOut := context.WithCancel(context.Background())

	var wgSources, wgIngest, wgOutputs sync.WaitGroup

	for _, w := range m.outputs {
		wgOutputs.Add(1)
		go func(w *outputWorker) {
			defer wgOutputs.Done()
			w.run(outCtx)
		}(w)
	}
	for i := 0; i < m.ingestWorkers; i++ {
		wgIngest.Add(1)
		go func() {
			defer wgIngest.Done()
			m.ingestLoop(ingestCtl, procCtx)
		}()
	}
	for _, s := range m.sources {
		wgSources.Add(1)
		go func(s *sourceSupervisor) {
			defer wgSources.Done()
			s.run(ctx)
		}(s)
	}

	<-ctx.Done()
	m.logger.Info("plugin manager stopping", "sources", len(m.sources), "outputs", len(m.outputs))

	// 1. Stop the sources so no new events enter the queue. Each source is
	// already bounded by its own shutdown timeout inside the supervisor.
	m.stopGroup(&wgSources, "sources", groupShutdownGrace)

	// 2. Stop ingestion after a bounded drain of already-queued events.
	cancelIngest()
	m.stopGroup(&wgIngest, "ingestion", drainTimeout+groupShutdownGrace)
	cancelProc()

	// 3. Stop the output workers; plugin resources are closed with bounded
	// timeouts inside each worker.
	cancelOut()
	m.stopGroup(&wgOutputs, "outputs", maxOutputTimeout(m.outputs)+groupShutdownGrace)

	m.logger.Info("plugin manager stopped")
}

// EmitterFunc adapts a function to the Emitter interface.
type EmitterFunc func(ctx context.Context, event core.HazardEvent) error

// Emit implements Emitter.
func (f EmitterFunc) Emit(ctx context.Context, event core.HazardEvent) error { return f(ctx, event) }

// emit queues a normalized event for the core pipeline. The queue is
// bounded: when full, the emitter applies backpressure with a bounded wait
// instead of silently dropping hazard events.
func (m *Manager) emit(ctx context.Context, event core.HazardEvent) error {
	select {
	case m.eventQueue <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(m.emitWait):
		return fmt.Errorf("ingestion queue full (capacity %d)", cap(m.eventQueue))
	}
}

// ingestLoop forwards queued events to the core and dispatches meaningful
// changes to every output worker. On shutdown it drains the queue for a
// bounded time.
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

// process runs one event through the core and fans the change out to all
// outputs. Duplicates never reach outputs.
func (m *Manager) process(ctx context.Context, event core.HazardEvent) {
	result, change, err := m.ingestFn(ctx, event)
	if err != nil {
		m.logger.Error("ingest failed",
			"source", event.Source, "source_id", event.SourceID, "error", err)
		return
	}
	if result == ingest.ResultDuplicate {
		return
	}
	for _, w := range m.outputs {
		if err := w.submit(ctx, change); err != nil {
			m.logger.Warn("event change not queued for output",
				"output_id", w.id, "change_type", change.Type, "error", err)
		}
	}
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
