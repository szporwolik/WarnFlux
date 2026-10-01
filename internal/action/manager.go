package action

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/metrics"
	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/trail"
)

// DeliveryRecoveryInterval is how often the manager sweeps delivery
// claims left "running" by a crashed process. Exported only so tests can
// shorten it; production code never mutates it.
var DeliveryRecoveryInterval = time.Second

// Manager owns all configured action instances: construction, workers,
// explicit submission, status aggregation and bounded shutdown.
//
// It deliberately does NOT fan events out to all actions: a future rule
// engine selects the action to execute. Submit is the only way an action
// runs.
type Manager struct {
	instances []*Instance
	disabled  []config.Action

	byID map[string]*Instance

	logger *slog.Logger
	trail  *trail.Recorder
	reg    *metrics.Registry

	// store is the optional durable delivery job queue. When attached,
	// every worker claims jobs from SQLite (payload, recipients, attempt
	// counter and next-attempt deadline) and records the result after
	// each execution; the in-memory queue remains a fallback path.
	store storage.DeliveryStore

	// deliveryGate is the optional staleness oracle consulted directly
	// BEFORE a queued job is transmitted: false means the hazard has
	// expired or was cancelled while the job waited, and the worker
	// settles the job as expired instead of executing it.
	deliveryGate func(ctx context.Context, eventKey string) bool

	// offline is the offline-mode switch: internet-backed actions hold
	// their queued requests (nothing executed, nothing lost) while it is
	// on.
	offline atomic.Bool

	cancel   context.CancelFunc
	cancelMu sync.Mutex
}

// NewManager builds every configured action instance via the registry.
// Unknown types fail even for disabled entries (configuration typos must
// fail at startup); factories are invoked only for enabled instances.
// Programmatic constructions with zero runtime values fall back to the
// configuration defaults (config.Load already applies them for YAML).
// trail is the optional per-alert audit recorder (may be nil); reg is
// the optional metrics registry (may be nil).
func NewManager(cfgs []config.Action, reg *Registry, logger *slog.Logger, trail *trail.Recorder, metricsReg *metrics.Registry) (*Manager, error) {
	m := &Manager{logger: logger, trail: trail, reg: metricsReg, byID: make(map[string]*Instance)}
	for _, cfg := range cfgs {
		if !reg.Known(cfg.Type) {
			return nil, fmt.Errorf("action %q: type %q is not registered", cfg.ID, cfg.Type)
		}
		if cfg.Runtime.QueueSize < 1 {
			cfg.Runtime.QueueSize = 128
		}
		if cfg.Runtime.CallTimeout <= 0 {
			cfg.Runtime.CallTimeout = 10 * time.Second
		}
		if cfg.Runtime.ShutdownTimeout <= 0 {
			cfg.Runtime.ShutdownTimeout = 10 * time.Second
		}
		if !cfg.Enabled {
			m.disabled = append(m.disabled, cfg)
			continue
		}
		p, err := reg.Create(cfg.Type, cfg.Config)
		if err != nil {
			return nil, fmt.Errorf("action %q (%s): %w", cfg.ID, cfg.Type, err)
		}
		inst := NewInstance(cfg.ID, cfg.Type, p, cfg.Runtime.QueueSize,
			cfg.Runtime.CallTimeout, cfg.Runtime.ShutdownTimeout, logger,
			m.trail, cfg.Runtime.Retries, m.reg)
		inst.internet = reg.Internet(cfg.Type)
		m.instances = append(m.instances, inst)
		m.byID[cfg.ID] = inst
	}
	return m, nil
}

// Start launches one worker per enabled instance.
func (m *Manager) Start(ctx context.Context) {
	m.cancelMu.Lock()
	if m.cancel != nil {
		m.cancelMu.Unlock()
		return
	}
	mgrCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.cancelMu.Unlock()

	for _, inst := range m.instances {
		if m.store != nil {
			inst.setDeliveryStore(m.store)
		}
		if m.deliveryGate != nil {
			inst.setDeliveryGate(m.deliveryGate)
		}
		inst.setOfflineFn(m.Offline)
		inst.Start(mgrCtx)
	}
	if m.store != nil {
		// The interval is captured NOW (synchronously under the caller)
		// so the goroutine never reads the mutable package var again —
		// tests shorten it and restore it at cleanup.
		go m.recoverStaleClaims(mgrCtx, DeliveryRecoveryInterval)
	}
}

// SetDeliveryStore attaches the durable delivery job queue. It must be
// called before Start.
func (m *Manager) SetDeliveryStore(st storage.DeliveryStore) {
	m.store = st
}

// SetDeliveryGate attaches the staleness oracle consulted directly
// before a queued job is transmitted (see DeliveryGate). It must be
// called before Start.
func (m *Manager) SetDeliveryGate(gate func(ctx context.Context, eventKey string) bool) {
	m.deliveryGate = gate
}

// SetOffline toggles the offline-mode switch for the actions. Internet-
// backed actions (smtp, http_webhook, discord) hold their queued work
// while offline; local actions keep running. Idempotent.
func (m *Manager) SetOffline(on bool) {
	if m.offline.Swap(on) == on {
		return
	}
	m.logger.Info("offline mode actions toggled", "offline", on)
}

// Offline reports the current offline-mode state of the actions.
func (m *Manager) Offline() bool { return m.offline.Load() }

// recoverStaleClaims re-queues jobs a previous process claimed but never
// settled (crash between claim and execution). One immediate pass runs
// before the workers are active, then a periodic sweep.
func (m *Manager) recoverStaleClaims(ctx context.Context, interval time.Duration) {
	if n, err := m.store.RecoverStaleClaims(ctx, time.Now()); err != nil {
		m.logger.Warn("action: stale claim recovery failed", "error", err)
	} else if n > 0 {
		m.logger.Info("action: stale delivery claims recovered", "jobs", n)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := m.store.RecoverStaleClaims(ctx, time.Now()); err != nil {
				m.logger.Warn("action: stale claim recovery failed", "error", err)
			} else if n > 0 {
				m.logger.Info("action: stale delivery claims recovered", "jobs", n)
			}
		}
	}
}

// Submit explicitly routes one request to exactly one action instance.
// It never blocks the dispatch ingress: a full queue returns a clear error
// immediately. Unknown or disabled actions are errors, never silent drops.
func (m *Manager) Submit(actionID string, req ActionRequest) error {
	inst, ok := m.byID[actionID]
	if !ok || inst == nil {
		return fmt.Errorf("action %q is not configured or not enabled", actionID)
	}
	if inst.disabled.Load() {
		return fmt.Errorf("action %q is disabled: %s", actionID, inst.Status().Reason)
	}
	select {
	case inst.queue <- req:
		return nil
	default:
		return fmt.Errorf("action %q queue full (capacity %d); request not accepted", actionID, cap(inst.queue))
	}
}

// Statuses returns the status of every configured instance (including
// config-disabled ones), sorted by ID.
func (m *Manager) Statuses() []Status {
	out := make([]Status, 0, len(m.instances)+len(m.disabled))
	for _, inst := range m.instances {
		out = append(out, inst.Status())
	}
	for _, cfg := range m.disabled {
		out = append(out, DisabledInstanceStatus(cfg.ID, cfg.Type, cfg.Runtime.QueueSize))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Shutdown stops intake, lets each instance drain its queue within its own
// shutdown_timeout and closes plugins. It returns when all clean plugins
// have finished or ctx expires; a hung plugin can never extend shutdown
// beyond the configured bounds.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.cancelMu.Lock()
	cancel := m.cancel
	m.cancelMu.Unlock()
	if cancel == nil {
		return errors.New("action: manager not started")
	}
	cancel()

	done := make(chan struct{})
	go func() {
		for _, inst := range m.instances {
			inst.wg.Wait()
		}
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("action: shutdown deadline exceeded: %w", ctx.Err())
	}
}

// MaxShutdownTimeout returns a safe outer bound for Manager.Shutdown:
// the largest instance shutdown_timeout plus slack for queue drains.
func (m *Manager) MaxShutdownTimeout() time.Duration {
	maxT := 0 * time.Second
	for _, inst := range m.instances {
		if inst.closeTO > maxT {
			maxT = inst.closeTO
		}
	}
	return maxT + 2*time.Second
}
