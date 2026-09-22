package mqttreceiver

import (
	"context"
	"log/slog"
	"sort"
	"sync"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
)

// Manager owns all configured receivers: construction, background
// connection attempts, per-receiver status and bounded shutdown.
// One receiver failing never stops another, never stops Router sources or
// outputs and never makes the web server unusable.
type Manager struct {
	receivers []*Receiver
	logger    *slog.Logger

	mu      sync.Mutex
	started bool
}

// NewManager builds one receiver per configured entry. Construction
// failures (e.g. unreadable password_file) are fatal before any runtime
// component starts; connection failures are not.
func NewManager(cfgs []config.Receiver, st *state.State, ingress *dispatch.Ingress, logger *slog.Logger) (*Manager, error) {
	m := &Manager{logger: logger}
	for _, cfg := range cfgs {
		if !cfg.Enabled {
			m.receivers = append(m.receivers, &Receiver{cfg: cfg, stats: &Stats{}, logger: logger, status: Status{
				ID:      cfg.ID,
				Enabled: false,
				Broker:  sanitizeBroker(cfg.Broker),
			}})
			continue
		}
		r, err := NewReceiver(cfg, st, ingress, logger)
		if err != nil {
			return nil, err
		}
		m.receivers = append(m.receivers, r)
	}
	return m, nil
}

// StartAll launches a background connect attempt for every enabled
// receiver. Initial connection failures are logged and paho keeps retrying
// in the background; they are never startup-critical.
func (m *Manager) StartAll() {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return
	}
	m.started = true
	m.mu.Unlock()

	for _, r := range m.receivers {
		if !r.cfg.Enabled {
			continue
		}
		go func(r *Receiver) {
			if err := r.Connect(context.Background()); err != nil {
				// Not fatal: paho reconnects in the background; the rest of
				// the application stays fully usable.
				m.logger.Warn("receiver: initial connection failed, retrying in background",
					"receiver", r.cfg.ID, "error", err)
			}
		}(r)
	}
}

// Statuses returns the status of every configured receiver (including
// disabled ones), sorted by ID.
func (m *Manager) Statuses() []Status {
	out := make([]Status, 0, len(m.receivers))
	for _, r := range m.receivers {
		out = append(out, r.Status())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// StopIntakeAll unsubscribes every receiver so no new messages reach the
// dispatch ingress.
func (m *Manager) StopIntakeAll() {
	for _, r := range m.receivers {
		r.StopIntake()
	}
}

// DisconnectAll stops intake and disconnects every receiver (bounded waits).
func (m *Manager) DisconnectAll() {
	for _, r := range m.receivers {
		r.Disconnect()
	}
}
