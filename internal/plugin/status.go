package plugin

import (
	"sync"
	"time"
)

// PluginKind distinguishes source plugins from output plugins.
type PluginKind string

const (
	KindSource PluginKind = "source"
	KindOutput PluginKind = "output"
)

// PluginState is the runtime state of a plugin instance.
type PluginState string

const (
	StateDisabled  PluginState = "disabled"
	StateStarting  PluginState = "starting"
	StateRunning   PluginState = "running"
	StateDegraded  PluginState = "degraded"
	StateSuspended PluginState = "suspended"
	StateStopping  PluginState = "stopping"
	StateStopped   PluginState = "stopped"
)

// PluginStatus is a point-in-time snapshot of a plugin instance's runtime
// health. A future health endpoint or MQTT status publisher can consume it
// without changing the plugin API.
type PluginStatus struct {
	ID                  string
	Type                string
	Kind                PluginKind
	State               PluginState
	StartedAt           time.Time
	LastSuccessAt       *time.Time
	LastErrorAt         *time.Time
	LastError           string
	ConsecutiveFailures int
	RestartCount        int
}

// statusTracker guards the mutable status of one plugin instance.
type statusTracker struct {
	mu sync.Mutex
	s  PluginStatus
}

func newStatusTracker(id, kind string, kindValue PluginKind) *statusTracker {
	return &statusTracker{s: PluginStatus{ID: id, Type: kind, Kind: kindValue, State: StateStopped}}
}

func (t *statusTracker) setState(state PluginState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.State = state
}

func (t *statusTracker) markStarted(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.StartedAt = now
	t.s.State = StateStarting
}

func (t *statusTracker) success(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.ConsecutiveFailures = 0
	ts := now
	t.s.LastSuccessAt = &ts
	t.s.State = StateRunning
}

// failure records a failure and reports whether the failure threshold was
// reached.
func (t *statusTracker) failure(err error, threshold int, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.ConsecutiveFailures++
	ts := now
	t.s.LastErrorAt = &ts
	t.s.LastError = err.Error()
	if threshold > 0 && t.s.ConsecutiveFailures >= threshold {
		t.s.State = StateSuspended
		return true
	}
	t.s.State = StateDegraded
	return false
}

func (t *statusTracker) restart() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.RestartCount++
}

func (t *statusTracker) failures() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.s.ConsecutiveFailures
}

func (t *statusTracker) snapshot() PluginStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Copy the pointer fields so callers cannot mutate tracker state.
	cp := t.s
	if t.s.LastSuccessAt != nil {
		ts := *t.s.LastSuccessAt
		cp.LastSuccessAt = &ts
	}
	if t.s.LastErrorAt != nil {
		ts := *t.s.LastErrorAt
		cp.LastErrorAt = &ts
	}
	return cp
}

// StatusRegistry tracks the runtime status of all configured plugin
// instances, enabled or not.
type StatusRegistry struct {
	mu       sync.RWMutex
	trackers map[string]*statusTracker
	order    []string
}

func newStatusRegistry() *StatusRegistry {
	return &StatusRegistry{trackers: make(map[string]*statusTracker)}
}

func (r *StatusRegistry) add(id, kind string, kindValue PluginKind) *statusTracker {
	r.mu.Lock()
	defer r.mu.Unlock()
	t := newStatusTracker(id, kind, kindValue)
	r.trackers[id] = t
	r.order = append(r.order, id)
	return t
}

// Snapshot returns the current status of every tracked instance in
// registration order.
func (r *StatusRegistry) Snapshot() []PluginStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]PluginStatus, 0, len(r.order))
	for _, id := range r.order {
		out = append(out, r.trackers[id].snapshot())
	}
	return out
}
