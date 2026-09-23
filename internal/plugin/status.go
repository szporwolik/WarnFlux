package plugin

import (
	"sync"
	"time"
	"unicode/utf8"
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
	ID            string
	Type          string
	Kind          PluginKind
	State         PluginState
	StartedAt     time.Time
	LastSuccessAt *time.Time
	LastErrorAt   *time.Time
	LastError     string
	LastSummary   string
	// Cumulative operational counters (provider polls/errors/filtered
	// events), exposed on /metrics as source_polls/errors/filtered.
	Polls               int64
	Errors              int64
	Filtered            int64
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

// setSummary stores the source's latest poll summary (human-readable,
// e.g. "42 items / 7 filtered").
func (t *statusTracker) setSummary(summary string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.LastSummary = summary
}

// countPoll increments the cumulative successful-poll counter.
func (t *statusTracker) countPoll() {
	t.mu.Lock()
	t.s.Polls++
	t.mu.Unlock()
}

// countPollError increments the cumulative failed-poll counter.
func (t *statusTracker) countPollError() {
	t.mu.Lock()
	t.s.Errors++
	t.mu.Unlock()
}

// countFiltered adds one poll's filtered-event count.
func (t *statusTracker) countFiltered(n int) {
	t.mu.Lock()
	t.s.Filtered += int64(n)
	t.mu.Unlock()
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

// maxStatusErrorBytes bounds PluginStatus.LastError. Plugin errors can
// embed arbitrary remote content, and LastError is exported through the
// retained MQTT status topic: the entire internal status model must stay
// bounded. The bound is applied where the error is recorded (not only at
// serialization), so no intermediate representation grows without limit.
const maxStatusErrorBytes = 2048

// truncateStatusError bounds an error string to maxStatusErrorBytes while
// preserving valid UTF-8 and making the truncation explicit.
func truncateStatusError(s string) string {
	if len(s) <= maxStatusErrorBytes {
		return s
	}
	const marker = " [... truncated]"
	limit := maxStatusErrorBytes - len(marker)
	// Never cut in the middle of a multi-byte rune.
	for limit > 0 && !utf8.RuneStart(s[limit]) {
		limit--
	}
	return s[:limit] + marker
}

// failure records a failure and reports whether the failure threshold was
// reached.
func (t *statusTracker) failure(err error, threshold int, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.ConsecutiveFailures++
	ts := now
	t.s.LastErrorAt = &ts
	t.s.LastError = truncateStatusError(err.Error())
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
