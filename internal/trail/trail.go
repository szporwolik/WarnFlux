// Package trail records the per-alert notification audit trail: why a
// concrete hazard alert was or was not delivered. The routing engine,
// the action workers and the web UI share one in-memory Recorder, so the
// "Notifications / Delivery history" page can answer the most frequent
// operational question — "why was THIS alert not sent?".
//
// Every trail is keyed by the canonical event key and is a linear
// sequence of steps (received → matched → route → submitted → delivered
// or failed → retry …). Trails are bounded in memory: the recorder keeps
// only the most recent alerts (the durable action_fires ledger remains
// the source of truth for deduplication).
package trail

import (
	"sync"
	"time"
)

// StepKind classifies one audit step.
type StepKind string

// The audit step vocabulary.
const (
	StepReceived  StepKind = "received"  // alert accepted into the dispatch flow
	StepMatched   StepKind = "matched"   // matched a notification group
	StepRoute     StepKind = "route"     // matrix cell selected: source → action ≥ threshold
	StepSubmitted StepKind = "submitted" // action worker accepted the request
	StepDelivered StepKind = "delivered" // action completed successfully
	StepFailed    StepKind = "failed"    // action attempt returned an error
	StepRetry     StepKind = "retry"     // a new attempt is scheduled
	StepSkipped   StepKind = "skipped"   // no notification: threshold/dedup/terminal
)

// Outcome is the terminal state of one alert's notification run.
type Outcome string

// The alert outcomes.
const (
	OutcomeSubmitted Outcome = "submitted" // queued to actions, no result yet
	OutcomeDelivered Outcome = "delivered" // all attempts succeeded
	OutcomeFailed    Outcome = "failed"    // a delivery attempt failed
	OutcomeSkipped   Outcome = "skipped"   // deliberately not notified
)

// Step is one audit entry.
type Step struct {
	Seq  int64    `json:"seq"`
	At   string   `json:"at"` // local time, RFC3339
	Kind StepKind `json:"kind"`
	Text string   `json:"text"`
}

// Trail is the complete audit view of one alert.
type Trail struct {
	Key        string  `json:"key"`
	Source     string  `json:"source"`
	Severity   string  `json:"severity"`
	Event      string  `json:"event"`
	Headline   string  `json:"headline"`
	ReceivedAt string  `json:"received_at"`
	Outcome    Outcome `json:"outcome"`
	Steps      []Step  `json:"steps"`
}

// DefaultMaxTrails bounds the in-memory audit history.
const DefaultMaxTrails = 200

// Recorder stores the most recent notification trails. It is safe for
// concurrent use and every method is a no-op on a nil receiver, so
// callers can thread an optional recorder without branching.
type Recorder struct {
	mu      sync.Mutex
	max     int
	nextSeq int64
	order   []string // oldest → newest
	byKey   map[string]*Trail
}

// NewRecorder builds a recorder retaining at most max trails.
func NewRecorder(max int) *Recorder {
	if max < 1 {
		max = DefaultMaxTrails
	}
	return &Recorder{max: max, byKey: make(map[string]*Trail)}
}

// Receive opens (or keeps) the trail for one alert and appends the
// "received" step. An existing trail is never reset.
func (r *Recorder) Receive(key, source, severity, event, headline string, at time.Time) {
	if r == nil || key == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byKey[key]; ok {
		return
	}
	r.byKey[key] = &Trail{
		Key:        key,
		Source:     source,
		Severity:   severity,
		Event:      event,
		Headline:   headline,
		ReceivedAt: at.Format(time.RFC3339),
		Outcome:    OutcomeSkipped,
	}
	r.order = append(r.order, key)
	r.addStepLocked(key, StepReceived, "received", at)
	r.trimLocked()
}

// Add appends one step to the trail of the given alert. Unknown keys are
// ignored: steps without a Receive anchor would confuse the UI.
func (r *Recorder) Add(key string, kind StepKind, text string, at time.Time) {
	if r == nil || key == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byKey[key]; !ok {
		return
	}
	r.addStepLocked(key, kind, text, at)
}

// SetOutcome updates the terminal outcome of one alert.
func (r *Recorder) SetOutcome(key string, outcome Outcome) {
	if r == nil || key == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.byKey[key]; ok {
		t.Outcome = outcome
	}
}

func (r *Recorder) addStepLocked(key string, kind StepKind, text string, at time.Time) {
	t := r.byKey[key]
	r.nextSeq++
	t.Steps = append(t.Steps, Step{
		Seq:  r.nextSeq,
		At:   at.Format(time.RFC3339),
		Kind: kind,
		Text: text,
	})
}

// trimLocked drops the oldest trails beyond the cap.
func (r *Recorder) trimLocked() {
	for len(r.order) > r.max {
		oldest := r.order[0]
		r.order = r.order[1:]
		delete(r.byKey, oldest)
	}
}

// Recent returns up to limit trails, newest first. limit <= 0 means all.
func (r *Recorder) Recent(limit int) []Trail {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.order)
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]Trail, 0, n)
	for i := len(r.order) - 1; i >= 0 && len(out) < n; i-- {
		t := r.byKey[r.order[i]]
		cp := *t
		cp.Steps = append([]Step(nil), t.Steps...)
		out = append(out, cp)
	}
	return out
}

// Get returns one trail by event key.
func (r *Recorder) Get(key string) (Trail, bool) {
	if r == nil {
		return Trail{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.byKey[key]
	if !ok {
		return Trail{}, false
	}
	cp := *t
	cp.Steps = append([]Step(nil), t.Steps...)
	return cp, true
}
