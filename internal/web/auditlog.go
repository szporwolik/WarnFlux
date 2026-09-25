package web

import (
	"sync"
	"time"
)

// DefaultAuditEntries bounds the in-memory user-action audit buffer.
const DefaultAuditEntries = 500

// auditEntry is one recorded user action performed through the web UI.
type auditEntry struct {
	Seq    int64  `json:"seq"`
	At     string `json:"at"`
	User   string `json:"user"`
	Action string `json:"action"`
	Detail string `json:"detail"`
}

// AuditBuffer is a bounded ring buffer of user actions. Every
// state-changing dashboard operation lands here (login/logout, users,
// groups, routing, compose, MQTT browse); the audit page
// polls snapshots of it.
type AuditBuffer struct {
	mu      sync.Mutex
	max     int
	nextSeq int64
	entries []auditEntry
}

// NewAuditBuffer builds a buffer retaining at most max entries.
func NewAuditBuffer(max int) *AuditBuffer {
	if max < 1 {
		max = DefaultAuditEntries
	}
	return &AuditBuffer{max: max}
}

// Add records one action.
func (b *AuditBuffer) Add(user, action, detail string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextSeq++
	b.entries = append(b.entries, auditEntry{
		Seq:    b.nextSeq,
		At:     time.Now().UTC().Format(time.RFC3339),
		User:   user,
		Action: action,
		Detail: detail,
	})
	if len(b.entries) > b.max {
		b.entries = b.entries[len(b.entries)-b.max:]
	}
}

// Snapshot returns every buffered entry with Seq > after (0 = everything).
// The caller uses the highest Seq as the next "after" cursor.
func (b *AuditBuffer) Snapshot(after int64) []auditEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]auditEntry, 0, len(b.entries))
	for _, e := range b.entries {
		if e.Seq > after {
			out = append(out, e)
		}
	}
	return out
}

// Max returns the configured retention bound.
func (b *AuditBuffer) Max() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.max
}
