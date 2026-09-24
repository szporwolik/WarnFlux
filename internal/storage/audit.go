package storage

import "time"

// AuditRetentionEntries bounds the persisted dashboard audit log: the
// oldest entries beyond this count are pruned on every insert.
const AuditRetentionEntries = 10000

// AuditEntry is one persisted dashboard user action.
type AuditEntry struct {
	Seq    int64  `json:"seq"`
	At     string `json:"at"`
	User   string `json:"user"`
	Action string `json:"action"`
	Detail string `json:"detail"`
}

// AuditStore persists the dashboard user-action audit log. The web UI
// records every state-changing operation and serves the audit page from
// here; stores that do not implement it fall back to the bounded
// in-memory buffer.
type AuditStore interface {
	// RecordAudit inserts one action; the store prunes the oldest
	// entries beyond AuditRetentionEntries.
	RecordAudit(user, action, detail string, at time.Time) error
	// ListAudit returns entries with Seq > after, oldest first, at most
	// limit entries.
	ListAudit(after int64, limit int) ([]AuditEntry, error)
}
