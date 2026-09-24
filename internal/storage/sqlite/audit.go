package sqlite

import (
	"fmt"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// RecordAudit inserts one dashboard user action and prunes the oldest
// entries beyond storage.AuditRetentionEntries so the log stays bounded.
func (s *Store) RecordAudit(user, action, detail string, at time.Time) error {
	if _, err := s.db.Exec(
		"INSERT INTO audit_log (at_ms, user, action, detail) VALUES (?, ?, ?, ?)",
		at.UnixMilli(), user, action, detail,
	); err != nil {
		return fmt.Errorf("insert audit entry: %w", err)
	}
	if _, err := s.db.Exec(
		"DELETE FROM audit_log WHERE seq <= (SELECT MAX(seq) - ? FROM audit_log)",
		storage.AuditRetentionEntries,
	); err != nil {
		return fmt.Errorf("prune audit log: %w", err)
	}
	return nil
}

// ListAudit returns entries with Seq > after, oldest first, at most limit
// entries (limit <= 0 means no bound).
func (s *Store) ListAudit(after int64, limit int) ([]storage.AuditEntry, error) {
	query := "SELECT seq, at_ms, user, action, detail FROM audit_log WHERE seq > ? ORDER BY seq ASC"
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := s.db.Query(query, after)
	if err != nil {
		return nil, fmt.Errorf("list audit entries: %w", err)
	}
	defer rows.Close()
	out := make([]storage.AuditEntry, 0, limit)
	for rows.Next() {
		var e storage.AuditEntry
		var atMs int64
		if err := rows.Scan(&e.Seq, &atMs, &e.User, &e.Action, &e.Detail); err != nil {
			return nil, fmt.Errorf("scan audit entry: %w", err)
		}
		e.At = time.UnixMilli(atMs).UTC().Format(time.RFC3339)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit entries: %w", err)
	}
	return out, nil
}

var _ storage.AuditStore = (*Store)(nil)
