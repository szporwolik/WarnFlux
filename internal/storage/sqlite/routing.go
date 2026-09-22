package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// GroupRouting returns the full routing matrix of one group: every
// assigned cell carries its own source and severity threshold, sorted by
// source then action ID.
func (s *Store) GroupRouting(groupID int64) (storage.GroupRouting, error) {
	var r storage.GroupRouting
	err := s.db.QueryRow(`SELECT id, name FROM groups WHERE id = ?`, groupID).
		Scan(&r.GroupID, &r.Name)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.GroupRouting{}, storage.ErrGroupNotFound
	}
	if err != nil {
		return storage.GroupRouting{}, fmt.Errorf("group %d routing: %w", groupID, err)
	}

	actions, err := s.groupActions(groupID)
	if err != nil {
		return storage.GroupRouting{}, err
	}
	r.Actions = actions
	return r, nil
}

// SetGroupRouting replaces the group's routing matrix in one transaction.
// Every assigned cell carries its own source and canonical severity
// threshold; IDs reference the configuration (not database rows) and are
// stored as-is, deduplicated per (source, ID).
func (s *Store) SetGroupRouting(groupID int64, actions []storage.ChannelAssignment) error {
	for _, a := range actions {
		if !storage.ValidSeverity(a.MinSeverity) {
			return storage.ErrInvalidSeverity
		}
	}
	actions = dedupeAssignments(actions)

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin routing update: %w", err)
	}
	defer tx.Rollback()

	now := s.now().UnixMilli()
	res, err := tx.Exec(`UPDATE groups SET updated_at_ms = ? WHERE id = ?`, now, groupID)
	if err != nil {
		return fmt.Errorf("touch group %d: %w", groupID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("touch group %d: %w", groupID, err)
	}
	if n == 0 {
		return storage.ErrGroupNotFound
	}

	if _, err := tx.Exec(`DELETE FROM group_actions WHERE group_id = ?`, groupID); err != nil {
		return fmt.Errorf("clear group %d actions: %w", groupID, err)
	}
	for _, a := range actions {
		if _, err := tx.Exec(`
			INSERT INTO group_actions (group_id, source, action_id, min_severity)
			VALUES (?, ?, ?, ?)`, groupID, a.Source, a.ID, a.MinSeverity); err != nil {
			return fmt.Errorf("assign action %q (source %q) to group %d: %w", a.ID, a.Source, groupID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit group %d routing: %w", groupID, err)
	}
	return nil
}

// ListGroupRoutings returns the routing matrix of every group ordered by
// name. Groups without any assigned action yield empty slices.
func (s *Store) ListGroupRoutings() ([]storage.GroupRouting, error) {
	rows, err := s.db.Query(`SELECT id, name FROM groups ORDER BY name COLLATE NOCASE ASC`)
	if err != nil {
		return nil, fmt.Errorf("list group routings: %w", err)
	}
	defer rows.Close()

	var out []storage.GroupRouting
	for rows.Next() {
		var r storage.GroupRouting
		if err := rows.Scan(&r.GroupID, &r.Name); err != nil {
			return nil, fmt.Errorf("scan group routing: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group routings: %w", err)
	}
	for i := range out {
		actions, err := s.groupActions(out[i].GroupID)
		if err != nil {
			return nil, err
		}
		out[i].Actions = actions
	}
	return out, nil
}

// groupActions reads the assigned cells with their thresholds, sorted.
func (s *Store) groupActions(groupID int64) ([]storage.ChannelAssignment, error) {
	rows, err := s.db.Query(`
		SELECT source, action_id, min_severity FROM group_actions
		WHERE group_id = ? ORDER BY source ASC, action_id ASC`, groupID)
	if err != nil {
		return nil, fmt.Errorf("list group %d actions: %w", groupID, err)
	}
	defer rows.Close()
	return scanAssignments(rows, fmt.Sprintf("scan group %d action", groupID))
}

type assignmentScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

// scanAssignments reads (source, id, min_severity) rows into assignments.
func scanAssignments(rows assignmentScanner, what string) ([]storage.ChannelAssignment, error) {
	var out []storage.ChannelAssignment
	for rows.Next() {
		var a storage.ChannelAssignment
		if err := rows.Scan(&a.Source, &a.ID, &a.MinSeverity); err != nil {
			return nil, fmt.Errorf("%s: %w", what, err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// dedupeAssignments removes duplicates (by source + ID) while preserving
// order.
func dedupeAssignments(list []storage.ChannelAssignment) []storage.ChannelAssignment {
	seen := make(map[string]struct{}, len(list))
	out := list[:0]
	for _, a := range list {
		if a.ID == "" {
			continue
		}
		key := a.Source + "\x00" + a.ID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, a)
	}
	return out
}

// ClaimActionFire records that (group, action) has delivered the event
// identified by dedupKey. It returns true when the row is newly inserted
// (the caller should fire the action) and false when that delivery was
// already recorded (duplicate, skip).
func (s *Store) ClaimActionFire(groupID int64, actionID, eventKey, dedupKey string, at time.Time) (bool, error) {
	res, err := s.db.Exec(`
		INSERT OR IGNORE INTO action_fires
			(group_id, action_id, event_key, dedup_key, fired_at_ms)
		VALUES (?, ?, ?, ?, ?)`, groupID, actionID, eventKey, dedupKey, at.UnixMilli())
	if err != nil {
		return false, fmt.Errorf("claim action %q fire for group %d: %w", actionID, groupID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claim action %q fire for group %d: %w", actionID, groupID, err)
	}
	return n == 1, nil
}

// PruneActionFires deletes ledger rows older than the cutoff and returns
// how many rows were removed.
func (s *Store) PruneActionFires(cutoff time.Time) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM action_fires WHERE fired_at_ms < ?`, cutoff.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("prune action fires: %w", err)
	}
	return res.RowsAffected()
}

// GroupRecipientEmails returns the distinct (case-insensitive), non-empty
// email addresses of the group's members, sorted. Missing groups yield an
// empty list (the membership table simply has no rows for them).
func (s *Store) GroupRecipientEmails(groupID int64) ([]string, error) {
	rows, err := s.db.Query(`
		SELECT u.email
		FROM users u
		JOIN user_groups ug ON ug.user_id = u.id
		WHERE ug.group_id = ? AND u.email <> ''
		ORDER BY u.email COLLATE NOCASE ASC`, groupID)
	if err != nil {
		return nil, fmt.Errorf("list group %d recipients: %w", groupID, err)
	}
	defer rows.Close()
	seen := make(map[string]struct{})
	var out []string
	for rows.Next() {
		var email string
		if err := rows.Scan(&email); err != nil {
			return nil, fmt.Errorf("scan group %d recipient: %w", groupID, err)
		}
		key := strings.ToLower(email)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, email)
	}
	return out, rows.Err()
}
