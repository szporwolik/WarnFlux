package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// GroupRouting returns the full routing matrix of one group: every
// assigned action and output carries its own severity threshold, sorted
// by ID.
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
	outputs, err := s.groupOutputs(groupID)
	if err != nil {
		return storage.GroupRouting{}, err
	}
	r.Actions = actions
	r.Outputs = outputs
	return r, nil
}

// SetGroupRouting replaces the group's routing matrix in one transaction.
// Every assigned channel carries its own canonical severity threshold;
// IDs reference the configuration (not database rows) and are stored
// as-is, deduplicated.
func (s *Store) SetGroupRouting(groupID int64, actions, outputs []storage.ChannelAssignment) error {
	for _, a := range actions {
		if !storage.ValidSeverity(a.MinSeverity) {
			return storage.ErrInvalidSeverity
		}
	}
	for _, o := range outputs {
		if !storage.ValidSeverity(o.MinSeverity) {
			return storage.ErrInvalidSeverity
		}
	}
	actions = dedupeAssignments(actions)
	outputs = dedupeAssignments(outputs)

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
	if _, err := tx.Exec(`DELETE FROM group_outputs WHERE group_id = ?`, groupID); err != nil {
		return fmt.Errorf("clear group %d outputs: %w", groupID, err)
	}
	for _, a := range actions {
		if _, err := tx.Exec(`
			INSERT INTO group_actions (group_id, action_id, min_severity)
			VALUES (?, ?, ?)`, groupID, a.ID, a.MinSeverity); err != nil {
			return fmt.Errorf("assign action %q to group %d: %w", a.ID, groupID, err)
		}
	}
	for _, o := range outputs {
		if _, err := tx.Exec(`
			INSERT INTO group_outputs (group_id, output_id, min_severity)
			VALUES (?, ?, ?)`, groupID, o.ID, o.MinSeverity); err != nil {
			return fmt.Errorf("assign output %q to group %d: %w", o.ID, groupID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit group %d routing: %w", groupID, err)
	}
	return nil
}

// ListGroupRoutings returns the routing matrix of every group ordered by
// name. Groups without any assigned channel yield empty slices.
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
		outputs, err := s.groupOutputs(out[i].GroupID)
		if err != nil {
			return nil, err
		}
		out[i].Actions = actions
		out[i].Outputs = outputs
	}
	return out, nil
}

// groupActions reads the assigned actions with their thresholds, sorted.
func (s *Store) groupActions(groupID int64) ([]storage.ChannelAssignment, error) {
	rows, err := s.db.Query(`
		SELECT action_id, min_severity FROM group_actions
		WHERE group_id = ? ORDER BY action_id ASC`, groupID)
	if err != nil {
		return nil, fmt.Errorf("list group %d actions: %w", groupID, err)
	}
	defer rows.Close()
	return scanAssignments(rows, fmt.Sprintf("scan group %d action", groupID))
}

// groupOutputs reads the assigned outputs with their thresholds, sorted.
func (s *Store) groupOutputs(groupID int64) ([]storage.ChannelAssignment, error) {
	rows, err := s.db.Query(`
		SELECT output_id, min_severity FROM group_outputs
		WHERE group_id = ? ORDER BY output_id ASC`, groupID)
	if err != nil {
		return nil, fmt.Errorf("list group %d outputs: %w", groupID, err)
	}
	defer rows.Close()
	return scanAssignments(rows, fmt.Sprintf("scan group %d output", groupID))
}

type assignmentScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

// scanAssignments reads (id, min_severity) rows into assignments.
func scanAssignments(rows assignmentScanner, what string) ([]storage.ChannelAssignment, error) {
	var out []storage.ChannelAssignment
	for rows.Next() {
		var a storage.ChannelAssignment
		if err := rows.Scan(&a.ID, &a.MinSeverity); err != nil {
			return nil, fmt.Errorf("%s: %w", what, err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// dedupeAssignments removes duplicates (by ID) while preserving order.
func dedupeAssignments(list []storage.ChannelAssignment) []storage.ChannelAssignment {
	seen := make(map[string]struct{}, len(list))
	out := list[:0]
	for _, a := range list {
		if a.ID == "" {
			continue
		}
		if _, ok := seen[a.ID]; ok {
			continue
		}
		seen[a.ID] = struct{}{}
		out = append(out, a)
	}
	return out
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
