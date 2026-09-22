package sqlite

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// GroupRouting returns the full routing of one group: severity threshold
// and the assigned action/output instance IDs, each sorted by ID.
func (s *Store) GroupRouting(groupID int64) (storage.GroupRouting, error) {
	var r storage.GroupRouting
	err := s.db.QueryRow(`
		SELECT id, name, min_severity FROM groups WHERE id = ?`, groupID).
		Scan(&r.GroupID, &r.Name, &r.MinSeverity)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.GroupRouting{}, storage.ErrGroupNotFound
	}
	if err != nil {
		return storage.GroupRouting{}, fmt.Errorf("group %d routing: %w", groupID, err)
	}

	actions, err := s.groupActionIDs(groupID)
	if err != nil {
		return storage.GroupRouting{}, err
	}
	outputs, err := s.groupOutputIDs(groupID)
	if err != nil {
		return storage.GroupRouting{}, err
	}
	r.Actions = actions
	r.Outputs = outputs
	return r, nil
}

// SetGroupRouting replaces the group's routing in one transaction. The
// severity threshold must be canonical; action/output IDs reference the
// configuration (not database rows) and are stored as-is, deduplicated.
func (s *Store) SetGroupRouting(groupID int64, minSeverity string, actions, outputs []string) error {
	if !storage.ValidSeverity(minSeverity) {
		return storage.ErrInvalidSeverity
	}
	actions = dedupeIDs(actions)
	outputs = dedupeIDs(outputs)

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin routing update: %w", err)
	}
	defer tx.Rollback()

	now := s.now().UnixMilli()
	res, err := tx.Exec(`
		UPDATE groups SET min_severity = ?, updated_at_ms = ? WHERE id = ?`,
		minSeverity, now, groupID)
	if err != nil {
		return fmt.Errorf("update group %d severity: %w", groupID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update group %d severity: %w", groupID, err)
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
	for _, id := range actions {
		if _, err := tx.Exec(`INSERT INTO group_actions (group_id, action_id) VALUES (?, ?)`, groupID, id); err != nil {
			return fmt.Errorf("assign action %q to group %d: %w", id, groupID, err)
		}
	}
	for _, id := range outputs {
		if _, err := tx.Exec(`INSERT INTO group_outputs (group_id, output_id) VALUES (?, ?)`, groupID, id); err != nil {
			return fmt.Errorf("assign output %q to group %d: %w", id, groupID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit group %d routing: %w", groupID, err)
	}
	return nil
}

// ListGroupRoutings returns the routing of every group ordered by name.
// Groups without any assigned channel yield empty slices.
func (s *Store) ListGroupRoutings() ([]storage.GroupRouting, error) {
	rows, err := s.db.Query(`
		SELECT g.id, g.name, g.min_severity, ga.action_id, go.output_id
		FROM groups g
		LEFT JOIN group_actions ga ON ga.group_id = g.id
		LEFT JOIN group_outputs go ON go.group_id = g.id
		ORDER BY g.name COLLATE NOCASE ASC, ga.action_id ASC, go.output_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list group routings: %w", err)
	}
	defer rows.Close()

	index := map[int64]int{}
	var out []storage.GroupRouting
	for rows.Next() {
		var gid int64
		var name, severity string
		var actionID, outputID sql.NullString
		if err := rows.Scan(&gid, &name, &severity, &actionID, &outputID); err != nil {
			return nil, fmt.Errorf("scan group routing: %w", err)
		}
		i, ok := index[gid]
		if !ok {
			out = append(out, storage.GroupRouting{GroupID: gid, Name: name, MinSeverity: severity})
			i = len(out) - 1
			index[gid] = i
		}
		if actionID.Valid {
			out[i].Actions = append(out[i].Actions, actionID.String)
		}
		if outputID.Valid {
			out[i].Outputs = append(out[i].Outputs, outputID.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group routings: %w", err)
	}
	for i := range out {
		out[i].Actions = dedupeIDs(out[i].Actions)
		out[i].Outputs = dedupeIDs(out[i].Outputs)
	}
	return out, nil
}

// groupActionIDs reads the assigned action IDs for a group, sorted.
func (s *Store) groupActionIDs(groupID int64) ([]string, error) {
	rows, err := s.db.Query(`
		SELECT action_id FROM group_actions WHERE group_id = ? ORDER BY action_id ASC`, groupID)
	if err != nil {
		return nil, fmt.Errorf("list group %d actions: %w", groupID, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan group %d action: %w", groupID, err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// groupOutputIDs reads the assigned output IDs for a group, sorted.
func (s *Store) groupOutputIDs(groupID int64) ([]string, error) {
	rows, err := s.db.Query(`
		SELECT output_id FROM group_outputs WHERE group_id = ? ORDER BY output_id ASC`, groupID)
	if err != nil {
		return nil, fmt.Errorf("list group %d outputs: %w", groupID, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan group %d output: %w", groupID, err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// dedupeIDs removes duplicates while preserving order.
func dedupeIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := ids[:0]
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
