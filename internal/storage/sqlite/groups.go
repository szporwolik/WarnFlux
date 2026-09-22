package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// groupColumns is the canonical group column list for SELECTs. Member
// counts are aggregated separately so the base list stays simple.
const groupColumns = `g.id, g.name, g.min_severity, g.created_at_ms, g.updated_at_ms`

// groupBaseSQL selects groups with their member counts; callers append
// WHERE/GROUP BY/ORDER BY/LIMIT clauses.
const groupBaseSQL = `
	SELECT ` + groupColumns + `, COUNT(ug.user_id)
	FROM groups g
	LEFT JOIN user_groups ug ON ug.group_id = g.id`

// groupListSQL selects groups with their member counts.
const groupListSQL = groupBaseSQL + ` GROUP BY g.id`

// ListGroups returns the groups on the given 1-based page plus the total
// count, each with its member count. Pages beyond the last valid one are
// clamped. Ordering is by name, case-insensitive.
func (s *Store) ListGroups(page, perPage int) ([]storage.Group, int, error) {
	if perPage < 1 {
		perPage = 1
	}
	var total int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM groups`).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count groups: %w", err)
	}
	pages := (total + perPage - 1) / perPage
	if pages < 1 {
		pages = 1
	}
	if page < 1 {
		page = 1
	}
	if page > pages {
		page = pages
	}
	rows, err := s.db.Query(groupListSQL+`
		ORDER BY g.name COLLATE NOCASE ASC
		LIMIT ? OFFSET ?`, perPage, (page-1)*perPage)
	if err != nil {
		return nil, 0, fmt.Errorf("list groups: %w", err)
	}
	defer rows.Close()
	out := make([]storage.Group, 0, perPage)
	for rows.Next() {
		g, err := scanGroup(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan group: %w", err)
		}
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate groups: %w", err)
	}
	return out, total, nil
}

// ListAllGroups returns every group with its member count, ordered by
// name. Used by the users page group picker.
func (s *Store) ListAllGroups() ([]storage.Group, error) {
	rows, err := s.db.Query(groupListSQL + `
		ORDER BY g.name COLLATE NOCASE ASC`)
	if err != nil {
		return nil, fmt.Errorf("list all groups: %w", err)
	}
	defer rows.Close()
	var out []storage.Group
	for rows.Next() {
		g, err := scanGroup(rows)
		if err != nil {
			return nil, fmt.Errorf("scan group: %w", err)
		}
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate groups: %w", err)
	}
	return out, nil
}

// GetGroup returns one group with its member count.
func (s *Store) GetGroup(id int64) (storage.Group, error) {
	row := s.db.QueryRow(groupBaseSQL+` WHERE g.id = ? GROUP BY g.id`, id)
	g, err := scanGroup(row)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.Group{}, storage.ErrGroupNotFound
	}
	if err != nil {
		return storage.Group{}, err
	}
	return g, nil
}

// CreateGroup inserts a new group. A duplicate name (case-insensitive)
// reports storage.ErrGroupNameTaken.
func (s *Store) CreateGroup(name string) (storage.Group, error) {
	taken, err := s.groupNameTaken(name, 0)
	if err != nil {
		return storage.Group{}, err
	}
	if taken {
		return storage.Group{}, storage.ErrGroupNameTaken
	}
	now := s.now().UnixMilli()
	res, err := s.db.Exec(`
		INSERT INTO groups (name, created_at_ms, updated_at_ms)
		VALUES (?, ?, ?)`, name, now, now)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return storage.Group{}, storage.ErrGroupNameTaken
		}
		return storage.Group{}, fmt.Errorf("insert group %q: %w", name, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return storage.Group{}, fmt.Errorf("insert group %q: %w", name, err)
	}
	return s.GetGroup(id)
}

// UpdateGroup renames a group. A duplicate name reports
// storage.ErrGroupNameTaken; a missing ID reports storage.ErrGroupNotFound.
func (s *Store) UpdateGroup(id int64, name string) (storage.Group, error) {
	taken, err := s.groupNameTaken(name, id)
	if err != nil {
		return storage.Group{}, err
	}
	if taken {
		return storage.Group{}, storage.ErrGroupNameTaken
	}
	now := s.now().UnixMilli()
	res, err := s.db.Exec(`
		UPDATE groups SET name = ?, updated_at_ms = ? WHERE id = ?`,
		name, now, id)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return storage.Group{}, storage.ErrGroupNameTaken
		}
		return storage.Group{}, fmt.Errorf("update group %d: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return storage.Group{}, fmt.Errorf("update group %d: %w", id, err)
	}
	if n == 0 {
		return storage.Group{}, storage.ErrGroupNotFound
	}
	return s.GetGroup(id)
}

// DeleteGroup removes a group and (via the foreign-key cascade) its
// membership rows.
func (s *Store) DeleteGroup(id int64) error {
	res, err := s.db.Exec(`DELETE FROM groups WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete group %d: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete group %d: %w", id, err)
	}
	if n == 0 {
		return storage.ErrGroupNotFound
	}
	return nil
}

// GroupIDsForUser returns the IDs of the groups the user belongs to,
// ascending.
func (s *Store) GroupIDsForUser(userID int64) ([]int64, error) {
	rows, err := s.db.Query(`
		SELECT group_id FROM user_groups WHERE user_id = ? ORDER BY group_id ASC`,
		userID)
	if err != nil {
		return nil, fmt.Errorf("list user %d groups: %w", userID, err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var gid int64
		if err := rows.Scan(&gid); err != nil {
			return nil, fmt.Errorf("scan user %d group: %w", userID, err)
		}
		out = append(out, gid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate user %d groups: %w", userID, err)
	}
	return out, nil
}

// SetUserGroups replaces the user's group membership with groupIDs.
// Unknown group or user IDs are rejected by the foreign-key constraint.
func (s *Store) SetUserGroups(userID int64, groupIDs []int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin set groups for user %d: %w", userID, err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM user_groups WHERE user_id = ?`, userID); err != nil {
		return fmt.Errorf("clear groups for user %d: %w", userID, err)
	}
	for _, gid := range groupIDs {
		if _, err := tx.Exec(`
			INSERT INTO user_groups (user_id, group_id) VALUES (?, ?)`,
			userID, gid); err != nil {
			return fmt.Errorf("assign group %d to user %d: %w", gid, userID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit set groups for user %d: %w", userID, err)
	}
	return nil
}

// groupNameTaken reports whether another row already uses the name.
func (s *Store) groupNameTaken(name string, excludeID int64) (bool, error) {
	var exists int
	err := s.db.QueryRow(`
		SELECT 1 FROM groups WHERE name = ? COLLATE NOCASE AND id != ?`,
		name, excludeID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check group name %q: %w", name, err)
	}
	return true, nil
}

// scanGroup reads one group row (id, name, min_severity, timestamps,
// member count).
type groupScanner interface {
	Scan(dest ...any) error
}

func scanGroup(sc groupScanner) (storage.Group, error) {
	var g storage.Group
	var created, updated int64
	if err := sc.Scan(&g.ID, &g.Name, &g.MinSeverity, &created, &updated, &g.Members); err != nil {
		return storage.Group{}, err
	}
	g.CreatedAt = time.UnixMilli(created)
	g.UpdatedAt = time.UnixMilli(updated)
	return g, nil
}
