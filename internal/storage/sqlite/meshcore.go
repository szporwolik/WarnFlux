package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// RecordMeshMessage appends one MeshCore message and prunes the table back
// to storage.MeshMessageRetentionEntries newest rows.
func (s *Store) RecordMeshMessage(ctx context.Context, direction, sender, channel, text, operator string, hops int, at time.Time) error {
	if direction != "rx" && direction != "tx" {
		return fmt.Errorf("mesh message: invalid direction %q", direction)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO meshcore_messages (direction, sender, channel, hops, operator, text, created_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		direction, sender, channel, hops, operator, text, at.UnixMilli()); err != nil {
		return fmt.Errorf("insert mesh message: %w", err)
	}
	if _, err := s.PruneMeshMessages(ctx, storage.MeshMessageRetentionEntries); err != nil {
		return fmt.Errorf("prune mesh messages: %w", err)
	}
	return nil
}

// ListMeshMessages returns history rows newest first. direction is "rx",
// "tx" or "" (both).
func (s *Store) ListMeshMessages(ctx context.Context, direction string, limit, offset int) ([]storage.MeshMessage, error) {
	query := `
		SELECT id, direction, sender, channel, hops, operator, text, created_at_ms
		FROM meshcore_messages`
	args := []any{}
	if direction == "rx" || direction == "tx" {
		query += ` WHERE direction = ?`
		args = append(args, direction)
	}
	query += ` ORDER BY id DESC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list mesh messages: %w", err)
	}
	defer rows.Close()

	out := make([]storage.MeshMessage, 0, limit)
	for rows.Next() {
		var m storage.MeshMessage
		var atMs int64
		if err := rows.Scan(&m.ID, &m.Direction, &m.Sender, &m.Channel, &m.Hops, &m.Operator, &m.Text, &atMs); err != nil {
			return nil, fmt.Errorf("scan mesh message: %w", err)
		}
		m.At = time.UnixMilli(atMs)
		out = append(out, m)
	}
	return out, rows.Err()
}

// CountMeshMessages counts history rows, optionally filtered by direction.
func (s *Store) CountMeshMessages(ctx context.Context, direction string) (int, error) {
	query := `SELECT COUNT(*) FROM meshcore_messages`
	args := []any{}
	if direction == "rx" || direction == "tx" {
		query += ` WHERE direction = ?`
		args = append(args, direction)
	}
	var n int
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("count mesh messages: %w", err)
	}
	return n, nil
}

// PruneMeshMessages deletes all but the newest keep rows.
func (s *Store) PruneMeshMessages(ctx context.Context, keep int) (int64, error) {
	if keep < 1 {
		return 0, nil
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM meshcore_messages WHERE id NOT IN (
			SELECT id FROM meshcore_messages ORDER BY id DESC LIMIT ?
		)`, keep)
	if err != nil {
		return 0, fmt.Errorf("prune mesh messages: %w", err)
	}
	return res.RowsAffected()
}
