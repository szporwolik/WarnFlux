package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// RecordGSMMessage appends one SMS message (rx or tx) to the durable
// history and prunes the table back to
// storage.GSMMessageRetentionEntries newest rows.
func (s *Store) RecordGSMMessage(ctx context.Context, direction, from, to, text string, at time.Time) error {
	if direction != "rx" && direction != "tx" {
		return fmt.Errorf("gsm message: invalid direction %q", direction)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gsm_messages (direction, from_number, to_number, text, created_at_ms)
		VALUES (?, ?, ?, ?, ?)`,
		direction, from, to, text, at.UnixMilli()); err != nil {
		return fmt.Errorf("insert gsm message: %w", err)
	}
	if _, err := s.PruneGSMMessages(ctx, storage.GSMMessageRetentionEntries); err != nil {
		return fmt.Errorf("prune gsm messages: %w", err)
	}
	return nil
}

// ListGSMMessages returns history rows newest first. direction is "rx",
// "tx" or "" (both).
func (s *Store) ListGSMMessages(ctx context.Context, direction string, limit, offset int) ([]storage.GSMMessage, error) {
	query := `
		SELECT id, direction, from_number, to_number, text, created_at_ms
		FROM gsm_messages`
	args := []any{}
	if direction == "rx" || direction == "tx" {
		query += ` WHERE direction = ?`
		args = append(args, direction)
	}
	query += ` ORDER BY id DESC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list gsm messages: %w", err)
	}
	defer rows.Close()

	out := make([]storage.GSMMessage, 0, limit)
	for rows.Next() {
		var m storage.GSMMessage
		var atMs int64
		if err := rows.Scan(&m.ID, &m.Direction, &m.From, &m.To, &m.Text, &atMs); err != nil {
			return nil, fmt.Errorf("scan gsm message: %w", err)
		}
		m.At = time.UnixMilli(atMs)
		out = append(out, m)
	}
	return out, rows.Err()
}

// CountGSMMessages counts history rows, optionally per direction.
func (s *Store) CountGSMMessages(ctx context.Context, direction string) (int, error) {
	query := `SELECT COUNT(*) FROM gsm_messages`
	args := []any{}
	if direction == "rx" || direction == "tx" {
		query += ` WHERE direction = ?`
		args = append(args, direction)
	}
	var n int
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("count gsm messages: %w", err)
	}
	return n, nil
}

// PruneGSMMessages keeps only the newest keep rows and reports how many
// were removed.
func (s *Store) PruneGSMMessages(ctx context.Context, keep int) (int, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM gsm_messages
		WHERE id NOT IN (SELECT id FROM gsm_messages ORDER BY id DESC LIMIT ?)`,
		keep)
	if err != nil {
		return 0, fmt.Errorf("prune gsm messages: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("prune gsm messages: %w", err)
	}
	return int(n), nil
}
