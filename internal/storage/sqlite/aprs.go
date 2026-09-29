package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// RecordAPRSMessage appends one APRS message (rx or tx) to the durable
// history and prunes the table back to storage.APRSMessageRetentionEntries
// newest rows.
func (s *Store) RecordAPRSMessage(ctx context.Context, direction, from, to, text, msgID, via string, at time.Time) error {
	if direction != "rx" && direction != "tx" {
		return fmt.Errorf("aprs message: invalid direction %q", direction)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO aprs_messages (direction, from_call, to_call, text, msg_id, via, created_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		direction, from, to, text, msgID, via, at.UnixMilli()); err != nil {
		return fmt.Errorf("insert aprs message: %w", err)
	}
	if _, err := s.PruneAPRSMessages(ctx, storage.APRSMessageRetentionEntries); err != nil {
		return fmt.Errorf("prune aprs messages: %w", err)
	}
	return nil
}

// ListAPRSMessages returns history rows newest first. direction is "rx",
// "tx" or "" (both).
func (s *Store) ListAPRSMessages(ctx context.Context, direction string, limit, offset int) ([]storage.APRSMessage, error) {
	query := `
		SELECT id, direction, from_call, to_call, text, msg_id, via, created_at_ms
		FROM aprs_messages`
	args := []any{}
	if direction == "rx" || direction == "tx" {
		query += ` WHERE direction = ?`
		args = append(args, direction)
	}
	query += ` ORDER BY id DESC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list aprs messages: %w", err)
	}
	defer rows.Close()

	out := make([]storage.APRSMessage, 0, limit)
	for rows.Next() {
		var m storage.APRSMessage
		var atMs int64
		if err := rows.Scan(&m.ID, &m.Direction, &m.From, &m.To, &m.Text, &m.MsgID, &m.Via, &atMs); err != nil {
			return nil, fmt.Errorf("scan aprs message: %w", err)
		}
		m.At = time.UnixMilli(atMs)
		out = append(out, m)
	}
	return out, rows.Err()
}

// CountAPRSMessages counts history rows, optionally filtered by direction.
func (s *Store) CountAPRSMessages(ctx context.Context, direction string) (int, error) {
	query := `SELECT COUNT(*) FROM aprs_messages`
	args := []any{}
	if direction == "rx" || direction == "tx" {
		query += ` WHERE direction = ?`
		args = append(args, direction)
	}
	var n int
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("count aprs messages: %w", err)
	}
	return n, nil
}

// PruneAPRSMessages deletes all but the newest keep rows.
func (s *Store) PruneAPRSMessages(ctx context.Context, keep int) (int64, error) {
	if keep < 1 {
		return 0, nil
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM aprs_messages WHERE id NOT IN (
			SELECT id FROM aprs_messages ORDER BY id DESC LIMIT ?
		)`, keep)
	if err != nil {
		return 0, fmt.Errorf("prune aprs messages: %w", err)
	}
	return res.RowsAffected()
}
