package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// EnqueueMeshOutbox stores one message waiting for the Meshtastic device.
func (s *Store) EnqueueMeshOutbox(ctx context.Context, item storage.MeshOutboxItem) (int64, error) {
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now()
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO mesh_outbox(kind, target_id, label, channel_idx, text, operator, created_ms)
		VALUES(?, ?, ?, ?, ?, ?, ?)`,
		item.Kind, item.TargetID, item.Label, item.ChannelIdx, item.Text, item.Operator,
		item.CreatedAt.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("enqueue mesh outbox: %w", err)
	}
	return res.LastInsertId()
}

// PendingMeshOutbox returns the rows still to deliver, oldest first. Rows that
// exhausted their attempts are left out: they are abandoned, not pending.
func (s *Store) PendingMeshOutbox(ctx context.Context, limit int) ([]storage.MeshOutboxItem, error) {
	if limit <= 0 {
		limit = 10
	}
	return s.listMeshOutbox(ctx, `
		SELECT id, kind, target_id, label, channel_idx, text, operator, created_ms, attempts, last_error, sent_ms
		FROM mesh_outbox WHERE sent_ms IS NULL AND attempts < ?
		ORDER BY id LIMIT ?`, storage.MeshOutboxMaxAttempts, limit)
}

// RecentMeshOutbox returns the newest rows for the panel, sent or not.
func (s *Store) RecentMeshOutbox(ctx context.Context, limit int) ([]storage.MeshOutboxItem, error) {
	if limit <= 0 {
		limit = 10
	}
	return s.listMeshOutbox(ctx, `
		SELECT id, kind, target_id, label, channel_idx, text, operator, created_ms, attempts, last_error, sent_ms
		FROM mesh_outbox ORDER BY id DESC LIMIT ?`, limit)
}

func (s *Store) listMeshOutbox(ctx context.Context, query string, args ...any) ([]storage.MeshOutboxItem, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list mesh outbox: %w", err)
	}
	defer rows.Close()
	var out []storage.MeshOutboxItem
	for rows.Next() {
		var (
			item      storage.MeshOutboxItem
			createdMS int64
			sentMS    *int64
		)
		if err := rows.Scan(&item.ID, &item.Kind, &item.TargetID, &item.Label, &item.ChannelIdx,
			&item.Text, &item.Operator, &createdMS, &item.Attempts, &item.LastError, &sentMS); err != nil {
			return nil, fmt.Errorf("scan mesh outbox: %w", err)
		}
		item.CreatedAt = time.UnixMilli(createdMS)
		if sentMS != nil {
			item.SentAt = time.UnixMilli(*sentMS)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// MarkMeshOutboxSent records a successful transmission.
func (s *Store) MarkMeshOutboxSent(ctx context.Context, id int64, at time.Time) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE mesh_outbox SET sent_ms = ? WHERE id = ?`, at.UnixMilli(), id); err != nil {
		return fmt.Errorf("mark mesh outbox sent: %w", err)
	}
	return nil
}

// MarkMeshOutboxAttempt records one failed delivery attempt.
func (s *Store) MarkMeshOutboxAttempt(ctx context.Context, id int64, errText string) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE mesh_outbox SET attempts = attempts + 1, last_error = ? WHERE id = ?`, errText, id); err != nil {
		return fmt.Errorf("mark mesh outbox attempt: %w", err)
	}
	return nil
}

// DeleteMeshOutbox drops one row.
func (s *Store) DeleteMeshOutbox(ctx context.Context, id int64) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM mesh_outbox WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete mesh outbox: %w", err)
	}
	return nil
}

// PruneMeshOutbox drops everything older than the cutoff.
func (s *Store) PruneMeshOutbox(ctx context.Context, before time.Time) error {
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM mesh_outbox WHERE created_ms < ?`, before.UnixMilli()); err != nil {
		return fmt.Errorf("prune mesh outbox: %w", err)
	}
	return nil
}
