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
		SELECT id, kind, target_id, label, channel_idx, text, operator, created_ms, attempts, last_error,
		       sent_ms, claimed_ms, finished_ms
		FROM mesh_outbox WHERE sent_ms IS NULL AND finished_ms IS NULL AND attempts < ?
		ORDER BY id LIMIT ?`, storage.MeshOutboxMaxAttempts, limit)
}

// RecentMeshOutbox returns the newest rows for the panel, sent or not.
func (s *Store) RecentMeshOutbox(ctx context.Context, limit int) ([]storage.MeshOutboxItem, error) {
	if limit <= 0 {
		limit = 10
	}
	return s.listMeshOutbox(ctx, `
		SELECT id, kind, target_id, label, channel_idx, text, operator, created_ms, attempts, last_error,
		       sent_ms, claimed_ms, finished_ms
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
			item       storage.MeshOutboxItem
			createdMS  int64
			sentMS     *int64
			claimedMS  *int64
			finishedMS *int64
		)
		if err := rows.Scan(&item.ID, &item.Kind, &item.TargetID, &item.Label, &item.ChannelIdx,
			&item.Text, &item.Operator, &createdMS, &item.Attempts, &item.LastError,
			&sentMS, &claimedMS, &finishedMS); err != nil {
			return nil, fmt.Errorf("scan mesh outbox: %w", err)
		}
		item.CreatedAt = time.UnixMilli(createdMS)
		if sentMS != nil {
			item.SentAt = time.UnixMilli(*sentMS)
		}
		if claimedMS != nil {
			item.ClaimedAt = time.UnixMilli(*claimedMS)
		}
		if finishedMS != nil {
			item.FinishedAt = time.UnixMilli(*finishedMS)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// ClaimMeshOutbox takes one pending row for a single transmission attempt. The
// UPDATE is the arbitration point: the operator's cancel and this claim compete
// for the same "still pending and unclaimed" state, and exactly one of them
// wins. A claim older than staleBefore belonged to a pass that died.
func (s *Store) ClaimMeshOutbox(ctx context.Context, id int64, at, staleBefore time.Time) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE mesh_outbox SET claimed_ms = ?
		WHERE id = ? AND sent_ms IS NULL AND finished_ms IS NULL
		  AND (claimed_ms IS NULL OR claimed_ms < ?)`,
		at.UnixMilli(), id, staleBefore.UnixMilli())
	if err != nil {
		return false, fmt.Errorf("claim mesh outbox: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claim mesh outbox: %w", err)
	}
	return n == 1, nil
}

// MarkMeshOutboxSent records a successful transmission and releases the claim.
func (s *Store) MarkMeshOutboxSent(ctx context.Context, id int64, at time.Time) error {
	ms := at.UnixMilli()
	if _, err := s.db.ExecContext(ctx,
		`UPDATE mesh_outbox SET sent_ms = ?, finished_ms = ?, claimed_ms = NULL WHERE id = ?`,
		ms, ms, id); err != nil {
		return fmt.Errorf("mark mesh outbox sent: %w", err)
	}
	return nil
}

// MarkMeshOutboxAttempt records one failed delivery attempt, releases the claim
// and finishes the row when it has no attempts left: from then on it is history,
// not a message waiting for the radio.
func (s *Store) MarkMeshOutboxAttempt(ctx context.Context, id int64, errText string, at time.Time) error {
	if _, err := s.db.ExecContext(ctx, `
		UPDATE mesh_outbox
		SET attempts = attempts + 1, last_error = ?, claimed_ms = NULL,
		    finished_ms = CASE WHEN attempts + 1 >= ? THEN ? ELSE finished_ms END
		WHERE id = ?`,
		errText, storage.MeshOutboxMaxAttempts, at.UnixMilli(), id); err != nil {
		return fmt.Errorf("mark mesh outbox attempt: %w", err)
	}
	return nil
}

// DeletePendingMeshOutbox removes one waiting row. It refuses rows the
// dispatcher has already claimed, so a cancel can never claim a success it did
// not achieve.
func (s *Store) DeletePendingMeshOutbox(ctx context.Context, id int64) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM mesh_outbox WHERE id = ? AND claimed_ms IS NULL AND finished_ms IS NULL`, id)
	if err != nil {
		return false, fmt.Errorf("delete mesh outbox: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delete mesh outbox: %w", err)
	}
	return n == 1, nil
}

// PruneMeshOutbox drops FINISHED rows older than the cutoff. Pending rows are
// never touched: a message that waited out a long outage is still owed a
// delivery attempt (WF-R4-01).
func (s *Store) PruneMeshOutbox(ctx context.Context, before time.Time) error {
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM mesh_outbox WHERE finished_ms IS NOT NULL AND finished_ms < ?`,
		before.UnixMilli()); err != nil {
		return fmt.Errorf("prune mesh outbox: %w", err)
	}
	return nil
}
