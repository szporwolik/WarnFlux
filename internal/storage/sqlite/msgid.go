package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// LoadMessageID returns the persisted short message id of eventKey;
// ok=false when the key was never assigned.
func (s *Store) LoadMessageID(ctx context.Context, eventKey string) (string, bool, error) {
	var id string
	err := s.db.QueryRowContext(ctx,
		`SELECT msg_id FROM message_ids WHERE event_key = ?`, eventKey).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("load message id for %q: %w", eventKey, err)
	}
	return id, true, nil
}

// HighestSeqInMinute returns the largest sequence already handed out in
// the given minute bucket (0 when none). It lets a restarted process
// resume the per-minute sequence instead of reusing a number.
func (s *Store) HighestSeqInMinute(ctx context.Context, minute string) (int, error) {
	var seq int
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) FROM message_ids WHERE minute = ?`, minute).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("message id sequence for minute %q: %w", minute, err)
	}
	return seq, nil
}

// SaveMessageID persists one event-key → message-id assignment. The
// mapping is immutable: the first writer wins, so a replayed assignment
// never renumbers an already-cited event.
func (s *Store) SaveMessageID(ctx context.Context, a core.MessageIDAssignment) error {
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO message_ids(event_key, msg_id, minute, seq, created_at_ms)
		VALUES(?, ?, ?, ?, ?)
		ON CONFLICT(event_key) DO NOTHING`,
		a.EventKey, a.MsgID, a.Minute, a.Seq, a.CreatedAt.UTC().UnixMilli()); err != nil {
		return fmt.Errorf("save message id for %q: %w", a.EventKey, err)
	}
	return nil
}

// PruneMessageIDs deletes assignments whose event no longer exists (in
// neither the current-state events nor the panel compose hazards) and that
// are older than olderThan. Ids of retained events are never touched, so a
// cited number cannot change while its event is kept.
func (s *Store) PruneMessageIDs(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM message_ids
		WHERE created_at_ms < ?
		  AND event_key NOT IN (SELECT event_key FROM events)
		  AND event_key NOT IN (SELECT event_key FROM compose_hazards)`,
		olderThan.UTC().UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("prune message ids: %w", err)
	}
	return res.RowsAffected()
}

var _ storage.MessageIDStore = (*Store)(nil)
