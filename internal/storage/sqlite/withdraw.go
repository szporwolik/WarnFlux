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

// WithdrawEvent cancels one still-active event and records the cancellation in
// the durable journal, in ONE transaction.
//
// It is the reconciliation step for a source that is switched off (or removed
// from the configuration): such a source never publishes again, so its events
// would stay "live" on the panel and the public page until their own expiry —
// road works carry expiries months ahead, so one disabled source kept nine
// stale warnings visible for a month (reported P2).
//
// Two properties matter:
//   - the journal record makes the broker side durable. The MQTT output removes
//     the retained document from its cursor, and a cursor is only acknowledged
//     after a successful publish, so a broker that is unreachable NOW is fixed
//     after a restart instead of leaving the document behind forever (a
//     best-effort delete from a goroutine lost it for good — reported P2);
//   - no inbox row is written. A withdrawal is the operator's decision, not an
//     alert for subscribers, so the routing engine must not notify anybody.
//
// A nil change means "nothing to do": the event is already cancelled or gone.
func (s *Store) WithdrawEvent(ctx context.Context, event core.HazardEvent, now time.Time) (*storage.Change, error) {
	key := event.Key()
	if now.IsZero() {
		now = s.now()
	}
	now = now.UTC()
	nowMs := now.UnixMilli()

	publisher, err := s.instanceID(ctx)
	if err != nil {
		return nil, fmt.Errorf("publisher id for withdrawal of %q: %w", key, err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin withdrawal of %q: %w", key, err)
	}
	defer tx.Rollback()

	var status string
	err = tx.QueryRowContext(ctx, "SELECT status FROM events WHERE event_key = ?", key).Scan(&status)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("get event %q: %w", key, err)
	case status != string(core.StatusActive):
		return nil, nil
	}

	at := now.Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx,
		"UPDATE events SET status = ?, updated_at = ? WHERE event_key = ?",
		string(core.StatusCancelled), at, key); err != nil {
		return nil, fmt.Errorf("cancel event %q: %w", key, err)
	}

	withdrawn := event
	withdrawn.Status = core.StatusCancelled
	withdrawn.UpdatedAt = now
	change, err := insertChange(tx, ctx, core.ChangeCancelled, key, nowMs, storage.SnapshotOf(withdrawn), publisher)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit withdrawal of %q: %w", key, err)
	}
	change.Event = withdrawn
	return change, nil
}
