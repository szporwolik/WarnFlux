package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// WithdrawSourceEvents cancels every ACTIVE event of one source and returns
// the withdrawn events so the caller can remove their retained documents from
// the broker.
//
// A source the operator switched off (or removed from the configuration) never
// publishes again, so its events would otherwise stay "live" on the panel and
// the public page until their own expiry — road works carry expiries months
// ahead, so a single disabled source kept nine stale warnings on the public
// page for a month after it was switched off (reported P2). The withdrawal is
// recorded as cancelled, which is the state the station already understands:
// cancelled events are not active, are never notified again, and stay in the
// archive as history.
//
// The update is one statement, so a crash cannot leave a source half
// withdrawn; the returned events carry the status the caller may rely on.
func (s *Store) WithdrawSourceEvents(ctx context.Context, source string, now time.Time) ([]core.HazardEvent, error) {
	if source == "" {
		return nil, fmt.Errorf("withdraw source events: empty source")
	}
	if now.IsZero() {
		now = s.now()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("withdraw %s events: %w", source, err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx,
		"SELECT "+eventColumns+" FROM events WHERE source = ? AND status = ? ORDER BY event_key",
		source, string(core.StatusActive))
	if err != nil {
		return nil, fmt.Errorf("select %s events: %w", source, err)
	}
	var withdrawn []core.HazardEvent
	for rows.Next() {
		event, err := scanEventRow(rows.Scan, nil)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan %s event: %w", source, err)
		}
		withdrawn = append(withdrawn, event)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate %s events: %w", source, err)
	}
	rows.Close()
	if len(withdrawn) == 0 {
		return nil, nil
	}

	at := now.UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx,
		"UPDATE events SET status = ?, updated_at = ? WHERE source = ? AND status = ?",
		string(core.StatusCancelled), at, source, string(core.StatusActive)); err != nil {
		return nil, fmt.Errorf("cancel %s events: %w", source, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit %s withdrawal: %w", source, err)
	}
	for i := range withdrawn {
		withdrawn[i].Status = core.StatusCancelled
		withdrawn[i].UpdatedAt = now
	}
	return withdrawn, nil
}
