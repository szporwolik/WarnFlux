package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// DigestCounters counts one reporting window for the daily admin status mail.
// Every message table stores its time as epoch milliseconds, so the window is
// half-open ([from, to)) and a message is never counted twice.
func (s *Store) DigestCounters(ctx context.Context, from, to time.Time) (storage.DigestCounters, error) {
	var c storage.DigestCounters
	fromMS, toMS := from.UnixMilli(), to.UnixMilli()

	count := func(query string, dst *int, args ...any) error {
		return s.db.QueryRowContext(ctx, query, args...).Scan(dst)
	}

	direction := func(table string) (rx, tx int, err error) {
		rows, err := s.db.QueryContext(ctx,
			fmt.Sprintf(`SELECT direction, COUNT(*) FROM %s
				WHERE created_at_ms >= ? AND created_at_ms < ? GROUP BY direction`, table),
			fromMS, toMS)
		if err != nil {
			return 0, 0, err
		}
		defer rows.Close()
		for rows.Next() {
			var dir string
			var n int
			if err := rows.Scan(&dir, &n); err != nil {
				return 0, 0, err
			}
			switch dir {
			case "rx":
				rx = n
			case "tx":
				tx = n
			}
		}
		return rx, tx, rows.Err()
	}

	var err error
	if c.APRSReceived, c.APRSSent, err = direction("aprs_messages"); err != nil {
		return c, fmt.Errorf("aprs counters: %w", err)
	}
	if c.SMSReceived, c.SMSSent, err = direction("gsm_messages"); err != nil {
		return c, fmt.Errorf("gsm counters: %w", err)
	}
	if c.MeshReceived, c.MeshSent, err = direction("meshtastic_messages"); err != nil {
		return c, fmt.Errorf("meshtastic counters: %w", err)
	}

	// The events table stores its human timestamps as RFC3339 text (only
	// last_seen_at has a millisecond twin), so the window is compared with
	// julianday() rather than as integers.
	if err := count(`SELECT COUNT(*) FROM events
		WHERE julianday(first_seen_at) >= julianday(?) AND julianday(first_seen_at) < julianday(?)`,
		&c.HazardsNew, from.UTC().Format(time.RFC3339Nano), to.UTC().Format(time.RFC3339Nano)); err != nil {
		return c, fmt.Errorf("new hazards: %w", err)
	}
	if err := count(`SELECT COUNT(*) FROM events WHERE status = 'active'`, &c.HazardsActive); err != nil {
		return c, fmt.Errorf("active hazards: %w", err)
	}
	if err := count(`SELECT COUNT(*) FROM events
		WHERE status IN ('cancelled','expired') AND julianday(updated_at) >= julianday(?)`,
		&c.HazardsClosed, from.UTC().Format(time.RFC3339Nano)); err != nil {
		return c, fmt.Errorf("closed hazards: %w", err)
	}
	if err := count(`SELECT COUNT(*) FROM action_fires WHERE fired_at_ms >= ? AND fired_at_ms < ?`,
		&c.Deliveries, fromMS, toMS); err != nil {
		return c, fmt.Errorf("deliveries: %w", err)
	}
	if err := count(`SELECT COUNT(*) FROM action_fires WHERE fired_at_ms >= ? AND fired_at_ms < ? AND status = 'failed'`,
		&c.DeliveriesFailed, fromMS, toMS); err != nil {
		return c, fmt.Errorf("failed deliveries: %w", err)
	}
	return c, nil
}

// AdminEmails lists the e-mail addresses of the administrator accounts: the
// daily digest is addressed to them unless the configuration names explicit
// recipients.
func (s *Store) AdminEmails(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT email FROM users WHERE email <> '' AND (is_admin = 1 OR role = 'admin') ORDER BY email`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var email string
		if err := rows.Scan(&email); err != nil {
			return nil, err
		}
		out = append(out, email)
	}
	return out, rows.Err()
}
