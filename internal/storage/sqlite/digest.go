package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// DigestCounters counts one reporting window for the daily admin status mail.
// The message counts come from the durable traffic aggregates
// (traffic_counters), NOT from the message history: the history tables keep
// only a few thousand newest rows, so a busy window was silently truncated at
// that bound (reported P3: 2001 APRS messages in one window were reported as
// 2000).
//
// The aggregates live in one-minute buckets and both window bounds are snapped
// down to that boundary — the digest builds its window the same way, so the
// numbers a report shows are exactly what it counted.
func (s *Store) DigestCounters(ctx context.Context, from, to time.Time) (storage.DigestCounters, error) {
	var c storage.DigestCounters
	fromMS, toMS := trafficMinuteBucket(from), trafficMinuteBucket(to)

	count := func(query string, dst *int, args ...any) error {
		return s.db.QueryRowContext(ctx, query, args...).Scan(dst)
	}

	directions := func(channel string) (rx, tx int, err error) {
		rows, err := s.db.QueryContext(ctx,
			`SELECT direction, SUM(messages) FROM traffic_counters
				WHERE channel = ? AND minute_ms >= ? AND minute_ms < ? GROUP BY direction`,
			channel, fromMS, toMS)
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
	if c.APRSReceived, c.APRSSent, err = directions(trafficChannelAPRS); err != nil {
		return c, fmt.Errorf("aprs counters: %w", err)
	}
	if c.SMSReceived, c.SMSSent, err = directions(trafficChannelGSM); err != nil {
		return c, fmt.Errorf("gsm counters: %w", err)
	}
	if c.MeshReceived, c.MeshSent, err = directions(trafficChannelMesh); err != nil {
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
