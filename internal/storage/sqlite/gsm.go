package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
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
	if _, err := s.PruneGSMMessagesOlderThan(ctx, at.Add(-storage.GSMMessageRetentionAge)); err != nil {
		return fmt.Errorf("prune gsm messages by age: %w", err)
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

// PruneGSMMessagesOlderThan deletes history rows created before cutoff
// and reports how many were removed. The 365-day age bound guarantees
// the SMS history never grows without bound even when the station is
// idle and the entry-count cap never engages.
func (s *Store) PruneGSMMessagesOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM gsm_messages WHERE created_at_ms < ?`,
		cutoff.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("prune gsm messages by age: %w", err)
	}
	return res.RowsAffected()
}

// legacyFullSegmentRunes are the payload sizes of a full segment of a
// concatenated message: 67 characters for UCS-2 (134 bytes / 2) and
// 152-153 septets for GSM-7 (160 minus the 6-octet concatenation
// header). Only the last segment of a message may be shorter, which is
// what makes a run of full segments recognisable as one split message.
var legacyFullSegmentRunes = map[int]bool{67: true, 152: true, 153: true}

// stripSMSPadding removes the zero-width characters senders use to pad a
// segment; they carry nothing and would otherwise sit invisibly in the
// middle of a joined message.
func stripSMSPadding(text string) string {
	return strings.NewReplacer("\u200B", "", "\uFEFF", "").Replace(text)
}

// joinLegacyGSMParts is the v58 migration: it joins the split parts of
// messages imported before this version, once, in the table.
//
// The modem reads stored messages in TEXT mode only (its firmware
// returns nothing for PDU-mode AT+CMGR), so a long message arrives as
// separate entries with no user-data header and no part identity to
// group them by. The display used to guess: every consecutive rx row
// from the same sender within the same minute was one message, which
// fused genuinely independent messages ("Test", "Gycji", "Yyy?" and "Test
// diagnostyczny ok" received within one minute became one text). That guess
// is gone from the read path; what is left is this one-off cleanup of
// the rows already stored, and it only joins a run whose parts are all
// FULL segments — the signature of a split message. A short row is its
// own message, and it also ends the previous one, so two concatenated
// messages delivered in the same minute stay two messages.
func joinLegacyGSMParts(tx *sql.Tx) error {
	rows, err := tx.Query(`
		SELECT id, from_number, text, created_at_ms
		FROM gsm_messages
		WHERE direction = 'rx' AND from_number <> '' AND from_number <> 'self'
		ORDER BY id`)
	if err != nil {
		return fmt.Errorf("read gsm messages: %w", err)
	}
	type legacyRow struct {
		id   int64
		from string
		text string
		atMS int64
	}
	var all []legacyRow
	for rows.Next() {
		var r legacyRow
		if err := rows.Scan(&r.id, &r.from, &r.text, &r.atMS); err != nil {
			rows.Close()
			return fmt.Errorf("scan gsm message: %w", err)
		}
		r.text = stripSMSPadding(r.text)
		all = append(all, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return fmt.Errorf("read gsm messages: %w", err)
	}

	const minuteMS = 60_000
	var group []legacyRow
	flush := func() error {
		defer func() { group = nil }()
		if len(group) < 2 {
			return nil
		}
		// The newest row keeps the joined text (the display lists newest
		// first, so that is where the message appeared) and the older
		// part rows are dropped.
		var sb strings.Builder
		for _, r := range group {
			sb.WriteString(r.text)
		}
		keep := group[len(group)-1]
		for _, r := range group[:len(group)-1] {
			if _, err := tx.Exec(`DELETE FROM gsm_messages WHERE id = ?`, r.id); err != nil {
				return fmt.Errorf("drop part %d: %w", r.id, err)
			}
		}
		if _, err := tx.Exec(`UPDATE gsm_messages SET text = ? WHERE id = ?`, sb.String(), keep.id); err != nil {
			return fmt.Errorf("join parts into %d: %w", keep.id, err)
		}
		return nil
	}

	for _, r := range all {
		if len(group) > 0 {
			prev := group[len(group)-1]
			if prev.from != r.from || prev.atMS/minuteMS != r.atMS/minuteMS {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		group = append(group, r)
		// A short segment is the last one: the message ends here, and the
		// next full segment starts a new message.
		if !legacyFullSegmentRunes[len([]rune(r.text))] {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}
