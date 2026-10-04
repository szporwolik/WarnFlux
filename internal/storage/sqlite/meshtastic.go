package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// RecordMeshtasticMessage appends one Meshtastic message and prunes the table back
// to storage.MeshMessageRetentionEntries newest rows.
func (s *Store) RecordMeshtasticMessage(ctx context.Context, direction, sender, channel, text, operator string, hops int, at time.Time) error {
	if direction != "rx" && direction != "tx" {
		return fmt.Errorf("mesh message: invalid direction %q", direction)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO meshtastic_messages (direction, sender, channel, hops, operator, text, created_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		direction, sender, channel, hops, operator, text, at.UnixMilli()); err != nil {
		return fmt.Errorf("insert mesh message: %w", err)
	}
	if _, err := s.PruneMeshtasticMessages(ctx, storage.MeshMessageRetentionEntries); err != nil {
		return fmt.Errorf("prune mesh messages: %w", err)
	}
	return nil
}

// UpdateMeshtasticMessageStatus marks the delivery state of the matching
// TX row (created_at_ms + text): the hub stamps "sent" when the radio
// transmits the frame, "delivered" on the recipient's acknowledgment,
// "failed" when the acknowledgment never arrives.
func (s *Store) UpdateMeshtasticMessageStatus(ctx context.Context, status string, at time.Time, text string) error {
	if _, err := s.db.ExecContext(ctx, `
		UPDATE meshtastic_messages SET status = ?
		WHERE direction = 'tx' AND created_at_ms = ? AND text = ?`,
		status, at.UnixMilli(), text); err != nil {
		return fmt.Errorf("update mesh message status: %w", err)
	}
	return nil
}

// ListMeshtasticMessages returns history rows newest first. The filter
// narrows by direction ("rx", "tx" or "" for both) and by exact channel
// label (Channel; Exclude inverts it).
func (s *Store) ListMeshtasticMessages(ctx context.Context, f storage.MeshtasticMessageFilter, limit, offset int) ([]storage.MeshMessage, error) {
	query := `
		SELECT id, direction, sender, channel, hops, operator, text, status, created_at_ms
		FROM meshtastic_messages` + meshMessageWhere(f)
	query += ` ORDER BY id DESC LIMIT ? OFFSET ?`
	args := append(meshMessageArgs(f), limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list mesh messages: %w", err)
	}
	defer rows.Close()

	out := make([]storage.MeshMessage, 0, limit)
	for rows.Next() {
		var m storage.MeshMessage
		var atMs int64
		if err := rows.Scan(&m.ID, &m.Direction, &m.Sender, &m.Channel, &m.Hops, &m.Operator, &m.Text, &m.Status, &atMs); err != nil {
			return nil, fmt.Errorf("scan mesh message: %w", err)
		}
		m.At = time.UnixMilli(atMs)
		out = append(out, m)
	}
	return out, rows.Err()
}

// meshMessageWhere builds the shared WHERE clause (direction and/or
// channel condition) for the message history queries.
func meshMessageWhere(f storage.MeshtasticMessageFilter) string {
	conds := make([]string, 0, 2)
	if f.Direction == "rx" || f.Direction == "tx" {
		conds = append(conds, `direction = ?`)
	}
	if f.Channel != "" {
		if f.Exclude {
			conds = append(conds, `channel != ?`)
		} else {
			conds = append(conds, `channel = ?`)
		}
	}
	if len(conds) == 0 {
		return ""
	}
	return ` WHERE ` + strings.Join(conds, " AND ")
}

// meshMessageArgs returns the filter's bind values in the same order the
// conditions appear in meshMessageWhere.
func meshMessageArgs(f storage.MeshtasticMessageFilter) []any {
	var args []any
	if f.Direction == "rx" || f.Direction == "tx" {
		args = append(args, f.Direction)
	}
	if f.Channel != "" {
		args = append(args, f.Channel)
	}
	return args
}

// CountMeshtasticMessages counts history rows under the same filter as
// ListMeshtasticMessages.
func (s *Store) CountMeshtasticMessages(ctx context.Context, f storage.MeshtasticMessageFilter) (int, error) {
	query := `SELECT COUNT(*) FROM meshtastic_messages` + meshMessageWhere(f)
	var n int
	if err := s.db.QueryRowContext(ctx, query, meshMessageArgs(f)...).Scan(&n); err != nil {
		return 0, fmt.Errorf("count mesh messages: %w", err)
	}
	return n, nil
}

// PruneMeshtasticMessages deletes all but the newest keep rows.
func (s *Store) PruneMeshtasticMessages(ctx context.Context, keep int) (int64, error) {
	if keep < 1 {
		return 0, nil
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM meshtastic_messages WHERE id NOT IN (
			SELECT id FROM meshtastic_messages ORDER BY id DESC LIMIT ?
		)`, keep)
	if err != nil {
		return 0, fmt.Errorf("prune mesh messages: %w", err)
	}
	return res.RowsAffected()
}

// LoadMeshtasticNodes returns the persisted heard-node directory.
func (s *Store) LoadMeshtasticNodes(ctx context.Context) ([]storage.MeshtasticNode, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, short, lat, lon, last_seen_ms, sends
		FROM meshtastic_nodes ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("load meshtastic nodes: %w", err)
	}
	defer rows.Close()
	var out []storage.MeshtasticNode
	for rows.Next() {
		var n storage.MeshtasticNode
		var seenMs int64
		var sends string
		if err := rows.Scan(&n.ID, &n.Name, &n.Short, &n.Lat, &n.Lon, &seenMs, &sends); err != nil {
			return nil, fmt.Errorf("scan meshtastic node: %w", err)
		}
		n.LastSeen = time.UnixMilli(seenMs)
		if sends != "" {
			n.Sends = strings.Split(sends, ",")
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// SaveMeshtasticNodes replaces the heard-node directory with the given
// snapshot (one transaction; the directory is a single small table).
func (s *Store) SaveMeshtasticNodes(ctx context.Context, nodes []storage.MeshtasticNode) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("save meshtastic nodes: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM meshtastic_nodes`); err != nil {
		return fmt.Errorf("save meshtastic nodes: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO meshtastic_nodes (id, name, short, lat, lon, last_seen_ms, sends)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("save meshtastic nodes: %w", err)
	}
	defer stmt.Close()
	for _, n := range nodes {
		if _, err := stmt.ExecContext(ctx, n.ID, n.Name, n.Short, n.Lat, n.Lon,
			n.LastSeen.UnixMilli(), strings.Join(n.Sends, ",")); err != nil {
			return fmt.Errorf("save meshtastic node %s: %w", n.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("save meshtastic nodes: %w", err)
	}
	return nil
}
