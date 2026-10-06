package sqlite

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// RecordMeshtasticMessage appends one Meshtastic message and prunes the table back
// to storage.MeshMessageRetentionEntries newest rows.
func (s *Store) RecordMeshtasticMessage(ctx context.Context, direction, sender, recipient, channel, text, operator string, hops int, at time.Time) error {
	if direction != "rx" && direction != "tx" {
		return fmt.Errorf("mesh message: invalid direction %q", direction)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO meshtastic_messages (direction, sender, recipient, channel, hops, operator, text, created_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		direction, sender, recipient, channel, hops, operator, text, at.UnixMilli()); err != nil {
		return fmt.Errorf("insert mesh message: %w", err)
	}
	if _, err := s.PruneMeshtasticMessages(ctx, storage.MeshMessageRetentionEntries); err != nil {
		return fmt.Errorf("prune mesh messages: %w", err)
	}
	return nil
}

// meshProgressRetention bounds the mesh action progress ledger: rows
// older than it are useless (a delivery job retries within minutes) and
// are pruned on every record.
const meshProgressRetention = 30 * 24 * time.Hour

// RecordMeshActionProgress durably marks one successful meshtastic
// action transmission: publisher + event key + change id (the message
// VERSION) + recipient (empty for a channel broadcast) + channel index
// (0 = direct message). The row is the resume ledger of the action's
// retries — independent of the message history, so a historical
// delivered message with the same text never suppresses a new alert
// version. Idempotent.
func (s *Store) RecordMeshActionProgress(ctx context.Context, publisher, eventKey string, changeID int64, recipient string, channel int, at time.Time) error {
	if _, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO mesh_action_progress (publisher, event_key, change_id, recipient, channel, created_at_ms)
		VALUES (?, ?, ?, ?, ?, ?)`,
		publisher, eventKey, changeID, recipient, channel, at.UnixMilli()); err != nil {
		return fmt.Errorf("record mesh action progress: %w", err)
	}
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM mesh_action_progress WHERE created_at_ms < ?`,
		s.now().Add(-meshProgressRetention).UnixMilli()); err != nil {
		return fmt.Errorf("prune mesh action progress: %w", err)
	}
	return nil
}

// MeshActionProgressDone reports whether the exact transmission (same
// version, recipient and channel) already has a progress row.
func (s *Store) MeshActionProgressDone(ctx context.Context, publisher, eventKey string, changeID int64, recipient string, channel int) (bool, error) {
	var done int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM mesh_action_progress
		WHERE publisher = ? AND event_key = ? AND change_id = ? AND recipient = ? AND channel = ?`,
		publisher, eventKey, changeID, recipient, channel).Scan(&done); err != nil {
		return false, fmt.Errorf("query mesh action progress: %w", err)
	}
	return done > 0, nil
}

// DeleteMeshActionProgress revokes the progress entry when the
// transmission ultimately failed: the next attempt sends the recipient
// again. Idempotent.
func (s *Store) DeleteMeshActionProgress(ctx context.Context, publisher, eventKey string, changeID int64, recipient string, channel int) error {
	if _, err := s.db.ExecContext(ctx, `
		DELETE FROM mesh_action_progress
		WHERE publisher = ? AND event_key = ? AND change_id = ? AND recipient = ? AND channel = ?`,
		publisher, eventKey, changeID, recipient, channel); err != nil {
		return fmt.Errorf("delete mesh action progress: %w", err)
	}
	return nil
}

// RecordMeshFailureAndRequeue applies the async TxFailed outcome in ONE
// transaction: it revokes the progress entry, records the failure marker
// and re-arms the already-settled delivery job of the concrete job
// identity (action + group + dedup key) as failed due now. A crash
// between these steps used to leave an accepted job with a durable
// marker and no retry (reported P1), and a version-only match used to
// re-arm OTHER groups' jobs of the same action (reported P2) — now
// either all of it lands for exactly this job, or none. Returns the
// number of re-armed jobs.
func (s *Store) RecordMeshFailureAndRequeue(ctx context.Context, actionID string, groupID int64, dedupKey, publisher, eventKey string, changeID int64, recipient string, channel int, now time.Time) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin mesh failure record for %q: %w", eventKey, err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		DELETE FROM mesh_action_progress
		WHERE publisher = ? AND event_key = ? AND change_id = ? AND recipient = ? AND channel = ?`,
		publisher, eventKey, changeID, recipient, channel); err != nil {
		return 0, fmt.Errorf("revoke mesh action progress: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO mesh_action_failures (action_id, group_id, dedup_key, publisher, event_key, change_id, recipient, channel, created_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		actionID, groupID, dedupKey, publisher, eventKey, changeID, recipient, channel, now.UnixMilli()); err != nil {
		return 0, fmt.Errorf("mark mesh action failure: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM mesh_action_failures WHERE created_at_ms < ?`,
		now.Add(-meshProgressRetention).UnixMilli()); err != nil {
		return 0, fmt.Errorf("prune mesh action failures: %w", err)
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE action_fires
		SET status = 'failed', next_attempt_at_ms = ?
		WHERE action_id = ? AND group_id = ? AND dedup_key = ?
		  AND status IN ('accepted', 'confirmed')`,
		now.UnixMilli(), actionID, groupID, dedupKey)
	if err != nil {
		return 0, fmt.Errorf("re-arm delivery job for %q: %w", eventKey, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("re-arm delivery job count for %q: %w", eventKey, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit mesh failure record for %q: %w", eventKey, err)
	}
	return n, nil
}

// SetMeshActionFailed marks (or clears) the durable failure marker of
// one versioned transmission of ONE CONCRETE JOB (action + group + dedup
// key). The marker ties the ASYNC modem result to that job: the worker
// consults MeshActionFailed before settling it as accepted, and the
// atomic failure record re-arms exactly that job. The job scope keeps
// one group's failure from touching another group's job of the same
// action (reported P2). A successful retransmission clears the marker.
// Failure rows age out on the same retention bound as the progress rows.
func (s *Store) SetMeshActionFailed(ctx context.Context, actionID string, groupID int64, dedupKey, publisher, eventKey string, changeID int64, recipient string, channel int, failed bool) error {
	if failed {
		now := s.now().UnixMilli()
		if _, err := s.db.ExecContext(ctx, `
			INSERT OR IGNORE INTO mesh_action_failures (action_id, group_id, dedup_key, publisher, event_key, change_id, recipient, channel, created_at_ms)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			actionID, groupID, dedupKey, publisher, eventKey, changeID, recipient, channel, now); err != nil {
			return fmt.Errorf("mark mesh action failure: %w", err)
		}
		if _, err := s.db.ExecContext(ctx,
			`DELETE FROM mesh_action_failures WHERE created_at_ms < ?`,
			s.now().Add(-meshProgressRetention).UnixMilli()); err != nil {
			return fmt.Errorf("prune mesh action failures: %w", err)
		}
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `
		DELETE FROM mesh_action_failures
		WHERE action_id = ? AND group_id = ? AND dedup_key = ? AND publisher = ? AND event_key = ? AND change_id = ? AND recipient = ? AND channel = ?`,
		actionID, groupID, dedupKey, publisher, eventKey, changeID, recipient, channel); err != nil {
		return fmt.Errorf("clear mesh action failure: %w", err)
	}
	return nil
}

// MeshActionFailed reports whether ANY transmission of the message
// version of ONE CONCRETE JOB carries a failure marker (the retry
// re-sends only the failed recipients; the job-level check is
// version-wide within the job).
func (s *Store) MeshActionFailed(ctx context.Context, actionID string, groupID int64, dedupKey, publisher, eventKey string, changeID int64) (bool, error) {
	var failed int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM mesh_action_failures
		WHERE action_id = ? AND group_id = ? AND dedup_key = ? AND publisher = ? AND event_key = ? AND change_id = ?`,
		actionID, groupID, dedupKey, publisher, eventKey, changeID).Scan(&failed); err != nil {
		return false, fmt.Errorf("query mesh action failures: %w", err)
	}
	return failed > 0, nil
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
// narrows by direction ("rx", "tx" or "" for both), by exact channel
// label (Channel; Exclude inverts it) and by conversation peer (rx sent
// by the node, tx addressed to it).
func (s *Store) ListMeshtasticMessages(ctx context.Context, f storage.MeshtasticMessageFilter, limit, offset int) ([]storage.MeshMessage, error) {
	query := `
		SELECT id, direction, sender, recipient, channel, hops, operator, text, status, created_at_ms
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
		if err := rows.Scan(&m.ID, &m.Direction, &m.Sender, &m.Recipient, &m.Channel, &m.Hops, &m.Operator, &m.Text, &m.Status, &atMs); err != nil {
			return nil, fmt.Errorf("scan mesh message: %w", err)
		}
		m.At = time.UnixMilli(atMs)
		out = append(out, m)
	}
	return out, rows.Err()
}

// meshMessageWhere builds the shared WHERE clause (direction, channel
// and/or peer conditions) for the message history queries.
func meshMessageWhere(f storage.MeshtasticMessageFilter) string {
	conds := make([]string, 0, 3)
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
	if f.Peer != "" {
		conds = append(conds, `((direction = 'rx' AND sender = ?) OR (direction = 'tx' AND recipient = ?))`)
	}
	if f.Text != "" {
		conds = append(conds, `text = ?`)
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
	if f.Peer != "" {
		args = append(args, f.Peer, f.Peer)
	}
	if f.Text != "" {
		args = append(args, f.Text)
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

// MeshtasticPeers returns the distinct node ids appearing in direct
// messages (rx senders and tx recipients), sorted.
func (s *Store) MeshtasticPeers(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT sender FROM meshtastic_messages
		WHERE channel = 'dm' AND direction = 'rx' AND sender != ''
		UNION
		SELECT recipient FROM meshtastic_messages
		WHERE channel = 'dm' AND direction = 'tx' AND recipient != ''`)
	if err != nil {
		return nil, fmt.Errorf("list mesh peers: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan mesh peer: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate mesh peers: %w", err)
	}
	sort.Strings(out)
	return out, nil
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
		SELECT id, name, short, lat, lon, last_seen_ms, sends, hops
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
		if err := rows.Scan(&n.ID, &n.Name, &n.Short, &n.Lat, &n.Lon, &seenMs, &sends, &n.Hops); err != nil {
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
		INSERT INTO meshtastic_nodes (id, name, short, lat, lon, last_seen_ms, sends, hops)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("save meshtastic nodes: %w", err)
	}
	defer stmt.Close()
	for _, n := range nodes {
		if _, err := stmt.ExecContext(ctx, n.ID, n.Name, n.Short, n.Lat, n.Lon,
			n.LastSeen.UnixMilli(), strings.Join(n.Sends, ","), n.Hops); err != nil {
			return fmt.Errorf("save meshtastic node %s: %w", n.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("save meshtastic nodes: %w", err)
	}
	return nil
}
