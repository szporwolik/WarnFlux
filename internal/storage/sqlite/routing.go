package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/severity"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// GroupRouting returns the full routing matrix of one group: every
// assigned cell carries its own source and severity threshold, sorted by
// source then action ID.
func (s *Store) GroupRouting(groupID int64) (storage.GroupRouting, error) {
	var r storage.GroupRouting
	err := s.db.QueryRow(`SELECT id, name FROM groups WHERE id = ?`, groupID).
		Scan(&r.GroupID, &r.Name)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.GroupRouting{}, storage.ErrGroupNotFound
	}
	if err != nil {
		return storage.GroupRouting{}, fmt.Errorf("group %d routing: %w", groupID, err)
	}

	actions, err := s.groupActions(groupID)
	if err != nil {
		return storage.GroupRouting{}, err
	}
	r.Actions = actions
	return r, nil
}

// SetGroupRouting replaces the group's routing matrix in one transaction.
// Every assigned cell carries its own source and canonical severity
// threshold; IDs reference the configuration (not database rows) and are
// stored as-is, deduplicated per (source, ID).
func (s *Store) SetGroupRouting(groupID int64, actions []storage.ChannelAssignment) error {
	for _, a := range actions {
		if !severity.Valid(a.MinSeverity) {
			return storage.ErrInvalidSeverity
		}
	}
	actions = dedupeAssignments(actions)

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin routing update: %w", err)
	}
	defer tx.Rollback()

	now := s.now().UnixMilli()
	res, err := tx.Exec(`UPDATE groups SET updated_at_ms = ? WHERE id = ?`, now, groupID)
	if err != nil {
		return fmt.Errorf("touch group %d: %w", groupID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("touch group %d: %w", groupID, err)
	}
	if n == 0 {
		return storage.ErrGroupNotFound
	}

	if _, err := tx.Exec(`DELETE FROM group_actions WHERE group_id = ?`, groupID); err != nil {
		return fmt.Errorf("clear group %d actions: %w", groupID, err)
	}
	for _, a := range actions {
		if _, err := tx.Exec(`
			INSERT INTO group_actions (group_id, source, action_id, min_severity)
			VALUES (?, ?, ?, ?)`, groupID, a.Source, a.ID, a.MinSeverity); err != nil {
			return fmt.Errorf("assign action %q (source %q) to group %d: %w", a.ID, a.Source, groupID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit group %d routing: %w", groupID, err)
	}
	return nil
}

// ListGroupRoutings returns the routing matrix of every group ordered by
// name. Groups without any assigned action yield empty slices.
func (s *Store) ListGroupRoutings() ([]storage.GroupRouting, error) {
	rows, err := s.db.Query(`SELECT id, name FROM groups ORDER BY name COLLATE NOCASE ASC`)
	if err != nil {
		return nil, fmt.Errorf("list group routings: %w", err)
	}
	defer rows.Close()

	var out []storage.GroupRouting
	for rows.Next() {
		var r storage.GroupRouting
		if err := rows.Scan(&r.GroupID, &r.Name); err != nil {
			return nil, fmt.Errorf("scan group routing: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group routings: %w", err)
	}
	for i := range out {
		actions, err := s.groupActions(out[i].GroupID)
		if err != nil {
			return nil, err
		}
		out[i].Actions = actions
	}
	return out, nil
}

// groupActions reads the assigned cells with their thresholds, sorted.
func (s *Store) groupActions(groupID int64) ([]storage.ChannelAssignment, error) {
	rows, err := s.db.Query(`
		SELECT source, action_id, min_severity FROM group_actions
		WHERE group_id = ? ORDER BY source ASC, action_id ASC`, groupID)
	if err != nil {
		return nil, fmt.Errorf("list group %d actions: %w", groupID, err)
	}
	defer rows.Close()
	return scanAssignments(rows, fmt.Sprintf("scan group %d action", groupID))
}

type assignmentScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

// scanAssignments reads (source, id, min_severity) rows into assignments.
func scanAssignments(rows assignmentScanner, what string) ([]storage.ChannelAssignment, error) {
	var out []storage.ChannelAssignment
	for rows.Next() {
		var a storage.ChannelAssignment
		if err := rows.Scan(&a.Source, &a.ID, &a.MinSeverity); err != nil {
			return nil, fmt.Errorf("%s: %w", what, err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// dedupeAssignments removes duplicates (by source + ID) while preserving
// order.
func dedupeAssignments(list []storage.ChannelAssignment) []storage.ChannelAssignment {
	seen := make(map[string]struct{}, len(list))
	out := list[:0]
	for _, a := range list {
		if a.ID == "" {
			continue
		}
		key := a.Source + "\x00" + a.ID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, a)
	}
	return out
}

// deliveryClaimLease is how long a claimed job may stay "running" before
// RecoverStaleClaims assumes the claiming process died and re-queues it.
// It must comfortably exceed the per-call timeout of any action.
const deliveryClaimLease = 5 * time.Minute

// statusFromDB maps a persisted action_fires.status onto the storage
// model. Legacy rows written before v23 statuses existed carry
// 'succeeded' and count as accepted (claimed-before-delivery).
func statusFromDB(s string) storage.DeliveryStatus {
	switch s {
	case "confirmed":
		return storage.DeliveryConfirmed
	case "accepted", "succeeded":
		return storage.DeliveryAccepted
	case "expired":
		return storage.DeliveryExpired
	case "failed":
		return storage.DeliveryFailed
	default: // "saved", "running"
		return storage.DeliverySaved
	}
}

// EnqueueDelivery persists one durable delivery job with its full
// payload. A fresh row reports (DeliverySaved, true). An existing row
// reports (status, false) — the transition was deduplicated — EXCEPT a
// terminally failed job, which is re-armed with the fresh payload and a
// clean budget and reports (DeliverySaved, true), so an alert that
// burned all its execution attempts is retried on replay instead of
// being suppressed as already delivered.
func (s *Store) EnqueueDelivery(ctx context.Context, job storage.DeliveryJob) (storage.DeliveryStatus, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, fmt.Errorf("begin delivery enqueue for action %q group %d: %w", job.ActionID, job.GroupID, err)
	}
	defer tx.Rollback()
	st, queued, err := s.enqueueDeliveryTx(ctx, tx, job)
	if err != nil {
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, fmt.Errorf("commit delivery enqueue for action %q group %d: %w", job.ActionID, job.GroupID, err)
	}
	return st, queued, nil
}

// enqueueDeliveryTx applies one job inside an open transaction: fresh
// insert, deduplicated read or terminal-failure re-arm (see
// EnqueueDelivery).
func (s *Store) enqueueDeliveryTx(ctx context.Context, tx *sql.Tx, job storage.DeliveryJob) (storage.DeliveryStatus, bool, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO action_fires
			(group_id, action_id, event_key, dedup_key, payload, status, attempts, next_attempt_at_ms, fired_at_ms)
		VALUES (?, ?, ?, ?, ?, 'saved', 0, 0, ?)`,
		job.GroupID, job.ActionID, job.EventKey, job.DedupKey, string(job.Payload), job.FiredAt.UnixMilli())
	if err != nil {
		return 0, false, fmt.Errorf("insert delivery job for action %q group %d: %w", job.ActionID, job.GroupID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return 0, false, fmt.Errorf("insert delivery job for action %q group %d: %w", job.ActionID, job.GroupID, err)
	} else if n == 1 {
		return storage.DeliverySaved, true, nil
	}

	var dbStatus string
	if err := tx.QueryRowContext(ctx, `
		SELECT status FROM action_fires
		WHERE group_id = ? AND action_id = ? AND dedup_key = ?`,
		job.GroupID, job.ActionID, job.DedupKey).Scan(&dbStatus); err != nil {
		return 0, false, fmt.Errorf("read delivery job for action %q group %d: %w", job.ActionID, job.GroupID, err)
	}
	if dbStatus != "failed" {
		return statusFromDB(dbStatus), false, nil
	}

	// Terminal failure: re-arm the job with the fresh payload and budget.
	if _, err := tx.ExecContext(ctx, `
		UPDATE action_fires
		SET payload = ?, status = 'saved', attempts = 0, next_attempt_at_ms = 0
		WHERE group_id = ? AND action_id = ? AND dedup_key = ?`,
		string(job.Payload), job.GroupID, job.ActionID, job.DedupKey); err != nil {
		return 0, false, fmt.Errorf("re-arm delivery job for action %q group %d: %w", job.ActionID, job.GroupID, err)
	}
	return storage.DeliverySaved, true, nil
}

// CommitInboxDelivery persists every delivery job of one evaluation and,
// for inbox events, deletes the inbox row — all in ONE transaction.
// Either all jobs exist durably and the inbox row is consumed, or
// nothing happened (the inbox row stays pending for recovery). It
// returns one result per job, in order.
func (s *Store) CommitInboxDelivery(ctx context.Context, inboxID int64, jobs []storage.DeliveryJob) ([]storage.DeliveryResult, error) {
	if inboxID == 0 && len(jobs) == 0 {
		return nil, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin inbox delivery commit: %w", err)
	}
	defer tx.Rollback()

	results := make([]storage.DeliveryResult, 0, len(jobs))
	for _, job := range jobs {
		st, queued, err := s.enqueueDeliveryTx(ctx, tx, job)
		if err != nil {
			return nil, err
		}
		results = append(results, storage.DeliveryResult{Status: st, Queued: queued})
	}
	if inboxID != 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM dispatch_inbox WHERE id = ?`, inboxID); err != nil {
			return nil, fmt.Errorf("consume inbox row %d: %w", inboxID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit inbox delivery: %w", err)
	}
	return results, nil
}

// ClaimNextDelivery atomically claims the next due job of one action.
// The claim flips the row to running, increments the attempt counter and
// moves the claim deadline past now; concurrent claimers never receive
// the same job. Jobs whose retry budget is exhausted are not claimed —
// EXCEPT a saved job recovered from a crashed claim (its spent attempt
// may equal the budget, but it never produced a result, so it must
// still execute; a later failure settles terminally).
func (s *Store) ClaimNextDelivery(ctx context.Context, actionID string, maxAttempts int, now time.Time) (storage.DeliveryJob, bool, error) {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	deadline := now.Add(deliveryClaimLease).UnixMilli()
	for {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return storage.DeliveryJob{}, false, fmt.Errorf("begin delivery claim for action %q: %w", actionID, err)
		}
		var gid int64
		var aid, dkey string
		err = tx.QueryRowContext(ctx, `
			SELECT group_id, action_id, dedup_key FROM action_fires
			WHERE action_id = ? AND status IN ('saved','failed')
			  AND next_attempt_at_ms <= ? AND (attempts < ? OR status = 'saved')
			ORDER BY next_attempt_at_ms ASC, fired_at_ms ASC
			LIMIT 1`, actionID, now.UnixMilli(), maxAttempts).Scan(&gid, &aid, &dkey)
		if errors.Is(err, sql.ErrNoRows) {
			tx.Rollback()
			return storage.DeliveryJob{}, false, nil
		}
		if err != nil {
			tx.Rollback()
			return storage.DeliveryJob{}, false, fmt.Errorf("pick delivery job for action %q: %w", actionID, err)
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE action_fires
			SET status = 'running', attempts = attempts + 1, next_attempt_at_ms = ?
			WHERE group_id = ? AND action_id = ? AND dedup_key = ? AND status IN ('saved','failed')`,
			deadline, gid, aid, dkey)
		if err != nil {
			tx.Rollback()
			return storage.DeliveryJob{}, false, fmt.Errorf("claim delivery job for action %q: %w", actionID, err)
		}
		if n, err := res.RowsAffected(); err != nil {
			tx.Rollback()
			return storage.DeliveryJob{}, false, fmt.Errorf("claim delivery job for action %q: %w", actionID, err)
		} else if n == 0 {
			tx.Rollback()
			continue // lost the race against another claimer; retry
		}
		var job storage.DeliveryJob
		var payload string
		var firedAt int64
		err = tx.QueryRowContext(ctx, `
			SELECT group_id, action_id, event_key, dedup_key, payload, attempts, fired_at_ms
			FROM action_fires WHERE group_id = ? AND action_id = ? AND dedup_key = ?`, gid, aid, dkey).
			Scan(&job.GroupID, &job.ActionID, &job.EventKey, &job.DedupKey, &payload, &job.Attempts, &firedAt)
		if err != nil {
			tx.Rollback()
			return storage.DeliveryJob{}, false, fmt.Errorf("read claimed delivery job for action %q: %w", actionID, err)
		}
		job.Payload = []byte(payload)
		job.FiredAt = time.UnixMilli(firedAt)
		if err := tx.Commit(); err != nil {
			return storage.DeliveryJob{}, false, fmt.Errorf("commit delivery claim for action %q: %w", actionID, err)
		}
		return job, true, nil
	}
}

// SettleDelivery records the post-execution stage of one job. A
// non-terminal failure (DeliveryFailed with a non-zero nextAttempt)
// schedules the retry deadline; a terminal settlement zeroes it.
func (s *Store) SettleDelivery(ctx context.Context, groupID int64, actionID, dedupKey string, stage storage.DeliveryStatus, nextAttempt time.Time) error {
	status, ok := map[storage.DeliveryStatus]string{
		storage.DeliverySaved:     "saved",
		storage.DeliveryAccepted:  "accepted",
		storage.DeliveryConfirmed: "confirmed",
		storage.DeliveryExpired:   "expired",
		storage.DeliveryFailed:    "failed",
	}[stage]
	if !ok {
		return fmt.Errorf("settle delivery for action %q group %d: unknown stage %v", actionID, groupID, stage)
	}
	next := int64(0)
	if stage == storage.DeliveryFailed && !nextAttempt.IsZero() {
		next = nextAttempt.UnixMilli()
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE action_fires SET status = ?, next_attempt_at_ms = ?
		WHERE group_id = ? AND action_id = ? AND dedup_key = ?`,
		status, next, groupID, actionID, dedupKey); err != nil {
		return fmt.Errorf("settle delivery for action %q group %d: %w", actionID, groupID, err)
	}
	return nil
}

// RecoverStaleClaims re-queues running jobs whose claim deadline has
// passed — the claiming process died between claim and settlement. The
// attempt counter stays spent, so the retry budget still applies.
func (s *Store) RecoverStaleClaims(ctx context.Context, now time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE action_fires SET status = 'saved'
		WHERE status = 'running' AND next_attempt_at_ms != 0 AND next_attempt_at_ms < ?`,
		now.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("recover stale delivery claims: %w", err)
	}
	return res.RowsAffected()
}

// PendingDeliveries counts the non-terminal jobs of one action.
func (s *Store) PendingDeliveries(ctx context.Context, actionID string) (int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM action_fires
		WHERE action_id = ? AND status IN ('saved','running','failed')`, actionID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending deliveries for action %q: %w", actionID, err)
	}
	return n, nil
}

// PruneActionFires deletes COMPLETED ledger rows older than the cutoff
// and returns how many rows were removed. Pending work never ages out:
// 'saved' (never executed), 'running' (claimed) and 'failed' rows that
// still carry a scheduled retry are unsent notifications and must
// survive retention — a long offline stretch, a disabled action or a
// paused system must not lose them. Removing a pending job would need a
// separate validity policy with an explicit outcome and an audit trail.
func (s *Store) PruneActionFires(cutoff time.Time) (int64, error) {
	res, err := s.db.Exec(`
		DELETE FROM action_fires
		WHERE fired_at_ms < ?
		  AND (status IN ('accepted','confirmed','expired','succeeded')
		       OR (status = 'failed' AND next_attempt_at_ms = 0))`,
		cutoff.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("prune action fires: %w", err)
	}
	return res.RowsAffected()
}

// GroupRecipientAPRS returns the distinct (case-insensitive), non-empty
// APRS callsigns registered for the group's members, sorted. Missing
// groups yield an empty list.
func (s *Store) GroupRecipientAPRS(groupID int64) ([]string, error) {
	rows, err := s.db.Query(`
		SELECT DISTINCT ua.callsign
		FROM user_aprs ua
		JOIN user_groups ug ON ug.user_id = ua.user_id
		WHERE ug.group_id = ?
		  AND NOT EXISTS (
			SELECT 1 FROM user_channel_opts uco
			WHERE uco.user_id = ua.user_id AND uco.channel = 'aprs')
		ORDER BY ua.callsign COLLATE NOCASE ASC`, groupID)
	if err != nil {
		return nil, fmt.Errorf("list group %d aprs recipients: %w", groupID, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var callsign string
		if err := rows.Scan(&callsign); err != nil {
			return nil, fmt.Errorf("scan group %d aprs recipient: %w", groupID, err)
		}
		out = append(out, callsign)
	}
	return out, rows.Err()
}

// GroupRecipientEmails returns the distinct (case-insensitive), non-empty
// email addresses of the group's members, sorted. Missing groups yield an
// empty list (the membership table simply has no rows for them).
func (s *Store) GroupRecipientEmails(groupID int64) ([]string, error) {
	rows, err := s.db.Query(`
		SELECT u.email
		FROM users u
		JOIN user_groups ug ON ug.user_id = u.id
		WHERE ug.group_id = ? AND u.email <> ''
		  AND NOT EXISTS (
			SELECT 1 FROM user_channel_opts uco
			WHERE uco.user_id = u.id AND uco.channel = 'smtp')
		ORDER BY u.email COLLATE NOCASE ASC`, groupID)
	if err != nil {
		return nil, fmt.Errorf("list group %d recipients: %w", groupID, err)
	}
	defer rows.Close()
	seen := make(map[string]struct{})
	var out []string
	for rows.Next() {
		var email string
		if err := rows.Scan(&email); err != nil {
			return nil, fmt.Errorf("scan group %d recipient: %w", groupID, err)
		}
		key := strings.ToLower(email)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, email)
	}
	return out, rows.Err()
}

// GroupRecipientDiscord returns the distinct (case-insensitive),
// non-empty Discord handles of the group's members, sorted. Missing
// groups yield an empty list.
func (s *Store) GroupRecipientDiscord(groupID int64) ([]string, error) {
	rows, err := s.db.Query(`
		SELECT u.discord
		FROM users u
		JOIN user_groups ug ON ug.user_id = u.id
		WHERE ug.group_id = ? AND u.discord <> ''
		  AND NOT EXISTS (
			SELECT 1 FROM user_channel_opts uco
			WHERE uco.user_id = u.id AND uco.channel = 'discord')
		ORDER BY u.discord COLLATE NOCASE ASC`, groupID)
	if err != nil {
		return nil, fmt.Errorf("list group %d discord recipients: %w", groupID, err)
	}
	defer rows.Close()
	seen := make(map[string]struct{})
	var out []string
	for rows.Next() {
		var handle string
		if err := rows.Scan(&handle); err != nil {
			return nil, fmt.Errorf("scan group %d discord recipient: %w", groupID, err)
		}
		key := strings.ToLower(handle)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, handle)
	}
	return out, rows.Err()
}
