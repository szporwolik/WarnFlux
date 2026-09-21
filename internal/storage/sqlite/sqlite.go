// Package sqlite implements storage.EventStore on top of a local SQLite
// database using the pure-Go modernc.org/sqlite driver.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// migration is one append-only schema evolution step. Steps already
// released must never be edited.
type migration struct {
	// SQL is executed inside the migration transaction.
	SQL string
	// run is an optional Go migration step executed in the same transaction
	// (used when plain SQL cannot convert existing data safely).
	run func(tx *sql.Tx) error
}

// migrations holds the schema evolution steps in order. Slice index +1 is
// the target schema version, persisted via SQLite's PRAGMA user_version.
// Existing databases are upgraded in place on startup, never recreated.
var migrations = []migration{
	{
		// v1: initial event storage.
		SQL: `
CREATE TABLE events (
	event_key     TEXT PRIMARY KEY,
	source        TEXT NOT NULL,
	source_id     TEXT NOT NULL,
	fingerprint   TEXT NOT NULL,
	status        TEXT NOT NULL,
	category      TEXT NOT NULL DEFAULT '',
	event         TEXT NOT NULL,
	severity      TEXT NOT NULL DEFAULT '',
	urgency       TEXT NOT NULL DEFAULT '',
	certainty     TEXT NOT NULL DEFAULT '',
	headline      TEXT NOT NULL DEFAULT '',
	description   TEXT NOT NULL DEFAULT '',
	instruction   TEXT NOT NULL DEFAULT '',
	effective_at  TEXT,
	expires_at    TEXT,
	latitude      REAL,
	longitude     REAL,
	areas         TEXT NOT NULL DEFAULT '[]',
	source_url    TEXT NOT NULL DEFAULT '',
	received_at   TEXT NOT NULL,
	first_seen_at TEXT NOT NULL,
	last_seen_at  TEXT NOT NULL,
	updated_at    TEXT NOT NULL
);

CREATE INDEX idx_events_source ON events(source);
CREATE INDEX idx_events_status ON events(status);
CREATE INDEX idx_events_expires_at ON events(expires_at);
CREATE INDEX idx_events_last_seen_at ON events(last_seen_at);
`,
	},
	{
		// v2: machine-sortable expiry timestamps + durable change journal.
		SQL: `
ALTER TABLE events ADD COLUMN expires_at_ms INTEGER;
CREATE INDEX idx_events_expires_at_ms ON events(expires_at_ms);

CREATE TABLE changes (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	change_type   TEXT NOT NULL,
	event_key     TEXT NOT NULL,
	created_at_ms INTEGER NOT NULL
);
CREATE INDEX idx_changes_id ON changes(id);

CREATE TABLE output_cursors (
	output_id     TEXT PRIMARY KEY,
	last_acked_id INTEGER NOT NULL DEFAULT 0
);
`,
		// Backfill expires_at_ms from the RFC3339Nano text column, parsing
		// each value explicitly instead of relying on string manipulation.
		run: func(tx *sql.Tx) error {
			rows, err := tx.Query("SELECT event_key, expires_at FROM events WHERE expires_at IS NOT NULL")
			if err != nil {
				return fmt.Errorf("read legacy expires_at: %w", err)
			}
			defer rows.Close()
			type backfill struct {
				key string
				ms  int64
			}
			var batch []backfill
			for rows.Next() {
				var key, text string
				if err := rows.Scan(&key, &text); err != nil {
					return fmt.Errorf("scan legacy expires_at: %w", err)
				}
				parsed, err := time.Parse(time.RFC3339Nano, text)
				if err != nil {
					return fmt.Errorf("event %q has invalid legacy expires_at %q: %w", key, text, err)
				}
				batch = append(batch, backfill{key: key, ms: parsed.UnixMilli()})
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("iterate legacy expires_at: %w", err)
			}
			for _, b := range batch {
				if _, err := tx.Exec("UPDATE events SET expires_at_ms = ? WHERE event_key = ?", b.ms, b.key); err != nil {
					return fmt.Errorf("backfill expires_at_ms for %q: %w", b.key, err)
				}
			}
			return nil
		},
	},
	{
		// v3: immutable event snapshots in the change journal. From now on
		// every change row carries the exact event state after its
		// transition; PollChanges never joins against the mutable events
		// table again.
		SQL: `ALTER TABLE changes ADD COLUMN event_snapshot TEXT NOT NULL DEFAULT '';`,
		// Pre-snapshot rows cannot be reconstructed historically with
		// perfect accuracy: backfill them with the CURRENT event state.
		// Databases created by the pre-release v2 schema therefore report
		// the present state for all their old journal rows (documented
		// limitation). Orphan rows (no matching event) are dropped: they
		// carry no reconstructable state.
		run: func(tx *sql.Tx) error {
			if _, err := tx.Exec(`
				DELETE FROM changes
				WHERE event_key NOT IN (SELECT event_key FROM events)`); err != nil {
				return fmt.Errorf("drop orphan changes: %w", err)
			}
			rows, err := tx.Query("SELECT id, event_key FROM changes WHERE event_snapshot = ''")
			if err != nil {
				return fmt.Errorf("read unsnapshotted changes: %w", err)
			}
			defer rows.Close()
			type row struct {
				id  int64
				key string
			}
			var batch []row
			for rows.Next() {
				var r row
				if err := rows.Scan(&r.id, &r.key); err != nil {
					return fmt.Errorf("scan change: %w", err)
				}
				batch = append(batch, r)
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("iterate changes: %w", err)
			}
			for _, r := range batch {
				event, err := loadEventTx(tx, r.key)
				if err != nil {
					return fmt.Errorf("backfill snapshot for change %d: %w", r.id, err)
				}
				data, err := json.Marshal(storage.SnapshotOf(event))
				if err != nil {
					return fmt.Errorf("marshal snapshot for change %d: %w", r.id, err)
				}
				if _, err := tx.Exec("UPDATE changes SET event_snapshot = ? WHERE id = ?", string(data), r.id); err != nil {
					return fmt.Errorf("write snapshot for change %d: %w", r.id, err)
				}
			}
			return nil
		},
	},
}

// eventColumns is the canonical column list used for SELECT and JOINs.
// expires_at_ms is the authoritative comparison value; expires_at remains
// as a human-readable copy.
const eventColumns = `event_key, source, source_id, fingerprint, status,
category, event, severity, urgency, certainty,
headline, description, instruction,
effective_at, expires_at_ms, latitude, longitude,
areas, source_url, received_at, first_seen_at, last_seen_at, updated_at`

// Store is a SQLite-backed storage.EventStore.
type Store struct {
	db *sql.DB

	// now is the clock used for timestamps; injectable in tests.
	now func() time.Time
}

// Option customizes a Store during Open.
type Option func(*Store)

// WithClock sets the clock used for persistence timestamps. Useful in tests
// and for deterministic behavior.
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// MigrationInfo reports what happened to the schema version during Open.
type MigrationInfo struct {
	From int
	To   int
}

// Open connects to the SQLite database at path, applies the configured
// pragmas and migrations, and returns the ready store.
//
// SQLite settings chosen for a long-running, low-volume daemon:
//   - journal_mode=WAL: readers do not block the writer; crash-safe journal.
//   - synchronous=FULL: the change journal is the source of truth for
//     output delivery; FULL keeps the durability/performance balance on
//     the safe side for a low-volume daemon.
//   - busy_timeout=5000: wait for the lock instead of failing immediately.
//   - foreign_keys=ON: integrity checks active.
//   - a single database connection (MaxOpenConns=1): serializes access,
//     keeps per-connection PRAGMAs stable and avoids SQLITE_BUSY.
func Open(path string, opts ...Option) (*Store, MigrationInfo, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, MigrationInfo{}, fmt.Errorf("open sqlite database %q: %w", path, err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, MigrationInfo{}, fmt.Errorf("connect to sqlite database %q: %w", path, err)
	}

	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, MigrationInfo{}, fmt.Errorf("apply %s: %w", pragma, err)
		}
	}

	info, err := migrate(db, migrations)
	if err != nil {
		db.Close()
		return nil, MigrationInfo{}, err
	}

	store := &Store{db: db, now: time.Now}
	for _, opt := range opts {
		opt(store)
	}
	return store, info, nil
}

// migrate applies pending schema steps, each in its own transaction.
// Existing databases are never recreated or deleted; a failed step rolls
// back and leaves the schema version unchanged.
func migrate(db *sql.DB, steps []migration) (MigrationInfo, error) {
	var current int
	if err := db.QueryRow("PRAGMA user_version").Scan(&current); err != nil {
		return MigrationInfo{}, fmt.Errorf("read schema version: %w", err)
	}
	if current > len(steps) {
		return MigrationInfo{From: current, To: current},
			fmt.Errorf("database schema version %d is newer than this binary supports (%d); upgrade WarnFlux, not the database", current, len(steps))
	}

	info := MigrationInfo{From: current, To: current}
	for v := current; v < len(steps); v++ {
		target := v + 1
		tx, err := db.Begin()
		if err != nil {
			return info, fmt.Errorf("begin migration to version %d: %w", target, err)
		}
		if _, err := tx.Exec(steps[v].SQL); err != nil {
			tx.Rollback()
			return info, fmt.Errorf("apply migration to version %d: %w", target, err)
		}
		if steps[v].run != nil {
			if err := steps[v].run(tx); err != nil {
				tx.Rollback()
				return info, fmt.Errorf("run migration step %d: %w", target, err)
			}
		}
		// PRAGMA does not support bound parameters; the value is a
		// validated integer from this package, so formatting is safe.
		if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", target)); err != nil {
			tx.Rollback()
			return info, fmt.Errorf("set schema version %d: %w", target, err)
		}
		if err := tx.Commit(); err != nil {
			return info, fmt.Errorf("commit migration to version %d: %w", target, err)
		}
		info.To = target
	}
	return info, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error { return s.db.Close() }

// Ingest atomically classifies and persists one normalized event. The event
// state transition and its change journal record (when meaningful) are
// written in a single transaction, so no other ingestion can observe the
// event half-way through.
func (s *Store) Ingest(ctx context.Context, event core.HazardEvent, fingerprint string) (storage.Outcome, *storage.Change, error) {
	now := s.now().UTC()
	nowMs := now.UnixMilli()
	key := event.Key()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("begin ingest transaction for %q: %w", key, err)
	}
	defer tx.Rollback()

	var storedFp, storedStatus, storedReceived, storedFirstSeen string
	err = tx.QueryRowContext(ctx,
		"SELECT fingerprint, status, received_at, first_seen_at FROM events WHERE event_key = ?", key,
	).Scan(&storedFp, &storedStatus, &storedReceived, &storedFirstSeen)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		// Previously unknown event. A first-seen cancellation is a
		// cancellation, never "new".
		outcome := storage.OutcomeNew
		changeType := core.ChangeNew
		if event.Status == core.StatusCancelled {
			outcome = storage.OutcomeCancelled
			changeType = core.ChangeCancelled
		}
		// Make the event carry exactly what is persisted before taking its
		// snapshot for the journal.
		event.UpdatedAt = orNow(event.UpdatedAt, now)
		event.ReceivedAt = orNow(event.ReceivedAt, event.UpdatedAt)
		snapshot := storage.SnapshotOf(event)
		if _, err := tx.ExecContext(ctx, insertSQL, insertArgs(event, fingerprint, now, now, event.UpdatedAt, expiryMs(event))...); err != nil {
			return 0, nil, fmt.Errorf("insert event %q: %w", key, err)
		}
		change, err := insertChange(tx, ctx, changeType, key, nowMs, snapshot)
		if err != nil {
			return 0, nil, err
		}
		if err := tx.Commit(); err != nil {
			return 0, nil, fmt.Errorf("commit ingest of %q: %w", key, err)
		}
		change.Event = event
		return outcome, change, nil

	case err != nil:
		return 0, nil, fmt.Errorf("get event %q: %w", key, err)
	}

	// Existing event: compare content fingerprint and lifecycle status.
	contentSame := storedFp == fingerprint
	incomingCancelled := event.Status == core.StatusCancelled
	// An incoming expiry is "live" when the provider removed it or moved
	// it into the future; a lapsed expiry by itself is NOT a
	// reactivation signal.
	incomingLive := expiryLive(event, nowMs)

	// Duplicate: no journal record, only last_seen_at refreshes. A lapsed
	// expiry never forces an update on its own — the maintenance worker
	// performs the single active → expired transition, so re-ingesting the
	// same stale provider event cannot create updated/expired flapping.
	duplicate := false
	switch {
	case incomingCancelled:
		duplicate = storedStatus == string(core.StatusCancelled) && contentSame
	case storedStatus == string(core.StatusActive):
		duplicate = contentSame
	case storedStatus == string(core.StatusExpired):
		// Keep the internal lifecycle expired unless the provider made the
		// event live again (future/removed expiry) or changed its content.
		duplicate = contentSame && !incomingLive
	default: // stored cancelled, incoming active: always a re-activation.
		duplicate = false
	}

	if duplicate {
		if _, err := tx.ExecContext(ctx,
			"UPDATE events SET last_seen_at = ? WHERE event_key = ?", formatTime(now), key); err != nil {
			return 0, nil, fmt.Errorf("touch event %q: %w", key, err)
		}
		if err := tx.Commit(); err != nil {
			return 0, nil, fmt.Errorf("commit duplicate of %q: %w", key, err)
		}
		return storage.OutcomeDuplicate, nil, nil
	}

	// Content and/or lifecycle changed. Preserve the original received_at
	// and first_seen_at.
	received, err := time.Parse(time.RFC3339Nano, storedReceived)
	if err != nil {
		return 0, nil, fmt.Errorf("event %q has invalid stored received_at: %w", key, err)
	}
	event.ReceivedAt = received
	event.UpdatedAt = orNow(event.UpdatedAt, now)

	var changeType core.ChangeType
	switch {
	case event.Status == core.StatusCancelled:
		changeType = core.ChangeCancelled
	case storedStatus == string(core.StatusExpired) && !incomingLive:
		// The provider changed content while its expiry is still lapsed:
		// persist the new content, keep the internal lifecycle expired.
		event.Status = core.StatusExpired
		changeType = core.ChangeUpdated
	default:
		changeType = core.ChangeUpdated
	}

	snapshot := storage.SnapshotOf(event)
	if _, err := tx.ExecContext(ctx, updateSQL, updateArgs(event, fingerprint, now, expiryMs(event))...); err != nil {
		return 0, nil, fmt.Errorf("update event %q: %w", key, err)
	}
	change, err := insertChange(tx, ctx, changeType, key, nowMs, snapshot)
	if err != nil {
		return 0, nil, err
	}
	if err := tx.Commit(); err != nil {
		return 0, nil, fmt.Errorf("commit update of %q: %w", key, err)
	}
	change.Event = event

	if changeType == core.ChangeCancelled {
		return storage.OutcomeCancelled, change, nil
	}
	return storage.OutcomeUpdated, change, nil
}

// Expire atomically marks stale active events expired, creating a durable
// ChangeExpired journal record for each. Events without an expiry are never
// touched.
func (s *Store) Expire(ctx context.Context, now time.Time) ([]storage.Change, error) {
	now = now.UTC()
	nowMs := now.UnixMilli()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin expiration transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx,
		"SELECT event_key FROM events WHERE status = ? AND expires_at_ms IS NOT NULL AND expires_at_ms <= ?",
		string(core.StatusActive), nowMs)
	if err != nil {
		return nil, fmt.Errorf("find expired events: %w", err)
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan expired event: %w", err)
		}
		keys = append(keys, key)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expired events: %w", err)
	}

	changes := make([]storage.Change, 0, len(keys))
	for _, key := range keys {
		if _, err := tx.ExecContext(ctx,
			"UPDATE events SET status = ?, updated_at = ? WHERE event_key = ?",
			string(core.StatusExpired), formatTime(now), key); err != nil {
			return nil, fmt.Errorf("expire event %q: %w", key, err)
		}
		event, err := loadEventTx(tx, key)
		if err != nil {
			return nil, err
		}
		change, err := insertChange(tx, ctx, core.ChangeExpired, key, nowMs, storage.SnapshotOf(event))
		if err != nil {
			return nil, err
		}
		change.Event = event
		changes = append(changes, *change)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit expiration: %w", err)
	}
	return changes, nil
}

// PollChanges returns unacknowledged journal changes for an output in
// stable ID order. Each change carries the immutable event snapshot taken
// when the transition happened; the mutable events table is never joined
// to reconstruct history.
func (s *Store) PollChanges(ctx context.Context, outputID string, limit int) ([]storage.Change, error) {
	cursor, err := s.cursor(ctx, outputID)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 256 {
		limit = 256
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, change_type, event_snapshot
		FROM changes
		WHERE id > ?
		ORDER BY id
		LIMIT ?`, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("poll changes for %q: %w", outputID, err)
	}
	defer rows.Close()

	var changes []storage.Change
	for rows.Next() {
		var (
			change     storage.Change
			changeType string
			snapshot   string
		)
		if err := rows.Scan(&change.ID, &changeType, &snapshot); err != nil {
			return nil, fmt.Errorf("scan change for %q: %w", outputID, err)
		}
		event, err := decodeSnapshot(snapshot)
		if err != nil {
			return nil, fmt.Errorf("decode snapshot for change %d: %w", change.ID, err)
		}
		change.ChangeType = core.ChangeType(changeType)
		change.Event = event
		changes = append(changes, change)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate changes for %q: %w", outputID, err)
	}
	return changes, nil
}

// SyncOutputs makes the output_cursors table match the set of enabled
// outputs exactly (see storage.EventStore). Newly enabled outputs start at
// cursor 0 and receive all changes still present in the retained journal;
// outputs that are no longer enabled are removed so they stop blocking
// cleanup.
func (s *Store) SyncOutputs(ctx context.Context, enabledOutputIDs []string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin output sync: %w", err)
	}
	defer tx.Rollback()

	for _, id := range enabledOutputIDs {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO output_cursors (output_id, last_acked_id) VALUES (?, 0)
			ON CONFLICT(output_id) DO NOTHING`, id); err != nil {
			return fmt.Errorf("create cursor for %q: %w", id, err)
		}
	}

	enabled := make(map[string]bool, len(enabledOutputIDs))
	for _, id := range enabledOutputIDs {
		enabled[id] = true
	}
	rows, err := tx.QueryContext(ctx, "SELECT output_id FROM output_cursors")
	if err != nil {
		return fmt.Errorf("read cursors: %w", err)
	}
	var stale []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("scan cursor: %w", err)
		}
		if !enabled[id] {
			stale = append(stale, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate cursors: %w", err)
	}
	for _, id := range stale {
		if _, err := tx.ExecContext(ctx, "DELETE FROM output_cursors WHERE output_id = ?", id); err != nil {
			return fmt.Errorf("remove cursor for %q: %w", id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit output sync: %w", err)
	}
	return nil
}

// AckChanges records the highest change ID an output has delivered.
func (s *Store) AckChanges(ctx context.Context, outputID string, lastChangeID int64) error {
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO output_cursors (output_id, last_acked_id) VALUES (?, ?)
		ON CONFLICT(output_id) DO UPDATE SET
			last_acked_id = MAX(last_acked_id, excluded.last_acked_id)`,
		outputID, lastChangeID); err != nil {
		return fmt.Errorf("ack changes for %q: %w", outputID, err)
	}
	return nil
}

// CleanupChanges deletes journal rows older than olderThan that every
// output has acknowledged. When no outputs exist, all rows are considered
// acknowledged (nothing relevant is waiting for them).
func (s *Store) CleanupChanges(ctx context.Context, olderThan time.Time) (int64, error) {
	cutoff := olderThan.UTC().UnixMilli()
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM changes
		WHERE created_at_ms < ?
		  AND id <= COALESCE(
			(SELECT MIN(last_acked_id) FROM output_cursors),
			(SELECT MAX(id) FROM changes),
			0
		  )`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("cleanup changes: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("cleanup changes: %w", err)
	}
	return n, nil
}

// CleanupEvents deletes cancelled/expired current-state records whose
// updated_at is older than olderThan. Active events are never touched, and
// the change journal is unaffected (it carries immutable snapshots).
// updated_at stores UTC RFC3339Nano strings, which compare correctly as
// plain text.
func (s *Store) CleanupEvents(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		"DELETE FROM events WHERE status IN (?, ?) AND updated_at < ?",
		string(core.StatusCancelled), string(core.StatusExpired), formatTime(olderThan))
	if err != nil {
		return 0, fmt.Errorf("cleanup events: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("cleanup events: %w", err)
	}
	return n, nil
}

// PendingStats reports undelivered change count and the age of the oldest
// undelivered change. It is consistent with CleanupChanges: both operate
// against the set of enabled output cursors (synchronized by SyncOutputs),
// and with no enabled outputs nothing is considered pending — retained
// changes are history, cleaned up by age.
func (s *Store) PendingStats(ctx context.Context) (int, time.Duration, error) {
	var pending int
	var oldestMs sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*), MIN(created_at_ms)
		FROM changes
		WHERE id > COALESCE(
			(SELECT MIN(last_acked_id) FROM output_cursors),
			(SELECT MAX(id) FROM changes),
			0
		)`).Scan(&pending, &oldestMs)
	if err != nil {
		return 0, 0, fmt.Errorf("pending stats: %w", err)
	}
	oldest := time.Duration(0)
	if oldestMs.Valid {
		oldest = time.Since(time.UnixMilli(oldestMs.Int64))
		if oldest < 0 {
			oldest = 0
		}
	}
	return pending, oldest, nil
}

// Get returns the stored event for key, or storage.ErrNotFound.
func (s *Store) Get(ctx context.Context, key string) (*storage.StoredEvent, error) {
	row := s.db.QueryRowContext(ctx, "SELECT "+eventColumns+" FROM events WHERE event_key = ?", key)
	var stored storage.StoredEvent
	event, err := scanEventRow(row.Scan, &stored)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("get event %q: %w", key, err)
	}
	stored.Event = event
	return &stored, nil
}

// Count returns the number of stored events.
func (s *Store) Count(ctx context.Context) (int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&n); err != nil {
		return 0, fmt.Errorf("count events: %w", err)
	}
	return n, nil
}

// ---- internals ----

const insertSQL = `
INSERT INTO events (
	event_key, source, source_id, fingerprint, status,
	category, event, severity, urgency, certainty,
	headline, description, instruction,
	effective_at, expires_at, expires_at_ms, latitude, longitude,
	areas, source_url, received_at, first_seen_at, last_seen_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(event_key) DO NOTHING`

func insertArgs(event core.HazardEvent, fingerprint string, firstSeen, lastSeen, updated time.Time, expiresMs int64) []any {
	received := orNow(event.ReceivedAt, updated)
	areas, _ := json.Marshal(event.Areas)
	return []any{
		event.Key(), event.Source, event.SourceID, fingerprint, string(event.Status),
		event.Category, event.Event, event.Severity, event.Urgency, event.Certainty,
		event.Headline, event.Description, event.Instruction,
		nullableTime(event.EffectiveAt), nullableTime(event.ExpiresAt), nullableInt64(expiresMs),
		nullableFloat(event.Latitude), nullableFloat(event.Longitude),
		string(areas), event.SourceURL,
		formatTime(received), formatTime(firstSeen), formatTime(lastSeen), formatTime(updated),
	}
}

// expiryMs returns the event's expiry as epoch milliseconds, or 0 when the
// event never expires.
func expiryMs(event core.HazardEvent) int64 {
	if event.ExpiresAt == nil {
		return 0
	}
	return event.ExpiresAt.UTC().UnixMilli()
}

// expiryLive reports whether the provider made the event live again: the
// expiry is absent or in the future relative to nowMs. A lapsed expiry by
// itself is NOT a reactivation signal — that would let a stale provider
// event create updated/expired flapping on every poll.
func expiryLive(event core.HazardEvent, nowMs int64) bool {
	if event.ExpiresAt == nil {
		return true
	}
	return event.ExpiresAt.UTC().UnixMilli() > nowMs
}

const updateSQL = `
UPDATE events SET
	source = ?, source_id = ?, fingerprint = ?, status = ?,
	category = ?, event = ?, severity = ?, urgency = ?, certainty = ?,
	headline = ?, description = ?, instruction = ?,
	effective_at = ?, expires_at = ?, expires_at_ms = ?, latitude = ?, longitude = ?,
	areas = ?, source_url = ?, received_at = ?, last_seen_at = ?, updated_at = ?
	WHERE event_key = ?`

func updateArgs(event core.HazardEvent, fingerprint string, now time.Time, expiresMs int64) []any {
	received := orNow(event.ReceivedAt, now)
	areas, _ := json.Marshal(event.Areas)
	return append([]any{
		event.Source, event.SourceID, fingerprint, string(event.Status),
		event.Category, event.Event, event.Severity, event.Urgency, event.Certainty,
		event.Headline, event.Description, event.Instruction,
		nullableTime(event.EffectiveAt), nullableTime(event.ExpiresAt), nullableInt64(expiresMs),
		nullableFloat(event.Latitude), nullableFloat(event.Longitude),
		string(areas), event.SourceURL,
		formatTime(received), formatTime(now), formatTime(orNow(event.UpdatedAt, now)),
	}, event.Key())
}

// insertChange writes one journal record (with its immutable event
// snapshot) and returns its stable ID.
func insertChange(tx *sql.Tx, ctx context.Context, changeType core.ChangeType, key string, nowMs int64, snapshot storage.EventSnapshot) (*storage.Change, error) {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot for %q: %w", key, err)
	}
	res, err := tx.ExecContext(ctx,
		"INSERT INTO changes (change_type, event_key, event_snapshot, created_at_ms) VALUES (?, ?, ?, ?)",
		string(changeType), key, string(data), nowMs)
	if err != nil {
		return nil, fmt.Errorf("journal change for %q: %w", key, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("journal change id for %q: %w", key, err)
	}
	return &storage.Change{ID: id, ChangeType: changeType}, nil
}

func (s *Store) cursor(ctx context.Context, outputID string) (int64, error) {
	var cursor int64
	err := s.db.QueryRowContext(ctx,
		"SELECT last_acked_id FROM output_cursors WHERE output_id = ?", outputID).Scan(&cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read cursor for %q: %w", outputID, err)
	}
	return cursor, nil
}

// loadEventTx reads a full event row from tx by key.
func loadEventTx(tx *sql.Tx, key string) (core.HazardEvent, error) {
	row := tx.QueryRow("SELECT "+eventColumns+" FROM events WHERE event_key = ?", key)
	return scanEventRow(row.Scan, nil)
}

// decodeSnapshot reconstructs the event state captured when a journal
// change was written.
func decodeSnapshot(data string) (core.HazardEvent, error) {
	var snap storage.EventSnapshot
	if err := json.Unmarshal([]byte(data), &snap); err != nil {
		return core.HazardEvent{}, fmt.Errorf("parse event snapshot: %w", err)
	}
	return snap.ToEvent(), nil
}

// scanEventRow scans the canonical event column list (eventColumns) in
// order. When stored is non-nil it also receives the persistence metadata.
func scanEventRow(scan func(dest ...any) error, stored *storage.StoredEvent) (core.HazardEvent, error) {
	var (
		key, fingerprint, status string
		effectiveAt              sql.NullString
		expiresMs                sql.NullInt64
		lat, lon                 sql.NullFloat64
		areasJSON                string
		received, first, last    string
		updated                  string
	)
	var event core.HazardEvent
	dests := []any{
		&key, &event.Source, &event.SourceID, &fingerprint, &status,
		&event.Category, &event.Event, &event.Severity, &event.Urgency, &event.Certainty,
		&event.Headline, &event.Description, &event.Instruction,
		&effectiveAt, &expiresMs, &lat, &lon,
		&areasJSON, &event.SourceURL,
		&received, &first, &last, &updated,
	}
	if err := scan(dests...); err != nil {
		return event, err
	}

	event.Status = core.EventStatus(status)
	if effectiveAt.Valid {
		t, err := time.Parse(time.RFC3339Nano, effectiveAt.String)
		if err != nil {
			return event, fmt.Errorf("parse effective_at: %w", err)
		}
		event.EffectiveAt = &t
	}
	if expiresMs.Valid {
		t := time.UnixMilli(expiresMs.Int64).UTC()
		event.ExpiresAt = &t
	}
	if lat.Valid {
		event.Latitude = &lat.Float64
	}
	if lon.Valid {
		event.Longitude = &lon.Float64
	}
	if err := json.Unmarshal([]byte(areasJSON), &event.Areas); err != nil {
		return event, fmt.Errorf("decode areas: %w", err)
	}
	storedTimes := []struct {
		dest *time.Time
		src  string
	}{
		{&event.ReceivedAt, received},
		{&event.UpdatedAt, updated},
	}
	for _, t := range storedTimes {
		parsed, err := time.Parse(time.RFC3339Nano, t.src)
		if err != nil {
			return event, fmt.Errorf("parse stored timestamp: %w", err)
		}
		*t.dest = parsed
	}

	if stored != nil {
		stored.Fingerprint = fingerprint
		for _, t := range []struct {
			dest *time.Time
			src  string
		}{
			{&stored.FirstSeenAt, first},
			{&stored.LastSeenAt, last},
		} {
			parsed, err := time.Parse(time.RFC3339Nano, t.src)
			if err != nil {
				return event, fmt.Errorf("parse stored timestamp: %w", err)
			}
			*t.dest = parsed
		}
	}
	return event, nil
}

func nullableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return formatTime(*t)
}

func nullableFloat(f *float64) any {
	if f == nil {
		return nil
	}
	return *f
}

func nullableInt64(ms int64) any {
	if ms == 0 {
		return nil
	}
	return ms
}

// formatTime renders timestamps in UTC RFC3339Nano for human-readable
// columns; SQL comparisons use the integer millisecond columns instead.
func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// orNow returns t when it is set, otherwise fallback.
func orNow(t, fallback time.Time) time.Time {
	if t.IsZero() {
		return fallback
	}
	return t
}
