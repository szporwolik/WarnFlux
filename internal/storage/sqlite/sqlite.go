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

	"warnflux/internal/core"
	"warnflux/internal/storage"
)

// migrations holds the schema evolution steps in order. Slice index +1 is
// the target schema version, persisted via SQLite's PRAGMA user_version.
//
// To evolve the schema: APPEND a new step — never edit or remove an entry
// that has been released, or existing databases would drift. Each step runs
// in its own transaction, so a failed step leaves the database untouched.
// Existing databases are upgraded in place on startup, never recreated.
var migrations = []string{
	// v1: initial schema.
	`
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
}

// eventColumns is the canonical column list used for SELECT and RETURNING.
const eventColumns = `event_key, source, source_id, fingerprint, status,
category, event, severity, urgency, certainty,
headline, description, instruction,
effective_at, expires_at, latitude, longitude,
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
	// From is the schema version found on disk.
	From int
	// To is the schema version after migration.
	To int
}

// Open connects to the SQLite database at path, applies the configured
// pragmas and migrations, and returns the ready store.
//
// SQLite settings chosen for a long-running, low-volume daemon:
//   - journal_mode=WAL: readers do not block the writer; crash-safe journal.
//   - synchronous=NORMAL: durable across process crashes; WAL checkpoints
//     may lose the last committed transactions only on power loss. Warning
//     data does not need synchronous=FULL.
//   - busy_timeout=5000: wait for the lock instead of failing immediately.
//   - foreign_keys=ON: integrity checks active if FKs are added later.
//   - a single database connection (MaxOpenConns=1): with one process and
//     low volume this serializes access, keeps per-connection PRAGMAs
//     stable and avoids SQLITE_BUSY entirely.
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
		"PRAGMA synchronous=NORMAL",
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
func migrate(db *sql.DB, steps []string) (MigrationInfo, error) {
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
		if _, err := tx.Exec(steps[v]); err != nil {
			tx.Rollback()
			return info, fmt.Errorf("apply migration to version %d: %w", target, err)
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

// Count returns the number of stored events. Useful for health checks and
// tests.
func (s *Store) Count(ctx context.Context) (int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&n); err != nil {
		return 0, fmt.Errorf("count events: %w", err)
	}
	return n, nil
}

// Get returns the stored event for key, or storage.ErrNotFound.
func (s *Store) Get(ctx context.Context, key string) (*storage.StoredEvent, error) {
	row := s.db.QueryRowContext(ctx, "SELECT "+eventColumns+" FROM events WHERE event_key = ?", key)
	e, err := scanStoredEvent(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get event %q: %w", key, err)
	}
	return e, nil
}

// Insert stores a new event and reports whether the row was inserted. When
// an event with the same key already exists it reports false without
// error, so concurrent ingestion inserts exactly one row per event.
func (s *Store) Insert(ctx context.Context, event core.HazardEvent, fingerprint string) (bool, error) {
	now := s.now().UTC()
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO events (
			event_key, source, source_id, fingerprint, status,
			category, event, severity, urgency, certainty,
			headline, description, instruction,
			effective_at, expires_at, latitude, longitude,
			areas, source_url, received_at, first_seen_at, last_seen_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_key) DO NOTHING`,
		eventArgs(event.Key(), fingerprint, event, now, now, now)...,
	)
	if err != nil {
		return false, fmt.Errorf("insert event %q: %w", event.Key(), err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("insert event %q: %w", event.Key(), err)
	}
	return n == 1, nil
}

// Update replaces the content of an existing event. first_seen_at is left
// untouched by omission; ReceivedAt must be provided by the caller (the
// ingest service preserves the original value).
func (s *Store) Update(ctx context.Context, event core.HazardEvent, fingerprint string) error {
	now := s.now().UTC()
	args := updateArgs(event, fingerprint, now)
	args = append(args, event.Key())
	_, err := s.db.ExecContext(ctx, `
		UPDATE events SET
			source = ?, source_id = ?, fingerprint = ?, status = ?,
			category = ?, event = ?, severity = ?, urgency = ?, certainty = ?,
			headline = ?, description = ?, instruction = ?,
			effective_at = ?, expires_at = ?, latitude = ?, longitude = ?,
			areas = ?, source_url = ?, received_at = ?, last_seen_at = ?, updated_at = ?
		WHERE event_key = ?`,
		args...,
	)
	if err != nil {
		return fmt.Errorf("update event %q: %w", event.Key(), err)
	}
	return nil
}

// Touch refreshes only last_seen_at for an unchanged event.
func (s *Store) Touch(ctx context.Context, key string) error {
	now := s.now().UTC()
	if _, err := s.db.ExecContext(ctx,
		"UPDATE events SET last_seen_at = ? WHERE event_key = ?",
		formatTime(now), key); err != nil {
		return fmt.Errorf("touch event %q: %w", key, err)
	}
	return nil
}

// MarkExpired flips active events whose ExpiresAt is at or before now to
// expired and returns them. The state change is persistent, not just an
// in-memory flag.
func (s *Store) MarkExpired(ctx context.Context, now time.Time) ([]core.HazardEvent, error) {
	now = now.UTC()
	rows, err := s.db.QueryContext(ctx, `
		UPDATE events
		SET status = ?, updated_at = ?
		WHERE status = ? AND expires_at IS NOT NULL AND expires_at <= ?
		RETURNING `+eventColumns,
		string(core.StatusExpired), formatTime(now),
		string(core.StatusActive), formatTime(now),
	)
	if err != nil {
		return nil, fmt.Errorf("mark expired events: %w", err)
	}
	defer rows.Close()

	var expired []core.HazardEvent
	for rows.Next() {
		e, err := scanStoredEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan expired event: %w", err)
		}
		expired = append(expired, e.Event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mark expired events: %w", err)
	}
	return expired, nil
}

// eventArgs builds the argument slice shared by Insert and Update. The
// timestamps firstSeen, lastSeen and updated are supplied by the caller.
func eventArgs(key, fingerprint string, event core.HazardEvent, firstSeen, lastSeen, updated time.Time) []any {
	received := event.ReceivedAt
	if received.IsZero() {
		received = updated
	}
	areas, _ := json.Marshal(event.Areas)
	return []any{
		key, event.Source, event.SourceID, fingerprint, string(event.Status),
		event.Category, event.Event, event.Severity, event.Urgency, event.Certainty,
		event.Headline, event.Description, event.Instruction,
		nullableTime(event.EffectiveAt), nullableTime(event.ExpiresAt),
		nullableFloat(event.Latitude), nullableFloat(event.Longitude),
		string(areas), event.SourceURL,
		formatTime(received), formatTime(firstSeen), formatTime(lastSeen), formatTime(updated),
	}
}

// updateArgs builds the argument slice for Update. first_seen_at is absent
// from the SET list so it is preserved across updates.
func updateArgs(event core.HazardEvent, fingerprint string, now time.Time) []any {
	received := event.ReceivedAt
	if received.IsZero() {
		received = now
	}
	areas, _ := json.Marshal(event.Areas)
	return []any{
		event.Source, event.SourceID, fingerprint, string(event.Status),
		event.Category, event.Event, event.Severity, event.Urgency, event.Certainty,
		event.Headline, event.Description, event.Instruction,
		nullableTime(event.EffectiveAt), nullableTime(event.ExpiresAt),
		nullableFloat(event.Latitude), nullableFloat(event.Longitude),
		string(areas), event.SourceURL,
		formatTime(received), formatTime(now), formatTime(now),
	}
}

func scanStoredEvent(row interface{ Scan(dest ...any) error }) (*storage.StoredEvent, error) {
	var (
		e                          storage.StoredEvent
		key, status                string
		effectiveAt, expiresAt     sql.NullString
		lat, lon                   sql.NullFloat64
		areasJSON                  string
		received, first, last, upd string
	)
	if err := row.Scan(
		&key, &e.Event.Source, &e.Event.SourceID, &e.Fingerprint, &status,
		&e.Event.Category, &e.Event.Event, &e.Event.Severity, &e.Event.Urgency, &e.Event.Certainty,
		&e.Event.Headline, &e.Event.Description, &e.Event.Instruction,
		&effectiveAt, &expiresAt, &lat, &lon,
		&areasJSON, &e.Event.SourceURL,
		&received, &first, &last, &upd,
	); err != nil {
		return nil, err
	}

	e.Event.Status = core.EventStatus(status)
	if effectiveAt.Valid {
		t, err := time.Parse(time.RFC3339Nano, effectiveAt.String)
		if err != nil {
			return nil, fmt.Errorf("parse effective_at: %w", err)
		}
		e.Event.EffectiveAt = &t
	}
	if expiresAt.Valid {
		t, err := time.Parse(time.RFC3339Nano, expiresAt.String)
		if err != nil {
			return nil, fmt.Errorf("parse expires_at: %w", err)
		}
		e.Event.ExpiresAt = &t
	}
	if lat.Valid {
		e.Event.Latitude = &lat.Float64
	}
	if lon.Valid {
		e.Event.Longitude = &lon.Float64
	}
	if err := json.Unmarshal([]byte(areasJSON), &e.Event.Areas); err != nil {
		return nil, fmt.Errorf("decode areas: %w", err)
	}
	storedTimes := []struct {
		dest *time.Time
		src  string
	}{
		{&e.Event.ReceivedAt, received},
		{&e.Event.UpdatedAt, upd},
		{&e.FirstSeenAt, first},
		{&e.LastSeenAt, last},
	}
	for _, t := range storedTimes {
		parsed, err := time.Parse(time.RFC3339Nano, t.src)
		if err != nil {
			return nil, fmt.Errorf("parse stored timestamp: %w", err)
		}
		*t.dest = parsed
	}
	return &e, nil
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

// formatTime renders timestamps in UTC RFC3339Nano so that the fixed-width
// text representation also sorts chronologically.
func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}
