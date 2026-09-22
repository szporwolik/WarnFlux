package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

func openRaw(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "migrate.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func schemaVersion(t *testing.T, db *sql.DB) int {
	t.Helper()
	var v int
	if err := db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	return v
}

func tableCount(t *testing.T, db *sql.DB, name string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(
		"SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", name,
	).Scan(&n); err != nil {
		t.Fatalf("count table %q: %v", name, err)
	}
	return n
}

func steps(sqls ...string) []migration {
	out := make([]migration, len(sqls))
	for i, s := range sqls {
		out[i] = migration{SQL: s}
	}
	return out
}

func TestMigrateAppliesPendingSteps(t *testing.T) {
	db := openRaw(t)
	migrations := steps(
		"CREATE TABLE one (id INTEGER PRIMARY KEY);",
		"CREATE TABLE two (id INTEGER PRIMARY KEY);",
	)

	info, err := migrate(db, migrations)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if info.From != 0 || info.To != 2 {
		t.Errorf("info = %+v, want {From:0 To:2}", info)
	}
	if v := schemaVersion(t, db); v != 2 {
		t.Errorf("user_version = %d, want 2", v)
	}
	if tableCount(t, db, "one") != 1 || tableCount(t, db, "two") != 1 {
		t.Error("both tables should exist")
	}

	info2, err := migrate(db, migrations)
	if err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if info2.From != 2 || info2.To != 2 {
		t.Errorf("second info = %+v, want {From:2 To:2}", info2)
	}
}

func TestMigrateRollsBackFailedStep(t *testing.T) {
	db := openRaw(t)
	migrations := steps(
		"CREATE TABLE one (id INTEGER PRIMARY KEY);",
		"CREATE TABLE two (id INTEGER PRIMARY KEY);\nTHIS IS NOT SQL;",
	)

	if _, err := migrate(db, migrations); err == nil {
		t.Fatal("expected migration error, got nil")
	}
	if v := schemaVersion(t, db); v != 1 {
		t.Errorf("user_version = %d, want 1", v)
	}
	if tableCount(t, db, "one") != 1 {
		t.Error("committed step must survive")
	}
	if tableCount(t, db, "two") != 0 {
		t.Error("failed step must be rolled back completely")
	}
}

func TestMigrateGoStepRollsBackOnError(t *testing.T) {
	db := openRaw(t)
	boom := migration{
		SQL: "CREATE TABLE one (id INTEGER PRIMARY KEY);",
		run: func(tx *sql.Tx) error {
			if _, err := tx.Exec("CREATE TABLE two (id INTEGER PRIMARY KEY);"); err != nil {
				return err
			}
			return sql.ErrConnDone // simulate failure after partial work
		},
	}

	if _, err := migrate(db, []migration{boom}); err == nil {
		t.Fatal("expected migration error, got nil")
	}
	if v := schemaVersion(t, db); v != 0 {
		t.Errorf("user_version = %d, want 0", v)
	}
	if tableCount(t, db, "one") != 0 || tableCount(t, db, "two") != 0 {
		t.Error("Go step must roll back completely")
	}
}

func TestMigrateResumesAfterFailure(t *testing.T) {
	db := openRaw(t)
	bad := steps(
		"CREATE TABLE one (id INTEGER PRIMARY KEY);",
		"INVALID SQL;",
	)
	if _, err := migrate(db, bad); err == nil {
		t.Fatal("expected error, got nil")
	}
	fixed := steps(
		"CREATE TABLE one (id INTEGER PRIMARY KEY);",
		"CREATE TABLE two (id INTEGER PRIMARY KEY);",
	)
	info, err := migrate(db, fixed)
	if err != nil {
		t.Fatalf("migrate after fix: %v", err)
	}
	if info.From != 1 || info.To != 2 {
		t.Errorf("info = %+v, want {From:1 To:2}", info)
	}
	if tableCount(t, db, "two") != 1 {
		t.Error("remaining step should be applied after a successful retry")
	}
}

func TestMigrateRefusesNewerDatabase(t *testing.T) {
	db := openRaw(t)
	steps := steps("CREATE TABLE one (id INTEGER PRIMARY KEY);")
	if _, err := migrate(db, steps); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.Exec("PRAGMA user_version = 5"); err != nil {
		t.Fatalf("set user_version: %v", err)
	}
	if _, err := migrate(db, steps); err == nil {
		t.Fatal("expected refusal to touch a newer database, got nil")
	}
	if v := schemaVersion(t, db); v != 5 {
		t.Errorf("user_version = %d, want 5 (untouched)", v)
	}
}

// buildV3DB creates a database at exactly schema version 3 (v1+v2+v3
// applied), so v4 upgrade behavior can be tested deterministically.
func buildV3DB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "v3.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := migrate(db, migrations[:3]); err != nil {
		db.Close()
		t.Fatalf("migrate to v3: %v", err)
	}
	db.Close()
	return path
}

// TestMigrationV8RoutingMatrixBackfill builds a v7 database with the
// group-wide threshold and verifies the v8 step moves it onto every
// existing assignment and drops the obsolete column.
func TestMigrationV8RoutingMatrixBackfill(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v7.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := migrate(db, migrations[:7]); err != nil {
		db.Close()
		t.Fatalf("migrate to v7: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO groups (name, min_severity, created_at_ms, updated_at_ms)
		VALUES ('spok', 'severe', 1, 1)`); err != nil {
		db.Close()
		t.Fatalf("insert group: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO group_actions (group_id, action_id) VALUES (1, 'log')`); err != nil {
		db.Close()
		t.Fatalf("insert action: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO group_outputs (group_id, output_id) VALUES (1, 'mqtt')`); err != nil {
		db.Close()
		t.Fatalf("insert output: %v", err)
	}
	db.Close()

	store, info, err := Open(path)
	if err != nil {
		t.Fatalf("Open of v7 database: %v", err)
	}
	defer store.Close()
	if info.From != 7 || info.To != 8 {
		t.Fatalf("migration = %+v, want {From:7 To:8}", info)
	}

	r, err := store.GroupRouting(1)
	if err != nil {
		t.Fatalf("GroupRouting: %v", err)
	}
	if len(r.Actions) != 1 || r.Actions[0].MinSeverity != "severe" {
		t.Errorf("backfilled action = %+v, want severe", r.Actions)
	}
	if len(r.Outputs) != 1 || r.Outputs[0].MinSeverity != "severe" {
		t.Errorf("backfilled output = %+v, want severe", r.Outputs)
	}

	// The obsolete column must be gone.
	var n int
	if err := store.db.QueryRow(`
		SELECT COUNT(*) FROM pragma_table_info('groups') WHERE name = 'min_severity'`).Scan(&n); err != nil {
		t.Fatalf("column check: %v", err)
	}
	if n != 0 {
		t.Errorf("groups.min_severity still present after v8")
	}
}

// TestMigrationV4BackfillsLastSeen builds a v3 database with a legacy text
// last_seen_at and verifies the v4 step backfills last_seen_at_ms.
func TestMigrationV4BackfillsLastSeen(t *testing.T) {
	path := buildV3DB(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	seen := "2026-06-01T12:30:45.123456789Z"
	if _, err := db.Exec(`
		INSERT INTO events (event_key, source, source_id, fingerprint, status, event,
			received_at, first_seen_at, last_seen_at, updated_at)
		VALUES ('src:1', 'src', '1', 'fp', 'active', 'E', ?, ?, ?, ?)`,
		seen, seen, seen, seen); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}
	db.Close()

	store, info, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if info.To != 8 {
		t.Fatalf("migrated to %d, want 8", info.To)
	}
	var ms int64
	if err := store.db.QueryRow("SELECT last_seen_at_ms FROM events WHERE event_key = 'src:1'").Scan(&ms); err != nil {
		t.Fatalf("read last_seen_at_ms: %v", err)
	}
	want, _ := time.Parse(time.RFC3339Nano, seen)
	if ms != want.UnixMilli() {
		t.Errorf("last_seen_at_ms = %d, want %d", ms, want.UnixMilli())
	}
}

// TestMigrationV4InvalidLastSeenFails: a legacy row with unparsable
// last_seen_at must fail the v4 step and leave the schema at v3.
func TestMigrationV4InvalidLastSeenFails(t *testing.T) {
	path := buildV3DB(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO events (event_key, source, source_id, fingerprint, status, event,
			received_at, first_seen_at, last_seen_at, updated_at)
		VALUES ('bad:1', 'bad', '1', 'fp', 'active', 'E', 'garbage', 'garbage', 'garbage', 'garbage')`); err != nil {
		t.Fatalf("insert invalid row: %v", err)
	}
	db.Close()

	if _, _, err := Open(path); err == nil {
		t.Fatal("Open must fail on invalid legacy last_seen_at")
	}
	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if got := schemaVersion(t, db); got != 3 {
		t.Errorf("schema version = %d after failed v4, want 3 (rolled back)", got)
	}
}

// TestOpenRefusesNewerSchema pins the forward-compatibility guard.
func TestOpenRefusesNewerSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "newer.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec("PRAGMA user_version = 99"); err != nil {
		t.Fatalf("set version: %v", err)
	}
	db.Close()
	if _, _, err := Open(path); err == nil {
		t.Fatal("Open must refuse a database newer than this binary supports")
	}
}

// buildV2DB creates a REALISTIC schema-v2 database using only migrations
// v1+v2, then inserts an event row (through the v2 column set) and,
// optionally, a journal change and an output cursor. This reproduces what
// pre-release binaries actually wrote; migration v3/v4 must upgrade it
// without loss.
func buildV2DB(t *testing.T, withJournal bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "v2.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := migrate(db, migrations[:2]); err != nil {
		db.Close()
		t.Fatalf("migrate to v2: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO events (event_key, source, source_id, fingerprint, status, event,
			effective_at, expires_at, expires_at_ms, areas,
			received_at, first_seen_at, last_seen_at, updated_at)
		VALUES ('v2src:1', 'v2src', '1', 'fp', 'active', 'Flood', NULL, NULL, NULL, '["area one"]',
			'2026-05-01T10:00:00Z', '2026-05-01T10:00:00Z',
			'2026-05-02T10:00:00.123456789Z', '2026-05-02T10:00:00.123456789Z')`); err != nil {
		db.Close()
		t.Fatalf("insert v2 event: %v", err)
	}
	if withJournal {
		if _, err := db.Exec(`INSERT INTO changes (change_type, event_key, created_at_ms)
			VALUES ('new', 'v2src:1', 1779958800123)`); err != nil {
			db.Close()
			t.Fatalf("insert v2 change: %v", err)
		}
		if _, err := db.Exec(`INSERT INTO output_cursors (output_id, last_acked_id)
			VALUES ('legacy-out', 0)`); err != nil {
			db.Close()
			t.Fatalf("insert v2 cursor: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return path
}

// TestMigrationV2WithJournalReachesCurrent is the regression test for the
// v3 backfill future-schema coupling: upgrading a real v2 database with
// journal rows must succeed and preserve everything. It fails on the bug
// (v3 selecting last_seen_at_ms before v4 creates it).
func TestMigrationV2WithJournalReachesCurrent(t *testing.T) {
	path := buildV2DB(t, true)
	store, info, err := Open(path)
	if err != nil {
		t.Fatalf("Open of real v2 database: %v", err)
	}
	defer store.Close()
	if info.From != 2 || info.To != 8 {
		t.Fatalf("migration = %+v, want {From:2 To:8}", info)
	}

	var v int
	if err := store.db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if v != 8 {
		t.Errorf("user_version = %d, want 8", v)
	}

	// The event row survives with a correct machine-time last_seen.
	got, err := store.Get(context.Background(), "v2src:1")
	if err != nil {
		t.Fatalf("Get after migration: %v", err)
	}
	if got.Event.Source != "v2src" || got.Event.SourceID != "1" {
		t.Errorf("event identity = %s:%s, want v2src:1", got.Event.Source, got.Event.SourceID)
	}
	var ms int64
	if err := store.db.QueryRow("SELECT last_seen_at_ms FROM events WHERE event_key = 'v2src:1'").Scan(&ms); err != nil {
		t.Fatalf("read last_seen_at_ms: %v", err)
	}
	want, _ := time.Parse(time.RFC3339Nano, "2026-05-02T10:00:00.123456789Z")
	if ms != want.UnixMilli() {
		t.Errorf("last_seen_at_ms = %d, want %d", ms, want.UnixMilli())
	}

	// The historical journal change is preserved and readable.
	var ct, key, snap string
	if err := store.db.QueryRow("SELECT change_type, event_key, event_snapshot FROM changes WHERE id = 1").Scan(&ct, &key, &snap); err != nil {
		t.Fatalf("read change: %v", err)
	}
	if ct != "new" || key != "v2src:1" {
		t.Errorf("change = (%q, %q), want (new, v2src:1)", ct, key)
	}
	if snap == "" {
		t.Error("v3 backfill must populate event_snapshot")
	}

	changes, err := store.PollChanges(context.Background(), "legacy-out", 32)
	if err != nil {
		t.Fatalf("PollChanges on migrated journal: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("PollChanges returned %d changes, want 1", len(changes))
	}
	c := changes[0]
	if c.ID != 1 || c.ChangeType != "new" || c.Event.Source != "v2src" || c.Event.SourceID != "1" {
		t.Errorf("polled change = %+v, want id 1, type new, identity v2src:1", c)
	}
}

// TestMigrationV2EventsWithoutJournalReachesCurrent: v2 with rows but no
// journal must also upgrade cleanly.
func TestMigrationV2EventsWithoutJournalReachesCurrent(t *testing.T) {
	path := buildV2DB(t, false)
	store, info, err := Open(path)
	if err != nil {
		t.Fatalf("Open of v2 database: %v", err)
	}
	defer store.Close()
	if info.To != 8 {
		t.Fatalf("migrated to %d, want 8", info.To)
	}
	if _, err := store.Get(context.Background(), "v2src:1"); err != nil {
		t.Fatalf("Get after migration: %v", err)
	}
}

// TestMigrationV1WithRowsReachesCurrent: a real v1 database (rows, no
// journal, no machine-time columns) upgrades through v2 backfill and v4
// backfill.
func TestMigrationV1WithRowsReachesCurrent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := migrate(db, migrations[:1]); err != nil {
		db.Close()
		t.Fatalf("migrate to v1: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO events (event_key, source, source_id, fingerprint, status, event, expires_at,
			received_at, first_seen_at, last_seen_at, updated_at)
		VALUES ('v1src:1', 'v1src', '1', 'fp', 'active', 'Fire', '2026-07-01T10:00:00Z',
			'2026-05-01T10:00:00Z', '2026-05-01T10:00:00Z',
			'2026-05-02T10:00:00.123456789Z', '2026-05-02T10:00:00.123456789Z')`); err != nil {
		db.Close()
		t.Fatalf("insert v1 event: %v", err)
	}
	db.Close()

	store, info, err := Open(path)
	if err != nil {
		t.Fatalf("Open of real v1 database: %v", err)
	}
	defer store.Close()
	if info.From != 1 || info.To != 8 {
		t.Fatalf("migration = %+v, want {From:1 To:8}", info)
	}
	var expMs, seenMs int64
	if err := store.db.QueryRow("SELECT expires_at_ms, last_seen_at_ms FROM events WHERE event_key = 'v1src:1'").Scan(&expMs, &seenMs); err != nil {
		t.Fatalf("read backfilled columns: %v", err)
	}
	wantExp, _ := time.Parse(time.RFC3339Nano, "2026-07-01T10:00:00Z")
	wantSeen, _ := time.Parse(time.RFC3339Nano, "2026-05-02T10:00:00.123456789Z")
	if expMs != wantExp.UnixMilli() || seenMs != wantSeen.UnixMilli() {
		t.Errorf("backfill = (expires %d, last_seen %d), want (%d, %d)",
			expMs, seenMs, wantExp.UnixMilli(), wantSeen.UnixMilli())
	}
}

// TestMigrationV2InvalidExpiresAtFailsAndRollsBack: unparsable legacy
// expires_at must fail the v2 step and leave the schema untouched.
func TestMigrationV2InvalidExpiresAtFailsAndRollsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1bad.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := migrate(db, migrations[:1]); err != nil {
		db.Close()
		t.Fatalf("migrate to v1: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO events (event_key, source, source_id, fingerprint, status, event, expires_at,
			received_at, first_seen_at, last_seen_at, updated_at)
		VALUES ('bad:1', 'bad', '1', 'fp', 'active', 'E', 'garbage',
			'2026-05-01T10:00:00Z', '2026-05-01T10:00:00Z',
			'2026-05-01T10:00:00Z', '2026-05-01T10:00:00Z')`); err != nil {
		db.Close()
		t.Fatalf("insert invalid v1 event: %v", err)
	}
	db.Close()

	if _, _, err := Open(path); err == nil {
		t.Fatal("Open must fail on invalid legacy expires_at")
	}
	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	if got := schemaVersion(t, db); got != 1 {
		t.Errorf("schema version = %d after failed v2, want 1 (rolled back)", got)
	}
}

// TestMigrationLegacyCursorWithoutTypeResetsOnSync pins the pre-v4 cursor
// upgrade decision: a legacy cursor row has no durable type identity, so
// SyncOutputs treats it as a new consumer (cursor 0, conservative replay;
// possible duplicate delivery, never silent loss). A cursor that already
// carries the matching type is preserved.
func TestMigrationLegacyCursorWithoutTypeResetsOnSync(t *testing.T) {
	path := buildV3DB(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec("INSERT INTO output_cursors (output_id, last_acked_id) VALUES ('legacy', 7)"); err != nil {
		db.Close()
		t.Fatalf("insert legacy cursor: %v", err)
	}
	db.Close()

	store, info, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if info.To != 8 {
		t.Fatalf("migrated to %d, want 8", info.To)
	}

	ctx := context.Background()
	if err := store.SyncOutputs(ctx, []storage.OutputRef{{ID: "legacy", Type: "mqtt"}}); err != nil {
		t.Fatalf("SyncOutputs: %v", err)
	}
	var typ string
	var ack int64
	if err := store.db.QueryRow("SELECT output_type, last_acked_id FROM output_cursors WHERE output_id = 'legacy'").Scan(&typ, &ack); err != nil {
		t.Fatalf("read cursor: %v", err)
	}
	if typ != "mqtt" || ack != 0 {
		t.Errorf("cursor after sync = (%q, %d), want (mqtt, 0) — legacy cursor without type identity is a new consumer", typ, ack)
	}

	// Once the type identity exists, further syncs preserve progress.
	if _, err := store.db.Exec("UPDATE output_cursors SET last_acked_id = 3 WHERE output_id = 'legacy'"); err != nil {
		t.Fatalf("simulate progress: %v", err)
	}
	if err := store.SyncOutputs(ctx, []storage.OutputRef{{ID: "legacy", Type: "mqtt"}}); err != nil {
		t.Fatalf("second SyncOutputs: %v", err)
	}
	if err := store.db.QueryRow("SELECT last_acked_id FROM output_cursors WHERE output_id = 'legacy'").Scan(&ack); err != nil {
		t.Fatalf("read cursor: %v", err)
	}
	if ack != 3 {
		t.Errorf("cursor = %d after same-type sync, want 3 (preserved)", ack)
	}
}
