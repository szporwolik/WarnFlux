package sqlite

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
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
	if info.To != 4 {
		t.Fatalf("migrated to %d, want 4", info.To)
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
