package sqlite

import (
	"database/sql"
	"path/filepath"
	"testing"

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
