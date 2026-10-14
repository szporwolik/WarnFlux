package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

func openGSMTestStore(t *testing.T) *Store {
	t.Helper()
	s, _, err := Open(t.TempDir() + "/gsm.db")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestGSMMessagesHistory(t *testing.T) {
	s := openGSMTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)

	for i := 0; i < 5; i++ {
		if err := s.RecordGSMMessage(ctx, "rx", "+48600111222", "self", "wiadomosc", base.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("record rx %d: %v", i, err)
		}
	}
	if err := s.RecordGSMMessage(ctx, "tx", "self", "+48600999888", "odpowiedz", base.Add(10*time.Minute)); err != nil {
		t.Fatalf("record tx: %v", err)
	}

	if n, err := s.CountGSMMessages(ctx, ""); err != nil || n != 6 {
		t.Fatalf("count all = %d, %v; want 6", n, err)
	}
	if n, err := s.CountGSMMessages(ctx, "rx"); err != nil || n != 5 {
		t.Fatalf("count rx = %d, %v; want 5", n, err)
	}

	rows, err := s.ListGSMMessages(ctx, "", 3, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 3 || rows[0].Direction != "tx" {
		t.Fatalf("newest first = %+v, want tx first", rows)
	}
	if rows[0].From != "self" || rows[0].To != "+48600999888" || rows[0].Text != "odpowiedz" {
		t.Fatalf("tx row = %+v", rows[0])
	}

	rx, err := s.ListGSMMessages(ctx, "rx", 10, 0)
	if err != nil || len(rx) != 5 {
		t.Fatalf("list rx = %d, %v; want 5", len(rx), err)
	}
	for _, m := range rx {
		if m.Direction != "rx" {
			t.Fatalf("rx filter leaked %+v", m)
		}
	}

	// Invalid direction is rejected.
	if err := s.RecordGSMMessage(ctx, "xx", "a", "b", "c", base); err == nil {
		t.Fatalf("invalid direction accepted")
	}

	// Prune keeps the newest N.
	if _, err := s.PruneGSMMessages(ctx, 2); err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n, err := s.CountGSMMessages(ctx, ""); err != nil || n != 2 {
		t.Fatalf("count after prune = %d, %v; want 2", n, err)
	}
}

// TestGSMMessagesAgeRetention pins the 365-day bound: rows older than
// the retention age are deleted, newer rows survive.
func TestGSMMessagesAgeRetention(t *testing.T) {
	s := openGSMTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

	old := base.Add(-400 * 24 * time.Hour)
	recent := base.Add(-100 * 24 * time.Hour)
	if err := s.RecordGSMMessage(ctx, "rx", "+48600111222", "self", "stary", old); err != nil {
		t.Fatalf("record old: %v", err)
	}
	if err := s.RecordGSMMessage(ctx, "rx", "+48600111222", "self", "swiezy", recent); err != nil {
		t.Fatalf("record recent: %v", err)
	}

	n, err := s.PruneGSMMessagesOlderThan(ctx, base.Add(-storage.GSMMessageRetentionAge))
	if err != nil || n != 1 {
		t.Fatalf("prune by age = %d, %v; want 1", n, err)
	}
	if got, err := s.CountGSMMessages(ctx, ""); err != nil || got != 1 {
		t.Fatalf("count after age prune = %d, %v; want 1", got, err)
	}
	rows, err := s.ListGSMMessages(ctx, "", 10, 0)
	if err != nil || len(rows) != 1 || rows[0].Text != "swiezy" {
		t.Fatalf("survivors = %+v, %v; want the recent row only", rows, err)
	}
}

// TestGSMMessageAgePruneOnInsert pins the insert-time age bound: a fresh
// insert deletes rows past the 365-day cutoff even though the
// entry-count cap never engaged.
func TestGSMMessageAgePruneOnInsert(t *testing.T) {
	s := openGSMTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

	if err := s.RecordGSMMessage(ctx, "rx", "+48600111222", "self", "zabytek", now.Add(-400*24*time.Hour)); err != nil {
		t.Fatalf("record old: %v", err)
	}
	if err := s.RecordGSMMessage(ctx, "rx", "+48600999888", "self", "nowosc", now); err != nil {
		t.Fatalf("record new: %v", err)
	}
	if n, err := s.CountGSMMessages(ctx, ""); err != nil || n != 1 {
		t.Fatalf("count = %d, %v; want 1 (the old row evicted on insert)", n, err)
	}
	rows, err := s.ListGSMMessages(ctx, "", 10, 0)
	if err != nil || len(rows) != 1 || rows[0].Text != "nowosc" {
		t.Fatalf("rows = %+v, %v; want only the fresh message", rows, err)
	}
}

// TestGSMMessageRetentionOnInsert pins the per-insert prune to the
// retention bound: one insert beyond the bound evicts the oldest row.
func TestGSMMessageRetentionOnInsert(t *testing.T) {
	s := openGSMTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

	bound := storage.GSMMessageRetentionEntries
	for i := 0; i < bound+3; i++ {
		if err := s.RecordGSMMessage(ctx, "rx", "+48600111222", "self", "x", base.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	if n, err := s.CountGSMMessages(ctx, ""); err != nil || n != bound {
		t.Fatalf("count = %d, %v; want %d", n, err, bound)
	}
	rows, err := s.ListGSMMessages(ctx, "", bound+10, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	// The oldest three (i=0..2) must be gone: the newest row is i=bound+2.
	oldest := rows[len(rows)-1].At
	if !oldest.After(base.Add(2 * time.Second)) {
		t.Fatalf("oldest row at %v, want later than i=2", oldest)
	}
}

// seedGSMRow inserts one history row directly, the way the text-mode
// ingest stores a message: one row per delivered message.
func seedGSMRow(t *testing.T, db *sql.DB, direction, from, to, text string, at time.Time) int64 {
	t.Helper()
	res, err := db.Exec(`
		INSERT INTO gsm_messages (direction, from_number, to_number, text, created_at_ms)
		VALUES (?, ?, ?, ?, ?)`, direction, from, to, text, at.UnixMilli())
	if err != nil {
		t.Fatalf("seed gsm row: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("seed gsm row id: %v", err)
	}
	return id
}

// gsmRowTexts returns the history as id -> text, oldest first.
func gsmRowTexts(t *testing.T, db *sql.DB) map[int64]string {
	t.Helper()
	rows, err := db.Query(`SELECT id, text FROM gsm_messages ORDER BY id`)
	if err != nil {
		t.Fatalf("read gsm rows: %v", err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var text string
		if err := rows.Scan(&id, &text); err != nil {
			t.Fatalf("scan gsm row: %v", err)
		}
		out[id] = text
	}
	return out
}

// TestGSMHistorySurvivesSchemaUpgrade pins the reported P1: the schema
// upgrade must never rewrite the SMS history. The v58 step used to join
// rows that LOOK like the parts of a split message (same sender, same
// minute, every part but the last of full segment length) and delete the
// originals — but the stored rows carry no segmentation metadata, so two
// independent messages from one sender inside one minute look exactly
// like that. Nothing about the history is touched on upgrade now.
func TestGSMHistorySurvivesSchemaUpgrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	// Build a GENUINE v57 database (the schema as it was), not a current
	// one with a rewound version counter: a step that changes the schema
	// (v60 adds a column) cannot re-run against a schema that has it.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := migrate(db, migrations[:57]); err != nil {
		db.Close()
		t.Fatalf("migrate to v57: %v", err)
	}
	base := time.Date(2026, 10, 8, 19, 7, 0, 0, time.UTC)
	full := strings.Repeat("a", 67)

	// Two independent short messages and two exactly-full-segment
	// messages, all from one sender inside the same minute.
	seedGSMRow(t, db, "rx", "80766589", "self", "Test", base)
	seedGSMRow(t, db, "rx", "80766589", "self", "Yyy?", base.Add(time.Second))
	seedGSMRow(t, db, "rx", "80766589", "self", full, base.Add(2*time.Second))
	seedGSMRow(t, db, "rx", "80766589", "self", "tail", base.Add(3*time.Second))
	seedGSMRow(t, db, "rx", "step", "self", full, base.Add(time.Second))
	// A sent copy, which no rule ever merged.
	seedGSMRow(t, db, "tx", "self", "+48509558155", full, base)
	// A split message as the modem actually delivers it: the parts carry
	// no header at all, so they stay separate rows.
	seedGSMRow(t, db, "rx", "8010897121", "self", full, base.Add(time.Minute))
	seedGSMRow(t, db, "rx", "8010897121", "self", "end", base.Add(time.Minute+time.Second))

	before := gsmRowTexts(t, db)
	if len(before) != 8 {
		t.Fatalf("seeded %d rows, want 8", len(before))
	}
	// Reopen without the version counter being touched: this is the
	// upgrade path an existing database takes.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	upgraded, info, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer upgraded.Close()
	if info.From != 57 || info.To < 58 {
		t.Fatalf("migration %d -> %d, want the upgrade to run from 57", info.From, info.To)
	}
	after := gsmRowTexts(t, upgraded.db)
	if len(after) != len(before) {
		t.Fatalf("history has %d rows after the upgrade, want %d (rows were joined)", len(after), len(before))
	}
	for id, text := range before {
		if after[id] != text {
			t.Errorf("row %d = %q after the upgrade, want %q untouched", id, after[id], text)
		}
	}
}
