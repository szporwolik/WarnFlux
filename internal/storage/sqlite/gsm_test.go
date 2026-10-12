package sqlite

import (
	"context"
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

// seedGSMRow inserts one history row directly, the way a pre-v58 process
// would have: the parts of a long message stored as separate entries.
func seedGSMRow(t *testing.T, store *Store, direction, from, to, text string, at time.Time) int64 {
	t.Helper()
	res, err := store.db.Exec(`
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
func gsmRowTexts(t *testing.T, store *Store) map[int64]string {
	t.Helper()
	rows, err := store.db.Query(`SELECT id, text FROM gsm_messages ORDER BY id`)
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

// runLegacyJoin applies the v58 step the way the migration runner does.
func runLegacyJoin(t *testing.T, store *Store) {
	t.Helper()
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := joinLegacyGSMParts(tx); err != nil {
		tx.Rollback()
		t.Fatalf("joinLegacyGSMParts: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// TestJoinLegacyGSMPartsIsConservative pins the fix on the stored
// history: independent messages from one sender within the same minute
// stay separate (the display used to fuse them), while a run whose parts
// are all full segments — the signature of a split message — is joined
// once, and two split messages delivered in the same minute stay two.
func TestJoinLegacyGSMPartsIsConservative(t *testing.T) {
	store := openGSMTestStore(t)
	// Every offset below stays inside one minute: a split message is not
	// joined across a minute boundary (one message, one delivery).
	base := time.Date(2026, 10, 8, 19, 7, 0, 0, time.UTC)

	// Independent messages inside one minute: the real case that exposed
	// the defect ("Test", "Gycji", "Yyy?", "Test diagnostyczny ok").
	independent := []string{"Test", "Gycji", "Yyy?", "Test diagnostyczny ok"}
	independentIDs := make([]int64, 0, len(independent))
	for i, text := range independent {
		independentIDs = append(independentIDs, seedGSMRow(t, store, "rx", "+48509558155", "self", text, base.Add(time.Duration(i)*time.Second)))
	}
	// One split message: two full UCS-2 segments (67 characters) and a
	// short tail, padded with a zero-width space like the real ones.
	full := strings.Repeat("a", 67)
	partIDs := []int64{
		seedGSMRow(t, store, "rx", "80766589", "self", "\u200b"+full, base),
		seedGSMRow(t, store, "rx", "80766589", "self", full, base.Add(time.Second)),
		seedGSMRow(t, store, "rx", "80766589", "self", "tail\u200b", base.Add(2*time.Second)),
	}
	// Two split messages delivered in the same minute.
	secondIDs := []int64{
		seedGSMRow(t, store, "rx", "8010897121", "self", full, base),
		seedGSMRow(t, store, "rx", "8010897121", "self", full, base.Add(10*time.Second)),
		seedGSMRow(t, store, "rx", "8010897121", "self", "first", base.Add(11*time.Second)),
		seedGSMRow(t, store, "rx", "8010897121", "self", full, base.Add(20*time.Second)),
		seedGSMRow(t, store, "rx", "8010897121", "self", "second", base.Add(21*time.Second)),
	}
	// A sent copy and a senderless row are never part of a received run.
	txID := seedGSMRow(t, store, "tx", "self", "+48509558155", full, base)
	emptyID := seedGSMRow(t, store, "rx", "", "self", full, base)

	runLegacyJoin(t, store)
	got := gsmRowTexts(t, store)

	for i, want := range independent {
		if got[independentIDs[i]] != want {
			t.Errorf("independent message %d = %q, want %q", independentIDs[i], got[independentIDs[i]], want)
		}
	}
	// The split message is one row now: the newest part keeps the joined
	// text (that is where it appeared in the newest-first list), the
	// earlier part rows are gone and the padding is stripped.
	if want := full + full + "tail"; got[partIDs[2]] != want {
		t.Errorf("joined message = %q, want %q", got[partIDs[2]], want)
	}
	for _, id := range partIDs[:2] {
		if _, ok := got[id]; ok {
			t.Errorf("part row %d survived the join", id)
		}
	}
	if want := full + full + "first"; got[secondIDs[2]] != want {
		t.Errorf("first same-minute message = %q, want %q", got[secondIDs[2]], want)
	}
	if want := full + "second"; got[secondIDs[4]] != want {
		t.Errorf("second same-minute message = %q, want %q", got[secondIDs[4]], want)
	}
	for _, id := range []int64{secondIDs[0], secondIDs[1], secondIDs[3]} {
		if _, ok := got[id]; ok {
			t.Errorf("row %d survived although its parts were joined", id)
		}
	}
	if got[txID] != full {
		t.Errorf("sent copy = %q, want it untouched", got[txID])
	}
	if got[emptyID] != full {
		t.Errorf("senderless row = %q, want it untouched", got[emptyID])
	}
	// 14 seeded rows: the three parts collapse to one and the five rows
	// of the two same-minute messages collapse to two.
	if want := 4 + 1 + 2 + 2; len(got) != want {
		t.Errorf("history has %d rows, want %d", len(got), want)
	}
}

// TestJoinLegacyGSMPartsIsIdempotent pins that a second pass changes
// nothing: a joined row no longer looks like a part.
func TestJoinLegacyGSMPartsIsIdempotent(t *testing.T) {
	store := openGSMTestStore(t)
	at := time.Date(2026, 10, 8, 19, 58, 4, 0, time.UTC)
	full := strings.Repeat("x", 67)
	seedGSMRow(t, store, "rx", "80766589", "self", full, at)
	seedGSMRow(t, store, "rx", "80766589", "self", "tail", at.Add(time.Second))

	runLegacyJoin(t, store)
	first := gsmRowTexts(t, store)
	runLegacyJoin(t, store)
	second := gsmRowTexts(t, store)

	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("rows after joins = %d then %d, want 1 and 1", len(first), len(second))
	}
	for id, text := range first {
		if second[id] != text {
			t.Fatalf("second pass changed row %d: %q -> %q", id, text, second[id])
		}
	}
}
