package sqlite

import (
	"context"
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
