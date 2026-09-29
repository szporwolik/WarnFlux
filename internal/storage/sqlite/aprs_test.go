package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

func openAPRSTestStore(t *testing.T) *Store {
	t.Helper()
	s, _, err := Open(filepath.Join(t.TempDir(), "aprs.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestAPRSMessagesHistory(t *testing.T) {
	s := openAPRSTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)

	for i := 0; i < 5; i++ {
		if err := s.RecordAPRSMessage(ctx, "rx", "SP9SPM", "SP9SPM-10", "msg", "", "aprs-radio", base.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("record rx %d: %v", i, err)
		}
	}
	if err := s.RecordAPRSMessage(ctx, "tx", "SP9SPM-10", "SP9WSS-2", "odpowiedz", "abc", "aprs-inet", base.Add(10*time.Minute)); err != nil {
		t.Fatalf("record tx: %v", err)
	}

	if n, err := s.CountAPRSMessages(ctx, ""); err != nil || n != 6 {
		t.Fatalf("count all = %d, %v; want 6", n, err)
	}
	if n, err := s.CountAPRSMessages(ctx, "rx"); err != nil || n != 5 {
		t.Fatalf("count rx = %d, %v; want 5", n, err)
	}

	rows, err := s.ListAPRSMessages(ctx, "", 3, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 3 || rows[0].Direction != "tx" {
		t.Fatalf("newest first = %+v, want tx first", rows)
	}

	rx, err := s.ListAPRSMessages(ctx, "rx", 10, 0)
	if err != nil || len(rx) != 5 {
		t.Fatalf("list rx = %d, %v; want 5", len(rx), err)
	}
	for _, m := range rx {
		if m.Direction != "rx" {
			t.Fatalf("rx filter leaked %+v", m)
		}
	}

	// Prune keeps the newest N.
	if _, err := s.PruneAPRSMessages(ctx, 2); err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n, err := s.CountAPRSMessages(ctx, ""); err != nil || n != 2 {
		t.Fatalf("count after prune = %d, %v; want 2", n, err)
	}
}

func TestAPRSMessageRetentionOnInsert(t *testing.T) {
	s := openAPRSTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)

	for i := 0; i < storage.APRSMessageRetentionEntries+10; i++ {
		if err := s.RecordAPRSMessage(ctx, "rx", "CALL", "SP9SPM-10", "x", "", "", base.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	if n, err := s.CountAPRSMessages(ctx, ""); err != nil || n != storage.APRSMessageRetentionEntries {
		t.Fatalf("count = %d, %v; want %d", n, err, storage.APRSMessageRetentionEntries)
	}
}
