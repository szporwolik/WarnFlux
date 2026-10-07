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

// TestAPRSMessageStatusRoundTrip pins the ack-status column (migration
// v48): tx rows start empty, UpdateAPRSMessageStatus flips the matching
// row by msg id and only for the tx direction, and the list carries the
// status back to the admin view.
func TestAPRSMessageStatusRoundTrip(t *testing.T) {
	s := openAPRSTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)

	if err := s.RecordAPRSMessage(ctx, "tx", "SP9SPM-10", "SP9WSS-2", "wiatr", "12345", "aprs-radio", base); err != nil {
		t.Fatalf("record tx: %v", err)
	}
	rows, err := s.ListAPRSMessages(ctx, "tx", 10, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list = %d, %v; want 1 tx row", len(rows), err)
	}
	if rows[0].Status != "" {
		t.Fatalf("fresh tx status = %q, want empty", rows[0].Status)
	}

	if err := s.UpdateAPRSMessageStatus(ctx, "12345", "delivered", base.Add(time.Minute)); err != nil {
		t.Fatalf("update status: %v", err)
	}
	rows, err = s.ListAPRSMessages(ctx, "tx", 10, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list after update = %d, %v", len(rows), err)
	}
	if rows[0].Status != "delivered" {
		t.Fatalf("tx status = %q, want delivered", rows[0].Status)
	}

	// A rej overwrites the outcome, and a no_ack marks an unanswered send.
	if err := s.UpdateAPRSMessageStatus(ctx, "12345", "failed", base.Add(2*time.Minute)); err != nil {
		t.Fatalf("update to failed: %v", err)
	}
	rows, _ = s.ListAPRSMessages(ctx, "tx", 10, 0)
	if rows[0].Status != "failed" {
		t.Fatalf("tx status = %q, want failed", rows[0].Status)
	}
	if err := s.UpdateAPRSMessageStatus(ctx, "12345", "no_ack", base.Add(3*time.Minute)); err != nil {
		t.Fatalf("update to no_ack: %v", err)
	}
	rows, _ = s.ListAPRSMessages(ctx, "tx", 10, 0)
	if rows[0].Status != "no_ack" {
		t.Fatalf("tx status = %q, want no_ack", rows[0].Status)
	}

	// The addressee lookup binds late acks: found for the tx row,
	// absent for unknown ids and never for rx rows.
	to, found, err := s.APRSMessageAddressee(ctx, "12345")
	if err != nil || !found || to != "SP9WSS-2" {
		t.Fatalf("addressee = %q, %v, %v; want SP9WSS-2", to, found, err)
	}
	if _, found, err := s.APRSMessageAddressee(ctx, "99999"); err != nil || found {
		t.Fatalf("unknown addressee = found %v, err %v; want absent", found, err)
	}

	// Unknown ids are silent no-ops, and rx rows are never touched.
	if err := s.UpdateAPRSMessageStatus(ctx, "99999", "delivered", base); err != nil {
		t.Fatalf("unknown id update: %v", err)
	}
	if err := s.UpdateAPRSMessageStatus(ctx, "", "delivered", base); err != nil {
		t.Fatalf("empty id update: %v", err)
	}
	if err := s.RecordAPRSMessage(ctx, "rx", "SP9WSS-2", "SP9SPM-10", "ok", "12345", "aprs-radio", base.Add(3*time.Minute)); err != nil {
		t.Fatalf("record rx: %v", err)
	}
	if err := s.UpdateAPRSMessageStatus(ctx, "12345", "delivered", base); err != nil {
		t.Fatalf("rx id update: %v", err)
	}
	rx, _ := s.ListAPRSMessages(ctx, "rx", 10, 0)
	if rx[0].Status != "" {
		t.Fatalf("rx status = %q, want untouched", rx[0].Status)
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
