package sqlite

import (
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

func TestAuditPersistsAndLists(t *testing.T) {
	store := newUsersStore(t)

	now := time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)
	if err := store.RecordAudit("admin", "login", "role=admin", now); err != nil {
		t.Fatalf("RecordAudit: %v", err)
	}
	if err := store.RecordAudit("admin", "user-create", "ops", now.Add(time.Minute)); err != nil {
		t.Fatalf("RecordAudit: %v", err)
	}

	entries, err := store.ListAudit(0, 100)
	if err != nil {
		t.Fatalf("ListAudit: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}
	if entries[0].Action != "login" || entries[0].User != "admin" {
		t.Errorf("entry 0 = %+v", entries[0])
	}
	if entries[1].Detail != "ops" || entries[1].At != "2026-09-24T10:01:00Z" {
		t.Errorf("entry 1 = %+v", entries[1])
	}

	// Incremental cursor: only entries after the first seq.
	cursor := entries[0].Seq
	rest, err := store.ListAudit(cursor, 100)
	if err != nil || len(rest) != 1 || rest[0].Seq <= cursor {
		t.Fatalf("incremental list = %v, %v", rest, err)
	}
}

func TestAuditPrunesToRetention(t *testing.T) {
	store := newUsersStore(t)

	const n = storage.AuditRetentionEntries + 250
	base := time.Now().UTC()
	for i := 0; i < n; i++ {
		if err := store.RecordAudit("admin", "test", "", base.Add(time.Duration(i)*time.Millisecond)); err != nil {
			t.Fatalf("RecordAudit %d: %v", i, err)
		}
	}
	entries, err := store.ListAudit(0, 0)
	if err != nil {
		t.Fatalf("ListAudit: %v", err)
	}
	if len(entries) != storage.AuditRetentionEntries {
		t.Fatalf("retained = %d, want %d", len(entries), storage.AuditRetentionEntries)
	}
	// The newest entries survive.
	if entries[0].Seq != int64(n-storage.AuditRetentionEntries+1) {
		t.Errorf("oldest retained seq = %d", entries[0].Seq)
	}
}
