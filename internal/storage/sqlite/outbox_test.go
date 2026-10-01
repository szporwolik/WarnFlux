package sqlite

import (
	"context"
	"path/filepath"
	"testing"
)

// TestIngestOutboxLifecycle pins the durable HTTP-ingest outbox: rows are
// appended per instance, listed in insertion order, invisible to other
// instances, survive a restart and are deleted only on acknowledgement.
func TestIngestOutboxLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	store, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	id, err := store.AppendOutbox(ctx, "news", "warnflux/events", []byte(`{"x":1}`))
	if err != nil || id == 0 {
		t.Fatalf("AppendOutbox = (%d, %v)", id, err)
	}
	if n, err := store.OutboxCount(ctx); err != nil || n != 1 {
		t.Fatalf("OutboxCount = (%d, %v), want 1", n, err)
	}

	items, err := store.PendingOutbox(ctx, "news", 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("PendingOutbox = (%v, %v), want 1 row", items, err)
	}
	if items[0].ID != id || items[0].Topic != "warnflux/events" || string(items[0].Payload) != `{"x":1}` {
		t.Errorf("row = %+v, want id %d topic warnflux/events payload {\"x\":1}", items[0], id)
	}

	// A different instance never sees another instance's rows.
	if items, err := store.PendingOutbox(ctx, "other", 10); err != nil || len(items) != 0 {
		t.Fatalf("other instance rows = (%v, %v), want none", items, err)
	}

	// Rows survive a restart (the durable broker-sync backlog).
	store.Close()
	restarted, _, err := Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if n, err := restarted.OutboxCount(ctx); err != nil || n != 1 {
		t.Fatalf("OutboxCount after restart = (%d, %v), want 1", n, err)
	}

	if err := restarted.AckOutbox(ctx, id); err != nil {
		t.Fatalf("AckOutbox: %v", err)
	}
	if n, err := restarted.OutboxCount(ctx); err != nil || n != 0 {
		t.Fatalf("OutboxCount after ack = (%d, %v), want 0", n, err)
	}
}
