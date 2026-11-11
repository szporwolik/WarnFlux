package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestMeshOutboxRoundTrip pins the queue the panel shows: a message waits, a
// failed attempt is recorded with its reason, a delivered one leaves the pending
// set, and an abandoned row is no longer offered to the dispatcher.
func TestMeshOutboxRoundTrip(t *testing.T) {
	store := newUsersStore(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 11, 20, 15, 0, 0, time.UTC)

	id, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{
		Kind: "dm", TargetID: "1ba292c8", Label: "SP9MOA", Text: "test", Operator: "admin", CreatedAt: at,
	})
	if err != nil {
		t.Fatalf("EnqueueMeshOutbox: %v", err)
	}
	pending, err := store.PendingMeshOutbox(ctx, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending = %v, %v", pending, err)
	}
	if pending[0].Text != "test" || pending[0].TargetID != "1ba292c8" || !pending[0].SentAt.IsZero() {
		t.Errorf("pending row = %+v", pending[0])
	}

	if err := store.MarkMeshOutboxAttempt(ctx, id, "device not connected"); err != nil {
		t.Fatalf("MarkMeshOutboxAttempt: %v", err)
	}
	pending, _ = store.PendingMeshOutbox(ctx, 10)
	if len(pending) != 1 || pending[0].Attempts != 1 || pending[0].LastError != "device not connected" {
		t.Fatalf("after a failed attempt: %+v", pending)
	}

	if err := store.MarkMeshOutboxSent(ctx, id, at.Add(time.Minute)); err != nil {
		t.Fatalf("MarkMeshOutboxSent: %v", err)
	}
	if pending, _ = store.PendingMeshOutbox(ctx, 10); len(pending) != 0 {
		t.Errorf("a delivered message is still pending: %+v", pending)
	}
	recent, err := store.RecentMeshOutbox(ctx, 10)
	if err != nil || len(recent) != 1 || recent[0].SentAt.IsZero() {
		t.Fatalf("recent = %+v, %v", recent, err)
	}

	// An abandoned row (all attempts spent) is not offered again.
	id2, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{
		Kind: "channel", ChannelIdx: 1, Label: "ch1", Text: "porzucona", CreatedAt: at,
	})
	if err != nil {
		t.Fatalf("EnqueueMeshOutbox: %v", err)
	}
	for i := 0; i < storage.MeshOutboxMaxAttempts; i++ {
		if err := store.MarkMeshOutboxAttempt(ctx, id2, "boom"); err != nil {
			t.Fatalf("MarkMeshOutboxAttempt: %v", err)
		}
	}
	if pending, _ = store.PendingMeshOutbox(ctx, 10); len(pending) != 0 {
		t.Errorf("an exhausted message is still pending: %+v", pending)
	}

	if err := store.DeleteMeshOutbox(ctx, id2); err != nil {
		t.Fatalf("DeleteMeshOutbox: %v", err)
	}
	if recent, _ = store.RecentMeshOutbox(ctx, 10); len(recent) != 1 {
		t.Errorf("recent after delete = %d rows, want 1", len(recent))
	}

	// Pruning drops rows older than the retention window.
	if err := store.PruneMeshOutbox(ctx, at.Add(time.Hour)); err != nil {
		t.Fatalf("PruneMeshOutbox: %v", err)
	}
	if recent, _ = store.RecentMeshOutbox(ctx, 10); len(recent) != 0 {
		t.Errorf("prune left %d rows", len(recent))
	}
}
