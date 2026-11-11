package sqlite

import (
	"context"
	"path/filepath"
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

	if err := store.MarkMeshOutboxAttempt(ctx, id, "device not connected", at); err != nil {
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
	recent, err := store.RecentFinishedMeshOutbox(ctx, 10)
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
		if err := store.MarkMeshOutboxAttempt(ctx, id2, "boom", at); err != nil {
			t.Fatalf("MarkMeshOutboxAttempt: %v", err)
		}
	}
	if pending, _ = store.PendingMeshOutbox(ctx, 10); len(pending) != 0 {
		t.Errorf("an exhausted message is still pending: %+v", pending)
	}

	// A finished row is history: cancelling it is not a pending cancel.
	if removed, err := store.DeletePendingMeshOutbox(ctx, id2); err != nil || removed {
		t.Fatalf("DeletePendingMeshOutbox(finished) = %v, %v", removed, err)
	}
	if recent, _ = store.RecentFinishedMeshOutbox(ctx, 10); len(recent) != 2 {
		t.Errorf("recent = %d rows, want the delivered row and the abandoned one", len(recent))
	}

	// Retention counts from the finish time, so a row that waited a day and was
	// delivered now still has its whole history window (WF-R4-01).
	if err := store.PruneMeshOutbox(ctx, at); err != nil {
		t.Fatalf("PruneMeshOutbox: %v", err)
	}
	if recent, _ = store.RecentFinishedMeshOutbox(ctx, 10); len(recent) != 2 {
		t.Errorf("prune took a freshly finished row: %d rows left", len(recent))
	}
	if err := store.PruneMeshOutbox(ctx, at.Add(2*time.Hour)); err != nil {
		t.Fatalf("PruneMeshOutbox: %v", err)
	}
	if recent, _ = store.RecentFinishedMeshOutbox(ctx, 10); len(recent) != 0 {
		t.Errorf("prune left %d rows past the window", len(recent))
	}
}

// TestMigrationFinishesOldAbandonedRows is WF-R5-03: a database written before
// the claim/finish state machine can hold a row that had already used every
// attempt. v73 only backfilled the delivered rows, and the retention removes
// finished rows only — so that row could never be pruned. The repair migration
// finishes it at migration time, leaves real pending rows alone, and keeps the
// delivery time of the sent ones.
func TestMigrationFinishesOldAbandonedRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "upgrade.db")

	// A v72 database: no claimed_ms / finished_ms columns at all.
	old, _, err := Open(path, WithMigrationCeiling(72))
	if err != nil {
		t.Fatalf("Open(v72): %v", err)
	}
	now := time.Now()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := old.db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	exec(`INSERT INTO mesh_outbox(kind, target_id, text, created_ms, attempts)
	      VALUES('dm', '12345678', 'porzucona', ?, 10)`, now.Add(-48*time.Hour).UnixMilli())
	exec(`INSERT INTO mesh_outbox(kind, target_id, text, created_ms, attempts)
	      VALUES('dm', '12345678', 'czeka', ?, 2)`, now.Add(-48*time.Hour).UnixMilli())
	exec(`INSERT INTO mesh_outbox(kind, target_id, text, created_ms, attempts, sent_ms)
	      VALUES('dm', '12345678', 'wyslana', ?, 1, ?)`,
		now.Add(-48*time.Hour).UnixMilli(), now.Add(-47*time.Hour).UnixMilli())
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	// Upgrade to the head schema.
	store, info, err := Open(path)
	if err != nil {
		t.Fatalf("Open(head): %v", err)
	}
	defer store.Close()
	if info.To != len(migrations) {
		t.Fatalf("migrated to %d, want %d", info.To, len(migrations))
	}
	ctx := context.Background()

	// The delivered row keeps its own delivery time.
	recent, err := store.RecentFinishedMeshOutbox(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	var delivered storage.MeshOutboxItem
	for _, item := range recent {
		if item.Text == "wyslana" {
			delivered = item
		}
	}
	if delivered.FinishedAt.IsZero() || !delivered.FinishedAt.Equal(delivered.SentAt) {
		t.Errorf("delivered row finish = %v, sent = %v; want them equal", delivered.FinishedAt, delivered.SentAt)
	}

	// The abandoned row is finished now and prunes away; the pending one stays
	// queued whatever the cutoff.
	if err := store.PruneMeshOutbox(ctx, time.Now().Add(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingMeshOutbox(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Text != "czeka" {
		t.Fatalf("pending = %+v, want only the queued message", pending)
	}
	// The whole table, not just the history view: the point is that the row is
	// GONE, and v73 alone would have left it unfinishable and unprunable.
	var total, abandoned int
	if err := store.db.QueryRowContext(ctx,
		`SELECT count(*) FROM mesh_outbox`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(ctx,
		`SELECT count(*) FROM mesh_outbox WHERE text = 'porzucona'`).Scan(&abandoned); err != nil {
		t.Fatal(err)
	}
	if abandoned != 0 {
		t.Errorf("an abandoned row survived the prune (%d rows left in total)", total)
	}
	// The cutoff is in the future, so both finished rows go — and the queued
	// message, which has no finish time, stays exactly where it is.
	if total != 1 {
		t.Errorf("table holds %d rows after the prune, want only the queued message", total)
	}
}
