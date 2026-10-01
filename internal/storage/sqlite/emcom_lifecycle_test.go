package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestEmcomSaveRecordsLifecycleAtomically pins the P1 fix at the storage
// boundary: every EMCOM network save joins the lifecycle ledger in the
// same transaction (instance publisher, version = updated_at_ms), so a
// level drop back to monitoring blocks a previously queued activation —
// even after the radio returns — and a later re-activation never revives
// the stale job.
func TestEmcomSaveRecordsLifecycleAtomically(t *testing.T) {
	store, _, err := Open(filepath.Join(t.TempDir(), "emcom.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	publisher, err := store.InstanceID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := "emcom:sp9moa"

	base := time.Now()
	raised := storage.EmcomNetwork{Slug: "sp9moa", Name: "SP9MOA", Level: 2, UpdatedAt: base}
	if err := store.SaveEmcomNetwork(ctx, raised); err != nil {
		t.Fatal(err)
	}
	// The queued activation (version = the raise) passes the gate.
	blocked, err := store.LifecycleBlocks(ctx, publisher, key, base.UnixMilli())
	if err != nil || blocked {
		t.Fatalf("LifecycleBlocks(raised) = (%v, %v), want allowed", blocked, err)
	}

	// Dropping to monitoring records the cancellation atomically.
	dropped := raised
	dropped.Level = 0
	dropped.UpdatedAt = base.Add(2 * time.Second)
	if err := store.SaveEmcomNetwork(ctx, dropped); err != nil {
		t.Fatal(err)
	}
	// The previously queued activation is now blocked.
	blocked, err = store.LifecycleBlocks(ctx, publisher, key, base.UnixMilli())
	if err != nil || !blocked {
		t.Fatalf("LifecycleBlocks(old job after drop) = (%v, %v), want blocked", blocked, err)
	}
	// Even a job at exactly the drop version is blocked (terminal state).
	blocked, err = store.LifecycleBlocks(ctx, publisher, key, dropped.UpdatedAt.UnixMilli())
	if err != nil || !blocked {
		t.Fatalf("LifecycleBlocks(drop version) = (%v, %v), want blocked", blocked, err)
	}

	// A later re-activation does NOT revive the old job: its version is
	// still older than the current one.
	reactivated := dropped
	reactivated.Level = 1
	reactivated.UpdatedAt = base.Add(4 * time.Second)
	if err := store.SaveEmcomNetwork(ctx, reactivated); err != nil {
		t.Fatal(err)
	}
	blocked, err = store.LifecycleBlocks(ctx, publisher, key, base.UnixMilli())
	if err != nil || !blocked {
		t.Fatalf("LifecycleBlocks(old job after re-activation) = (%v, %v), want still blocked", blocked, err)
	}
	// The new activation itself is allowed.
	blocked, err = store.LifecycleBlocks(ctx, publisher, key, reactivated.UpdatedAt.UnixMilli())
	if err != nil || blocked {
		t.Fatalf("LifecycleBlocks(re-activation) = (%v, %v), want allowed", blocked, err)
	}
}
