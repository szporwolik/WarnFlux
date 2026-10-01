package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestOutputPendingDeletesLifecycle pins the durable unresolved-deletion
// mirror: SavePendingDeletes replaces the set atomically, Load returns it
// in stable key order, per-output sets never leak across outputs and the
// rows survive a restart.
func TestOutputPendingDeletesLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deletes.db")
	store, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	set := []storage.PendingDelete{
		{Key: "imgw:a", Topic: "warnflux/active/imgw/hash-a"},
		{Key: "rso:b", Topic: "warnflux/active/rso/hash-b"},
	}
	if err := store.SavePendingDeletes(ctx, "mqtt-local", set); err != nil {
		t.Fatal(err)
	}
	got, err := store.LoadPendingDeletes(ctx, "mqtt-local")
	if err != nil {
		t.Fatal(err)
	}
	// Stable key order.
	if len(got) != 2 || got[0].Key != "imgw:a" || got[1].Key != "rso:b" ||
		got[0].Topic != "warnflux/active/imgw/hash-a" {
		t.Fatalf("loaded deletes = %+v, want the saved set in key order", got)
	}
	// Another output's set is independent.
	if got, err := store.LoadPendingDeletes(ctx, "other"); err != nil || len(got) != 0 {
		t.Fatalf("other output rows = (%v, %v), want none", got, err)
	}

	// Replace-all: the confirmed deletion disappears.
	if err := store.SavePendingDeletes(ctx, "mqtt-local", []storage.PendingDelete{set[1]}); err != nil {
		t.Fatal(err)
	}
	got, err = store.LoadPendingDeletes(ctx, "mqtt-local")
	if err != nil || len(got) != 1 || got[0].Key != "rso:b" {
		t.Fatalf("loaded deletes after replace = (%v, %v), want only rso:b", got, err)
	}

	// Restart persistence: a fresh handle sees the same rows.
	store.Close()
	restarted, _, err := Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	got, err = restarted.LoadPendingDeletes(ctx, "mqtt-local")
	if err != nil || len(got) != 1 || got[0].Key != "rso:b" {
		t.Fatalf("loaded deletes after restart = (%v, %v), want only rso:b", got, err)
	}

	// An empty snapshot clears the set.
	if err := restarted.SavePendingDeletes(ctx, "mqtt-local", nil); err != nil {
		t.Fatal(err)
	}
	got, err = restarted.LoadPendingDeletes(ctx, "mqtt-local")
	if err != nil || len(got) != 0 {
		t.Fatalf("loaded deletes after clear = (%v, %v), want none", got, err)
	}
}
