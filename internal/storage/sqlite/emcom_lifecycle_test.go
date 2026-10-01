package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestEmcomSaveRecordsLifecycleAtomically pins the P1 fix at the storage
// boundary: every EMCOM network save joins the lifecycle ledger in the
// same transaction (instance publisher, version from the database-owned
// counter), so a level drop back to monitoring blocks a previously
// queued activation — even after the radio returns — and a later
// re-activation never revives the stale job.
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
	vRaise, err := store.SaveEmcomNetwork(ctx, raised)
	if err != nil {
		t.Fatal(err)
	}
	// The queued activation (version = the raise) passes the gate.
	blocked, err := store.LifecycleBlocks(ctx, publisher, key, vRaise)
	if err != nil || blocked {
		t.Fatalf("LifecycleBlocks(raised) = (%v, %v), want allowed", blocked, err)
	}

	// Dropping to monitoring records the cancellation atomically.
	dropped := raised
	dropped.Level = 0
	dropped.UpdatedAt = base.Add(2 * time.Second)
	vDrop, err := store.SaveEmcomNetwork(ctx, dropped)
	if err != nil {
		t.Fatal(err)
	}
	// The previously queued activation is now blocked.
	blocked, err = store.LifecycleBlocks(ctx, publisher, key, vRaise)
	if err != nil || !blocked {
		t.Fatalf("LifecycleBlocks(old job after drop) = (%v, %v), want blocked", blocked, err)
	}
	// Even a job at exactly the drop version is blocked (terminal state).
	blocked, err = store.LifecycleBlocks(ctx, publisher, key, vDrop)
	if err != nil || !blocked {
		t.Fatalf("LifecycleBlocks(drop version) = (%v, %v), want blocked", blocked, err)
	}

	// A later re-activation does NOT revive the old job: its version is
	// still older than the current one.
	reactivated := dropped
	reactivated.Level = 1
	reactivated.UpdatedAt = base.Add(4 * time.Second)
	vReactivate, err := store.SaveEmcomNetwork(ctx, reactivated)
	if err != nil {
		t.Fatal(err)
	}
	blocked, err = store.LifecycleBlocks(ctx, publisher, key, vRaise)
	if err != nil || !blocked {
		t.Fatalf("LifecycleBlocks(old job after re-activation) = (%v, %v), want still blocked", blocked, err)
	}
	// The new activation itself is allowed.
	blocked, err = store.LifecycleBlocks(ctx, publisher, key, vReactivate)
	if err != nil || blocked {
		t.Fatalf("LifecycleBlocks(re-activation) = (%v, %v), want allowed", blocked, err)
	}
}

// TestEmcomVersionCounterIndependentOfClock pins the reported P1: the
// lifecycle version is a database-owned monotonic counter, NOT the wall
// clock — after a backward clock correction the level drop must still
// supersede (and block) the previously queued activation. With
// clock-derived versions the ledger guard rejected the "older" drop and
// the stale activation stayed deliverable.
func TestEmcomVersionCounterIndependentOfClock(t *testing.T) {
	store, _, err := Open(filepath.Join(t.TempDir(), "emcom-clock.db"))
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
	vRaise, err := store.SaveEmcomNetwork(ctx, raised)
	if err != nil {
		t.Fatal(err)
	}

	// The clock jumps BACKWARD: the drop carries an OLDER timestamp than
	// the raise. The counter version must still be strictly newer — the
	// cancellation must never be rejected as "older".
	dropped := raised
	dropped.Level = 0
	dropped.UpdatedAt = base.Add(-time.Hour)
	vDrop, err := store.SaveEmcomNetwork(ctx, dropped)
	if err != nil {
		t.Fatal(err)
	}
	if vDrop <= vRaise {
		t.Fatalf("counter version after the rollback = %d, want > %d (strictly increasing, clock-independent)", vDrop, vRaise)
	}

	// The network row reflects the drop…
	nets, err := store.EmcomNetworks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nets) != 1 || nets[0].Level != 0 {
		t.Fatalf("network after the rollback drop = %+v, want level 0", nets)
	}

	// …and the previously queued activation is BLOCKED.
	blocked, err := store.LifecycleBlocks(ctx, publisher, key, vRaise)
	if err != nil || !blocked {
		t.Fatalf("gate for the activation after the rollback drop = (%v, %v), want blocked (the clock rollback must not break the cancellation)", blocked, err)
	}
	// Exactly the drop version is terminal too.
	blocked, err = store.LifecycleBlocks(ctx, publisher, key, vDrop)
	if err != nil || !blocked {
		t.Fatalf("gate for the drop version = (%v, %v), want blocked", blocked, err)
	}
}

// TestEmcomTombstoneAtomic pins the reported P1 at the storage boundary:
// deleting a network commits the tombstone, the lifecycle record and the
// expiry transition's durable inbox row in ONE transaction — a crash
// between the deletion and the dispatch can never lose the cancellation
// (previously they were separate steps). The stored inbox row carries the
// allocated version as its ChangeID, identical to the live handoff.
func TestEmcomTombstoneAtomic(t *testing.T) {
	store, _, err := Open(filepath.Join(t.TempDir(), "emcom-tombstone.db"))
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

	raised := storage.EmcomNetwork{Slug: "sp9moa", Name: "SP9MOA", Level: 2, UpdatedAt: time.Now()}
	vRaise, err := store.SaveEmcomNetwork(ctx, raised)
	if err != nil {
		t.Fatal(err)
	}

	ev := dispatch.Event{
		Kind:   dispatch.EventHazardTransition,
		Origin: dispatch.Origin{Type: "web", ReceiverID: "emcom"},
		Hazard: &dispatch.HazardTransition{
			Type:      dispatch.TransitionExpired,
			Key:       key,
			Source:    "emcom",
			Publisher: publisher,
			Hazard:    dispatch.Hazard{EventKey: key},
		},
	}
	inboxID, vTomb, err := store.TombstoneEmcomNetwork(ctx, "sp9moa", ev)
	if err != nil {
		t.Fatal(err)
	}
	if vTomb <= vRaise {
		t.Fatalf("tombstone version = %d, want > %d", vTomb, vRaise)
	}
	if inboxID == 0 {
		t.Fatal("the tombstone committed no inbox row for the expiry transition — a crash after the deletion would lose the cancellation")
	}
	// The caller's event was stamped with the allocated version (the live
	// handoff reuses it, identical to the stored row).
	if ev.Hazard.ChangeID != vTomb {
		t.Fatalf("stamped ChangeID = %d, want the allocated version %d", ev.Hazard.ChangeID, vTomb)
	}

	// The network is tombstoned and the old activation is blocked.
	nets, err := store.EmcomNetworks(ctx)
	if err != nil || len(nets) != 1 || nets[0].Level != -1 {
		t.Fatalf("networks after the tombstone = (%v, %v), want one level -1 tombstone", nets, err)
	}
	blocked, err := store.LifecycleBlocks(ctx, publisher, key, vRaise)
	if err != nil || !blocked {
		t.Fatalf("gate for the activation after the tombstone = (%v, %v), want blocked", blocked, err)
	}

	// The durable inbox holds the expiry transition with the SAME
	// identity (publisher + allocated version).
	rows, err := store.PendingInboxEvents(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("pending inbox after the tombstone = (%v, %v), want 1 durable row", rows, err)
	}
	stored := rows[0].Event
	if stored.Hazard == nil || stored.Hazard.Type != dispatch.TransitionExpired || stored.Hazard.ChangeID != vTomb || stored.Hazard.Publisher != publisher {
		t.Fatalf("stored expiry = %+v, want the identical transition (version %d, publisher %q)", stored.Hazard, vTomb, publisher)
	}
}
