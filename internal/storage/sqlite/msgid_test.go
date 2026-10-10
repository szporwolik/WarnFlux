package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// TestMessageIDRoundTripAndImmutability pins the durable assignment: the
// first write wins, so a replay never renumbers an already-cited event.
func TestMessageIDRoundTripAndImmutability(t *testing.T) {
	store := newUsersStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 7, 19, 58, 0, 0, time.UTC)

	if _, ok, err := store.LoadMessageID(ctx, "imgw-meteo:1"); err != nil || ok {
		t.Fatalf("unexpected pre-existing id: ok=%v err=%v", ok, err)
	}
	first := core.MessageIDAssignment{
		EventKey: "imgw-meteo:1", MsgID: "WF-1007195801",
		Minute: "20261007-1958", Seq: 1, CreatedAt: now,
	}
	if err := store.SaveMessageID(ctx, first); err != nil {
		t.Fatalf("SaveMessageID: %v", err)
	}
	// A later, different assignment for the same key must be ignored.
	if err := store.SaveMessageID(ctx, core.MessageIDAssignment{
		EventKey: "imgw-meteo:1", MsgID: "WF-1007195902",
		Minute: "20261007-1959", Seq: 2, CreatedAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("SaveMessageID (conflict): %v", err)
	}
	id, ok, err := store.LoadMessageID(ctx, "imgw-meteo:1")
	if err != nil || !ok || id != "WF-1007195801" {
		t.Fatalf("LoadMessageID = %q, %v, %v; want the first WF-1007195801", id, ok, err)
	}
}

// TestHighestSeqInMinute pins the restart seam: the sequence is scoped to
// the minute bucket, so a new minute restarts at zero.
func TestHighestSeqInMinute(t *testing.T) {
	store := newUsersStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 7, 19, 58, 0, 0, time.UTC)

	if n, err := store.HighestSeqInMinute(ctx, "20261007-1958"); err != nil || n != 0 {
		t.Fatalf("empty minute = %d, %v; want 0", n, err)
	}
	for i, key := range []string{"a", "b", "c"} {
		if err := store.SaveMessageID(ctx, core.MessageIDAssignment{
			EventKey: key, MsgID: "WF-10071958" + string(rune('0'+i+1)),
			Minute: "20261007-1958", Seq: i + 1, CreatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := store.HighestSeqInMinute(ctx, "20261007-1958"); err != nil || n != 3 {
		t.Fatalf("seq = %d, %v; want 3", n, err)
	}
	if n, err := store.HighestSeqInMinute(ctx, "20261007-1959"); err != nil || n != 0 {
		t.Fatalf("other minute = %d, %v; want 0", n, err)
	}
}

// TestPruneMessageIDsKeepsLiveEvents pins the retention rule: ids of
// retained events and panel communications survive, ids of events that no
// longer exist are dropped.
func TestPruneMessageIDsKeepsLiveEvents(t *testing.T) {
	store := newUsersStore(t)
	ctx := context.Background()
	old := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	cutoff := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)

	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO events(event_key, source, source_id, fingerprint, status, event,
			received_at, first_seen_at, last_seen_at, updated_at)
		VALUES('imgw-meteo:1','imgw-meteo','1','fp','active','Storm','t','t','t','t')`); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO compose_hazards(event_key, hazard_json, status, updated_at_ms)
		VALUES('compose:1','{}','active',0)`); err != nil {
		t.Fatalf("seed compose hazard: %v", err)
	}
	for _, key := range []string{"imgw-meteo:1", "compose:1", "gone:1"} {
		if err := store.SaveMessageID(ctx, core.MessageIDAssignment{
			EventKey: key, MsgID: "WF-0901100001", Minute: "20260901-1000", Seq: 1, CreatedAt: old,
		}); err != nil {
			t.Fatal(err)
		}
	}

	n, err := store.PruneMessageIDs(ctx, cutoff)
	if err != nil {
		t.Fatalf("PruneMessageIDs: %v", err)
	}
	if n != 1 {
		t.Fatalf("pruned %d rows, want 1", n)
	}
	for key, want := range map[string]bool{"imgw-meteo:1": true, "compose:1": true, "gone:1": false} {
		_, ok, err := store.LoadMessageID(ctx, key)
		if err != nil {
			t.Fatalf("LoadMessageID(%q): %v", key, err)
		}
		if ok != want {
			t.Errorf("LoadMessageID(%q) present=%v, want %v", key, ok, want)
		}
	}
}
