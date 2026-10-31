package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// TestWithdrawEventJournalsTheCancellation is the durability guarantee: the
// status change AND the journal record commit together, so an output that was
// offline when it happened still removes the retained document after a restart.
func TestWithdrawEventJournalsTheCancellation(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()

	road := activeEv("road-1")
	road.Source = "gddkia"
	road.Producer = "gddkia-roads"
	road.ExpiresAt = timePtr(time.Now().Add(300 * 24 * time.Hour)) // months ahead
	ingestOne(t, s, road)

	before := countChanges(t, s)
	change, err := s.WithdrawEvent(ctx, road, time.Now())
	if err != nil {
		t.Fatalf("WithdrawEvent: %v", err)
	}
	if change == nil {
		t.Fatal("no journal change recorded for the withdrawal")
	}
	if change.ChangeType != core.ChangeCancelled {
		t.Fatalf("change type = %s, want cancelled", change.ChangeType)
	}
	if got := countChanges(t, s); got != before+1 {
		t.Fatalf("journal rows = %d, want %d", got, before+1)
	}

	// The event is no longer active and the call is idempotent.
	active, err := s.ListActiveEvents(ctx, "", 10)
	if err != nil {
		t.Fatalf("ListActiveEvents: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("still active: %+v", active)
	}
	again, err := s.WithdrawEvent(ctx, road, time.Now())
	if err != nil {
		t.Fatalf("second WithdrawEvent: %v", err)
	}
	if again != nil {
		t.Fatal("a second withdrawal must record nothing")
	}
	if got := countChanges(t, s); got != before+1 {
		t.Fatalf("journal rows = %d, want no second record", got)
	}
}

// TestWithdrawEventWritesNoInboxRow pins the operator decision: a withdrawal is
// not an alert, so the routing engine must not notify subscribers about it.
func TestWithdrawEventWritesNoInboxRow(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()

	ev := activeEv("quiet-1")
	ev.Source = "imgw-meteo"
	ev.Producer = "imgw-warnings"
	ingestOne(t, s, ev)

	// The ingest itself queued one inbox row for routing; remember the count.
	before := countInbox(t, s)
	if _, err := s.WithdrawEvent(ctx, ev, time.Now()); err != nil {
		t.Fatalf("WithdrawEvent: %v", err)
	}
	if got := countInbox(t, s); got != before {
		t.Fatalf("inbox rows = %d, want %d (no notification)", got, before)
	}
}

// countChanges reports the journal size.
func countChanges(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM changes").Scan(&n); err != nil {
		t.Fatalf("count changes: %v", err)
	}
	return n
}

// countInbox reports the durable routing inbox size.
func countInbox(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM dispatch_inbox").Scan(&n); err != nil {
		t.Fatalf("count inbox: %v", err)
	}
	return n
}
