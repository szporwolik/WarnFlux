package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// activeEv builds a normalized active event with a stable short key.
func activeEv(id string) core.HazardEvent {
	e := normEvent()
	e.SourceID = id
	e.Normalize()
	return e
}

// ackAllChanges drains and acknowledges every pending journal change for an
// output, so later tests can run with a fully-caught-up cursor. The cursor
// advances per batch: PollChanges re-returns the same batch until
// AckChanges moves it forward.
func ackAllChanges(t *testing.T, s *Store, outputID string) {
	t.Helper()
	ctx := context.Background()
	for {
		batch, err := s.PollChanges(ctx, outputID, 256)
		if err != nil {
			t.Fatalf("PollChanges: %v", err)
		}
		if len(batch) == 0 {
			return
		}
		if err := s.AckChanges(ctx, outputID, batch[len(batch)-1].ID); err != nil {
			t.Fatalf("AckChanges: %v", err)
		}
	}
}

func TestListActiveEventsOnlyActive(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()

	for _, id := range []string{"b", "d", "a", "c"} {
		ingestOne(t, s, activeEv(id))
	}
	// One cancelled event (same key updated to cancelled).
	cancelled := activeEv("cancel-me")
	ingestOne(t, s, cancelled)
	cancelled.Status = core.StatusCancelled
	ingestOne(t, s, cancelled)
	// One expired event.
	expired := activeEv("expire-me")
	expired.ExpiresAt = timePtr(time.Now().Add(-time.Hour))
	ingestOne(t, s, expired)
	if _, err := s.Expire(ctx, time.Now()); err != nil {
		t.Fatalf("Expire: %v", err)
	}

	got, err := s.ListActiveEvents(ctx, "", 10)
	if err != nil {
		t.Fatalf("ListActiveEvents: %v", err)
	}
	var keys []string
	for _, e := range got {
		keys = append(keys, e.Key())
		if e.Status != core.StatusActive {
			t.Errorf("event %q has status %q, want active", e.Key(), e.Status)
		}
	}
	want := []string{"meteoalarm:a", "meteoalarm:b", "meteoalarm:c", "meteoalarm:d"}
	if len(keys) != len(want) {
		t.Fatalf("keys = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Errorf("keys[%d] = %q, want %q", i, keys[i], want[i])
		}
	}
}

func TestListActiveEventsStablePagination(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	for _, id := range []string{"03", "01", "05", "02", "04"} {
		ingestOne(t, s, activeEv("pg-"+id))
	}

	var got []string
	after := ""
	for {
		batch, err := s.ListActiveEvents(ctx, after, 2)
		if err != nil {
			t.Fatalf("ListActiveEvents(after=%q): %v", after, err)
		}
		if len(batch) == 0 {
			break
		}
		if len(batch) > 2 {
			t.Fatalf("batch size %d exceeds the page limit", len(batch))
		}
		for _, e := range batch {
			got = append(got, e.Key())
		}
		after = batch[len(batch)-1].Key()
	}
	want := []string{"meteoalarm:pg-01", "meteoalarm:pg-02", "meteoalarm:pg-03", "meteoalarm:pg-04", "meteoalarm:pg-05"}
	if len(got) != len(want) {
		t.Fatalf("paged keys = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("paged keys[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// A page boundary equal to the full count returns everything at once.
	full, err := s.ListActiveEvents(ctx, "", 5)
	if err != nil || len(full) != 5 {
		t.Errorf("full page = %d events (err %v), want 5", len(full), err)
	}
	// afterKey is strictly exclusive.
	one, err := s.ListActiveEvents(ctx, "meteoalarm:pg-04", 10)
	if err != nil || len(one) != 1 || one[0].Key() != "meteoalarm:pg-05" {
		t.Errorf("after last-but-one = %v (err %v), want only meteoalarm:pg-05", one, err)
	}
}

func TestListActiveEventsEmptyAndCancelledContext(t *testing.T) {
	s := openTemp(t)
	got, err := s.ListActiveEvents(context.Background(), "", 256)
	if err != nil || len(got) != 0 {
		t.Errorf("empty database = %d events (err %v), want none", len(got), err)
	}

	ingestOne(t, s, activeEv("x"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ListActiveEvents(ctx, "", 256); err == nil {
		t.Error("cancelled context must fail the query")
	}

	if _, err := s.ListActiveEvents(context.Background(), "", 0); err == nil {
		t.Error("non-positive limit must be rejected")
	}
}

func timePtr(t time.Time) *time.Time { return &t }

var _ storage.ActiveEventLister = (*Store)(nil)
