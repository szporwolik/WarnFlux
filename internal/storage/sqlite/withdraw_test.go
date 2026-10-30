package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// TestWithdrawSourceEventsCancelsOnlyThatSource pins the production case: a
// source switched off in the configuration left nine active road works on the
// public page, because nothing ever withdrew them.
func TestWithdrawSourceEventsCancelsOnlyThatSource(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()

	roads := activeEv("road-1")
	roads.Source = "gddkia"
	roads.ExpiresAt = timePtr(time.Now().Add(300 * 24 * time.Hour)) // months ahead
	ingestOne(t, s, roads)
	roads2 := activeEv("road-2")
	roads2.Source = "gddkia"
	roads2.ExpiresAt = roads.ExpiresAt
	ingestOne(t, s, roads2)

	other := activeEv("meteo-1")
	other.Source = "imgw-meteo"
	ingestOne(t, s, other)

	withdrawn, err := s.WithdrawSourceEvents(ctx, "gddkia", time.Now())
	if err != nil {
		t.Fatalf("WithdrawSourceEvents: %v", err)
	}
	if len(withdrawn) != 2 {
		t.Fatalf("withdrawn = %d, want 2", len(withdrawn))
	}
	for _, ev := range withdrawn {
		if ev.Status != core.StatusCancelled {
			t.Errorf("returned status = %s, want cancelled", ev.Status)
		}
	}

	active, err := s.ListActiveEvents(ctx, "", 10)
	if err != nil {
		t.Fatalf("ListActiveEvents: %v", err)
	}
	if len(active) != 1 || active[0].Source != "imgw-meteo" {
		t.Fatalf("active after withdrawal = %+v, want only the other source", active)
	}

	// Idempotent: a second pass has nothing to withdraw.
	again, err := s.WithdrawSourceEvents(ctx, "gddkia", time.Now())
	if err != nil {
		t.Fatalf("second WithdrawSourceEvents: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("second pass withdrew %d, want 0", len(again))
	}
}

// TestWithdrawSourceEventsRejectsEmptySource keeps a programming mistake from
// cancelling every event of every source.
func TestWithdrawSourceEventsRejectsEmptySource(t *testing.T) {
	s := openTemp(t)
	if _, err := s.WithdrawSourceEvents(context.Background(), "", time.Now()); err == nil {
		t.Fatal("an empty source must be rejected")
	}
}
