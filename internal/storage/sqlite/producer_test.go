package sqlite

import (
	"context"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestEventProducerPersists pins the owner of an event as durable state: two
// instances of one plugin type publish under the same source slug, so the
// producer is what tells their events apart — and it must survive a restart
// (the reconciliation of a snapshot source reads it back from SQLite).
func TestEventProducerPersists(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()

	owned := normEvent()
	owned.Producer = "burze-main"
	ingestOne(t, s, owned)

	got, err := s.Get(ctx, owned.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Producer != "burze-main" {
		t.Errorf("producer = %q, want %q", got.Event.Producer, "burze-main")
	}

	active, err := s.ListActiveEvents(ctx, "", 10)
	if err != nil {
		t.Fatalf("ListActiveEvents: %v", err)
	}
	if len(active) != 1 || active[0].Producer != "burze-main" {
		t.Errorf("active events = %+v, want one event owned by burze-main", active)
	}
}

// TestEventProducerFollowsTheReportingInstance pins the adoption rules of a
// core-owned field: a path that carries no producer (the broker loopback, an
// ingest endpoint, another station) never wipes the owner, while the instance
// that keeps reporting the key adopts an ownerless row (the events stored
// before the producer column existed).
func TestEventProducerFollowsTheReportingInstance(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()

	legacy := normEvent()
	ingestOne(t, s, legacy)

	// The (only) instance reporting the same event takes ownership.
	adopted := legacy
	adopted.Producer = "burze-main"
	if outcome, _ := ingestOne(t, s, adopted); outcome != storage.OutcomeDuplicate {
		t.Fatalf("outcome = %v, want a duplicate (nothing about the event changed)", outcome)
	}
	got, err := s.Get(ctx, legacy.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Producer != "burze-main" {
		t.Fatalf("producer after adoption = %q, want %q", got.Event.Producer, "burze-main")
	}

	// A producer-less path reporting the same event keeps the owner.
	if outcome, _ := ingestOne(t, s, legacy); outcome != storage.OutcomeDuplicate {
		t.Fatalf("outcome = %v, want a duplicate", outcome)
	}
	got, err = s.Get(ctx, legacy.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Producer != "burze-main" {
		t.Errorf("producer = %q after a producer-less ingest, want the owner to be kept", got.Event.Producer)
	}

	// A sibling instance publishing the same event (content change) does not
	// steal it either while that path is what carries no producer.
	changed := legacy
	changed.Headline = "Heavy rain in force"
	ingestOne(t, s, changed)
	got, err = s.Get(ctx, legacy.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Producer != "burze-main" {
		t.Errorf("producer = %q after an owner-less update, want the owner to be kept", got.Event.Producer)
	}

	// An explicit owner always wins: the stamp is core-owned.
	elsewhere := legacy
	elsewhere.Producer = "burze-krakow"
	ingestOne(t, s, elsewhere)
	got, err = s.Get(ctx, legacy.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Producer != "burze-krakow" {
		t.Errorf("producer = %q, want the reporting instance to win", got.Event.Producer)
	}
}
