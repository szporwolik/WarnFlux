package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"warnflux/internal/core"
	"warnflux/internal/storage"
	"warnflux/internal/storage/sqlite"
)

func newTestIngester(t *testing.T) (*Ingester, *sqlite.Store) {
	t.Helper()
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewIngester(store, logger), store
}

func baseEvent() core.HazardEvent {
	return core.HazardEvent{
		Source:   "meteoalarm",
		SourceID: "2.49.0.1",
		Event:    "Rain",
		Severity: "yellow",
		Headline: "Rain expected",
		Areas:    []string{"DE-NW"},
	}
}

func TestIngestLifecycle(t *testing.T) {
	ing, _ := newTestIngester(t)
	ctx := context.Background()

	event := baseEvent()

	// First ingestion -> new.
	result, change, err := ing.Ingest(ctx, event)
	if err != nil {
		t.Fatalf("first Ingest: %v", err)
	}
	if result != ResultNew {
		t.Errorf("result = %v, want new", result)
	}
	if change.Type != core.ChangeNew || change.Event.Key() != event.Key() {
		t.Errorf("change = %+v, want new change for %s", change, event.Key())
	}

	// Identical event -> duplicate, no change emitted.
	result, change, err = ing.Ingest(ctx, event)
	if err != nil {
		t.Fatalf("duplicate Ingest: %v", err)
	}
	if result != ResultDuplicate {
		t.Errorf("result = %v, want duplicate", result)
	}
	if change.Type != "" {
		t.Errorf("duplicates must not emit a change, got %+v", change)
	}

	// Changed severity -> updated.
	updated := event
	updated.Severity = "orange"
	result, change, err = ing.Ingest(ctx, updated)
	if err != nil {
		t.Fatalf("update Ingest: %v", err)
	}
	if result != ResultUpdated {
		t.Errorf("result = %v, want updated", result)
	}
	if change.Type != core.ChangeUpdated || change.Event.Severity != "orange" {
		t.Errorf("change = %+v, want updated with new severity", change)
	}

	// Cancellation -> cancelled.
	cancelled := updated
	cancelled.Status = core.StatusCancelled
	result, change, err = ing.Ingest(ctx, cancelled)
	if err != nil {
		t.Fatalf("cancel Ingest: %v", err)
	}
	if result != ResultCancelled {
		t.Errorf("result = %v, want cancelled", result)
	}
	if change.Type != core.ChangeCancelled || change.Event.Status != core.StatusCancelled {
		t.Errorf("change = %+v, want cancelled", change)
	}

	// Repeated identical cancellation -> duplicate.
	result, _, err = ing.Ingest(ctx, cancelled)
	if err != nil {
		t.Fatalf("repeated cancel Ingest: %v", err)
	}
	if result != ResultDuplicate {
		t.Errorf("result = %v, want duplicate", result)
	}
}

func TestIngestDuplicateUpdatesOnlyLastSeen(t *testing.T) {
	ctx := context.Background()

	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)

	// Both the service and the store read from the same test clock.
	var current = t0
	clock := func() time.Time { return current }
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"), sqlite.WithClock(clock))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	ing := NewIngester(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ing.now = clock

	event := baseEvent()
	if result, _, err := ing.Ingest(ctx, event); err != nil || result != ResultNew {
		t.Fatalf("first Ingest = %v, %v", result, err)
	}

	current = t1
	if result, _, err := ing.Ingest(ctx, event); err != nil || result != ResultDuplicate {
		t.Fatalf("second Ingest = %v, %v", result, err)
	}

	got, err := store.Get(ctx, event.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.FirstSeenAt.Equal(t0) {
		t.Errorf("first_seen_at = %v, want %v", got.FirstSeenAt, t0)
	}
	if !got.LastSeenAt.Equal(t1) {
		t.Errorf("last_seen_at = %v, want %v", got.LastSeenAt, t1)
	}
	if !got.Event.UpdatedAt.Equal(t0) {
		t.Errorf("updated_at = %v, want %v (unchanged for duplicates)", got.Event.UpdatedAt, t0)
	}
}

func TestIngestUpdatePreservesIdentityAndFirstSeen(t *testing.T) {
	ctx := context.Background()

	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)

	var current = t0
	clock := func() time.Time { return current }
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"), sqlite.WithClock(clock))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	ing := NewIngester(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ing.now = clock

	event := baseEvent()
	if _, _, err := ing.Ingest(ctx, event); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	current = t1
	updated := event
	updated.Severity = "red"
	result, change, err := ing.Ingest(ctx, updated)
	if err != nil {
		t.Fatalf("update Ingest: %v", err)
	}
	if result != ResultUpdated {
		t.Errorf("result = %v, want updated", result)
	}

	got, err := store.Get(ctx, event.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Source != event.Source || got.Event.SourceID != event.SourceID {
		t.Error("stable identity changed on update")
	}
	if !got.FirstSeenAt.Equal(t0) {
		t.Errorf("first_seen_at = %v, want %v", got.FirstSeenAt, t0)
	}
	if !got.Event.ReceivedAt.Equal(t0) {
		t.Errorf("received_at = %v, want original %v", got.Event.ReceivedAt, t0)
	}
	if !got.Event.UpdatedAt.Equal(t1) {
		t.Errorf("updated_at = %v, want %v", got.Event.UpdatedAt, t1)
	}
	if change.Event.Severity != "red" {
		t.Errorf("change carries stale content: %+v", change.Event)
	}
}

func TestIngestInvalidEventNotPersisted(t *testing.T) {
	ing, store := newTestIngester(t)
	ctx := context.Background()

	event := baseEvent()
	event.Source = ""
	if _, _, err := ing.Ingest(ctx, event); err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if n, err := store.Count(ctx); err != nil || n != 0 {
		t.Errorf("store count = %d, %v; invalid events must not be persisted", n, err)
	}
}

func TestIngestZeroExpiryStaysActive(t *testing.T) {
	// A zero-value ExpiresAt pointer must be treated as absent, not as an
	// event that expires immediately.
	ing, store := newTestIngester(t)
	ctx := context.Background()

	zero := time.Time{}
	event := baseEvent()
	event.SourceID = "zero-expiry"
	event.ExpiresAt = &zero
	if r, _, err := ing.Ingest(ctx, event); err != nil || r != ResultNew {
		t.Fatalf("Ingest = %v, %v", r, err)
	}

	if _, err := ing.Expire(ctx, time.Now().UTC()); err != nil {
		t.Fatalf("Expire: %v", err)
	}

	got, err := store.Get(ctx, event.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Status != core.StatusActive {
		t.Errorf("status = %q, want active (zero expiry must not expire)", got.Event.Status)
	}
}

func TestIngestChangeCarriesPersistedTimestamps(t *testing.T) {
	ctx := context.Background()

	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	var current = t0
	clock := func() time.Time { return current }
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"), sqlite.WithClock(clock))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	ing := NewIngester(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ing.now = clock

	event := baseEvent()
	result, change, err := ing.Ingest(ctx, event)
	if err != nil || result != ResultNew {
		t.Fatalf("Ingest = %v, %v", result, err)
	}
	if change.Event.UpdatedAt.IsZero() {
		t.Error("new event change must carry a non-zero UpdatedAt")
	}
	got, err := store.Get(ctx, event.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !change.Event.UpdatedAt.Equal(got.Event.UpdatedAt) {
		t.Errorf("change UpdatedAt %v != stored %v", change.Event.UpdatedAt, got.Event.UpdatedAt)
	}
}

func TestExpire(t *testing.T) {
	ing, store := newTestIngester(t)
	ctx := context.Background()

	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)

	withExpiry := baseEvent()
	withExpiry.SourceID = "with-expiry"
	withExpiry.ExpiresAt = &past
	if r, _, err := ing.Ingest(ctx, withExpiry); err != nil || r != ResultNew {
		t.Fatalf("ingest expiring event = %v, %v", r, err)
	}

	noExpiry := baseEvent()
	noExpiry.SourceID = "no-expiry"
	if r, _, err := ing.Ingest(ctx, noExpiry); err != nil || r != ResultNew {
		t.Fatalf("ingest no-expiry event = %v, %v", r, err)
	}

	futureExpiry := baseEvent()
	futureExpiry.SourceID = "future-expiry"
	futureExpiry.ExpiresAt = &future
	if r, _, err := ing.Ingest(ctx, futureExpiry); err != nil || r != ResultNew {
		t.Fatalf("ingest future event = %v, %v", r, err)
	}

	changes, err := ing.Expire(ctx, now)
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(changes))
	}
	if changes[0].Type != core.ChangeExpired || changes[0].Event.SourceID != "with-expiry" {
		t.Errorf("unexpected change: %+v", changes[0])
	}

	// State is persistent.
	got, err := store.Get(ctx, withExpiry.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Status != core.StatusExpired {
		t.Errorf("status = %q, want expired", got.Event.Status)
	}

	// Events without expiry stay active.
	got, err = store.Get(ctx, noExpiry.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Status != core.StatusActive {
		t.Errorf("no-expiry event status = %q, want active", got.Event.Status)
	}
}

func TestRestartRecognizesDuplicate(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "events.db")

	store1, _, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	ing1 := NewIngester(store1, slog.New(slog.NewTextHandler(io.Discard, nil)))

	event := baseEvent()
	if result, _, err := ing1.Ingest(ctx, event); err != nil || result != ResultNew {
		t.Fatalf("first Ingest = %v, %v", result, err)
	}
	store1.Close()

	store2, _, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()
	ing2 := NewIngester(store2, slog.New(slog.NewTextHandler(io.Discard, nil)))

	result, _, err := ing2.Ingest(ctx, event)
	if err != nil {
		t.Fatalf("Ingest after restart: %v", err)
	}
	if result != ResultDuplicate {
		t.Errorf("result after restart = %v, want duplicate", result)
	}
}

func TestConcurrentIngestSameEvent(t *testing.T) {
	ing, store := newTestIngester(t)
	ctx := context.Background()

	event := baseEvent()
	const workers = 20

	results := make([]Result, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], _, errs[i] = ing.Ingest(ctx, event)
		}(i)
	}
	wg.Wait()

	newCount := 0
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d error: %v", i, errs[i])
		}
		switch results[i] {
		case ResultNew:
			newCount++
		case ResultDuplicate:
		default:
			t.Errorf("worker %d result = %v, want new or duplicate", i, results[i])
		}
	}
	if newCount != 1 {
		t.Errorf("new results = %d, want exactly 1", newCount)
	}
	if n, err := store.Count(ctx); err != nil || n != 1 {
		t.Errorf("stored rows = %d, %v; want exactly 1", n, err)
	}
}

func TestRunExpirationStopsOnCancel(t *testing.T) {
	ing, store := newTestIngester(t)
	ctx := context.Background()

	// Seed an event that expires very soon.
	expires := time.Now().UTC().Add(30 * time.Millisecond)
	event := baseEvent()
	event.SourceID = "soon"
	event.ExpiresAt = &expires
	if r, _, err := ing.Ingest(ctx, event); err != nil || r != ResultNew {
		t.Fatalf("Ingest = %v, %v", r, err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ing.RunExpiration(runCtx, 10*time.Millisecond)
	}()

	// Wait (generously) until the worker expires the event.
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := store.Get(ctx, event.Key())
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Event.Status == core.StatusExpired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not expire the event in time")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunExpiration did not stop after cancellation")
	}
}

func TestIngestErrorPropagates(t *testing.T) {
	// A store error must surface, not be swallowed.
	failing := failingStore{}
	ing := NewIngester(failing, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, _, err := ing.Ingest(context.Background(), baseEvent())
	if err == nil {
		t.Fatal("expected error from failing store, got nil")
	}
}

type failingStore struct{}

func (failingStore) Get(context.Context, string) (*storage.StoredEvent, error) {
	return nil, errors.New("boom")
}
func (failingStore) Insert(context.Context, core.HazardEvent, string) (bool, error) {
	return false, nil
}
func (failingStore) Update(context.Context, core.HazardEvent, string) error { return nil }
func (failingStore) Touch(context.Context, string) error                    { return nil }
func (failingStore) MarkExpired(context.Context, time.Time) ([]core.HazardEvent, error) {
	return nil, nil
}
func (failingStore) Close() error { return nil }
