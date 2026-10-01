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

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
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
		Severity: "severe",
		Headline: "Rain expected",
		Areas:    []string{"DE-NW"},
	}
}

// TestDispatchSink pins the local-first source pipeline: every journal
// change produced by a successful ingest or expiration reaches the
// dispatch sink (the application routes it into the local ingress), with
// the publisher identity stamped; duplicates and empty expirations never
// fire it.
func TestDispatchSink(t *testing.T) {
	ing, store := newTestIngester(t)
	ctx := context.Background()

	var got []core.EventChange
	var inboxIDs []int64
	ing.SetDispatchSink(func(c core.EventChange, inboxID int64) {
		got = append(got, c)
		inboxIDs = append(inboxIDs, inboxID)
	})

	event := baseEvent()
	if _, change, err := ing.Ingest(ctx, event); err != nil || change.Type != core.ChangeNew {
		t.Fatalf("first Ingest = %+v, %v", change, err)
	}
	// Duplicate: no sink call.
	if _, _, err := ing.Ingest(ctx, event); err != nil {
		t.Fatalf("duplicate Ingest: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("sink calls after new+duplicate = %d, want 1", len(got))
	}
	if got[0].ID == 0 || got[0].Publisher == "" {
		t.Errorf("sink change missing identity: %+v", got[0])
	}
	// The store committed the inbox row atomically with the journal: the
	// sink must receive its ID so the live enqueue reuses the row.
	if inboxIDs[0] == 0 {
		t.Error("sink inboxID = 0, want the durable inbox row committed with the journal change")
	}

	// Expiration: a second event with a lapsed expiry is swept by the
	// maintenance expiration and reaches the sink as ChangeExpired.
	expired := baseEvent().Clone()
	expired.SourceID = "expired-1"
	past := time.Now().Add(-time.Minute)
	expired.ExpiresAt = &past
	if _, _, err := ing.Ingest(ctx, expired); err != nil {
		t.Fatalf("expired ingest: %v", err)
	}
	got = got[:0]
	inboxIDs = inboxIDs[:0]
	changes, err := ing.Expire(ctx, time.Now())
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if len(changes) == 0 {
		t.Fatal("Expire produced no changes, want at least one expired event")
	}
	if len(got) != len(changes) {
		t.Fatalf("sink calls = %d, want %d", len(got), len(changes))
	}
	for i, c := range got {
		if c.Type != core.ChangeExpired || c.ID == 0 {
			t.Errorf("sink change = %+v, want journaled expired", c)
		}
		if inboxIDs[i] == 0 {
			t.Errorf("sink inboxID for expired change %d = 0, want a durable inbox row", c.ID)
		}
	}
	_ = store
}

func TestIngestLifecycle(t *testing.T) {
	ing, _ := newTestIngester(t)
	ctx := context.Background()

	event := baseEvent()

	result, change, err := ing.Ingest(ctx, event)
	if err != nil || result != ResultNew {
		t.Fatalf("first Ingest = %v, %+v, %v", result, change, err)
	}
	if change.Type != core.ChangeNew || change.ID == 0 {
		t.Errorf("change = %+v, want journaled new change", change)
	}

	result, change, err = ing.Ingest(ctx, event)
	if err != nil || result != ResultDuplicate {
		t.Fatalf("duplicate Ingest = %v, %v", result, err)
	}
	if change.Type != "" || change.ID != 0 {
		t.Errorf("duplicates must not produce a change, got %+v", change)
	}

	updated := event.Clone()
	updated.Severity = "moderate"
	result, change, err = ing.Ingest(ctx, updated)
	if err != nil || result != ResultUpdated {
		t.Fatalf("update Ingest = %v, %v", result, err)
	}
	if change.Type != core.ChangeUpdated || change.Event.Severity != "moderate" {
		t.Errorf("change = %+v, want updated", change)
	}

	cancelled := updated.Clone()
	cancelled.Status = core.StatusCancelled
	result, change, err = ing.Ingest(ctx, cancelled)
	if err != nil || result != ResultCancelled {
		t.Fatalf("cancel Ingest = %v, %v", result, err)
	}
	if change.Type != core.ChangeCancelled || change.Event.Status != core.StatusCancelled {
		t.Errorf("change = %+v, want cancelled", change)
	}

	result, _, err = ing.Ingest(ctx, cancelled)
	if err != nil || result != ResultDuplicate {
		t.Fatalf("repeated cancel Ingest = %v, %v", result, err)
	}
}

func TestIngestUnknownCancellationIsNotNew(t *testing.T) {
	ing, _ := newTestIngester(t)
	ctx := context.Background()

	event := baseEvent()
	event.Status = core.StatusCancelled
	result, change, err := ing.Ingest(ctx, event)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if result != ResultCancelled {
		t.Errorf("result = %v, want cancelled (never new)", result)
	}
	if change.Type != core.ChangeCancelled {
		t.Errorf("change type = %v, want cancelled", change.Type)
	}
}

func TestIngestDuplicateUpdatesOnlyLastSeen(t *testing.T) {
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
	updated := event.Clone()
	updated.Severity = "minor"
	result, change, err := ing.Ingest(ctx, updated)
	if err != nil || result != ResultUpdated {
		t.Fatalf("update Ingest = %v, %v", result, err)
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
	if change.Event.Severity != "minor" {
		t.Errorf("change carries stale content: %+v", change.Event)
	}
}

func TestIngestInvalidEventNotPersisted(t *testing.T) {
	ing, store := newTestIngester(t)
	ctx := context.Background()

	event := baseEvent()
	event.Source = "BAD SOURCE!"
	if _, _, err := ing.Ingest(ctx, event); err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if n, err := store.Count(ctx); err != nil || n != 0 {
		t.Errorf("store count = %d, %v; invalid events must not be persisted", n, err)
	}
}

func TestIngestZeroExpiryStaysActive(t *testing.T) {
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
		t.Errorf("status = %q, want active", got.Event.Status)
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

func TestExpireReturnsJournaledChanges(t *testing.T) {
	ing, store := newTestIngester(t)
	ctx := context.Background()

	now := time.Now().UTC()
	past := now.Add(-time.Hour)

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

	changes, err := ing.Expire(ctx, now)
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(changes))
	}
	if changes[0].Type != core.ChangeExpired || changes[0].ID == 0 {
		t.Errorf("unexpected change: %+v", changes[0])
	}

	got, err := store.Get(ctx, withExpiry.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Status != core.StatusExpired {
		t.Errorf("status = %q, want expired", got.Event.Status)
	}
	got, err = store.Get(ctx, noExpiry.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Status != core.StatusActive {
		t.Errorf("no-expiry event status = %q, want active", got.Event.Status)
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
			results[i], _, errs[i] = ing.Ingest(ctx, event.Clone())
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

func TestIngestErrorPropagates(t *testing.T) {
	ing := NewIngester(failingStore{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, _, err := ing.Ingest(context.Background(), baseEvent())
	if err == nil {
		t.Fatal("expected error from failing store, got nil")
	}
}

type failingStore struct{}

func (failingStore) Get(context.Context, string) (*storage.StoredEvent, error) {
	return nil, errors.New("boom")
}
func (failingStore) Ingest(context.Context, core.HazardEvent, string) (storage.Outcome, *storage.Change, error) {
	return 0, nil, errors.New("boom")
}
func (failingStore) Expire(context.Context, time.Time) ([]storage.Change, error) {
	return nil, nil
}
func (failingStore) PollChanges(context.Context, string, int) ([]storage.Change, error) {
	return nil, nil
}
func (failingStore) AckChanges(context.Context, string, int64) error        { return nil }
func (failingStore) SyncOutputs(context.Context, []storage.OutputRef) error { return nil }
func (failingStore) CleanupChanges(context.Context, time.Time) (int64, error) {
	return 0, nil
}
func (failingStore) CleanupEvents(context.Context, time.Time) (int64, error) { return 0, nil }
func (failingStore) Count(context.Context) (int, error)                      { return 0, nil }
func (failingStore) PendingStats(context.Context) (int, time.Duration, error) {
	return 0, 0, nil
}
func (failingStore) ListArchiveEvents(context.Context, time.Time, int, int) ([]storage.StoredEvent, error) {
	return nil, nil
}
func (failingStore) CountArchiveEvents(context.Context, time.Time) (int, error) {
	return 0, nil
}
func (failingStore) Close() error { return nil }

// TestJournalChangeCommitsInboxAtomically pins the P1: the durable inbox
// row is committed in the SAME transaction as the journal change, so an
// interruption between the journal commit and the live dispatch (no
// dispatch sink at all here) can never strand an alert without a
// notification path — the inbox row survives and is recovered after a
// restart, while a re-polled duplicate cannot repair anything (and does
// not need to).
func TestJournalChangeCommitsInboxAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	store, _, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ing := NewIngester(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	// The crash window: no dispatch sink is installed, so the live
	// dispatch never runs — exactly the interruption between the journal
	// commit and the inbox write.
	event := baseEvent()
	_, change, err := ing.Ingest(ctx, event)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// The inbox row was committed with the journal change and carries
	// the same journal identity.
	items, err := store.PendingInboxEvents(ctx, 10)
	if err != nil {
		t.Fatalf("PendingInboxEvents: %v", err)
	}
	var inboxRow int64
	found := false
	for _, it := range items {
		if it.Event.Hazard == nil || it.Event.Hazard.ChangeID != change.ID {
			continue
		}
		found = true
		inboxRow = it.ID
		if it.Event.Origin.ReceiverID != "local" {
			t.Errorf("inbox receiver = %q, want local", it.Event.Origin.ReceiverID)
		}
		if it.Event.Hazard.Publisher != change.Publisher {
			t.Errorf("inbox publisher = %q, want %q", it.Event.Hazard.Publisher, change.Publisher)
		}
	}
	if !found {
		t.Fatal("journal change committed without a durable inbox row")
	}

	// The provider re-poll is a duplicate (the reported symptom): it must
	// not matter, because the first change is already durably accepted.
	if result, _, err := ing.Ingest(ctx, event); err != nil || result != ResultDuplicate {
		t.Fatalf("re-poll = %v, %v, want duplicate", result, err)
	}

	// Restart simulation: a fresh handle over the same file still sees
	// the pending row, so inbox recovery evaluates the alert.
	store.Close()
	restarted, _, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	items, err = restarted.PendingInboxEvents(ctx, 10)
	if err != nil {
		t.Fatalf("PendingInboxEvents after restart: %v", err)
	}
	found = false
	for _, it := range items {
		if it.Event.Hazard != nil && it.Event.Hazard.ChangeID == change.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("inbox row for change %d lost across the restart", change.ID)
	}

	// The engine acknowledges the row after evaluation.
	if err := restarted.AckInboxEvent(ctx, inboxRow); err != nil {
		t.Fatalf("AckInboxEvent: %v", err)
	}
	items, err = restarted.PendingInboxEvents(ctx, 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("inbox after ack = (%v, %v), want empty", items, err)
	}
}

// TestExpirationCommitsInboxAtomically extends the same guarantee to the
// maintenance expiration: every ChangeExpired journal record is accepted
// into the inbox in the same transaction.
func TestExpirationCommitsInboxAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	store, _, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ing := NewIngester(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	event := baseEvent().Clone()
	event.SourceID = "expire-me"
	past := time.Now().Add(-time.Minute)
	event.ExpiresAt = &past
	if _, _, err := ing.Ingest(ctx, event); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// No dispatch sink: the expiry transition must still be durably
	// accepted together with its journal record.
	changes, err := ing.Expire(ctx, time.Now())
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("expired changes = %d, want 1", len(changes))
	}
	change := changes[0]
	if change.ID == 0 {
		t.Fatal("expired change has no journal ID")
	}
	// The inbox row for the expired change is pending and durable.
	items, err := store.PendingInboxEvents(ctx, 10)
	if err != nil {
		t.Fatalf("PendingInboxEvents: %v", err)
	}
	found := false
	for _, it := range items {
		if it.Event.Hazard != nil && it.Event.Hazard.ChangeID == change.ID {
			found = true
			if it.Event.Hazard.Type != dispatch.TransitionExpired {
				t.Errorf("inbox transition = %q, want expired", it.Event.Hazard.Type)
			}
			break
		}
	}
	if !found {
		t.Fatalf("no inbox row for expired journal change %d", change.ID)
	}
}
