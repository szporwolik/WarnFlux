package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

func openTemp(t *testing.T, opts ...Option) *Store {
	t.Helper()
	store, _, err := Open(filepath.Join(t.TempDir(), "test.db"), opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func normEvent() core.HazardEvent {
	eff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	exp := time.Date(2099, 1, 2, 0, 0, 0, 0, time.UTC)
	lat, lon := 50.06, 19.94
	e := core.HazardEvent{
		Source:      "meteoalarm",
		SourceID:    "2.49.0.1.616.0.DEU",
		Category:    "met",
		Event:       "Rain",
		Severity:    "orange",
		Headline:    "Heavy rain expected",
		EffectiveAt: &eff,
		ExpiresAt:   &exp,
		Latitude:    &lat,
		Longitude:   &lon,
		Areas:       []string{"DE-NW", "DE-RP"},
		Status:      core.StatusActive,
	}
	e.Normalize()
	return e
}

func ingestOne(t *testing.T, s *Store, e core.HazardEvent) (storage.Outcome, *storage.Change) {
	t.Helper()
	outcome, change, err := s.Ingest(context.Background(), e, core.Fingerprint(e))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	return outcome, change
}

func TestOpenInitializesSchema(t *testing.T) {
	store := openTemp(t)
	var version int
	if err := store.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != len(migrations) {
		t.Errorf("schema version = %d, want %d", version, len(migrations))
	}
}

func TestOpenMigrationInfo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	store, info, err := Open(path)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if info.From != 0 || info.To != len(migrations) {
		t.Errorf("fresh open info = %+v, want {From:0 To:%d}", info, len(migrations))
	}
	store.Close()

	store2, info2, err := Open(path)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	store2.Close()
	if info2.From != len(migrations) || info2.To != len(migrations) {
		t.Errorf("reopen info = %+v, want no migration", info2)
	}
}

// TestMigrationV2BackfillsLegacyExpiry creates a v1 database by hand,
// inserts a row with a text expires_at, then opens it and verifies the
// integer column is backfilled.
func TestMigrationV2BackfillsLegacyExpiry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := db.Exec(migrations[0].SQL); err != nil {
		t.Fatalf("apply v1: %v", err)
	}
	expiry := time.Date(2026, 6, 1, 12, 30, 45, 123456789, time.UTC)
	_, err = db.Exec(fmt.Sprintf(`
		INSERT INTO events (event_key, source, source_id, fingerprint, status, event, expires_at, received_at, first_seen_at, last_seen_at, updated_at)
		VALUES ('src:1', 'src', '1', 'fp', 'active', 'E', '%s', '%s', '%s', '%s', '%s')`,
		expiry.Format(time.RFC3339Nano),
		expiry.Format(time.RFC3339Nano),
		expiry.Format(time.RFC3339Nano),
		expiry.Format(time.RFC3339Nano),
		expiry.Format(time.RFC3339Nano)))
	if err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}
	if _, err := db.Exec("PRAGMA user_version = 1"); err != nil {
		t.Fatalf("set version: %v", err)
	}
	db.Close()

	store := openTempAt(t, path)
	got, err := store.Get(context.Background(), "src:1")
	if err != nil {
		t.Fatalf("Get after migration: %v", err)
	}
	if got.Event.ExpiresAt == nil || !got.Event.ExpiresAt.Equal(expiry.Truncate(time.Millisecond)) {
		t.Errorf("expires_at after backfill = %v, want ~%v", got.Event.ExpiresAt, expiry)
	}
}

func openTempAt(t *testing.T, path string) *Store {
	t.Helper()
	store, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// TestIngestTransitionMatrix covers the lifecycle semantics table-driven.
func TestIngestTransitionMatrix(t *testing.T) {
	cases := []struct {
		name         string
		steps        []core.HazardEvent
		wantOutcomes []storage.Outcome
		wantTypes    []core.ChangeType
		wantFinal    core.EventStatus
	}{
		{
			name:         "unknown active -> new",
			steps:        []core.HazardEvent{normEvent()},
			wantOutcomes: []storage.Outcome{storage.OutcomeNew},
			wantTypes:    []core.ChangeType{core.ChangeNew},
			wantFinal:    core.StatusActive,
		},
		{
			name: "unknown cancelled is cancelled, not new",
			steps: func() []core.HazardEvent {
				e := normEvent()
				e.Status = core.StatusCancelled
				return []core.HazardEvent{e}
			}(),
			wantOutcomes: []storage.Outcome{storage.OutcomeCancelled},
			wantTypes:    []core.ChangeType{core.ChangeCancelled},
			wantFinal:    core.StatusCancelled,
		},
		{
			name: "identical active -> duplicate",
			steps: func() []core.HazardEvent {
				e := normEvent()
				return []core.HazardEvent{e, e}
			}(),
			wantOutcomes: []storage.Outcome{storage.OutcomeNew, storage.OutcomeDuplicate},
			wantTypes:    []core.ChangeType{core.ChangeNew},
			wantFinal:    core.StatusActive,
		},
		{
			name: "active -> updated",
			steps: func() []core.HazardEvent {
				e := normEvent()
				e2 := e.Clone()
				e2.Severity = "red"
				return []core.HazardEvent{e, e2}
			}(),
			wantOutcomes: []storage.Outcome{storage.OutcomeNew, storage.OutcomeUpdated},
			wantTypes:    []core.ChangeType{core.ChangeNew, core.ChangeUpdated},
			wantFinal:    core.StatusActive,
		},
		{
			name: "active -> cancelled",
			steps: func() []core.HazardEvent {
				e := normEvent()
				e2 := e.Clone()
				e2.Status = core.StatusCancelled
				return []core.HazardEvent{e, e2}
			}(),
			wantOutcomes: []storage.Outcome{storage.OutcomeNew, storage.OutcomeCancelled},
			wantTypes:    []core.ChangeType{core.ChangeNew, core.ChangeCancelled},
			wantFinal:    core.StatusCancelled,
		},
		{
			name: "repeated cancellation -> duplicate",
			steps: func() []core.HazardEvent {
				e := normEvent()
				e2 := e.Clone()
				e2.Status = core.StatusCancelled
				return []core.HazardEvent{e, e2, e2.Clone()}
			}(),
			wantOutcomes: []storage.Outcome{storage.OutcomeNew, storage.OutcomeCancelled, storage.OutcomeDuplicate},
			wantTypes:    []core.ChangeType{core.ChangeNew, core.ChangeCancelled},
			wantFinal:    core.StatusCancelled,
		},
		{
			name: "cancelled + active again -> updated (re-activation)",
			steps: func() []core.HazardEvent {
				e := normEvent()
				cancelled := e.Clone()
				cancelled.Status = core.StatusCancelled
				return []core.HazardEvent{e, cancelled, e.Clone()}
			}(),
			wantOutcomes: []storage.Outcome{storage.OutcomeNew, storage.OutcomeCancelled, storage.OutcomeUpdated},
			wantTypes:    []core.ChangeType{core.ChangeNew, core.ChangeCancelled, core.ChangeUpdated},
			wantFinal:    core.StatusActive,
		},
		{
			name: "expired + same source event -> updated (re-activation)",
			steps: func() []core.HazardEvent {
				e := normEvent()
				past := time.Now().Add(-time.Hour)
				e.ExpiresAt = &past
				again := e.Clone()
				return []core.HazardEvent{e, again}
			}(),
			wantOutcomes: []storage.Outcome{storage.OutcomeNew, storage.OutcomeUpdated},
			wantTypes:    []core.ChangeType{core.ChangeNew, core.ChangeUpdated},
			wantFinal:    core.StatusActive,
		},
		{
			name: "expired + cancellation -> cancelled",
			steps: func() []core.HazardEvent {
				e := normEvent()
				past := time.Now().Add(-time.Hour)
				e.ExpiresAt = &past
				cancelled := e.Clone()
				cancelled.Status = core.StatusCancelled
				return []core.HazardEvent{e, cancelled}
			}(),
			wantOutcomes: []storage.Outcome{storage.OutcomeNew, storage.OutcomeCancelled},
			wantTypes:    []core.ChangeType{core.ChangeNew, core.ChangeCancelled},
			wantFinal:    core.StatusCancelled,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := openTemp(t)
			for i, step := range tc.steps {
				outcome, change := ingestOne(t, store, step)
				if outcome != tc.wantOutcomes[i] {
					t.Fatalf("step %d: outcome = %v, want %v", i, outcome, tc.wantOutcomes[i])
				}
				if i < len(tc.wantTypes) {
					if change == nil || change.ChangeType != tc.wantTypes[i] {
						t.Fatalf("step %d: change = %+v, want type %v", i, change, tc.wantTypes[i])
					}
					if change.ID == 0 {
						t.Fatalf("step %d: change has no journal ID", i)
					}
				} else if change != nil {
					t.Fatalf("step %d: expected no change, got %+v", i, change)
				}
			}
			got, err := store.Get(context.Background(), tc.steps[0].Key())
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.Event.Status != tc.wantFinal {
				t.Errorf("final status = %v, want %v", got.Event.Status, tc.wantFinal)
			}
		})
	}
}

// TestExpireJournalsChanges verifies the durable path: expiration creates a
// ChangeExpired record in the same transaction and a restart redelivers it.
func TestExpireJournalsChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	store := openTempAt(t, path)

	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	e := normEvent()
	e.SourceID = "expiring"
	e.ExpiresAt = &past
	if outcome, _ := ingestOne(t, store, e); outcome != storage.OutcomeNew {
		t.Fatalf("outcome = %v", outcome)
	}

	changes, err := store.Expire(context.Background(), now)
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(changes))
	}
	if changes[0].ChangeType != core.ChangeExpired || changes[0].ID == 0 {
		t.Fatalf("unexpected change: %+v", changes[0])
	}

	// Events without expiry stay active.
	noExpiry := normEvent()
	noExpiry.SourceID = "no-expiry"
	if outcome, _ := ingestOne(t, store, noExpiry); outcome != storage.OutcomeNew {
		t.Fatalf("outcome = %v", outcome)
	}

	// Restart: the journaled change is still deliverable.
	store.Close()
	store2 := openTempAt(t, path)
	polled, err := store2.PollChanges(context.Background(), "out-a", 10)
	if err != nil {
		t.Fatalf("PollChanges: %v", err)
	}
	if len(polled) != 3 { // new (expiring), expired, new (no-expiry)
		t.Fatalf("polled = %d, want 3", len(polled))
	}
	if polled[0].ChangeType != core.ChangeNew || polled[1].ChangeType != core.ChangeExpired || polled[2].ChangeType != core.ChangeNew {
		t.Fatalf("unexpected poll order: %+v", polled)
	}
	if polled[1].Event.SourceID != "expiring" {
		t.Fatalf("expired change carries wrong event: %+v", polled[1].Event)
	}
}

func TestJournalCursorsIndependentAndAtLeastOnce(t *testing.T) {
	store := openTemp(t)

	e := normEvent()
	outcome, change := ingestOne(t, store, e)
	if outcome != storage.OutcomeNew || change == nil {
		t.Fatalf("ingest = %v %+v", outcome, change)
	}

	// Output A acknowledges; output B does not.
	if err := store.AckChanges(context.Background(), "out-a", change.ID); err != nil {
		t.Fatalf("AckChanges: %v", err)
	}

	// A: nothing left; B: still pending.
	nextA, err := store.PollChanges(context.Background(), "out-a", 10)
	if err != nil {
		t.Fatalf("PollChanges A: %v", err)
	}
	if len(nextA) != 0 {
		t.Fatalf("A polled %d changes, want 0", len(nextA))
	}
	nextB, err := store.PollChanges(context.Background(), "out-b", 10)
	if err != nil {
		t.Fatalf("PollChanges B: %v", err)
	}
	if len(nextB) != 1 || nextB[0].ID != change.ID {
		t.Fatalf("B polled %+v, want the unacked change", nextB)
	}

	// Ack is monotonic.
	if err := store.AckChanges(context.Background(), "out-b", change.ID-1); err != nil {
		t.Fatalf("AckChanges lower: %v", err)
	}
	nextB, _ = store.PollChanges(context.Background(), "out-b", 10)
	if len(nextB) != 1 {
		t.Fatalf("B polled %d after lower ack, want 1 (monotonic)", len(nextB))
	}
}

func TestCleanupChanges(t *testing.T) {
	store := openTemp(t)

	e := normEvent()
	_, change := ingestOne(t, store, e)
	if err := store.AckChanges(context.Background(), "out-a", change.ID); err != nil {
		t.Fatalf("AckChanges: %v", err)
	}

	// Not old enough yet: nothing deleted.
	n, err := store.CleanupChanges(context.Background(), time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("CleanupChanges: %v", err)
	}
	if n != 0 {
		t.Fatalf("deleted %d rows, want 0 (too fresh)", n)
	}

	// Old enough: acknowledged rows are deleted.
	n, err = store.CleanupChanges(context.Background(), time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("CleanupChanges: %v", err)
	}
	if n != 1 {
		t.Fatalf("deleted %d rows, want 1", n)
	}
}

func TestPendingStats(t *testing.T) {
	store := openTemp(t)

	e := normEvent()
	_, change := ingestOne(t, store, e)

	pending, oldest, err := store.PendingStats(context.Background())
	if err != nil {
		t.Fatalf("PendingStats: %v", err)
	}
	if pending != 1 {
		t.Errorf("pending = %d, want 1", pending)
	}
	if oldest < 0 {
		t.Errorf("oldest age = %v, want non-negative", oldest)
	}

	if err := store.AckChanges(context.Background(), "out-a", change.ID); err != nil {
		t.Fatalf("AckChanges: %v", err)
	}
	pending, _, err = store.PendingStats(context.Background())
	if err != nil {
		t.Fatalf("PendingStats: %v", err)
	}
	if pending != 0 {
		t.Errorf("pending = %d, want 0 after ack", pending)
	}
}

// TestTimestampsMs verifies millisecond precision expiry boundaries.
func TestTimestampsMs(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	// Whole second.
	e := normEvent()
	e.SourceID = "whole-second"
	e.ExpiresAt = &base
	ingestOne(t, store, e)

	// Millisecond precision.
	e2 := normEvent()
	e2.SourceID = "millisecond"
	exp2 := base.Add(500 * time.Millisecond)
	e2.ExpiresAt = &exp2
	ingestOne(t, store, e2)

	// Nanosecond precision (stored truncated to ms).
	e3 := normEvent()
	e3.SourceID = "nanosecond"
	exp3 := base.Add(500*time.Millisecond + 999*time.Microsecond)
	e3.ExpiresAt = &exp3
	ingestOne(t, store, e3)

	// No expiry.
	e4 := normEvent()
	e4.SourceID = "nil"
	e4.ExpiresAt = nil
	ingestOne(t, store, e4)

	changes, err := store.Expire(ctx, base)
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if len(changes) != 1 || changes[0].Event.SourceID != "whole-second" {
		t.Fatalf("expired at boundary = %+v, want only whole-second", changes)
	}

	changes, err = store.Expire(ctx, base.Add(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	got := map[string]bool{}
	for _, c := range changes {
		got[c.Event.SourceID] = true
	}
	if !got["millisecond"] || !got["nanosecond"] {
		t.Fatalf("expired at ms boundary = %+v", changes)
	}
	if got["nil"] {
		t.Fatal("event without expiry was expired")
	}

	// Second expiry run is idempotent (nothing new to expire).
	changes, err = store.Expire(ctx, base.Add(time.Hour))
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("second expiration produced %d changes, want 0", len(changes))
	}
}

func TestConcurrentIngestIdenticalNew(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	e := normEvent()
	const workers = 20
	outcomes := make([]storage.Outcome, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			outcomes[i], _, errs[i] = store.Ingest(ctx, e.Clone(), core.Fingerprint(e))
		}(i)
	}
	wg.Wait()

	newCount := 0
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		switch outcomes[i] {
		case storage.OutcomeNew:
			newCount++
		case storage.OutcomeDuplicate:
		default:
			t.Errorf("worker %d outcome = %v", i, outcomes[i])
		}
	}
	if newCount != 1 {
		t.Errorf("new outcomes = %d, want exactly 1", newCount)
	}
	if n, err := store.Count(ctx); err != nil || n != 1 {
		t.Errorf("rows = %d, %v; want 1", n, err)
	}
}

func TestConcurrentIngestUpdateRace(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	e := normEvent()
	if outcome, _ := ingestOne(t, store, e); outcome != storage.OutcomeNew {
		t.Fatalf("seed = %v", outcome)
	}

	updated := e.Clone()
	updated.Severity = "red"
	const workers = 10
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := store.Ingest(ctx, updated.Clone(), core.Fingerprint(updated)); err != nil {
				t.Errorf("concurrent update: %v", err)
			}
		}()
	}
	wg.Wait()

	got, err := store.Get(ctx, e.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Severity != "red" || got.Event.Status != core.StatusActive {
		t.Errorf("final state = %+v", got.Event)
	}
}

func TestGetMissingReturnsErrNotFound(t *testing.T) {
	store := openTemp(t)
	_, err := store.Get(context.Background(), "nope:1")
	if err != storage.ErrNotFound {
		t.Errorf("Get missing = %v, want storage.ErrNotFound", err)
	}
}
