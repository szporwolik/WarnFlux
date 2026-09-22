package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
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

// refs builds OutputRef entries (each with a distinct type) for the given
// output IDs, mirroring how main computes the enabled output set.
func refs(ids ...string) []storage.OutputRef {
	out := make([]storage.OutputRef, len(ids))
	for i, id := range ids {
		out[i] = storage.OutputRef{ID: id, Type: "type-" + id}
	}
	return out
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

func openTempAt(t *testing.T, path string, opts ...Option) *Store {
	t.Helper()
	store, _, err := Open(path, opts...)
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
			name: "lapsed expiry + identical re-ingest -> duplicate (no flapping)",
			steps: func() []core.HazardEvent {
				e := normEvent()
				past := time.Now().Add(-time.Hour)
				e.ExpiresAt = &past
				again := e.Clone()
				return []core.HazardEvent{e, again}
			}(),
			wantOutcomes: []storage.Outcome{storage.OutcomeNew, storage.OutcomeDuplicate},
			wantTypes:    []core.ChangeType{core.ChangeNew},
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

	// Ack is monotonic: acking below the current cursor cannot rewind it.
	// (AckChanges also rejects non-positive and beyond-max IDs.)
	e2 := e.Clone()
	e2.Severity = "severe"
	_, change2 := ingestOne(t, store, e2)
	if err := store.AckChanges(context.Background(), "out-b", change2.ID); err != nil {
		t.Fatalf("AckChanges higher: %v", err)
	}
	if err := store.AckChanges(context.Background(), "out-b", change.ID); err != nil {
		t.Fatalf("AckChanges lower: %v", err)
	}
	nextB, _ = store.PollChanges(context.Background(), "out-b", 10)
	if len(nextB) != 0 {
		t.Fatalf("B polled %d after full ack, want 0", len(nextB))
	}
	if err := store.AckChanges(context.Background(), "out-b", 0); err == nil {
		t.Fatal("AckChanges with 0 must be rejected")
	}
	if err := store.AckChanges(context.Background(), "out-b", change2.ID+1000); err == nil {
		t.Fatal("AckChanges beyond the journal must be rejected")
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
	ctx := context.Background()

	e := normEvent()
	_, change := ingestOne(t, store, e)

	// No enabled outputs: nothing is pending. Retained changes are history,
	// cleaned up by age, not an ever-growing backlog.
	pending, _, err := store.PendingStats(ctx)
	if err != nil {
		t.Fatalf("PendingStats: %v", err)
	}
	if pending != 0 {
		t.Errorf("pending = %d with zero outputs, want 0", pending)
	}

	// An enabled output with cursor 0 sees the retained change as pending.
	if err := store.SyncOutputs(ctx, refs("out-a")); err != nil {
		t.Fatalf("SyncOutputs: %v", err)
	}
	pending, oldest, err := store.PendingStats(ctx)
	if err != nil {
		t.Fatalf("PendingStats: %v", err)
	}
	if pending != 1 {
		t.Errorf("pending = %d, want 1", pending)
	}
	if oldest < 0 {
		t.Errorf("oldest age = %v, want non-negative", oldest)
	}

	if err := store.AckChanges(ctx, "out-a", change.ID); err != nil {
		t.Fatalf("AckChanges: %v", err)
	}
	pending, _, err = store.PendingStats(ctx)
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

// TestCleanupEvents verifies current-state retention semantics: cancelled/
// expired records are deleted only when the provider has NOT been observed
// (last_seen_at_ms) for event_retention — updated_at is irrelevant here.
// A provider that keeps repeating a stale event keeps it retained.
func TestCleanupEvents(t *testing.T) {
	clock := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	store := openTemp(t, WithClock(func() time.Time { return clock }))
	ctx := context.Background()

	// Active event ingested first: must survive any cleanup.
	active := normEvent()
	active.SourceID = "active-keep"
	if outcome, _ := ingestOne(t, store, active); outcome != storage.OutcomeNew {
		t.Fatalf("active = %v", outcome)
	}

	// Old cancelled event.
	oldCancelled := normEvent()
	oldCancelled.SourceID = "old-cancelled"
	oldCancelled.Status = core.StatusCancelled
	if outcome, _ := ingestOne(t, store, oldCancelled); outcome != storage.OutcomeCancelled {
		t.Fatalf("old cancelled = %v", outcome)
	}

	// Old expired event (expired via maintenance).
	oldExpired := normEvent()
	oldExpired.SourceID = "old-expired"
	exp := clock
	oldExpired.ExpiresAt = &exp
	if outcome, _ := ingestOne(t, store, oldExpired); outcome != storage.OutcomeNew {
		t.Fatalf("old expired = %v", outcome)
	}
	if _, err := store.Expire(ctx, clock); err != nil {
		t.Fatalf("Expire: %v", err)
	}

	// 29 days later the provider repeats the identical stale events:
	// last_seen advances; retention (30d) must keep them.
	clock = clock.Add(29 * 24 * time.Hour)
	if outcome, _ := ingestOne(t, store, oldCancelled.Clone()); outcome != storage.OutcomeDuplicate {
		t.Fatalf("repeat cancelled = %v, want duplicate", outcome)
	}
	if outcome, _ := ingestOne(t, store, oldExpired.Clone()); outcome != storage.OutcomeDuplicate {
		t.Fatalf("repeat expired = %v, want duplicate", outcome)
	}

	// 31 days after the original observation (2 days after the repeats):
	// cleanup with 30d retention must keep them (last_seen is recent).
	clock = clock.Add(2 * 24 * time.Hour)
	n, err := store.CleanupEvents(ctx, clock.Add(-30*24*time.Hour))
	if err != nil {
		t.Fatalf("CleanupEvents: %v", err)
	}
	if n != 0 {
		t.Fatalf("deleted %d events while provider still repeats them, want 0", n)
	}

	// Provider disappears: advance >30d past the last observation.
	clock = clock.Add(31 * 24 * time.Hour)
	n, err = store.CleanupEvents(ctx, clock.Add(-30*24*time.Hour))
	if err != nil {
		t.Fatalf("CleanupEvents: %v", err)
	}
	if n != 2 {
		t.Fatalf("deleted %d events, want 2 (cancelled + expired)", n)
	}
	for _, key := range []string{oldCancelled.Key(), oldExpired.Key()} {
		if _, err := store.Get(ctx, key); err != storage.ErrNotFound {
			t.Errorf("event %q should have been deleted, got %v", key, err)
		}
	}
	if _, err := store.Get(ctx, active.Key()); err != nil {
		t.Errorf("active event should never be deleted: %v", err)
	}
}

// TestCleanupEventsBoundary pins the cutoff comparison: last_seen_at_ms equal
// to the cutoff is RETAINED (only strictly older is deleted).
func TestCleanupEventsBoundary(t *testing.T) {
	clock := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	store := openTemp(t, WithClock(func() time.Time { return clock }))
	ctx := context.Background()

	old := normEvent()
	old.SourceID = "older-than-cutoff"
	old.Status = core.StatusCancelled
	if outcome, _ := ingestOne(t, store, old); outcome != storage.OutcomeCancelled {
		t.Fatalf("ingest = %v", outcome)
	}
	// Ingest the boundary event exactly 24h later.
	clock = clock.Add(24 * time.Hour)
	atBoundary := normEvent()
	atBoundary.SourceID = "at-cutoff"
	atBoundary.Status = core.StatusCancelled
	if outcome, _ := ingestOne(t, store, atBoundary); outcome != storage.OutcomeCancelled {
		t.Fatalf("ingest = %v", outcome)
	}
	// 1h later: cutoff = 24h ago = the boundary event's exact last_seen.
	clock = clock.Add(time.Hour)
	n, err := store.CleanupEvents(ctx, clock.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("CleanupEvents: %v", err)
	}
	if n != 1 {
		t.Fatalf("deleted %d events, want 1 (only the strictly-older one)", n)
	}
	if _, err := store.Get(ctx, old.Key()); err != storage.ErrNotFound {
		t.Errorf("event older than the cutoff must be deleted: %v", err)
	}
	if _, err := store.Get(ctx, atBoundary.Key()); err != nil {
		t.Errorf("event exactly at the cutoff should be retained: %v", err)
	}
}

// TestCleanupEventsLastSeenSurvivesRestart verifies last_seen_at_ms survives a
// database reopen and retention behavior remains correct afterwards.
func TestCleanupEventsLastSeenSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	clock := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	store := openTempAt(t, path, WithClock(func() time.Time { return clock }))
	ctx := context.Background()

	e := normEvent()
	e.SourceID = "restart-retention"
	e.Status = core.StatusCancelled
	if outcome, _ := ingestOne(t, store, e); outcome != storage.OutcomeCancelled {
		t.Fatalf("ingest = %v", outcome)
	}
	clock = clock.Add(30 * 24 * time.Hour)
	store.Close()

	// Reopen (migrations idempotent at v4) and cleanup: not yet old enough.
	store2 := openTempAt(t, path, WithClock(func() time.Time { return clock }))
	n, err := store2.CleanupEvents(ctx, clock.Add(-31*24*time.Hour))
	if err != nil {
		t.Fatalf("CleanupEvents: %v", err)
	}
	if n != 0 {
		t.Fatalf("deleted %d events, want 0 (30d < 31d cutoff)", n)
	}

	// Past the cutoff after restart: deleted.
	n, err = store2.CleanupEvents(ctx, clock.Add(-29*24*time.Hour))
	if err != nil {
		t.Fatalf("CleanupEvents: %v", err)
	}
	if n != 1 {
		t.Fatalf("deleted %d events, want 1", n)
	}
}

func TestGetMissingReturnsErrNotFound(t *testing.T) {
	store := openTemp(t)
	_, err := store.Get(context.Background(), "nope:1")
	if err != storage.ErrNotFound {
		t.Errorf("Get missing = %v, want storage.ErrNotFound", err)
	}
}

// ---- regression tests: cursor configuration semantics ----

func cursorRow(t *testing.T, s *Store, outputID string) (exists bool, lastAcked int64) {
	t.Helper()
	var id int64
	err := s.db.QueryRow("SELECT last_acked_id FROM output_cursors WHERE output_id = ?", outputID).Scan(&id)
	if err == sql.ErrNoRows {
		return false, 0
	}
	if err != nil {
		t.Fatalf("read cursor: %v", err)
	}
	return true, id
}

// TestCursorConfigurationSemantics pins: enabled → cursor exists; disabled
// → cursor removed; re-enabled → cursor 0 (replays retained journal);
// renamed → old cursor removed, new cursor 0; same ID (different type) →
// cursor preserved — the ID, not the plugin type, is the durable consumer
// identity.
func TestCursorConfigurationSemantics(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	e := normEvent()
	_, change := ingestOne(t, store, e)

	// Enabled → cursor exists at 0.
	if err := store.SyncOutputs(ctx, refs("out-a")); err != nil {
		t.Fatalf("SyncOutputs: %v", err)
	}
	if exists, acked := cursorRow(t, store, "out-a"); !exists || acked != 0 {
		t.Fatalf("out-a cursor = %v/%d, want exists at 0", exists, acked)
	}

	// Disabled → cursor removed.
	if err := store.SyncOutputs(ctx, nil); err != nil {
		t.Fatalf("SyncOutputs: %v", err)
	}
	if exists, _ := cursorRow(t, store, "out-a"); exists {
		t.Fatal("disabled output still has a cursor")
	}

	// Re-enabled → cursor 0 and it replays the retained journal.
	if err := store.SyncOutputs(ctx, refs("out-a")); err != nil {
		t.Fatalf("SyncOutputs: %v", err)
	}
	polled, err := store.PollChanges(ctx, "out-a", 10)
	if err != nil {
		t.Fatalf("PollChanges: %v", err)
	}
	if len(polled) != 1 || polled[0].ID != change.ID {
		t.Fatalf("re-enabled output polled %+v, want the retained change", polled)
	}

	// Renamed → old cursor removed, new cursor 0.
	if err := store.AckChanges(ctx, "out-a", change.ID); err != nil {
		t.Fatalf("AckChanges: %v", err)
	}
	if err := store.SyncOutputs(ctx, refs("out-b")); err != nil {
		t.Fatalf("SyncOutputs: %v", err)
	}
	if exists, _ := cursorRow(t, store, "out-a"); exists {
		t.Fatal("renamed-away output still has a cursor")
	}
	if exists, acked := cursorRow(t, store, "out-b"); !exists || acked != 0 {
		t.Fatalf("out-b cursor = %v/%d, want exists at 0", exists, acked)
	}

	// Same ID with a DIFFERENT plugin type: a new durable consumer. The
	// cursor is reset to 0 so the new destination replays retained history
	// instead of inheriting another destination's progress.
	if err := store.AckChanges(ctx, "out-b", change.ID); err != nil {
		t.Fatalf("AckChanges: %v", err)
	}
	if err := store.SyncOutputs(ctx, []storage.OutputRef{{ID: "out-b", Type: "webhook"}}); err != nil {
		t.Fatalf("SyncOutputs: %v", err)
	}
	if exists, acked := cursorRow(t, store, "out-b"); !exists || acked != 0 {
		t.Fatalf("out-b cursor after type change = %v/%d, want reset to 0", exists, acked)
	}
	polled, err = store.PollChanges(ctx, "out-b", 10)
	if err != nil {
		t.Fatalf("PollChanges: %v", err)
	}
	if len(polled) != 1 || polled[0].ID != change.ID {
		t.Fatalf("type-changed output polled %+v, want the retained change", polled)
	}
}

// TestPollChangesCorruptedSnapshotErrors: invalid snapshot JSON must be a
// PollChanges error — no panic, no silent skip, no cursor advancement.
func TestPollChangesCorruptedSnapshotErrors(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	e := normEvent()
	_, change := ingestOne(t, store, e)
	if _, err := store.db.Exec("UPDATE changes SET event_snapshot = 'not-json' WHERE id = ?", change.ID); err != nil {
		t.Fatalf("corrupt snapshot: %v", err)
	}

	if _, err := store.PollChanges(ctx, "out", 10); err == nil {
		t.Fatal("PollChanges with corrupted snapshot must return an error")
	}
	if exists, acked := cursorRow(t, store, "out"); exists && acked != 0 {
		t.Fatalf("cursor advanced to %d after failed poll, want no advancement", acked)
	}
}

// TestPollChangesUnknownChangeTypeRejected: a garbage change_type must not
// be silently emitted as an invalid public wire value.
func TestPollChangesUnknownChangeTypeRejected(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	e := normEvent()
	_, change := ingestOne(t, store, e)
	if _, err := store.db.Exec("UPDATE changes SET change_type = 'bogus' WHERE id = ?", change.ID); err != nil {
		t.Fatalf("corrupt change_type: %v", err)
	}
	if _, err := store.PollChanges(ctx, "out", 10); err == nil {
		t.Fatal("PollChanges with unknown change_type must return an error")
	}
	if exists, acked := cursorRow(t, store, "out"); exists && acked != 0 {
		t.Fatalf("cursor advanced to %d after failed poll, want no advancement", acked)
	}
}

// TestPollChangesSnapshotKeyMismatchRejected: a snapshot whose
// source:source_id does not match the journal event_key is corruption and
// must not be delivered silently.
func TestPollChangesSnapshotKeyMismatchRejected(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	e := normEvent()
	_, change := ingestOne(t, store, e)
	other := normEvent()
	other.SourceID = "different-id"
	data, err := json.Marshal(storage.SnapshotOf(other))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := store.db.Exec("UPDATE changes SET event_snapshot = ? WHERE id = ?", string(data), change.ID); err != nil {
		t.Fatalf("corrupt snapshot key: %v", err)
	}
	if _, err := store.PollChanges(ctx, "out", 10); err == nil {
		t.Fatal("PollChanges with mismatched snapshot key must return an error")
	}
}

// TestJournalSnapshotsIndependentOfEvents: deleting the current-state event
// never damages historical journal delivery (immutable snapshots).
func TestJournalSnapshotsIndependentOfEvents(t *testing.T) {
	clock := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	store := openTemp(t, WithClock(func() time.Time { return clock }))
	ctx := context.Background()

	e := normEvent()
	e.SourceID = "snap-independent"
	exp := clock
	e.ExpiresAt = &exp
	if outcome, _ := ingestOne(t, store, e); outcome != storage.OutcomeNew {
		t.Fatalf("new = %v", outcome)
	}
	if _, err := store.Expire(ctx, clock); err != nil {
		t.Fatalf("Expire: %v", err)
	}

	// Age out the current-state record; the journal remains unacked.
	clock = clock.Add(31 * 24 * time.Hour)
	if n, err := store.CleanupEvents(ctx, clock.Add(-30*24*time.Hour)); err != nil || n != 1 {
		t.Fatalf("CleanupEvents = %d, %v; want 1 deletion", n, err)
	}
	if _, err := store.Get(ctx, e.Key()); err != storage.ErrNotFound {
		t.Fatalf("current-state event should be gone: %v", err)
	}

	polled, err := store.PollChanges(ctx, "out", 10)
	if err != nil {
		t.Fatalf("PollChanges: %v", err)
	}
	if len(polled) != 2 {
		t.Fatalf("polled %d changes after event deletion, want 2 snapshots", len(polled))
	}
	if polled[0].ChangeType != core.ChangeNew || polled[0].Event.Status != core.StatusActive {
		t.Errorf("new snapshot damaged: %+v", polled[0])
	}
	if polled[1].ChangeType != core.ChangeExpired || polled[1].Event.Status != core.StatusExpired {
		t.Errorf("expired snapshot damaged: %+v", polled[1])
	}
}

// ---- regression tests: immutable journal snapshots ----

// TestJournalSnapshotsAreHistorical is the CRITICAL snapshot test: NEW
// (moderate) → UPDATED (severe) → CANCELLED must replay each record with
// the event state AT THE TIME of its transition, never the current state.
func TestJournalSnapshotsAreHistorical(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	moderate := normEvent()
	moderate.SourceID = "snap"
	moderate.Severity = "moderate"
	if outcome, _ := ingestOne(t, store, moderate); outcome != storage.OutcomeNew {
		t.Fatalf("new = %v", outcome)
	}
	severe := moderate.Clone()
	severe.Severity = "severe"
	if outcome, _ := ingestOne(t, store, severe); outcome != storage.OutcomeUpdated {
		t.Fatalf("updated = %v", outcome)
	}
	cancelled := severe.Clone()
	cancelled.Status = core.StatusCancelled
	if outcome, _ := ingestOne(t, store, cancelled); outcome != storage.OutcomeCancelled {
		t.Fatalf("cancelled = %v", outcome)
	}

	polled, err := store.PollChanges(ctx, "out", 10)
	if err != nil {
		t.Fatalf("PollChanges: %v", err)
	}
	if len(polled) != 3 {
		t.Fatalf("polled %d changes, want 3", len(polled))
	}
	if polled[0].ChangeType != core.ChangeNew || polled[0].Event.Severity != "moderate" || polled[0].Event.Status != core.StatusActive {
		t.Errorf("#1 = %+v, want new moderate active", polled[0])
	}
	if polled[1].ChangeType != core.ChangeUpdated || polled[1].Event.Severity != "severe" || polled[1].Event.Status != core.StatusActive {
		t.Errorf("#2 = %+v, want updated severe active", polled[1])
	}
	if polled[2].ChangeType != core.ChangeCancelled || polled[2].Event.Severity != "severe" || polled[2].Event.Status != core.StatusCancelled {
		t.Errorf("#3 = %+v, want cancelled severe", polled[2])
	}
}

// TestJournalSnapshotsNewAndExpiredDistinct is mandatory: the NEW record
// must keep status active in its snapshot while the EXPIRED record carries
// the expired state.
func TestJournalSnapshotsNewAndExpiredDistinct(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()
	now := time.Now().UTC()
	past := now.Add(-time.Hour)

	e := normEvent()
	e.SourceID = "new-expire"
	e.ExpiresAt = &past
	if outcome, _ := ingestOne(t, store, e); outcome != storage.OutcomeNew {
		t.Fatalf("new = %v", outcome)
	}
	if _, err := store.Expire(ctx, now); err != nil {
		t.Fatalf("Expire: %v", err)
	}

	polled, err := store.PollChanges(ctx, "out", 10)
	if err != nil {
		t.Fatalf("PollChanges: %v", err)
	}
	if len(polled) != 2 {
		t.Fatalf("polled %d changes, want 2", len(polled))
	}
	if polled[0].ChangeType != core.ChangeNew || polled[0].Event.Status != core.StatusActive {
		t.Errorf("#1 = %+v, want new active snapshot", polled[0])
	}
	if polled[1].ChangeType != core.ChangeExpired || polled[1].Event.Status != core.StatusExpired {
		t.Errorf("#2 = %+v, want expired snapshot", polled[1])
	}
}

// TestJournalSnapshotsSurviveRestart verifies the immutable payloads
// survive a database reopen exactly, and that ACKing the first change
// leaves only the second pending after another restart.
func TestJournalSnapshotsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	ctx := context.Background()

	store := openTempAt(t, path)
	e := normEvent()
	e.SourceID = "restart-snap"
	if outcome, _ := ingestOne(t, store, e); outcome != storage.OutcomeNew {
		t.Fatalf("new = %v", outcome)
	}
	u := e.Clone()
	u.Severity = "severe"
	if outcome, _ := ingestOne(t, store, u); outcome != storage.OutcomeUpdated {
		t.Fatalf("updated = %v", outcome)
	}
	store.Close()

	store2 := openTempAt(t, path)
	polled, err := store2.PollChanges(ctx, "out", 10)
	if err != nil {
		t.Fatalf("PollChanges: %v", err)
	}
	if len(polled) != 2 {
		t.Fatalf("after restart polled %d changes, want 2", len(polled))
	}
	if polled[0].Event.Severity != "orange" || polled[0].Event.Status != core.StatusActive {
		t.Errorf("#1 after restart = %+v", polled[0].Event)
	}
	if polled[1].Event.Severity != "severe" || polled[1].Event.Status != core.StatusActive {
		t.Errorf("#2 after restart = %+v", polled[1].Event)
	}

	if err := store2.AckChanges(ctx, "out", polled[0].ID); err != nil {
		t.Fatalf("AckChanges: %v", err)
	}
	secondID := polled[1].ID
	store2.Close()

	store3 := openTempAt(t, path)
	polled, err = store3.PollChanges(ctx, "out", 10)
	if err != nil {
		t.Fatalf("PollChanges: %v", err)
	}
	if len(polled) != 1 || polled[0].ID != secondID {
		t.Fatalf("after ack+restart polled %+v, want only the second change (%d)", polled, secondID)
	}
	if polled[0].Event.Severity != "severe" {
		t.Errorf("remaining snapshot = %+v, want severe", polled[0].Event)
	}
}

// ---- regression tests: output cursor lifecycle ----

// TestSyncOutputsNeverSuccessfulOutputBlocksCleanup covers the data-loss
// window: an output that never ACKed must block cleanup, because its
// cursor (created by SyncOutputs, before any delivery) stays at 0.
func TestSyncOutputsNeverSuccessfulOutputBlocksCleanup(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	if err := store.SyncOutputs(ctx, refs("out-a")); err != nil {
		t.Fatalf("SyncOutputs: %v", err)
	}
	var cursors int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM output_cursors WHERE output_id = 'out-a' AND last_acked_id = 0").Scan(&cursors); err != nil {
		t.Fatalf("read cursor: %v", err)
	}
	if cursors != 1 {
		t.Fatalf("cursor not created before first ACK")
	}

	e := normEvent()
	if outcome, _ := ingestOne(t, store, e); outcome != storage.OutcomeNew {
		t.Fatalf("ingest = %v", outcome)
	}

	// The change is old enough for cleanup but was never acknowledged.
	n, err := store.CleanupChanges(ctx, time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("CleanupChanges: %v", err)
	}
	if n != 0 {
		t.Fatalf("cleaned %d changes, want 0 (unacked changes must remain)", n)
	}
	polled, err := store.PollChanges(ctx, "out-a", 10)
	if err != nil {
		t.Fatalf("PollChanges: %v", err)
	}
	if len(polled) != 1 {
		t.Fatalf("output polled %d changes, want the retained one", len(polled))
	}
}

// TestSyncOutputsRemovedOutputStopsBlockingCleanup: a cursor belonging to
// a removed output is dropped by SyncOutputs and no longer blocks cleanup.
func TestSyncOutputsRemovedOutputStopsBlockingCleanup(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	if err := store.SyncOutputs(ctx, refs("out-a", "out-b")); err != nil {
		t.Fatalf("SyncOutputs: %v", err)
	}
	e := normEvent()
	_, change := ingestOne(t, store, e)
	if err := store.AckChanges(ctx, "out-a", change.ID); err != nil {
		t.Fatalf("AckChanges: %v", err)
	}

	// out-b never acks: cleanup is blocked while it exists.
	n, err := store.CleanupChanges(ctx, time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("CleanupChanges: %v", err)
	}
	if n != 0 {
		t.Fatalf("cleaned %d changes, want 0 while out-b exists", n)
	}

	// Remove out-b from the configured outputs.
	if err := store.SyncOutputs(ctx, refs("out-a")); err != nil {
		t.Fatalf("SyncOutputs: %v", err)
	}
	n, err = store.CleanupChanges(ctx, time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("CleanupChanges: %v", err)
	}
	if n != 1 {
		t.Fatalf("cleaned %d changes after removal, want 1", n)
	}
}

// TestSyncOutputsNewOutputReceivesRetainedChanges: a newly enabled output
// starts at cursor 0 and receives all changes still present in the journal.
func TestSyncOutputsNewOutputReceivesRetainedChanges(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	if err := store.SyncOutputs(ctx, refs("out-a")); err != nil {
		t.Fatalf("SyncOutputs: %v", err)
	}
	e := normEvent()
	_, change := ingestOne(t, store, e)
	if err := store.AckChanges(ctx, "out-a", change.ID); err != nil {
		t.Fatalf("AckChanges: %v", err)
	}

	// Add a new output after the change already exists.
	if err := store.SyncOutputs(ctx, refs("out-a", "out-c")); err != nil {
		t.Fatalf("SyncOutputs: %v", err)
	}
	polled, err := store.PollChanges(ctx, "out-c", 10)
	if err != nil {
		t.Fatalf("PollChanges: %v", err)
	}
	if len(polled) != 1 || polled[0].ID != change.ID {
		t.Fatalf("new output polled %+v, want the retained change %d", polled, change.ID)
	}
}

// ---- regression tests: expired-event lifecycle ----

// TestStaleExpiredEventDoesNotFlap is mandatory: re-ingesting the same
// already-expired provider event many times produces exactly ONE expiration
// transition and zero reactivation loops.
func TestStaleExpiredEventDoesNotFlap(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()
	now := time.Now().UTC()
	past := now.Add(-time.Hour)

	e := normEvent()
	e.SourceID = "stale"
	e.ExpiresAt = &past
	if outcome, _ := ingestOne(t, store, e); outcome != storage.OutcomeNew {
		t.Fatalf("new = %v", outcome)
	}

	changes, err := store.Expire(ctx, now)
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if len(changes) != 1 || changes[0].ChangeType != core.ChangeExpired {
		t.Fatalf("expiration produced %+v, want one expired change", changes)
	}

	for i := 0; i < 10; i++ {
		if outcome, ch := ingestOne(t, store, e.Clone()); outcome != storage.OutcomeDuplicate || ch != nil {
			t.Fatalf("ingest %d = %v %+v, want duplicate with no change", i, outcome, ch)
		}
	}

	polled, err := store.PollChanges(ctx, "out", 50)
	if err != nil {
		t.Fatalf("PollChanges: %v", err)
	}
	if len(polled) != 2 {
		t.Fatalf("journal has %d changes after 10 stale re-ingests, want 2 (new + expired)", len(polled))
	}
	if polled[0].ChangeType != core.ChangeNew || polled[1].ChangeType != core.ChangeExpired {
		t.Fatalf("unexpected journal: %+v", polled)
	}
}

// TestExpiredReactivationRequiresLiveExpiry: reactivation only happens when
// the provider makes the event live again — a future expiry or none at all.
func TestExpiredReactivationRequiresLiveExpiry(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)

	t.Run("expiry moved into the future", func(t *testing.T) {
		store := openTemp(t)
		e := normEvent()
		e.SourceID = "reactivate-future"
		e.ExpiresAt = &past
		ingestOne(t, store, e)
		if _, err := store.Expire(ctx, now); err != nil {
			t.Fatalf("Expire: %v", err)
		}
		if outcome, _ := ingestOne(t, store, e.Clone()); outcome != storage.OutcomeDuplicate {
			t.Fatalf("identical lapsed re-ingest = %v, want duplicate", outcome)
		}
		live := e.Clone()
		live.ExpiresAt = &future
		outcome, ch := ingestOne(t, store, live)
		if outcome != storage.OutcomeUpdated || ch == nil || ch.ChangeType != core.ChangeUpdated {
			t.Fatalf("re-activation = %v %+v, want updated", outcome, ch)
		}
		got, err := store.Get(ctx, e.Key())
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Event.Status != core.StatusActive {
			t.Errorf("status after re-activation = %v, want active", got.Event.Status)
		}
	})

	t.Run("expiry removed", func(t *testing.T) {
		store := openTemp(t)
		e := normEvent()
		e.SourceID = "reactivate-nil"
		e.ExpiresAt = &past
		ingestOne(t, store, e)
		if _, err := store.Expire(ctx, now); err != nil {
			t.Fatalf("Expire: %v", err)
		}
		live := e.Clone()
		live.ExpiresAt = nil
		outcome, ch := ingestOne(t, store, live)
		if outcome != storage.OutcomeUpdated || ch == nil || ch.ChangeType != core.ChangeUpdated {
			t.Fatalf("re-activation = %v %+v, want updated", outcome, ch)
		}
		got, err := store.Get(ctx, e.Key())
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Event.Status != core.StatusActive || got.Event.ExpiresAt != nil {
			t.Errorf("state after re-activation = %+v", got.Event)
		}
	})
}

// TestExpiredContentChangeKeepsExpiredStatus: content changes with a still-
// lapsed expiry persist the new content but keep the lifecycle expired — no
// active/expired flapping.
func TestExpiredContentChangeKeepsExpiredStatus(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()
	now := time.Now().UTC()
	past := now.Add(-time.Hour)

	e := normEvent()
	e.SourceID = "expired-content"
	e.ExpiresAt = &past
	if outcome, _ := ingestOne(t, store, e); outcome != storage.OutcomeNew {
		t.Fatalf("new = %v", outcome)
	}
	if _, err := store.Expire(ctx, now); err != nil {
		t.Fatalf("Expire: %v", err)
	}

	changed := e.Clone()
	changed.Severity = "extreme"
	outcome, ch := ingestOne(t, store, changed)
	if outcome != storage.OutcomeUpdated || ch == nil || ch.ChangeType != core.ChangeUpdated {
		t.Fatalf("content change = %v %+v, want updated", outcome, ch)
	}

	got, err := store.Get(ctx, e.Key())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Event.Severity != "extreme" {
		t.Errorf("severity = %q, want persisted content", got.Event.Severity)
	}
	if got.Event.Status != core.StatusExpired {
		t.Errorf("status = %v, want still expired", got.Event.Status)
	}

	// The updated journal record's snapshot must carry the expired status.
	polled, err := store.PollChanges(ctx, "out", 10)
	if err != nil {
		t.Fatalf("PollChanges: %v", err)
	}
	if len(polled) != 3 { // new, expired, updated (content change, still expired)
		t.Fatalf("journal has %d changes, want 3", len(polled))
	}
	if polled[2].ChangeType != core.ChangeUpdated || polled[2].Event.Status != core.StatusExpired || polled[2].Event.Severity != "extreme" {
		t.Fatalf("unexpected updated snapshot: %+v", polled[2])
	}
}
