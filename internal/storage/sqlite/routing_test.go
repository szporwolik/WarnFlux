package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

func newRoutingStore(t *testing.T) *Store {
	t.Helper()
	store, _, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestGroupRoutingRoundTrip(t *testing.T) {
	store := newRoutingStore(t)

	g, err := store.CreateGroup("spok-ops")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	// A fresh group defaults to the permissive threshold and no channels.
	r, err := store.GroupRouting(g.ID)
	if err != nil {
		t.Fatalf("GroupRouting: %v", err)
	}
	if len(r.Actions) != 0 {
		t.Fatalf("default routing = %+v, want no actions", r)
	}

	err = store.SetGroupRouting(g.ID,
		[]storage.ChannelAssignment{
			{ID: "log-alerts", MinSeverity: "severe"},
			{Source: "imgw-meteo", ID: "log-alerts", MinSeverity: "minor"},    // same ID, other source: kept
			{Source: "imgw-meteo", ID: "log-alerts", MinSeverity: "moderate"}, // duplicate cell: first kept
			{Source: "rso", ID: "sms", MinSeverity: "moderate"},
		})
	if err != nil {
		t.Fatalf("SetGroupRouting: %v", err)
	}

	r, err = store.GroupRouting(g.ID)
	if err != nil {
		t.Fatalf("GroupRouting after set: %v", err)
	}
	wantActions := []storage.ChannelAssignment{
		{ID: "log-alerts", MinSeverity: "severe"},
		{Source: "imgw-meteo", ID: "log-alerts", MinSeverity: "minor"},
		{Source: "rso", ID: "sms", MinSeverity: "moderate"},
	}
	if !reflect.DeepEqual(r.Actions, wantActions) {
		t.Errorf("Actions = %+v, want %+v (deduplicated per source+ID, first severity kept, sorted)", r.Actions, wantActions)
	}

	// Empty assignment clears the matrix but keeps the group.
	if err := store.SetGroupRouting(g.ID, nil); err != nil {
		t.Fatalf("SetGroupRouting clear: %v", err)
	}
	r, err = store.GroupRouting(g.ID)
	if err != nil {
		t.Fatalf("GroupRouting after clear: %v", err)
	}
	if len(r.Actions) != 0 {
		t.Fatalf("routing after clear = %+v, want empty", r)
	}
}

// TestInboxPruneAndBacklog pins the controlled auxiliary-data policy for
// the durable dispatch inbox: unevaluated rows older than the cutoff are
// pruned (stale alerts the staleness gates would suppress anyway), and
// the free-space probe reports the database filesystem for the low-disk
// alarm.
func TestInboxPruneAndBacklog(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	store, _, err := Open(":memory:", WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	ctx := context.Background()

	ev := dispatch.Event{Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{Key: "imgw:1", Source: "imgw"}}
	if _, err := store.AppendEvent(ctx, ev); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if _, err := store.AppendEvent(ctx, ev); err != nil {
		t.Fatalf("AppendEvent 2: %v", err)
	}
	if n, err := store.InboxCount(ctx); err != nil || n != 2 {
		t.Fatalf("backlog = (%d, %v), want (2, nil)", n, err)
	}

	// A cutoff before both rows removes nothing.
	if n, err := store.PruneInbox(ctx, now.Add(-time.Hour)); err != nil || n != 0 {
		t.Fatalf("prune past = (%d, %v), want (0, nil)", n, err)
	}
	// A cutoff after both rows removes everything.
	if n, err := store.PruneInbox(ctx, now.Add(time.Hour)); err != nil || n != 2 {
		t.Fatalf("prune future = (%d, %v), want (2, nil)", n, err)
	}
	if n, err := store.InboxCount(ctx); err != nil || n != 0 {
		t.Fatalf("backlog after prune = (%d, %v), want (0, nil)", n, err)
	}

	free, err := store.FreeBytes(ctx)
	if err != nil {
		t.Fatalf("FreeBytes: %v", err)
	}
	if free <= 0 {
		t.Fatalf("free bytes = %d, want > 0", free)
	}
}

func TestGroupRoutingErrors(t *testing.T) {
	store := newRoutingStore(t)

	if err := store.SetGroupRouting(999, nil); !errors.Is(err, storage.ErrGroupNotFound) {
		t.Fatalf("missing group error = %v, want ErrGroupNotFound", err)
	}
	if _, err := store.GroupRouting(999); !errors.Is(err, storage.ErrGroupNotFound) {
		t.Fatalf("GroupRouting missing = %v, want ErrGroupNotFound", err)
	}

	g, err := store.CreateGroup("rsp")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if err := store.SetGroupRouting(g.ID, []storage.ChannelAssignment{{ID: "a", MinSeverity: "orange"}}); !errors.Is(err, storage.ErrInvalidSeverity) {
		t.Fatalf("invalid action severity error = %v, want ErrInvalidSeverity", err)
	}
	if err := store.SetGroupRouting(g.ID, []storage.ChannelAssignment{{ID: "a", MinSeverity: "EXTREME"}}); !errors.Is(err, storage.ErrInvalidSeverity) {
		t.Fatalf("non-canonical severity error = %v, want ErrInvalidSeverity", err)
	}
}

// TestCommitInboxDeliveryAtomic pins the P1 contract: every delivery job
// and the inbox consumption happen in ONE transaction — either the jobs
// exist durably and the inbox row is gone, or nothing happened. A
// failure (here: a foreign-key violation on an unknown group) must roll
// everything back and leave the inbox row pending.
func TestCommitInboxDeliveryAtomic(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()

	g, err := store.CreateGroup("spok")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	at := time.Now()
	inboxEv := func() dispatch.Event {
		return dispatch.Event{Kind: dispatch.EventHazardTransition,
			Hazard: &dispatch.HazardTransition{Key: "imgw:1", Source: "imgw"}}
	}

	// Happy path: two jobs + the inbox row in one commit.
	id, err := store.AppendEvent(ctx, inboxEv())
	if err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	res, err := store.CommitInboxDelivery(ctx, id, []storage.DeliveryJob{
		{GroupID: g.ID, ActionID: "log", EventKey: "imgw:1", DedupKey: "c:1", Payload: []byte("{}"), FiredAt: at},
		{GroupID: g.ID, ActionID: "sms", EventKey: "imgw:1", DedupKey: "c:2", Payload: []byte("{}"), FiredAt: at},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(res) != 2 || !res[0].Queued || !res[1].Queued {
		t.Fatalf("results = %+v, want 2 queued jobs", res)
	}
	if pending, err := store.PendingInboxEvents(ctx, 10); err != nil || len(pending) != 0 {
		t.Fatalf("pending after commit = (%v, %v), want (empty, nil)", pending, err)
	}

	// Failure path: an unknown group violates the foreign key, so the
	// whole commit (jobs AND inbox consumption) must roll back.
	id2, err := store.AppendEvent(ctx, inboxEv())
	if err != nil {
		t.Fatalf("AppendEvent 2: %v", err)
	}
	if _, err := store.CommitInboxDelivery(ctx, id2, []storage.DeliveryJob{
		{GroupID: 999, ActionID: "log", EventKey: "imgw:1", DedupKey: "c:bad", Payload: []byte("{}"), FiredAt: at},
	}); err == nil {
		t.Fatal("commit with unknown group succeeded, want FK failure")
	}
	pending, err := store.PendingInboxEvents(ctx, 10)
	if err != nil {
		t.Fatalf("pending read: %v", err)
	}
	found := false
	for _, p := range pending {
		if p.ID == id2 {
			found = true
		}
	}
	if !found {
		t.Fatal("inbox row consumed by a failed commit; it must stay pending")
	}
	// No half-written job either: the same dedup key enqueues fresh.
	st, queued, err := store.EnqueueDelivery(ctx, storage.DeliveryJob{
		GroupID: g.ID, ActionID: "log", EventKey: "imgw:1", DedupKey: "c:bad", Payload: []byte("{}"), FiredAt: at,
	})
	if err != nil || st != storage.DeliverySaved || !queued {
		t.Fatalf("job after rolled-back commit = (%v, %v, %v), want (saved, true, nil)", st, queued, err)
	}
}

// TestHazardActive pins the staleness oracle used by the routing engine
// and the action workers: an active hazard with a future expiry counts
// as active, a cancelled/expired row or a passed expiry counts as
// inactive, and an unknown key counts as active (fail-open).
func TestHazardActive(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()
	now := time.Now()

	// Unknown key: active (producers without local storage still notify).
	if active, err := store.HazardActive(ctx, "nope", now); err != nil || !active {
		t.Fatalf("unknown key = (%v, %v), want (true, nil)", active, err)
	}

	future := now.Add(time.Hour)
	ev := core.HazardEvent{
		Source: "imgw-meteo", SourceID: "1", Event: "Storm",
		Severity: "moderate", Status: core.StatusActive, ExpiresAt: &future,
		ReceivedAt: now, UpdatedAt: now,
	}
	if _, _, err := store.Ingest(ctx, ev, core.Fingerprint(ev)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if active, err := store.HazardActive(ctx, "imgw-meteo:1", now); err != nil || !active {
		t.Fatalf("live hazard = (%v, %v), want (true, nil)", active, err)
	}

	// A newer cancellation: the stale update must not outrank it.
	if _, err := store.db.Exec(`UPDATE events SET status = 'cancelled' WHERE event_key = 'imgw-meteo:1'`); err != nil {
		t.Fatal(err)
	}
	if active, err := store.HazardActive(ctx, "imgw-meteo:1", now); err != nil || active {
		t.Fatalf("cancelled hazard = (%v, %v), want (false, nil)", active, err)
	}

	// Back to active but with a passed expiry: inactive.
	if _, err := store.db.Exec(`UPDATE events SET status = 'active', expires_at_ms = ? WHERE event_key = 'imgw-meteo:1'`,
		now.Add(-time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if active, err := store.HazardActive(ctx, "imgw-meteo:1", now); err != nil || active {
		t.Fatalf("time-expired hazard = (%v, %v), want (false, nil)", active, err)
	}
}

// TestDeliveryJobs pins the durable job lifecycle: a fresh job is queued
// with its payload, a replay while pending or delivered deduplicates, a
// claim increments attempts and returns the payload, and settled stages
// are reported back to the enqueue call.
func TestDeliveryJobs(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()

	g, err := store.CreateGroup("spok")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	at := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"id":"x"}`)
	mk := func(dedup string, fired time.Time) storage.DeliveryJob {
		return storage.DeliveryJob{GroupID: g.ID, ActionID: "log", EventKey: "imgw:1", DedupKey: dedup, Payload: payload, FiredAt: fired}
	}

	// Fresh job: queued with payload.
	st, queued, err := store.EnqueueDelivery(ctx, mk("c:1", at))
	if err != nil || st != storage.DeliverySaved || !queued {
		t.Fatalf("fresh job = (%v, %v, %v), want (saved, true, nil)", st, queued, err)
	}
	// Replay while pending: deduplicated (the worker will execute it).
	st, queued, err = store.EnqueueDelivery(ctx, mk("c:1", at.Add(time.Second)))
	if err != nil || st != storage.DeliverySaved || queued {
		t.Fatalf("pending replay = (%v, %v, %v), want (saved, false, nil)", st, queued, err)
	}
	// Claim: attempt counter increments, payload round-trips.
	job, ok, err := store.ClaimNextDelivery(ctx, "log", 3, at)
	if err != nil || !ok || job.Attempts != 1 || string(job.Payload) != string(payload) || job.EventKey != "imgw:1" {
		t.Fatalf("claim = (%+v, %v, %v), want attempts 1 + payload round-trip", job, ok, err)
	}
	// Replay while running: deduplicated.
	st, queued, err = store.EnqueueDelivery(ctx, mk("c:1", at.Add(time.Second)))
	if err != nil || st != storage.DeliverySaved || queued {
		t.Fatalf("running replay = (%v, %v, %v), want (saved, false, nil)", st, queued, err)
	}
	// Settle accepted: terminal.
	if err := store.SettleDelivery(ctx, g.ID, "log", "c:1", storage.DeliveryAccepted, time.Time{}); err != nil {
		t.Fatalf("settle accepted: %v", err)
	}
	st, queued, err = store.EnqueueDelivery(ctx, mk("c:1", at.Add(2*time.Second)))
	if err != nil || st != storage.DeliveryAccepted || queued {
		t.Fatalf("accepted replay = (%v, %v, %v), want (accepted, false, nil)", st, queued, err)
	}

	// Same dedup key with another action or group is a fresh job.
	st, queued, err = store.EnqueueDelivery(ctx, storage.DeliveryJob{GroupID: g.ID, ActionID: "sms", EventKey: "imgw:1", DedupKey: "c:1", Payload: payload, FiredAt: at})
	if err != nil || st != storage.DeliverySaved || !queued {
		t.Fatalf("other action job = (%v, %v, %v), want (saved, true, nil)", st, queued, err)
	}
	h, err := store.CreateGroup("rsp")
	if err != nil {
		t.Fatalf("CreateGroup rsp: %v", err)
	}
	st, queued, err = store.EnqueueDelivery(ctx, storage.DeliveryJob{GroupID: h.ID, ActionID: "log", EventKey: "imgw:1", DedupKey: "c:1", Payload: payload, FiredAt: at})
	if err != nil || st != storage.DeliverySaved || !queued {
		t.Fatalf("other group job = (%v, %v, %v), want (saved, true, nil)", st, queued, err)
	}

	// Prune with a cutoff before every row: nothing removed.
	n, err := store.PruneActionFires(at.Add(-time.Minute))
	if err != nil || n != 0 {
		t.Fatalf("young prune = (%d, %v), want (0, nil)", n, err)
	}
	// A later change from the same event key is a fresh job.
	st, queued, err = store.EnqueueDelivery(ctx, mk("c:2", at.Add(time.Hour)))
	if err != nil || st != storage.DeliverySaved || !queued {
		t.Fatalf("fresh change job = (%v, %v, %v), want (saved, true, nil)", st, queued, err)
	}
	// Cutoff after the first rows but before the fresh change: exactly
	// the old rows go.
	n, err = store.PruneActionFires(at.Add(30 * time.Minute))
	if err != nil || n != 3 {
		t.Fatalf("partial prune = (%d, %v), want (3, nil)", n, err)
	}
	// Pruning everything: the ledger is empty and dedup resets.
	n, err = store.PruneActionFires(at.Add(2 * time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("full prune = (%d, %v), want (1, nil)", n, err)
	}
	st, queued, err = store.EnqueueDelivery(ctx, mk("c:1", at))
	if err != nil || st != storage.DeliverySaved || !queued {
		t.Fatalf("job after prune = (%v, %v, %v), want (saved, true, nil)", st, queued, err)
	}
}

// TestDeliveryRetryScheduleAndReArm pins the scheduler contract: a
// failed attempt schedules the next one at the given deadline, the
// attempt budget caps claims, and a terminally failed job is re-armed
// (fresh budget) by a replayed transition.
func TestDeliveryRetryScheduleAndReArm(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()

	g, err := store.CreateGroup("spok")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	at := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	job := storage.DeliveryJob{GroupID: g.ID, ActionID: "log", EventKey: "imgw:1", DedupKey: "c:1", Payload: []byte("{}"), FiredAt: at}
	if _, queued, err := store.EnqueueDelivery(ctx, job); err != nil || !queued {
		t.Fatalf("enqueue: (%v, %v)", queued, err)
	}
	// Attempt 1 fails transiently: next attempt in one hour.
	if _, ok, err := store.ClaimNextDelivery(ctx, "log", 2, at); err != nil || !ok {
		t.Fatalf("claim 1: (%v, %v)", ok, err)
	}
	if err := store.SettleDelivery(ctx, g.ID, "log", "c:1", storage.DeliveryFailed, at.Add(time.Hour)); err != nil {
		t.Fatalf("settle retryable: %v", err)
	}
	// Before the deadline nothing is claimable; after it, attempt 2 runs.
	if _, ok, err := store.ClaimNextDelivery(ctx, "log", 2, at.Add(30*time.Minute)); err != nil || ok {
		t.Fatalf("claim before deadline = (%v, %v), want (false, nil)", ok, err)
	}
	claimed, ok, err := store.ClaimNextDelivery(ctx, "log", 2, at.Add(time.Hour))
	if err != nil || !ok || claimed.Attempts != 2 {
		t.Fatalf("claim 2 = (%+v, %v, %v), want attempts 2", claimed, ok, err)
	}
	// Attempt 2 fails terminally (zero next deadline): budget spent.
	if err := store.SettleDelivery(ctx, g.ID, "log", "c:1", storage.DeliveryFailed, time.Time{}); err != nil {
		t.Fatalf("settle terminal: %v", err)
	}
	if _, ok, err := store.ClaimNextDelivery(ctx, "log", 2, at.Add(2*time.Hour)); err != nil || ok {
		t.Fatalf("claim past budget = (%v, %v), want (false, nil)", ok, err)
	}
	// A replay of the transition re-arms the job with a fresh budget.
	st, queued, err := store.EnqueueDelivery(ctx, job)
	if err != nil || st != storage.DeliverySaved || !queued {
		t.Fatalf("re-arm = (%v, %v, %v), want (saved, true, nil)", st, queued, err)
	}
	claimed, ok, err = store.ClaimNextDelivery(ctx, "log", 2, at.Add(2*time.Hour))
	if err != nil || !ok || claimed.Attempts != 1 {
		t.Fatalf("re-armed claim = (%+v, %v, %v), want attempts 1", claimed, ok, err)
	}
}

// TestDeliveryStaleClaimRecovery pins the crash window: a job claimed by
// a process that died before settling is re-queued after its claim lease
// expires, so the alert still executes.
func TestDeliveryStaleClaimRecovery(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()

	g, err := store.CreateGroup("spok")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	at := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	job := storage.DeliveryJob{GroupID: g.ID, ActionID: "log", EventKey: "imgw:1", DedupKey: "c:1", Payload: []byte("{}"), FiredAt: at}
	if _, queued, err := store.EnqueueDelivery(ctx, job); err != nil || !queued {
		t.Fatalf("enqueue: (%v, %v)", queued, err)
	}
	if _, ok, err := store.ClaimNextDelivery(ctx, "log", 2, at); err != nil || !ok {
		t.Fatalf("claim: (%v, %v)", ok, err)
	}
	// Crash before settlement. Before the lease expires nothing recovers.
	if n, err := store.RecoverStaleClaims(ctx, at.Add(time.Minute)); err != nil || n != 0 {
		t.Fatalf("early recovery = (%d, %v), want (0, nil)", n, err)
	}
	// After the lease the job is back in the queue and claimable again
	// (its spent attempt keeps counting against the budget).
	if n, err := store.RecoverStaleClaims(ctx, at.Add(deliveryClaimLease+time.Second)); err != nil || n != 1 {
		t.Fatalf("late recovery = (%d, %v), want (1, nil)", n, err)
	}
	claimed, ok, err := store.ClaimNextDelivery(ctx, "log", 2, at.Add(deliveryClaimLease+time.Second))
	if err != nil || !ok || claimed.Attempts != 2 {
		t.Fatalf("post-recovery claim = (%+v, %v, %v), want attempts 2", claimed, ok, err)
	}
}

// TestDeliveryStatuses pins the stage separation: confirmed is the
// strongest terminal stage, pending counts track the queue, and legacy
// 'succeeded' rows count as accepted.
func TestDeliveryStatuses(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()

	g, err := store.CreateGroup("spok")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	at := time.Now()
	job := storage.DeliveryJob{GroupID: g.ID, ActionID: "log", EventKey: "imgw:1", DedupKey: "c:1", Payload: []byte("{}"), FiredAt: at}
	if _, queued, err := store.EnqueueDelivery(ctx, job); err != nil || !queued {
		t.Fatalf("enqueue: (%v, %v)", queued, err)
	}
	if n, err := store.PendingDeliveries(ctx, "log"); err != nil || n != 1 {
		t.Fatalf("pending = (%d, %v), want (1, nil)", n, err)
	}
	if _, ok, err := store.ClaimNextDelivery(ctx, "log", 2, at); err != nil || !ok {
		t.Fatalf("claim: (%v, %v)", ok, err)
	}
	if err := store.SettleDelivery(ctx, g.ID, "log", "c:1", storage.DeliveryConfirmed, time.Time{}); err != nil {
		t.Fatalf("settle confirmed: %v", err)
	}
	st, queued, err := store.EnqueueDelivery(ctx, job)
	if err != nil || st != storage.DeliveryConfirmed || queued {
		t.Fatalf("confirmed replay = (%v, %v, %v), want (confirmed, false, nil)", st, queued, err)
	}
	if n, err := store.PendingDeliveries(ctx, "log"); err != nil || n != 0 {
		t.Fatalf("pending after settle = (%d, %v), want (0, nil)", n, err)
	}
}

// TestDeliveryJobsSurviveRestart pins the crash-recovery contract across
// a real reopen: delivered jobs stay deduplicated, jobs left running by
// the dead process are re-queued and executed again.
func TestDeliveryJobsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delivery.db")
	s1, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	g, err := s1.CreateGroup("spok")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	at := time.Now()
	if _, queued, err := s1.EnqueueDelivery(ctx, storage.DeliveryJob{GroupID: g.ID, ActionID: "log", EventKey: "imgw:1", DedupKey: "c:done", Payload: []byte("{}"), FiredAt: at}); err != nil || !queued {
		t.Fatalf("enqueue done: (%v, %v)", queued, err)
	}
	if _, ok, err := s1.ClaimNextDelivery(ctx, "log", 2, at); err != nil || !ok {
		t.Fatalf("claim done: (%v, %v)", ok, err)
	}
	if err := s1.SettleDelivery(ctx, g.ID, "log", "c:done", storage.DeliveryAccepted, time.Time{}); err != nil {
		t.Fatalf("settle done: %v", err)
	}
	// Left running: the process "crashed" between claim and execution.
	if _, queued, err := s1.EnqueueDelivery(ctx, storage.DeliveryJob{GroupID: g.ID, ActionID: "log", EventKey: "imgw:2", DedupKey: "c:crashed", Payload: []byte("{}"), FiredAt: at}); err != nil || !queued {
		t.Fatalf("enqueue crashed: (%v, %v)", queued, err)
	}
	if _, ok, err := s1.ClaimNextDelivery(ctx, "log", 2, at); err != nil || !ok {
		t.Fatalf("claim crashed: (%v, %v)", ok, err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, _, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	st, queued, err := s2.EnqueueDelivery(ctx, storage.DeliveryJob{GroupID: g.ID, ActionID: "log", EventKey: "imgw:1", DedupKey: "c:done", Payload: []byte("{}"), FiredAt: at})
	if err != nil || st != storage.DeliveryAccepted || queued {
		t.Fatalf("reopened delivered job = (%v, %v, %v), want (accepted, false, nil)", st, queued, err)
	}
	// The crashed job re-queues after its lease and executes again.
	if n, err := s2.RecoverStaleClaims(ctx, at.Add(deliveryClaimLease+time.Second)); err != nil || n != 1 {
		t.Fatalf("recovery after reopen = (%d, %v), want (1, nil)", n, err)
	}
	claimed, ok, err := s2.ClaimNextDelivery(ctx, "log", 2, at.Add(deliveryClaimLease+time.Second))
	if err != nil || !ok || claimed.EventKey != "imgw:2" || claimed.Attempts != 2 {
		t.Fatalf("recovered claim = (%+v, %v, %v), want imgw:2 attempts 2", claimed, ok, err)
	}
}

func TestListGroupRoutings(t *testing.T) {
	store := newRoutingStore(t)

	a, err := store.CreateGroup("alpha")
	if err != nil {
		t.Fatalf("CreateGroup alpha: %v", err)
	}
	b, err := store.CreateGroup("bravo")
	if err != nil {
		t.Fatalf("CreateGroup bravo: %v", err)
	}
	if err := store.SetGroupRouting(a.ID,
		[]storage.ChannelAssignment{{ID: "log", MinSeverity: "moderate"}}); err != nil {
		t.Fatalf("route alpha: %v", err)
	}
	// bravo stays untouched: no actions.

	all, err := store.ListGroupRoutings()
	if err != nil {
		t.Fatalf("ListGroupRoutings: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("ListGroupRoutings returned %d rules, want 2", len(all))
	}
	if all[0].Name != "alpha" ||
		!reflect.DeepEqual(all[0].Actions, []storage.ChannelAssignment{{ID: "log", MinSeverity: "moderate"}}) {
		t.Errorf("alpha rule = %+v", all[0])
	}
	if all[1].Name != "bravo" || len(all[1].Actions) != 0 {
		t.Errorf("bravo rule = %+v", all[1])
	}

	// Deleting a group removes its routing rows (FK cascade).
	if err := store.DeleteGroup(a.ID); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	all, err = store.ListGroupRoutings()
	if err != nil {
		t.Fatalf("ListGroupRoutings after delete: %v", err)
	}
	if len(all) != 1 || all[0].GroupID != b.ID {
		t.Fatalf("routings after delete = %+v, want only bravo", all)
	}
}

func TestGroupRecipientEmails(t *testing.T) {
	store := newRoutingStore(t)

	g, err := store.CreateGroup("spok")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	// An empty group has no recipients (and a missing group is equally
	// empty, never an error).
	got, err := store.GroupRecipientEmails(g.ID)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty group recipients = %v, %v", got, err)
	}
	got, err = store.GroupRecipientEmails(999)
	if err != nil || len(got) != 0 {
		t.Fatalf("missing group recipients = %v, %v", got, err)
	}

	// Four users: one without email, one duplicate address (different
	// case), all in the group.
	users := []struct{ name, email string }{
		{"ada", "ada@example.com"},
		{"bea", ""},
		{"cya", "cya@example.com"},
		{"dea", "ADA@example.com"},
	}
	for _, u := range users {
		u2, err := store.CreateUser(u.name, "", u.email, "", "", "")
		if err != nil {
			t.Fatalf("CreateUser %s: %v", u.name, err)
		}
		if err := store.SetUserGroups(u2.ID, []int64{g.ID}); err != nil {
			t.Fatalf("SetUserGroups %s: %v", u.name, err)
		}
	}

	got, err = store.GroupRecipientEmails(g.ID)
	if err != nil {
		t.Fatalf("recipients: %v", err)
	}
	// Sorted, distinct, non-empty: ada + cya only.
	if len(got) != 2 || got[0] != "ada@example.com" || got[1] != "cya@example.com" {
		t.Fatalf("recipients = %v, want [ada@example.com cya@example.com]", got)
	}
}

// TestGroupRecipientDiscord mirrors the email test for Discord handles:
// sorted, distinct, non-empty, and never an error for missing groups.
func TestGroupRecipientDiscord(t *testing.T) {
	store := newRoutingStore(t)

	g, err := store.CreateGroup("spok")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	got, err := store.GroupRecipientDiscord(g.ID)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty group handles = %v, %v", got, err)
	}
	got, err = store.GroupRecipientDiscord(999)
	if err != nil || len(got) != 0 {
		t.Fatalf("missing group handles = %v, %v", got, err)
	}

	users := []struct{ name, handle string }{
		{"ada", "ada#1234"},
		{"bea", ""},
		{"cya", "@cya"},
		{"dea", "ADA#1234"},
	}
	for _, u := range users {
		u2, err := store.CreateUser(u.name, "", "", u.handle, "", "")
		if err != nil {
			t.Fatalf("CreateUser %s: %v", u.name, err)
		}
		if err := store.SetUserGroups(u2.ID, []int64{g.ID}); err != nil {
			t.Fatalf("SetUserGroups %s: %v", u.name, err)
		}
	}

	got, err = store.GroupRecipientDiscord(g.ID)
	if err != nil {
		t.Fatalf("handles: %v", err)
	}
	// Sorted, distinct (case-insensitive), non-empty: @cya + ada only.
	if len(got) != 2 || got[0] != "@cya" || got[1] != "ada#1234" {
		t.Fatalf("handles = %v, want [@cya ada#1234]", got)
	}
}
