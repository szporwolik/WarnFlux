package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
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

	expired := now.Add(-2 * time.Hour)
	future := now.Add(2 * time.Hour)
	noExpiry := dispatch.Event{Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{Key: "imgw:1", Source: "imgw"}}
	expiredEv := dispatch.Event{Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{Key: "imgw:2", Source: "imgw",
			Hazard: dispatch.Hazard{ExpiresAt: &expired}}}
	futureEv := dispatch.Event{Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{Key: "imgw:3", Source: "imgw",
			Hazard: dispatch.Hazard{ExpiresAt: &future}}}
	for _, ev := range []dispatch.Event{noExpiry, expiredEv, futureEv} {
		if _, err := store.AppendEvent(ctx, ev); err != nil {
			t.Fatalf("AppendEvent: %v", err)
		}
	}
	if n, err := store.InboxCount(ctx); err != nil || n != 3 {
		t.Fatalf("backlog = (%d, %v), want (3, nil)", n, err)
	}

	// A cutoff before the rows removes nothing.
	if n, err := store.PruneInbox(ctx, now.Add(-time.Hour)); err != nil || n != 0 {
		t.Fatalf("prune past = (%d, %v), want (0, nil)", n, err)
	}
	// A cutoff after every row removes ONLY the expired hazard: the
	// no-expiry and future-expiry rows are pending messages that must
	// survive ordinary retention.
	if n, err := store.PruneInbox(ctx, now.Add(time.Hour)); err != nil || n != 1 {
		t.Fatalf("prune future = (%d, %v), want (1, nil)", n, err)
	}
	if n, err := store.InboxCount(ctx); err != nil || n != 2 {
		t.Fatalf("backlog after prune = (%d, %v), want (2, nil)", n, err)
	}
	// An unreadable row is kept too (fail-open), even past the cutoff.
	if _, err := store.db.Exec(`INSERT INTO dispatch_inbox (event_json, received_at_ms, receiver) VALUES ('not-json', ?, 'x')`, now.Add(-time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if n, err := store.PruneInbox(ctx, now.Add(2*time.Hour)); err != nil || n != 0 {
		t.Fatalf("garbage prune = (%d, %v), want (0, nil)", n, err)
	}
	if n, err := store.InboxCount(ctx); err != nil || n != 3 {
		t.Fatalf("backlog with garbage = (%d, %v), want (3, nil)", n, err)
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
// and the action workers: active with a future expiry = HazardActive,
// cancelled/expired rows and passed expiry = HazardInactive, unknown
// keys = HazardUnknown. A cancelled or expired event WITHOUT an expiry
// must yield a clean verdict — scanning NULL into int64 used to fail
// the whole check before the status was even read.
func TestHazardActive(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()
	now := time.Now()

	// Unknown key: HazardUnknown (producers without local storage notify).
	if verdict, err := store.HazardActive(ctx, "", "nope", now); err != nil || verdict != storage.HazardUnknown {
		t.Fatalf("unknown key = (%v, %v), want (HazardUnknown, nil)", verdict, err)
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
	if verdict, err := store.HazardActive(ctx, "", "imgw-meteo:1", now); err != nil || verdict != storage.HazardActive {
		t.Fatalf("live hazard = (%v, %v), want (HazardActive, nil)", verdict, err)
	}

	// A cancellation WITHOUT an expiry: the reported P1 — the nullable
	// expiry used to abort the scan before the status was evaluated.
	if _, err := store.db.Exec(`UPDATE events SET status = 'cancelled', expires_at_ms = NULL, expires_at = NULL WHERE event_key = 'imgw-meteo:1'`); err != nil {
		t.Fatal(err)
	}
	if verdict, err := store.HazardActive(ctx, "", "imgw-meteo:1", now); err != nil || verdict != storage.HazardInactive {
		t.Fatalf("cancelled no-expiry hazard = (%v, %v), want (HazardInactive, nil)", verdict, err)
	}

	// Expired WITHOUT an expiry (internal lifecycle, no timestamp).
	if _, err := store.db.Exec(`UPDATE events SET status = 'expired' WHERE event_key = 'imgw-meteo:1'`); err != nil {
		t.Fatal(err)
	}
	if verdict, err := store.HazardActive(ctx, "", "imgw-meteo:1", now); err != nil || verdict != storage.HazardInactive {
		t.Fatalf("expired no-expiry hazard = (%v, %v), want (HazardInactive, nil)", verdict, err)
	}

	// Back to active with a passed expiry: HazardInactive.
	if _, err := store.db.Exec(`UPDATE events SET status = 'active', expires_at_ms = ? WHERE event_key = 'imgw-meteo:1'`,
		now.Add(-time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if verdict, err := store.HazardActive(ctx, "", "imgw-meteo:1", now); err != nil || verdict != storage.HazardInactive {
		t.Fatalf("time-expired hazard = (%v, %v), want (HazardInactive, nil)", verdict, err)
	}
}

// TestHazardActivePublisherScoped pins the reported P1: the freshness
// oracle must apply the SAME publisher identity as the lifecycle ledger.
// A locally-cancelled event key must never suppress another publisher's
// active transition for that key (independent publishers never collide),
// while the local publisher and legacy payloads keep the key-only check.
func TestHazardActivePublisherScoped(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()
	now := time.Now()

	local, err := store.InstanceID(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// The local instance cancelled imgw-meteo:1 (status expired).
	future := now.Add(time.Hour)
	ev := core.HazardEvent{
		Source: "imgw-meteo", SourceID: "1", Event: "Storm",
		Severity: "moderate", Status: core.StatusExpired, ExpiresAt: &future,
		ReceivedAt: now, UpdatedAt: now,
	}
	if _, _, err := store.Ingest(ctx, ev, core.Fingerprint(ev)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if verdict, err := store.HazardActive(ctx, local, "imgw-meteo:1", now); err != nil || verdict != storage.HazardInactive {
		t.Fatalf("local publisher = (%v, %v), want (HazardInactive, nil)", verdict, err)
	}

	// An INDEPENDENT publisher's transition for the same key must not be
	// suppressed by the local row — the local record is not about it.
	if verdict, err := store.HazardActive(ctx, "other-instance-uuid", "imgw-meteo:1", now); err != nil || verdict != storage.HazardUnknown {
		t.Fatalf("foreign publisher = (%v, %v), want (HazardUnknown, nil)", verdict, err)
	}

	// Legacy payloads (empty publisher) keep the key-only check.
	if verdict, err := store.HazardActive(ctx, "", "imgw-meteo:1", now); err != nil || verdict != storage.HazardInactive {
		t.Fatalf("empty publisher = (%v, %v), want (HazardInactive, nil)", verdict, err)
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
	// Cutoff after the first rows but before the fresh change: ONLY the
	// completed row ages out — the saved jobs (sms, rsp) are pending
	// work and survive retention.
	n, err = store.PruneActionFires(at.Add(30 * time.Minute))
	if err != nil || n != 1 {
		t.Fatalf("partial prune = (%d, %v), want (1, nil — only completed history)", n, err)
	}
	// A far-future cutoff still keeps every pending job.
	n, err = store.PruneActionFires(at.Add(2 * time.Hour))
	if err != nil || n != 0 {
		t.Fatalf("full prune = (%d, %v), want (0, nil — pending jobs never age out)", n, err)
	}
	// The unsent sms job survived the pruning and is still claimable.
	if job, ok, err := store.ClaimNextDelivery(ctx, "sms", 3, at.Add(3*time.Hour)); err != nil || !ok || job.EventKey != "imgw:1" {
		t.Fatalf("pending job after prune = (%+v, %v, %v), want claimable imgw:1", job, ok, err)
	}
	// The completed c:1 row is gone: its dedup window is over.
	st, queued, err = store.EnqueueDelivery(ctx, mk("c:1", at))
	if err != nil || st != storage.DeliverySaved || !queued {
		t.Fatalf("job after prune = (%v, %v, %v), want (saved, true, nil)", st, queued, err)
	}
}

// TestPruneKeepsPendingDeliveries pins the reported P1: retention removes
// ONLY completed history. A job never executed (saved), a claimed job
// (running) and a failed job with a scheduled retry all survive any
// cutoff; terminal rows (accepted, retries spent) age out.
func TestPruneKeepsPendingDeliveries(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()

	g, err := store.CreateGroup("spok")
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	at := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"id":"x"}`)
	mk := func(dedup string) storage.DeliveryJob {
		return storage.DeliveryJob{GroupID: g.ID, ActionID: "log", EventKey: "imgw:1", DedupKey: dedup, Payload: payload, FiredAt: at}
	}
	for _, d := range []string{"accepted", "terminal-failed", "retry-failed", "running", "saved"} {
		if _, queued, err := store.EnqueueDelivery(ctx, mk(d)); err != nil || !queued {
			t.Fatalf("enqueue %s = (%v, %v), want queued", d, queued, err)
		}
	}
	// Terminal accepted.
	if err := store.SettleDelivery(ctx, g.ID, "log", "accepted", storage.DeliveryAccepted, time.Time{}); err != nil {
		t.Fatal(err)
	}
	// Terminal failed: retry budget spent (zero next attempt).
	if err := store.SettleDelivery(ctx, g.ID, "log", "terminal-failed", storage.DeliveryFailed, time.Time{}); err != nil {
		t.Fatal(err)
	}
	// Transient failed: a retry is scheduled — still pending work.
	if err := store.SettleDelivery(ctx, g.ID, "log", "retry-failed", storage.DeliveryFailed, at.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	// Claimed but never settled: drive the row state directly so the
	// claim cannot steal a neighbouring pending row (claim order ties on
	// next_attempt_at_ms).
	if _, err := store.db.Exec(`UPDATE action_fires SET status = 'running', attempts = 1, next_attempt_at_ms = ? WHERE dedup_key = 'running'`,
		at.Add(5*time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}

	n, err := store.PruneActionFires(at.Add(24 * time.Hour))
	if err != nil || n != 2 {
		t.Fatalf("prune = (%d, %v), want (2, nil — accepted + terminal-failed only)", n, err)
	}
	if n, err := store.PendingDeliveries(ctx, "log"); err != nil || n != 3 {
		t.Fatalf("pending after prune = (%d, %v), want (3, nil — saved, running, retry-failed)", n, err)
	}
	rows, err := store.db.Query(`SELECT status FROM action_fires ORDER BY status`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var statuses []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		statuses = append(statuses, s)
	}
	if got := strings.Join(statuses, ","); got != "failed,running,saved" {
		t.Fatalf("surviving statuses = %q, want %q", got, "failed,running,saved")
	}
}

// TestRecentDeliveries pins the notification-details reader: the newest
// ledger rows come back bounded, newest first, with their event key,
// action, status, attempts and times.
func TestRecentDeliveries(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()
	at := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"id":"x"}`)
	g, err := store.CreateGroup("ops")
	if err != nil {
		t.Fatal(err)
	}
	for i, d := range []string{"a", "b", "c"} {
		job := storage.DeliveryJob{GroupID: g.ID, ActionID: "mesh-main", EventKey: "imgw:" + d, DedupKey: d, Payload: payload, FiredAt: at.Add(time.Duration(i) * time.Minute)}
		if _, queued, err := store.EnqueueDelivery(ctx, job); err != nil || !queued {
			t.Fatalf("enqueue %s = (%v, %v)", d, queued, err)
		}
	}

	got, err := store.RecentDeliveries(ctx, 2)
	if err != nil {
		t.Fatalf("RecentDeliveries: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("rows = %d, want the bounded 2", len(got))
	}
	// Newest first: imgw:c, then imgw:b.
	if got[0].EventKey != "imgw:c" || got[1].EventKey != "imgw:b" {
		t.Fatalf("order = %s, %s — want newest first", got[0].EventKey, got[1].EventKey)
	}
	if got[0].ActionID != "mesh-main" || got[0].Status != "saved" || got[0].Attempts != 0 {
		t.Fatalf("row = %+v, want the action id, saved status and zero attempts", got[0])
	}
	if got[0].Payload != `{"id":"x"}` {
		t.Fatalf("payload = %q, want the raw delivery-job JSON", got[0].Payload)
	}
	if got[0].FiredAt.IsZero() || got[0].FiredAt.Before(at) {
		t.Fatalf("fired_at = %v, want the enqueue time", got[0].FiredAt)
	}
	if !got[0].NextAttemptAt.IsZero() {
		t.Fatalf("next attempt = %v, want zero for a queued job", got[0].NextAttemptAt)
	}
}

// TestLifecycleBlocksVersionSemantics pins the shared lifecycle ledger:
// unknown messages never block (fail-open), a newer version supersedes
// earlier jobs, a terminal state of the exact version blocks, an older
// replay never downgrades, and versionless jobs (the compose panel) are
// judged by the current state only.
func TestLifecycleBlocksVersionSemantics(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()

	if blocked, err := store.LifecycleBlocks(ctx, "pub", "k", 3); err != nil || blocked {
		t.Fatalf("unknown message = (%v, %v), want (false, nil)", blocked, err)
	}
	if err := store.RecordLifecycle(ctx, "pub", "k", 3, "active"); err != nil {
		t.Fatal(err)
	}
	if blocked, _ := store.LifecycleBlocks(ctx, "pub", "k", 3); blocked {
		t.Fatal("the current active version must not block")
	}
	if blocked, _ := store.LifecycleBlocks(ctx, "pub", "k", 2); !blocked {
		t.Fatal("an older job must be blocked by a newer version")
	}
	if err := store.RecordLifecycle(ctx, "pub", "k", 5, "cancelled"); err != nil {
		t.Fatal(err)
	}
	if blocked, _ := store.LifecycleBlocks(ctx, "pub", "k", 3); !blocked {
		t.Fatal("a job superseded by a cancellation must block")
	}
	if blocked, _ := store.LifecycleBlocks(ctx, "pub", "k", 5); !blocked {
		t.Fatal("the cancelled version itself must block")
	}
	if blocked, _ := store.LifecycleBlocks(ctx, "pub", "k", 6); blocked {
		t.Fatal("a newer unknown version must not block")
	}
	// An older replay never downgrades the ledger.
	if err := store.RecordLifecycle(ctx, "pub", "k", 4, "active"); err != nil {
		t.Fatal(err)
	}
	if blocked, _ := store.LifecycleBlocks(ctx, "pub", "k", 5); !blocked {
		t.Fatal("replay must not revive a cancelled version")
	}

	// Versionless jobs (panel messages) are judged by the current state.
	if err := store.RecordLifecycle(ctx, "", "panel:1", 100, "active"); err != nil {
		t.Fatal(err)
	}
	if blocked, _ := store.LifecycleBlocks(ctx, "", "panel:1", 0); blocked {
		t.Fatal("an active panel message must not block")
	}
	if err := store.RecordLifecycle(ctx, "", "panel:1", 101, "expired"); err != nil {
		t.Fatal(err)
	}
	if blocked, _ := store.LifecycleBlocks(ctx, "", "panel:1", 0); !blocked {
		t.Fatal("an expired panel message must block")
	}
}

// TestComposeLifecycleBlocksDelivery pins the reported P1 end to end at
// the store level: SaveComposeHazard feeds the lifecycle ledger, so an
// expired panel message blocks its queued delivery and an older save can
// never revive it.
func TestComposeLifecycleBlocksDelivery(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	h := storage.ComposeHazard{EventKey: "compose:1", State: []byte(`{"x":1}`), Status: "active", UpdatedAt: base}
	if err := store.SaveComposeHazard(ctx, h); err != nil {
		t.Fatal(err)
	}
	if blocked, err := store.LifecycleBlocks(ctx, "", "compose:1", 0); err != nil || blocked {
		t.Fatalf("active panel message blocked = (%v, %v), want (false, nil)", blocked, err)
	}
	h.Status = "expired"
	h.UpdatedAt = base.Add(time.Minute)
	if err := store.SaveComposeHazard(ctx, h); err != nil {
		t.Fatal(err)
	}
	if blocked, err := store.LifecycleBlocks(ctx, "", "compose:1", 0); err != nil || !blocked {
		t.Fatalf("expired panel message blocked = (%v, %v), want (true, nil)", blocked, err)
	}
	// A re-posted form is a NEW transition: the lifecycle version comes
	// from the database-owned counter (strictly increasing, independent
	// of the form's timestamps), so the message is re-activated and the
	// fresh state governs the delivery queue.
	h.Status = "active"
	h.UpdatedAt = base.Add(-time.Minute)
	if err := store.SaveComposeHazard(ctx, h); err != nil {
		t.Fatal(err)
	}
	if blocked, err := store.LifecycleBlocks(ctx, "", "compose:1", 0); err != nil || blocked {
		t.Fatalf("re-posted panel message blocked = (%v, %v), want (false, nil — a re-posted form is a newer transition)", blocked, err)
	}
}

// TestComposeSaveAtomic pins the reported P1 at the storage boundary: the
// compose record and its lifecycle state commit in ONE transaction — a
// failed save writes NEITHER. Previously the expired record committed
// first and a failed lifecycle write left the delivery queue open for a
// message the panel just retired.
func TestComposeSaveAtomic(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()

	// Break the lifecycle write the way the reported fault injection
	// did: the ledger table is gone, so only a separate second write
	// could have failed while the record survived.
	if _, err := store.db.Exec(`DROP TABLE message_lifecycle`); err != nil {
		t.Fatal(err)
	}
	h := storage.ComposeHazard{EventKey: "compose:1", State: []byte(`{"x":1}`), Status: "expired", UpdatedAt: time.Now()}
	if err := store.SaveComposeHazard(ctx, h); err == nil {
		t.Fatal("save with a broken lifecycle table succeeded, want failure")
	}
	// Nothing was accepted: the operation failed as a whole.
	rows, err := store.ComposeHazards(ctx)
	if err != nil || len(rows) != 0 {
		t.Fatalf("compose record after the failed save = (%v, %v), want none (atomic rollback)", rows, err)
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

// TestSettleDeliveryGuarded pins the atomic settlement: a failure marker
// landing while the job was still running downgrades the later accepted
// settlement to a scheduled retry — the async failure can never be
// overwritten by a stale accepted write (reported P1). The attempt
// generation additionally blocks settlements from an older claim.
func TestSettleDeliveryGuarded(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()

	// TWO groups route the SAME action for the SAME event version: the
	// failure marker is job-scoped, so one group's TxFailed must not
	// downgrade the other group's settlement (reported P2).
	gA, err := store.CreateGroup("mesh-a")
	if err != nil {
		t.Fatal(err)
	}
	gB, err := store.CreateGroup("mesh-b")
	if err != nil {
		t.Fatal(err)
	}
	ev := dispatch.Event{Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{Key: "imgw:1", Source: "imgw", ChangeID: 7, Publisher: "pub-1"}}
	payload, err := json.Marshal(action.ActionRequest{ID: "imgw:1/mesh", Event: ev})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	if st, queued, err := store.EnqueueDelivery(ctx, storage.DeliveryJob{
		GroupID: gA.ID, ActionID: "mesh", EventKey: "imgw:1",
		DedupKey: "c:a", Payload: payload, FiredAt: at,
	}); err != nil || !queued || st != storage.DeliverySaved {
		t.Fatalf("enqueue a = (%v, %v, %v)", st, queued, err)
	}
	if st, queued, err := store.EnqueueDelivery(ctx, storage.DeliveryJob{
		GroupID: gB.ID, ActionID: "mesh", EventKey: "imgw:1",
		DedupKey: "c:b", Payload: payload, FiredAt: at.Add(time.Second),
	}); err != nil || !queued || st != storage.DeliverySaved {
		t.Fatalf("enqueue b = (%v, %v, %v)", st, queued, err)
	}

	// The worker claims group A's job (generation 1) and executes.
	job, ok, err := store.ClaimNextDelivery(ctx, "mesh", 3, time.Now())
	if err != nil || !ok || job.Attempts != 1 || job.DedupKey != "c:a" || job.GroupID != gA.ID {
		t.Fatalf("claim a = (%+v, %v, %v), want the group-a job at attempts 1", job, ok, err)
	}

	// TxFailed for group A's job arrives while it is still running: the
	// marker lands and the atomic record re-arms nothing (the row is
	// running — the settlement itself must guard).
	if err := store.SetMeshActionFailed(ctx, "mesh", gA.ID, "c:a", "pub-1", "imgw:1", 7, "a0a85934", 0, true); err != nil {
		t.Fatal(err)
	}
	if n, err := store.RecordMeshFailureAndRequeue(ctx, "mesh", gA.ID, "c:a", "pub-1", "imgw:1", 7, "a0a85934", 0, time.Now()); err != nil || n != 0 {
		t.Fatalf("re-arm on running = (%d, %v), want 0 (the guarded settle covers it)", n, err)
	}

	// The worker settles accepted: the guard downgrades atomically.
	applied, err := store.SettleDeliveryGuarded(ctx, storage.DeliverySettle{
		GroupID: gA.ID, ActionID: "mesh", DedupKey: "c:a",
		Stage: storage.DeliveryAccepted, NextAttempt: time.Now().Add(time.Minute),
		Attempts: 1,
		Version:  &storage.DeliveryVersion{ActionID: "mesh", Publisher: "pub-1", EventKey: "imgw:1", ChangeID: 7},
	})
	if err != nil || applied != storage.DeliveryFailed {
		t.Fatalf("applied = (%v, %v), want downgraded failed", applied, err)
	}
	var status string
	var next int64
	if err := store.db.QueryRow(`SELECT status, next_attempt_at_ms FROM action_fires WHERE dedup_key = 'c:a'`).
		Scan(&status, &next); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || next == 0 {
		t.Fatalf("row a = (%q, next %d), want failed with the retry deadline", status, next)
	}

	// P2: group B's accepted job of the SAME action+version is NOT
	// downgraded by group A's failure marker — the guard is job-scoped.
	jobB, ok, err := store.ClaimNextDelivery(ctx, "mesh", 3, time.Now())
	if err != nil || !ok || jobB.Attempts != 1 || jobB.DedupKey != "c:b" || jobB.GroupID != gB.ID {
		t.Fatalf("claim b = (%+v, %v, %v), want the group-b job at attempts 1", jobB, ok, err)
	}
	appliedB, err := store.SettleDeliveryGuarded(ctx, storage.DeliverySettle{
		GroupID: gB.ID, ActionID: "mesh", DedupKey: "c:b",
		Stage: storage.DeliveryAccepted, Attempts: 1,
		Version: &storage.DeliveryVersion{ActionID: "mesh", Publisher: "pub-1", EventKey: "imgw:1", ChangeID: 7},
	})
	if err != nil || appliedB != storage.DeliveryAccepted {
		t.Fatalf("group-b settle = (%v, %v), want accepted (the marker is job-scoped)", appliedB, err)
	}

	// A newer claim (the retry, generation 2) and a STALE settlement
	// from the old worker: the generation guard blocks it.
	if _, ok, err := store.ClaimNextDelivery(ctx, "mesh", 3, time.Now().Add(2*time.Minute)); err != nil || !ok {
		t.Fatalf("retry claim = (%v, %v)", ok, err)
	}
	if _, err := store.SettleDeliveryGuarded(ctx, storage.DeliverySettle{
		GroupID: gA.ID, ActionID: "mesh", DedupKey: "c:a",
		Stage: storage.DeliveryAccepted, Attempts: 1,
		Version: &storage.DeliveryVersion{ActionID: "mesh", Publisher: "pub-1", EventKey: "imgw:1", ChangeID: 7},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT status FROM action_fires WHERE dedup_key = 'c:a'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "running" {
		t.Fatalf("stale settle overwrote the newer attempt: status %q, want running", status)
	}

	// The retransmission succeeded (marker cleared): the generation-2
	// settlement lands accepted.
	if err := store.SetMeshActionFailed(ctx, "mesh", gA.ID, "c:a", "pub-1", "imgw:1", 7, "a0a85934", 0, false); err != nil {
		t.Fatal(err)
	}
	applied, err = store.SettleDeliveryGuarded(ctx, storage.DeliverySettle{
		GroupID: gA.ID, ActionID: "mesh", DedupKey: "c:a",
		Stage: storage.DeliveryAccepted, Attempts: 2,
		Version: &storage.DeliveryVersion{ActionID: "mesh", Publisher: "pub-1", EventKey: "imgw:1", ChangeID: 7},
	})
	if err != nil || applied != storage.DeliveryAccepted {
		t.Fatalf("cleared-marker settle = (%v, %v), want accepted", applied, err)
	}
}

// TestRecordMeshFailureAndRequeue pins the atomic async-failure outcome:
// ONE call revokes the progress entry, records the failure marker and
// re-arms the settled delivery JOB of the concrete job identity — a
// crash between those steps is impossible (single transaction, reported
// P1) and other groups' jobs of the same action stay untouched (P2).
func TestRecordMeshFailureAndRequeue(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()

	// TWO groups route the same action for the same version; the failure
	// of group A's job must re-arm only that job.
	gA, err := store.CreateGroup("mesh-a")
	if err != nil {
		t.Fatal(err)
	}
	gB, err := store.CreateGroup("mesh-b")
	if err != nil {
		t.Fatal(err)
	}
	ev := dispatch.Event{Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{Key: "imgw:1", Source: "imgw", ChangeID: 7, Publisher: "pub-1"}}
	meshPayload, err := json.Marshal(action.ActionRequest{ID: "imgw:1/mesh", Event: ev})
	if err != nil {
		t.Fatal(err)
	}
	smtpPayload, err := json.Marshal(action.ActionRequest{ID: "imgw:1/smtp", Event: ev})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	for _, spec := range []struct {
		groupID int64
		action  string
		dedup   string
		payload []byte
	}{
		{gA.ID, "mesh", "c:a", meshPayload}, // the failing job: re-armed
		{gB.ID, "mesh", "c:b", meshPayload}, // another GROUP, same action+version: untouched (P2)
		{gA.ID, "smtp", "c:s", smtpPayload}, // another ACTION of the same group: untouched
	} {
		if st, queued, err := store.EnqueueDelivery(ctx, storage.DeliveryJob{
			GroupID: spec.groupID, ActionID: spec.action, EventKey: "imgw:1",
			DedupKey: spec.dedup, Payload: spec.payload, FiredAt: at,
		}); err != nil || !queued || st != storage.DeliverySaved {
			t.Fatalf("enqueue %s = (%v, %v, %v)", spec.dedup, st, queued, err)
		}
		if err := store.SettleDelivery(ctx, spec.groupID, spec.action, spec.dedup, storage.DeliveryAccepted, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.RecordMeshActionProgress(ctx, "pub-1", "imgw:1", 7, "a0a85934", 0, at); err != nil {
		t.Fatal(err)
	}

	n, err := store.RecordMeshFailureAndRequeue(ctx, "mesh", gA.ID, "c:a", "pub-1", "imgw:1", 7, "a0a85934", 0, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("re-armed %d jobs, want exactly the group-a mesh job", n)
	}
	var status string
	var nextMs int64
	if err := store.db.QueryRow(`SELECT status, next_attempt_at_ms FROM action_fires WHERE dedup_key = 'c:a'`).
		Scan(&status, &nextMs); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || nextMs == 0 {
		t.Fatalf("group-a mesh job = (%q, next %d), want failed due now", status, nextMs)
	}
	for _, dedup := range []string{"c:b", "c:s"} {
		if err := store.db.QueryRow(`SELECT status FROM action_fires WHERE dedup_key = ?`, dedup).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "accepted" {
			t.Fatalf("job %s = %q, want untouched accepted", dedup, status)
		}
	}
	if done, err := store.MeshActionProgressDone(ctx, "pub-1", "imgw:1", 7, "a0a85934", 0); err != nil || done {
		t.Fatalf("progress after failure = (%v, %v), want revoked", done, err)
	}
	if failed, err := store.MeshActionFailed(ctx, "mesh", gA.ID, "c:a", "pub-1", "imgw:1", 7); err != nil || !failed {
		t.Fatalf("marker = (%v, %v), want set", failed, err)
	}
	if failed, err := store.MeshActionFailed(ctx, "mesh", gB.ID, "c:b", "pub-1", "imgw:1", 7); err != nil || failed {
		t.Fatalf("group-b marker = (%v, %v), want unset", failed, err)
	}
}

// TestRequeueMarkedDeliveries pins the startup sweep: the inconsistent
// state a crash could leave behind (accepted job + durable marker, set
// separately) is re-armed for exactly the marked job; other groups and
// actions stay untouched.
func TestRequeueMarkedDeliveries(t *testing.T) {
	store := newRoutingStore(t)
	ctx := context.Background()

	gA, err := store.CreateGroup("mesh-a")
	if err != nil {
		t.Fatal(err)
	}
	gB, err := store.CreateGroup("mesh-b")
	if err != nil {
		t.Fatal(err)
	}
	ev := dispatch.Event{Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{Key: "imgw:1", Source: "imgw", ChangeID: 7, Publisher: "pub-1"}}
	meshPayload, _ := json.Marshal(action.ActionRequest{ID: "imgw:1/mesh", Event: ev})
	smtpPayload, _ := json.Marshal(action.ActionRequest{ID: "imgw:1/smtp", Event: ev})
	at := time.Now()
	for _, spec := range []struct {
		groupID int64
		action  string
		dedup   string
		payload []byte
	}{
		{gA.ID, "mesh", "c:a", meshPayload}, // the marked job: re-armed
		{gB.ID, "mesh", "c:b", meshPayload}, // same action+version, other group: untouched (P2)
		{gA.ID, "smtp", "c:s", smtpPayload},
	} {
		if st, queued, err := store.EnqueueDelivery(ctx, storage.DeliveryJob{
			GroupID: spec.groupID, ActionID: spec.action, EventKey: "imgw:1",
			DedupKey: spec.dedup, Payload: spec.payload, FiredAt: at,
		}); err != nil || !queued || st != storage.DeliverySaved {
			t.Fatalf("enqueue %s = (%v, %v, %v)", spec.dedup, st, queued, err)
		}
		if err := store.SettleDelivery(ctx, spec.groupID, spec.action, spec.dedup, storage.DeliveryAccepted, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	// The crash state: the marker exists, the job is still accepted.
	if err := store.SetMeshActionFailed(ctx, "mesh", gA.ID, "c:a", "pub-1", "imgw:1", 7, "a0a85934", 0, true); err != nil {
		t.Fatal(err)
	}

	n, err := store.RequeueMarkedDeliveries(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("sweep re-armed %d jobs, want exactly the group-a mesh job", n)
	}
	var status string
	if err := store.db.QueryRow(`SELECT status FROM action_fires WHERE dedup_key = 'c:a'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Fatalf("group-a mesh job = %q, want re-armed failed", status)
	}
	for _, dedup := range []string{"c:b", "c:s"} {
		if err := store.db.QueryRow(`SELECT status FROM action_fires WHERE dedup_key = ?`, dedup).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "accepted" {
			t.Fatalf("job %s = %q, want untouched accepted", dedup, status)
		}
	}
}
