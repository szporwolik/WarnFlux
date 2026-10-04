package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestIngestOutboxLifecycle pins the durable HTTP-ingest outbox: rows are
// appended per instance, listed in insertion order, invisible to other
// instances, survive a restart and are deleted only on acknowledgement.
func TestIngestOutboxLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	store, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	id, err := store.AppendOutbox(ctx, "news", "warnflux/events", []byte(`{"x":1}`))
	if err != nil || id == 0 {
		t.Fatalf("AppendOutbox = (%d, %v)", id, err)
	}
	if n, err := store.OutboxCount(ctx); err != nil || n != 1 {
		t.Fatalf("OutboxCount = (%d, %v), want 1", n, err)
	}

	items, err := store.PendingOutbox(ctx, "news", 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("PendingOutbox = (%v, %v), want 1 row", items, err)
	}
	if items[0].ID != id || items[0].Topic != "warnflux/events" || string(items[0].Payload) != `{"x":1}` {
		t.Errorf("row = %+v, want id %d topic warnflux/events payload {\"x\":1}", items[0], id)
	}

	// A different instance never sees another instance's rows.
	if items, err := store.PendingOutbox(ctx, "other", 10); err != nil || len(items) != 0 {
		t.Fatalf("other instance rows = (%v, %v), want none", items, err)
	}

	// Rows survive a restart (the durable broker-sync backlog).
	store.Close()
	restarted, _, err := Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if n, err := restarted.OutboxCount(ctx); err != nil || n != 1 {
		t.Fatalf("OutboxCount after restart = (%d, %v), want 1", n, err)
	}

	if err := restarted.AckOutbox(ctx, id); err != nil {
		t.Fatalf("AckOutbox: %v", err)
	}
	if n, err := restarted.OutboxCount(ctx); err != nil || n != 0 {
		t.Fatalf("OutboxCount after ack = (%d, %v), want 0", n, err)
	}
}

// wirePayload builds a /events-shaped payload with the given status and
// expiry (the fields outboxLifecycle reads).
func wirePayload(status string, expiry time.Duration) []byte {
	b, err := json.Marshal(map[string]any{"event": map[string]any{
		"status": status, "expires_at": time.Now().Add(expiry).UTC().Format(time.RFC3339),
	}})
	if err != nil {
		panic(err)
	}
	return b
}

// wireEventPayload builds a /events-shaped payload carrying a proper
// event identity (publisher + event key) plus the lifecycle fields; the
// change_type matches the status so the payloads stay realistic.
func wireEventPayload(status, eventKey string, expiry time.Duration) []byte {
	changeType := "new"
	switch status {
	case "cancelled":
		changeType = "cancelled"
	case "expired":
		changeType = "expired"
	}
	b, err := json.Marshal(map[string]any{
		"event_key":   eventKey,
		"publisher":   "publisher-1",
		"change_type": changeType,
		"event": map[string]any{
			"status": status, "expires_at": time.Now().Add(expiry).UTC().Format(time.RFC3339),
		},
	})
	if err != nil {
		panic(err)
	}
	return b
}

// commitEvent builds the dispatch.Event half of a CommitIngest call.
func commitEvent(key string, typ dispatch.TransitionType) dispatch.Event {
	now := time.Now()
	return dispatch.Event{
		Kind:       dispatch.EventHazardTransition,
		ReceivedAt: now,
		Origin:     dispatch.Origin{Type: "http", ReceiverID: "news"},
		Hazard: &dispatch.HazardTransition{
			Type:      typ,
			Key:       key,
			ChangeID:  7,
			Publisher: "publisher-1",
			Timestamp: now,
			Hazard:    dispatch.Hazard{EventKey: key},
		},
	}
}

// TestCommitIngestOutboxMetadata pins the P1 half: the production atomic
// path must write the same lifecycle metadata as AppendOutbox and merge
// repeated transitions of one EVENT — otherwise age-pruning never
// recognizes expired rows and the queue fills with repeats.
func TestCommitIngestOutboxMetadata(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	if _, err := store.CommitIngest(ctx, "news", "warnflux/events",
		wireEventPayload("active", "aprs:k1", -time.Hour), commitEvent("k1", dispatch.TransitionNew)); err != nil {
		t.Fatalf("CommitIngest active: %v", err)
	}
	if _, err := store.CommitIngest(ctx, "news", "warnflux/events",
		wireEventPayload("cancelled", "aprs:k1", -time.Hour), commitEvent("k1", dispatch.TransitionCancelled)); err != nil {
		t.Fatalf("CommitIngest cancelled: %v", err)
	}
	// A DIFFERENT event on the same topic must never merge away.
	if _, err := store.CommitIngest(ctx, "news", "warnflux/events",
		wireEventPayload("active", "aprs:k2", time.Hour), commitEvent("k2", dispatch.TransitionNew)); err != nil {
		t.Fatalf("CommitIngest second event: %v", err)
	}

	var count int
	var status string
	var expiresAt sql.NullInt64
	if err := store.db.QueryRow(
		`SELECT COUNT(*), MAX(status), expires_at_ms FROM ingest_outbox WHERE event_key = 'aprs:k1'`,
	).Scan(&count, &status, &expiresAt); err != nil {
		t.Fatal(err)
	}
	if count != 1 || status != "cancelled" || !expiresAt.Valid || expiresAt.Int64 == 0 {
		t.Fatalf("CommitIngest k1 row = (%d rows, status %q, expires %v), want 1 merged row carrying metadata",
			count, status, expiresAt)
	}
	if n, err := store.OutboxCount(ctx); err != nil || n != 2 {
		t.Fatalf("OutboxCount = (%d, %v), want 2 (k1 merged + independent k2)", n, err)
	}
}

// TestOutboxMergeByIdentity pins the P2 legacy path: rows merge by
// publisher + event key, NEVER by topic — independent alarms sharing
// warnflux/events all survive.
func TestOutboxMergeByIdentity(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	// Two independent events on the same topic.
	if _, err := store.AppendOutbox(ctx, "news", "warnflux/events", wireEventPayload("active", "aprs:a", time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendOutbox(ctx, "news", "warnflux/events", wireEventPayload("active", "aprs:b", time.Hour)); err != nil {
		t.Fatal(err)
	}
	if n, err := store.OutboxCount(ctx); err != nil || n != 2 {
		t.Fatalf("OutboxCount after two events = (%d, %v), want 2 (independent alarms survive)", n, err)
	}

	// Repeated transitions of ONE event merge into the newest state.
	if _, err := store.AppendOutbox(ctx, "news", "warnflux/events", wireEventPayload("cancelled", "aprs:a", -time.Hour)); err != nil {
		t.Fatal(err)
	}
	var count int
	var status string
	if err := store.db.QueryRow(
		`SELECT COUNT(*), MAX(status) FROM ingest_outbox WHERE event_key = 'aprs:a'`).Scan(&count, &status); err != nil {
		t.Fatal(err)
	}
	if count != 1 || status != "cancelled" {
		t.Fatalf("merged aprs:a = (%d rows, status %q), want 1 cancelled row", count, status)
	}
	if n, err := store.OutboxCount(ctx); err != nil || n != 2 {
		t.Fatalf("OutboxCount after merge = (%d, %v), want 2 (a merged, b untouched)", n, err)
	}
}

// wireVersioned builds a versioned /events payload (publisher + event
// key + change id) with a realistic change_type.
func wireVersioned(status string, changeID int64, expiry time.Duration) []byte {
	changeType := "new"
	switch status {
	case "cancelled":
		changeType = "cancelled"
	case "expired":
		changeType = "expired"
	}
	b, err := json.Marshal(map[string]any{
		"event_key": "aprs:v", "publisher": "publisher-1", "change_id": changeID,
		"change_type": changeType,
		"event": map[string]any{
			"status": status, "expires_at": time.Now().Add(expiry).UTC().Format(time.RFC3339),
		},
	})
	if err != nil {
		panic(err)
	}
	return b
}

// TestOutboxMergeVersionGuard pins the P1: a delayed OLDER transition
// must never replace a newer pending state. Sequence: cancel v2, then a
// delayed new v1 — the outbox keeps only the v2 cancellation.
func TestOutboxMergeVersionGuard(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	// The newer cancellation queues first.
	if _, err := store.AppendOutbox(ctx, "news", "warnflux/events", wireVersioned("cancelled", 2, time.Hour)); err != nil {
		t.Fatal(err)
	}
	// The delayed older version arrives later.
	if _, err := store.AppendOutbox(ctx, "news", "warnflux/events", wireVersioned("active", 1, time.Hour)); err != nil {
		t.Fatal(err)
	}
	var count int
	var status string
	var changeID int64
	if err := store.db.QueryRow(
		`SELECT COUNT(*), MAX(status), MAX(change_id) FROM ingest_outbox WHERE event_key = 'aprs:v'`).
		Scan(&count, &status, &changeID); err != nil {
		t.Fatal(err)
	}
	if count != 1 || status != "cancelled" || changeID != 2 {
		t.Fatalf("after stale v1 = (%d rows, %q, change %d), want 1 cancelled row at version 2", count, status, changeID)
	}

	// A newer version still replaces.
	if _, err := store.AppendOutbox(ctx, "news", "warnflux/events", wireVersioned("active", 3, time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(
		`SELECT status, change_id FROM ingest_outbox WHERE event_key = 'aprs:v'`).Scan(&status, &changeID); err != nil {
		t.Fatal(err)
	}
	if status != "active" || changeID != 3 {
		t.Fatalf("after v3 = (%q, %d), want active at version 3", status, changeID)
	}

	// The production CommitIngest path applies the same guard.
	if _, err := store.CommitIngest(ctx, "news", "warnflux/events",
		wireVersioned("cancelled", 5, time.Hour), commitEvent("v", dispatch.TransitionCancelled)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitIngest(ctx, "news", "warnflux/events",
		wireVersioned("active", 4, time.Hour), commitEvent("v", dispatch.TransitionNew)); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(
		`SELECT status, change_id FROM ingest_outbox WHERE event_key = 'aprs:v'`).Scan(&status, &changeID); err != nil {
		t.Fatal(err)
	}
	if status != "cancelled" || changeID != 5 {
		t.Fatalf("after stale CommitIngest v4 = (%q, %d), want cancelled at version 5", status, changeID)
	}
}

// TestOutboxVersionWatermarkSurvivesAck pins the P1: the version guard
// must survive the queue drain. The v2 cancellation is published and
// ACKed (the row is gone) — a delayed older v1 still finds the durable
// watermark and is skipped, so it can never resurrect the cancelled
// alarm in the broker's active view.
func TestOutboxVersionWatermarkSurvivesAck(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	// The newer cancellation is accepted and fully published.
	id, err := store.AppendOutbox(ctx, "news", "warnflux/events", wireVersioned("cancelled", 2, time.Hour))
	if err != nil || id == 0 {
		t.Fatalf("AppendOutbox v2 = (%d, %v)", id, err)
	}
	if err := store.AckOutbox(ctx, id); err != nil {
		t.Fatal(err)
	}
	if n, err := store.OutboxCount(ctx); err != nil || n != 0 {
		t.Fatalf("OutboxCount after ack = (%d, %v), want 0", n, err)
	}

	// The delayed older version arrives AFTER the ack: the watermark
	// still guards — a stale skip, the queue stays empty.
	if id, err := store.AppendOutbox(ctx, "news", "warnflux/events", wireVersioned("active", 1, time.Hour)); err != nil || id != 0 {
		t.Fatalf("stale AppendOutbox v1 after ack = (%d, %v), want (0, nil)", id, err)
	}
	if n, err := store.OutboxCount(ctx); err != nil || n != 0 {
		t.Fatalf("OutboxCount after stale v1 = (%d, %v), want 0", n, err)
	}

	// The CommitIngest path applies the same surviving guard.
	if _, err := store.CommitIngest(ctx, "news", "warnflux/events",
		wireVersioned("active", 1, time.Hour), commitEvent("v", dispatch.TransitionNew)); err != nil {
		t.Fatalf("stale CommitIngest v1 after ack: %v", err)
	}
	if n, err := store.OutboxCount(ctx); err != nil || n != 0 {
		t.Fatalf("OutboxCount after stale CommitIngest v1 = (%d, %v), want 0", n, err)
	}

	// A newer version still replaces the watermark and publishes.
	if _, err := store.AppendOutbox(ctx, "news", "warnflux/events", wireVersioned("active", 3, time.Hour)); err != nil {
		t.Fatalf("AppendOutbox v3: %v", err)
	}
	if n, err := store.OutboxCount(ctx); err != nil || n != 1 {
		t.Fatalf("OutboxCount after v3 = (%d, %v), want 1", n, err)
	}
}

// TestOutboxWatermarkExpiryPrune pins the watermark lifetime: once the
// event's own expiry passed, the guard is gone too — a later transition
// is harmless (the broker's document is self-expired) and is accepted
// again instead of being skipped forever.
func TestOutboxWatermarkExpiryPrune(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	// The v2 cancellation's hazard is already expired: the watermark is
	// pruned on the next sweep.
	id, err := store.AppendOutbox(ctx, "news", "warnflux/events", wireVersioned("cancelled", 2, -time.Hour))
	if err != nil || id == 0 {
		t.Fatalf("AppendOutbox expired v2 = (%d, %v)", id, err)
	}
	if err := store.AckOutbox(ctx, id); err != nil {
		t.Fatal(err)
	}
	var watermark int
	if err := store.db.QueryRow(
		`SELECT COUNT(*) FROM ingest_outbox_watermark WHERE event_key = 'aprs:v'`).Scan(&watermark); err != nil {
		t.Fatal(err)
	}
	if watermark != 0 {
		t.Fatalf("expired watermark rows = %d, want 0 (pruned with its expiry)", watermark)
	}

	// No guard left: the (equally expired) older version is accepted.
	if id, err := store.AppendOutbox(ctx, "news", "warnflux/events", wireVersioned("active", 1, -time.Hour)); err != nil || id == 0 {
		t.Fatalf("AppendOutbox v1 after watermark expiry = (%d, %v), want accepted", id, err)
	}
}

// TestOutboxCapacityProtectsExpiredCorrective pins the P1: a corrective
// row's expiry does NOT prove the broker retired the earlier active
// document — cancellations/expirations are only ever removed by the ACK
// or by a newer version of the same identity. An overflow of EXPIRED
// corrective rows is therefore explicit backpressure (ErrOutboxFull),
// never a silent eviction.
func TestOutboxCapacityProtectsExpiredCorrective(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()
	past := time.Now().Add(-time.Hour).UnixMilli()

	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < outboxRowCap+5; i++ {
		if _, err := tx.Exec(
			`INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms, status, expires_at_ms)
			 VALUES ('news', ?, '{}', 1, 'cancelled', ?)`,
			fmt.Sprintf("t/cancel-%d", i), past); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Both append paths reject and drop NOTHING: the expired
	// cancellations stay queued for the broker.
	if _, err := store.AppendOutbox(ctx, "news", "warnflux/events",
		wirePayload("active", time.Hour)); !errors.Is(err, storage.ErrOutboxFull) {
		t.Fatalf("AppendOutbox on expired corrective backlog = %v, want ErrOutboxFull", err)
	}
	if _, err := store.CommitIngest(ctx, "news", "warnflux/events",
		wirePayload("active", time.Hour), commitEvent("k2", dispatch.TransitionNew)); !errors.Is(err, storage.ErrOutboxFull) {
		t.Fatalf("CommitIngest on expired corrective backlog = %v, want ErrOutboxFull", err)
	}
	if n, err := store.OutboxCount(ctx); err != nil || n != outboxRowCap+5 {
		t.Fatalf("OutboxCount after rejected appends = (%d, %v), want %d (nothing dropped)",
			n, err, outboxRowCap+5)
	}

	// The periodic prune keeps them too.
	if n, err := store.PruneOutbox(ctx, time.Now().Add(-24*time.Hour)); err != nil || n != 0 {
		t.Fatalf("PruneOutbox = (%d, %v), want 0 (expired corrective rows are protected)", n, err)
	}

	// A NEWER version of the same identity still replaces them (the one
	// allowed removal): stamp the backlog with a version and add the
	// newer row — the append evicts the superseded rows and succeeds.
	store2 := openTemp(t)
	tx2, err := store2.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < outboxRowCap+5; i++ {
		if _, err := tx2.Exec(
			`INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms, status, expires_at_ms, event_key, publisher, change_id)
			 VALUES ('news', ?, '{}', 1, 'cancelled', ?, 'aprs:k', 'publisher-1', 1)`,
			fmt.Sprintf("t/old-%d", i), past); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx2.Exec(
		`INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms, status, expires_at_ms, event_key, publisher, change_id)
		 VALUES ('news', 't/new', '{}', 1, 'active', ?, 'aprs:k', 'publisher-1', 2)`,
		time.Now().Add(time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.AppendOutbox(ctx, "news", "warnflux/events", []byte(`{"x":1}`)); err != nil {
		t.Fatalf("AppendOutbox with superseded expired corrective backlog: %v", err)
	}
	var old int
	if err := store2.db.QueryRow(
		`SELECT COUNT(*) FROM ingest_outbox WHERE event_key = 'aprs:k' AND change_id = 1`).Scan(&old); err != nil {
		t.Fatal(err)
	}
	if old != 0 {
		t.Fatalf("superseded corrective rows = %d, want 0 (replaced by the newer version)", old)
	}
}

// TestOutboxCapacityProtectsCorrective pins the P1 half: the capacity
// bound may only evict empty/active rows; a cancellation in the overflow
// zone survives pruning (the broker may still hold an old active
// document it must retire).
func TestOutboxCapacityProtectsCorrective(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()
	past := time.Now().Add(-time.Hour).UnixMilli()

	// Direct bulk insert keeps the test fast: 10002 EXPIRED evictable
	// rows + 1 corrective cancellation appended LAST (newest). The
	// expired rows are provably obsolete; the corrective row is not.
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < outboxRowCap+2; i++ {
		if _, err := tx.Exec(
			`INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms, status, expires_at_ms)
			 VALUES ('news', ?, '{}', 1, 'active', ?)`,
			fmt.Sprintf("t/fill-%d", i), past); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(
		`INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms, status, expires_at_ms)
		 VALUES ('news', 't/cancel', ?, 1, 'cancelled', NULL)`,
		string(wirePayload("cancelled", -time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	n, err := store.PruneOutbox(ctx, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("PruneOutbox: %v", err)
	}
	_ = n

	var total, corrective int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM ingest_outbox`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(
		`SELECT COUNT(*) FROM ingest_outbox WHERE status = 'cancelled'`).Scan(&corrective); err != nil {
		t.Fatal(err)
	}
	if corrective != 1 {
		t.Fatalf("corrective rows after capacity prune = %d, want 1 (never evicted)", corrective)
	}
	var topic string
	if err := store.db.QueryRow(
		`SELECT topic FROM ingest_outbox WHERE status = 'cancelled'`).Scan(&topic); err != nil {
		t.Fatal(err)
	}
	if topic != "t/cancel" {
		t.Fatalf("surviving corrective topic = %q, want t/cancel", topic)
	}
	if total != 1 {
		t.Fatalf("total rows after prune = %d, want 1 (every expired row is obsolete, the corrective one survives)", total)
	}
}

// TestOutboxFullBackpressure pins the explicit failure: when pending
// rows alone exceed the capacity bound and none is provably obsolete,
// appends fail with storage.ErrOutboxFull instead of dropping a
// still-valid sync.
func TestOutboxFullBackpressure(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < outboxRowCap+1; i++ {
		if _, err := tx.Exec(
			`INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms, status, expires_at_ms)
			 VALUES ('news', ?, '{}', 1, 'cancelled', NULL)`,
			fmt.Sprintf("t/cancel-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := store.AppendOutbox(ctx, "news", "warnflux/events", wirePayload("active", time.Hour)); !errors.Is(err, storage.ErrOutboxFull) {
		t.Fatalf("AppendOutbox on full corrective outbox = %v, want ErrOutboxFull", err)
	}
	if _, err := store.CommitIngest(ctx, "news", "warnflux/events",
		wirePayload("active", time.Hour), commitEvent("k2", dispatch.TransitionNew)); !errors.Is(err, storage.ErrOutboxFull) {
		t.Fatalf("CommitIngest on full corrective outbox = %v, want ErrOutboxFull", err)
	}

	// The provably obsolete overflow still succeeds: expired rows are
	// evicted and the append lands. Bulk insert keeps the test fast; the
	// single append triggers the capacity enforcement inside its own
	// transaction.
	store2 := openTemp(t)
	past := time.Now().Add(-time.Hour).UnixMilli()
	tx2, err := store2.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < outboxRowCap+5; i++ {
		if _, err := tx2.Exec(
			`INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms, status, expires_at_ms)
			 VALUES ('news', ?, '{}', 1, 'active', ?)`,
			fmt.Sprintf("t/%d", i), past); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.AppendOutbox(ctx, "news", "warnflux/events", []byte(`{"x":1}`)); err != nil {
		t.Fatalf("AppendOutbox with obsolete overflow: %v", err)
	}
	if n, err := store2.OutboxCount(ctx); err != nil || n > outboxRowCap {
		t.Fatalf("OutboxCount after obsolete overflow = (%d, %v), want <= %d", n, err, outboxRowCap)
	}
}

// TestOutboxCapacityNeverDropsStillValid pins the reported P1: when the
// queue overflows with still-valid pending events (expiry in the
// future) there is nothing safe to evict, so appends fail with
// storage.ErrOutboxFull — explicit backpressure with no partial write —
// and the accepted backlog stays untouched until publishing drains it.
func TestOutboxCapacityNeverDropsStillValid(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()
	future := time.Now().Add(time.Hour).UnixMilli()

	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < outboxRowCap+5; i++ {
		if _, err := tx.Exec(
			`INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms, status, expires_at_ms)
			 VALUES ('news', ?, '{}', 1, 'active', ?)`,
			fmt.Sprintf("t/valid-%d", i), future); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Both append paths reject without touching the backlog.
	if _, err := store.AppendOutbox(ctx, "news", "warnflux/events",
		wirePayload("active", time.Hour)); !errors.Is(err, storage.ErrOutboxFull) {
		t.Fatalf("AppendOutbox on still-valid backlog = %v, want ErrOutboxFull", err)
	}
	if _, err := store.CommitIngest(ctx, "news", "warnflux/events",
		wirePayload("active", time.Hour), commitEvent("k2", dispatch.TransitionNew)); !errors.Is(err, storage.ErrOutboxFull) {
		t.Fatalf("CommitIngest on still-valid backlog = %v, want ErrOutboxFull", err)
	}
	if n, err := store.OutboxCount(ctx); err != nil || n != outboxRowCap+5 {
		t.Fatalf("OutboxCount after rejected appends = (%d, %v), want %d (nothing dropped, no partial write)",
			n, err, outboxRowCap+5)
	}

	// Once the pending events actually expire, they become provably
	// obsolete and the next append evicts them and succeeds.
	if _, err := store.db.Exec(
		`UPDATE ingest_outbox SET expires_at_ms = ?`, time.Now().Add(-time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendOutbox(ctx, "news", "warnflux/events", wirePayload("active", time.Hour)); err != nil {
		t.Fatalf("AppendOutbox after expiry: %v", err)
	}
	if n, err := store.OutboxCount(ctx); err != nil || n > outboxRowCap {
		t.Fatalf("OutboxCount after expiry eviction = (%d, %v), want <= %d", n, err, outboxRowCap)
	}
}

// TestOutboxCapacityEvictsSuperseded pins the second obsolete class: a
// backlog of OLD versions of one event identity (still valid expiry, so
// the expiry clause must not apply) is evicted in favour of the newest
// version — only rows replaced by a newer version go.
func TestOutboxCapacityEvictsSuperseded(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()
	future := time.Now().Add(time.Hour).UnixMilli()

	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < outboxRowCap+5; i++ {
		if _, err := tx.Exec(
			`INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms, status, expires_at_ms, event_key, publisher, change_id)
			 VALUES ('news', ?, '{}', 1, 'active', ?, 'aprs:k', 'publisher-1', 1)`,
			fmt.Sprintf("t/old-%d", i), future); err != nil {
			t.Fatal(err)
		}
	}
	// The newest version of the same identity.
	if _, err := tx.Exec(
		`INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms, status, expires_at_ms, event_key, publisher, change_id)
		 VALUES ('news', 't/new', '{}', 1, 'active', ?, 'aprs:k', 'publisher-1', 2)`, future); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// The append evicts the superseded rows; the newest version and the
	// appended row survive.
	if _, err := store.AppendOutbox(ctx, "news", "warnflux/events", []byte(`{"x":1}`)); err != nil {
		t.Fatalf("AppendOutbox with superseded backlog: %v", err)
	}
	var newest, old int
	if err := store.db.QueryRow(
		`SELECT COUNT(*) FROM ingest_outbox WHERE event_key = 'aprs:k' AND change_id = 2`).Scan(&newest); err != nil {
		t.Fatal(err)
	}
	if newest != 1 {
		t.Fatalf("newest version rows = %d, want 1", newest)
	}
	if err := store.db.QueryRow(
		`SELECT COUNT(*) FROM ingest_outbox WHERE event_key = 'aprs:k' AND change_id = 1`).Scan(&old); err != nil {
		t.Fatal(err)
	}
	if old != 0 {
		t.Fatalf("superseded rows after append = %d, want 0", old)
	}
}

// TestOutboxLifecycleFromChangeType pins the P1 root: the queue status
// derives from the canonical change_type, never from the wire
// event.status — the simplified builder wrote "active" even for a
// cancellation, and the retention read exactly that field.
func TestOutboxLifecycleFromChangeType(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	// The buggy builder shape: corrective transition, active status,
	// expired long ago.
	buggy := []byte(`{"change_type":"cancelled","event_key":"k","publisher":"p1",` +
		`"event":{"status":"active","expires_at":"` + time.Now().Add(-time.Hour).UTC().Format(time.RFC3339) + `"}}`)
	if _, err := store.AppendOutbox(ctx, "news", "warnflux/events", buggy); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := store.db.QueryRow(`SELECT status FROM ingest_outbox WHERE event_key = 'k'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "cancelled" {
		t.Fatalf("queued status = %q, want cancelled (derived from change_type)", status)
	}

	// Retention must keep the corrective row even though its expiry
	// passed long ago.
	if n, err := store.PruneOutbox(ctx, time.Now().Add(-24*time.Hour)); err != nil || n != 0 {
		t.Fatalf("PruneOutbox = (%d, %v), want 0 (the cancellation never ages out)", n, err)
	}
	if n, err := store.OutboxCount(ctx); err != nil || n != 1 {
		t.Fatalf("OutboxCount after prune = (%d, %v), want 1", n, err)
	}

	// Legacy payloads without a change_type still fall back to the wire
	// status (backward compatible).
	legacy := []byte(`{"event_key":"l","event":{"status":"active","expires_at":"` +
		time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + `"}}`)
	if _, err := store.AppendOutbox(ctx, "news", "warnflux/events", legacy); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT status FROM ingest_outbox WHERE event_key = 'l'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "active" {
		t.Fatalf("legacy status = %q, want active (event.status fallback)", status)
	}
}

// TestOutboxMigrationRepairsCancellationStatus pins the v40 repair: rows
// persisted before the fix (corrective transition stored as active)
// re-derive their status from the payload's change_type on upgrade.
func TestOutboxMigrationRepairsCancellationStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "repair.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := migrate(db, migrations[:39]); err != nil {
		t.Fatalf("migrate to v39: %v", err)
	}
	buggy := []byte(`{"change_type":"cancelled","change_id":42,"event_key":"aprs:fix","publisher":"p1",` +
		`"event":{"status":"active","expires_at":"` + time.Now().Add(-time.Hour).UTC().Format(time.RFC3339) + `"}}`)
	if _, err := db.Exec(
		`INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms, status, expires_at_ms, event_key, publisher)
		 VALUES ('news', 'warnflux/events', ?, 1, 'active', NULL, 'aprs:fix', 'p1')`, string(buggy)); err != nil {
		t.Fatalf("insert buggy row: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store := openTempAt(t, path)
	var status string
	var changeID int64
	if err := store.db.QueryRow(`SELECT status, change_id FROM ingest_outbox WHERE event_key = 'aprs:fix'`).Scan(&status, &changeID); err != nil {
		t.Fatal(err)
	}
	if status != "cancelled" {
		t.Fatalf("repaired status = %q, want cancelled", status)
	}
	if changeID != 42 {
		t.Fatalf("backfilled change_id = %d, want 42", changeID)
	}
}
