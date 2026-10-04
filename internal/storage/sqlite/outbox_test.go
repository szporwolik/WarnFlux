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
// event identity (publisher + event key) plus the lifecycle fields.
func wireEventPayload(status, eventKey string, expiry time.Duration) []byte {
	b, err := json.Marshal(map[string]any{
		"event_key": eventKey,
		"publisher": "publisher-1",
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

// TestOutboxCapacityProtectsCorrective pins the P1 half: the capacity
// bound may only evict empty/active rows; a cancellation in the overflow
// zone survives pruning (the broker may still hold an old active
// document it must retire).
func TestOutboxCapacityProtectsCorrective(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	// Direct bulk insert keeps the test fast: 10002 evictable rows
	// (empty status) + 1 corrective cancellation appended LAST (newest).
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < outboxRowCap+2; i++ {
		if _, err := tx.Exec(
			`INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms) VALUES ('news', ?, '{}', ?)`,
			fmt.Sprintf("t/fill-%d", i), 1); err != nil {
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
	if total != outboxRowCap+1 {
		t.Fatalf("total rows after prune = %d, want %d (cap of evictable + 1 corrective)", total, outboxRowCap+1)
	}
}

// TestOutboxFullBackpressure pins the explicit failure: when corrective
// rows alone exceed the capacity bound there is nothing evictable, so
// appends fail with storage.ErrOutboxFull instead of dropping
// corrective sync.
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

	// The normal evictable overflow still succeeds: eviction clears room.
	// Bulk insert keeps the test fast; the single append triggers the
	// capacity enforcement inside its own transaction.
	store2 := openTemp(t)
	tx2, err := store2.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < outboxRowCap+5; i++ {
		if _, err := tx2.Exec(
			`INSERT INTO ingest_outbox (instance_id, topic, payload, created_at_ms) VALUES ('news', ?, '{}', 1)`,
			fmt.Sprintf("t/%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.AppendOutbox(ctx, "news", "warnflux/events", []byte(`{"x":1}`)); err != nil {
		t.Fatalf("AppendOutbox with evictable overflow: %v", err)
	}
	if n, err := store2.OutboxCount(ctx); err != nil || n > outboxRowCap {
		t.Fatalf("OutboxCount after evictable overflow = (%d, %v), want <= %d", n, err, outboxRowCap)
	}
}
