package web

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
)

// outboxDB is the real store: the queue's rules live in SQL, so a stub that
// mirrored them in Go would test the stub.
func outboxDB(t *testing.T) *sqlite.Store {
	t.Helper()
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "outbox.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// TestOldBacklogSurvivesTheFirstBatch is WF-R4-01: six messages waited out a
// 25-hour outage. Sending the first five must not delete the sixth — the prune
// aged rows by creation time, so the message nobody had even tried yet
// disappeared together with the history.
func TestOldBacklogSurvivesTheFirstBatch(t *testing.T) {
	store := outboxDB(t)
	ctx := context.Background()
	old := time.Now().Add(-25 * time.Hour)
	for i := 0; i < 6; i++ {
		if _, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{
			Kind: "dm", TargetID: "12345678", Label: "SP9MOA", Text: string(rune('0' + i)),
			Operator: "admin", CreatedAt: old,
		}); err != nil {
			t.Fatal(err)
		}
	}

	sender := &countingSender{connected: true}
	srv := quietServer()
	srv.flushMeshOutboxWith(ctx, sender, store)

	if sender.sent != 5 {
		t.Fatalf("sent %d messages, want the batch limit of 5", sender.sent)
	}
	pending, err := store.PendingMeshOutbox(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending = %d after one batch, want the sixth message still queued", len(pending))
	}

	// The second pass delivers it, and only then does the history start ageing.
	srv.flushMeshOutboxWith(ctx, sender, store)
	if sender.sent != 6 {
		t.Fatalf("sent %d messages in total, want 6", sender.sent)
	}
	recent, err := store.RecentFinishedMeshOutbox(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recent) != 6 {
		t.Fatalf("history = %d rows, want all six kept for the retention window", len(recent))
	}
	for _, item := range recent {
		if item.FinishedAt.IsZero() {
			t.Errorf("row %d has no finish time: %+v", item.ID, item)
		}
	}
}

// TestCancelDuringTheBatchWins is WF-R4-02: the dispatcher takes a batch, the
// operator cancels the second message while the first is on the air. The claim
// decides it — the cancelled message must never be transmitted, and the panel
// must not report a cancel it did not achieve.
func TestCancelDuringTheBatchWins(t *testing.T) {
	store := outboxDB(t)
	ctx := context.Background()
	now := time.Now()
	for i := 0; i < 2; i++ {
		if _, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{
			Kind: "dm", TargetID: "12345678", Text: string(rune('0' + i)), Operator: "admin", CreatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}

	sender := &countingSender{connected: true}
	sender.onFirstSend = func() {
		// The operator cancels the message that is next in the batch.
		if removed, err := store.DeletePendingMeshOutbox(ctx, 2); err != nil || !removed {
			t.Errorf("cancel during the batch: removed=%v err=%v", removed, err)
		}
	}
	quietServer().flushMeshOutboxWith(ctx, sender, store)

	if sender.sent != 1 {
		t.Fatalf("sent %d messages, want only the one that was already on the air", sender.sent)
	}

	// And the reverse order is honest: a row the dispatcher holds cannot be
	// reported as cancelled.
	id, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{
		Kind: "dm", TargetID: "12345678", Text: "zajeta", CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := store.ClaimMeshOutbox(ctx, id, time.Now(), time.Now().Add(-time.Minute)); err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	if removed, err := store.DeletePendingMeshOutbox(ctx, id); err != nil || removed {
		t.Errorf("a claimed row was reported cancelled: removed=%v err=%v", removed, err)
	}

	// A fresh claim is respected, a stale one (a crash between claim and send)
	// is up for grabs again — that is how the dispatcher asks.
	if ok, err := store.ClaimMeshOutbox(ctx, id, time.Now(),
		time.Now().Add(-storage.MeshOutboxClaimTTL)); err != nil || ok {
		t.Errorf("a fresh claim was taken over: ok=%v err=%v", ok, err)
	}
	staleID, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{
		Kind: "dm", TargetID: "12345678", Text: "porzucona przez restart", CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// A pass that claimed it ten minutes ago and died.
	if ok, err := store.ClaimMeshOutbox(ctx, staleID, time.Now().Add(-10*time.Minute),
		time.Now().Add(-time.Hour)); err != nil || !ok {
		t.Fatalf("stale setup claim = %v, %v", ok, err)
	}
	if ok, err := store.ClaimMeshOutbox(ctx, staleID, time.Now(),
		time.Now().Add(-storage.MeshOutboxClaimTTL)); err != nil || !ok {
		t.Errorf("a stale claim was not reclaimable: ok=%v err=%v", ok, err)
	}
}

// TestAbandonedMessageStaysVisible is WF-R4-03: after the last attempt fails the
// row is still in the database with its reason, and the panel has to show it —
// an operator who sees an empty queue believes the message went out.
func TestAbandonedMessageStaysVisible(t *testing.T) {
	store := outboxDB(t)
	ctx := context.Background()
	id, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{
		Kind: "dm", TargetID: "12345678", Label: "SP9MOA", Text: "nigdy nie wyslana",
		Operator: "admin", CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < storage.MeshOutboxMaxAttempts; i++ {
		if err := store.MarkMeshOutboxAttempt(ctx, id, "meshtastic: device not connected", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	// It is no longer offered to the dispatcher ...
	if pending, err := store.PendingMeshOutbox(ctx, 10); err != nil || len(pending) != 0 {
		t.Fatalf("pending = %v, %v; an exhausted message must not be retried", pending, err)
	}

	// ... but the panel still lists it, as abandoned, with the reason.
	srv := quietServer()
	srv.meshOutbox = store
	var v meshtasticView
	srv.fillMeshOutbox(ctx, &v)
	if len(v.Outbox) != 1 {
		t.Fatalf("panel rows = %d, want the abandoned message", len(v.Outbox))
	}
	if v.Outbox[0].State != "abandoned" {
		t.Errorf("state = %q, want abandoned", v.Outbox[0].State)
	}
	if v.Outbox[0].Attempts != storage.MeshOutboxMaxAttempts || v.Outbox[0].Error == "" {
		t.Errorf("the failure is not shown: %+v", v.Outbox[0])
	}
	if v.OutboxWaiting != 0 {
		t.Errorf("an abandoned message counts as waiting")
	}
}

// countingSender answers the two send calls and can act while the first one is
// in flight — the exact moment a cancel arrives.
type countingSender struct {
	connected   bool
	sent        int
	onFirstSend func()
}

func (c *countingSender) Connected() bool { return c.connected }

func (c *countingSender) SendContactMessage(_ context.Context, _, _, _ string) error {
	c.sent++
	if c.sent == 1 && c.onFirstSend != nil {
		c.onFirstSend()
	}
	return nil
}

func (c *countingSender) SendChannelText(_ context.Context, _ int, _, _ string) error {
	c.sent++
	if c.sent == 1 && c.onFirstSend != nil {
		c.onFirstSend()
	}
	return nil
}

var _ = slog.New

// TestPendingNotHiddenByHistory is WF-R5-02: one waiting message, twelve newer
// finished ones. The panel used to fetch a single "newest 12" page, so the
// queue looked empty and its counter said zero while a message was still owed a
// delivery. Pending and history are separate queries now, and the counter is the
// real queue size.
func TestPendingNotHiddenByHistory(t *testing.T) {
	store := outboxDB(t)
	ctx := context.Background()
	start := time.Now().Add(-time.Hour)

	waitingID, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{
		Kind: "dm", TargetID: "12345678", Label: "SP9MOA", Text: "czeka od godziny",
		Operator: "admin", CreatedAt: start,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		id, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{
			Kind: "dm", TargetID: "12345678", Text: "poszla", Operator: "admin",
			CreatedAt: start.Add(time.Duration(i+1) * time.Minute),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.MarkMeshOutboxSent(ctx, id, time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	srv := quietServer()
	srv.meshOutbox = store
	var v meshtasticView
	srv.fillMeshOutbox(ctx, &v)

	if v.OutboxWaiting != 1 {
		t.Errorf("waiting counter = %d, want the one pending message", v.OutboxWaiting)
	}
	found := false
	for _, row := range v.Outbox {
		if row.ID == waitingID {
			found = true
			if row.State != "waiting" {
				t.Errorf("pending row state = %q, want waiting", row.State)
			}
		}
	}
	if !found {
		t.Errorf("the pending message is not in the panel: %+v", v.Outbox)
	}
	// The active queue leads the list: it is what the operator has to act on.
	if len(v.Outbox) == 0 || v.Outbox[0].ID != waitingID {
		t.Errorf("the queue does not lead the list: %+v", v.Outbox)
	}

	// More pending than the page holds: the counter stays exact and the page
	// says how many are left over.
	for i := 0; i < meshOutboxPendingView+3; i++ {
		if _, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{
			Kind: "dm", TargetID: "12345678", Text: "kolejna", CreatedAt: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	v = meshtasticView{}
	srv.fillMeshOutbox(ctx, &v)
	if want := meshOutboxPendingView + 4; v.OutboxWaiting != want {
		t.Errorf("waiting counter = %d, want %d (the real queue size)", v.OutboxWaiting, want)
	}
	if v.OutboxMore != 4 {
		t.Errorf("OutboxMore = %d, want 4 messages beyond the page", v.OutboxMore)
	}
}
