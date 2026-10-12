package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// TestMessageIDRoundTripAndImmutability pins the durable assignment: the
// first write wins, so a replay never renumbers an already-cited event.
func TestMessageIDRoundTripAndImmutability(t *testing.T) {
	store := newUsersStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 7, 19, 58, 0, 0, time.UTC)

	if _, ok, err := store.LoadMessageID(ctx, "imgw-meteo:1"); err != nil || ok {
		t.Fatalf("unexpected pre-existing id: ok=%v err=%v", ok, err)
	}
	first := core.MessageIDAssignment{
		EventKey: "imgw-meteo:1", MsgID: "WF-1007195801",
		Minute: "20261007-1958", Seq: 1, CreatedAt: now,
	}
	if err := store.SaveMessageID(ctx, first); err != nil {
		t.Fatalf("SaveMessageID: %v", err)
	}
	// A later, different assignment for the same key must be ignored.
	if err := store.SaveMessageID(ctx, core.MessageIDAssignment{
		EventKey: "imgw-meteo:1", MsgID: "WF-1007195902",
		Minute: "20261007-1959", Seq: 2, CreatedAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("SaveMessageID (conflict): %v", err)
	}
	id, ok, err := store.LoadMessageID(ctx, "imgw-meteo:1")
	if err != nil || !ok || id != "WF-1007195801" {
		t.Fatalf("LoadMessageID = %q, %v, %v; want the first WF-1007195801", id, ok, err)
	}
}

// TestHighestSeqInMinute pins the restart seam: the sequence is scoped to
// the minute bucket, so a new minute restarts at zero.
func TestHighestSeqInMinute(t *testing.T) {
	store := newUsersStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 7, 19, 58, 0, 0, time.UTC)

	if n, err := store.HighestSeqInMinute(ctx, "20261007-1958"); err != nil || n != 0 {
		t.Fatalf("empty minute = %d, %v; want 0", n, err)
	}
	for i, key := range []string{"a", "b", "c"} {
		if err := store.SaveMessageID(ctx, core.MessageIDAssignment{
			EventKey: key, MsgID: "WF-10071958" + string(rune('0'+i+1)),
			Minute: "20261007-1958", Seq: i + 1, CreatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := store.HighestSeqInMinute(ctx, "20261007-1958"); err != nil || n != 3 {
		t.Fatalf("seq = %d, %v; want 3", n, err)
	}
	if n, err := store.HighestSeqInMinute(ctx, "20261007-1959"); err != nil || n != 0 {
		t.Fatalf("other minute = %d, %v; want 0", n, err)
	}
}

// TestPruneMessageIDsKeepsLiveEvents pins the retention rule: ids of
// retained events and panel communications survive, ids of events that no
// longer exist are dropped.
func TestPruneMessageIDsKeepsLiveEvents(t *testing.T) {
	store := newUsersStore(t)
	ctx := context.Background()
	old := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	cutoff := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)

	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO events(event_key, source, source_id, fingerprint, status, event,
			received_at, first_seen_at, last_seen_at, updated_at)
		VALUES('imgw-meteo:1','imgw-meteo','1','fp','active','Storm','t','t','t','t')`); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO compose_hazards(event_key, hazard_json, status, updated_at_ms)
		VALUES('compose:1','{}','active',0)`); err != nil {
		t.Fatalf("seed compose hazard: %v", err)
	}
	for _, key := range []string{"imgw-meteo:1", "compose:1", "gone:1"} {
		if err := store.SaveMessageID(ctx, core.MessageIDAssignment{
			EventKey: key, MsgID: "WF-0901100001", Minute: "20260901-1000", Seq: 1, CreatedAt: old,
		}); err != nil {
			t.Fatal(err)
		}
	}

	n, err := store.PruneMessageIDs(ctx, cutoff)
	if err != nil {
		t.Fatalf("PruneMessageIDs: %v", err)
	}
	if n != 1 {
		t.Fatalf("pruned %d rows, want 1", n)
	}
	for key, want := range map[string]bool{"imgw-meteo:1": true, "compose:1": true, "gone:1": false} {
		_, ok, err := store.LoadMessageID(ctx, key)
		if err != nil {
			t.Fatalf("LoadMessageID(%q): %v", key, err)
		}
		if ok != want {
			t.Errorf("LoadMessageID(%q) present=%v, want %v", key, ok, want)
		}
	}
}

// TestMessageIDAccessInsideTransactionDoesNotDeadlock pins the P0 that
// wedged the panel: the ingest flow stamps an event's message id while
// its transaction is open, and a store lookup there waited for the very
// connection the transaction held (16 goroutines parked, every later
// request hanging, until the daemon was restarted). Serves from memory,
// so an open transaction cannot block it.
func TestMessageIDAccessInsideTransactionDoesNotDeadlock(t *testing.T) {
	store := newUsersStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 12, 15, 30, 0, 0, time.UTC)
	assignment := core.MessageIDAssignment{
		EventKey: "compose:17", MsgID: "WF-1012153001",
		Minute: "20261012-1530", Seq: 1, CreatedAt: now,
	}

	done := make(chan error, 1)
	go func() {
		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			done <- err
			return
		}
		defer tx.Rollback()
		// A write first: the transaction holds the connection and its
		// write lock from here on.
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO compose_hazards(event_key, hazard_json, status, updated_at_ms)
			VALUES('compose:17', '{}', 'active', 0)`); err != nil {
			done <- err
			return
		}
		if err := store.SaveMessageID(ctx, assignment); err != nil {
			done <- err
			return
		}
		id, ok, err := store.LoadMessageID(ctx, assignment.EventKey)
		if err != nil {
			done <- err
			return
		}
		if !ok || id != assignment.MsgID {
			done <- fmt.Errorf("LoadMessageID = %q, %v; want %q", id, ok, assignment.MsgID)
			return
		}
		if n, err := store.HighestSeqInMinute(ctx, assignment.Minute); err != nil || n != 1 {
			done <- fmt.Errorf("HighestSeqInMinute = %d, %v; want 1", n, err)
			return
		}
		done <- tx.Commit()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("message-id access with a transaction open: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("message-id access blocked with a transaction open (single-connection deadlock)")
	}
}

// TestMessageIDsSurviveReopen pins the promise the in-memory mirror must
// keep: an assignment reported as recorded is on disk once the store is
// closed, and a fresh store serves it without renumbering.
func TestMessageIDsSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ids.db")
	ctx := context.Background()
	now := time.Date(2026, 10, 12, 15, 30, 0, 0, time.UTC)

	first, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := first.SaveMessageID(ctx, core.MessageIDAssignment{
		EventKey: "imgw-meteo:7", MsgID: "WF-1012153002",
		Minute: "20261012-1530", Seq: 2, CreatedAt: now,
	}); err != nil {
		t.Fatalf("SaveMessageID: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, _, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()
	id, ok, err := second.LoadMessageID(ctx, "imgw-meteo:7")
	if err != nil || !ok || id != "WF-1012153002" {
		t.Fatalf("after reopen LoadMessageID = %q, %v, %v; want WF-1012153002", id, ok, err)
	}
	if n, err := second.HighestSeqInMinute(ctx, "20261012-1530"); err != nil || n != 2 {
		t.Fatalf("after reopen HighestSeqInMinute = %d, %v; want 2", n, err)
	}
}

// logCapture collects the background writer's log lines.
type logCapture struct {
	mu    sync.Mutex
	lines []string
}

func (c *logCapture) logf(msg string, args ...any) {
	c.mu.Lock()
	c.lines = append(c.lines, fmt.Sprint(append([]any{msg}, args...)...))
	c.mu.Unlock()
}

// count returns how many captured lines contain needle.
func (c *logCapture) count(needle string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, line := range c.lines {
		if strings.Contains(line, needle) {
			n++
		}
	}
	return n
}

// waitForMessageIDs polls cond until it holds or the timeout expires.
func waitForMessageIDs(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// breakMessageIDTable makes every insert into message_ids fail, the way a
// database that is briefly unavailable does. A trigger is used instead of
// dropping the table: the schema (and the rows already written) stay as
// they are, so only the writes fail.
func breakMessageIDTable(t *testing.T, store *Store) {
	t.Helper()
	if _, err := store.db.Exec(`
		CREATE TRIGGER message_ids_offline BEFORE INSERT ON message_ids
		BEGIN SELECT RAISE(ABORT, 'message_ids is offline'); END`); err != nil {
		t.Fatalf("break message_ids: %v", err)
	}
}

func restoreMessageIDTable(t *testing.T, store *Store) {
	t.Helper()
	if _, err := store.db.Exec(`DROP TRIGGER message_ids_offline`); err != nil {
		t.Fatalf("restore message_ids: %v", err)
	}
}

// messageIDRows counts the persisted assignments.
func messageIDRows(t *testing.T, store *Store) int {
	t.Helper()
	var n int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM message_ids`).Scan(&n); err != nil {
		t.Fatalf("count message_ids: %v", err)
	}
	return n
}

// TestMessageIDsSurviveATransientWriteFailure pins the P2 report: a
// database that refuses the insert for a while must not make the writer
// throw the assignments away. The ids are already assigned (and possibly
// already cited on the air), so dropping them silently renumbers the same
// events at the next restart — the writer has to keep the batch, retry it
// and write it once the database accepts writes again.
func TestMessageIDsSurviveATransientWriteFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ids.db")
	ctx := context.Background()
	now := time.Date(2026, 10, 12, 19, 40, 0, 0, time.UTC)
	logs := &logCapture{}

	store, _, err := Open(path,
		WithMessageIDLog(logs.logf),
		WithMessageIDRetry([]time.Duration{time.Millisecond, 2 * time.Millisecond}))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			store.Close()
		}
	}()

	assignments := []core.MessageIDAssignment{
		{EventKey: "imgw-meteo:11", MsgID: "WF-1012194001", Minute: "20261012-1940", Seq: 1, CreatedAt: now},
		{EventKey: "imgw-meteo:12", MsgID: "WF-1012194002", Minute: "20261012-1940", Seq: 2, CreatedAt: now},
		{EventKey: "compose:21", MsgID: "WF-1012194003", Minute: "20261012-1940", Seq: 3, CreatedAt: now},
	}

	breakMessageIDTable(t, store)
	for _, a := range assignments {
		if err := store.SaveMessageID(ctx, a); err != nil {
			t.Fatalf("SaveMessageID: %v", err)
		}
	}
	// The writer must burn through far more attempts than the old policy
	// allowed (ten, then the queue was thrown away) and still hold them.
	waitForMessageIDs(t, 5*time.Second, "the writer to retry the batch", func() bool {
		return logs.count("message id write failed") >= 15 || logs.count("abandoned") > 0
	})
	if n := logs.count("abandoned"); n != 0 {
		t.Fatalf("the writer abandoned %d assignments while the station was running:\n%s",
			n, strings.Join(logs.lines, "\n"))
	}
	if n := messageIDRows(t, store); n != 0 {
		t.Fatalf("persisted %d assignments while the table was missing", n)
	}
	// The ids are still served from memory: the station keeps citing them.
	for _, a := range assignments {
		id, ok, err := store.LoadMessageID(ctx, a.EventKey)
		if err != nil || !ok || id != a.MsgID {
			t.Fatalf("LoadMessageID(%q) = %q, %v, %v; want %q", a.EventKey, id, ok, err, a.MsgID)
		}
	}

	// Writes recover: every queued assignment lands, unasked.
	restoreMessageIDTable(t, store)
	waitForMessageIDs(t, 5*time.Second, "the queued batch to be written", func() bool {
		return messageIDRows(t, store) == len(assignments)
	})

	// A restart serves exactly the numbers the station reported.
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true
	second, _, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()
	for _, a := range assignments {
		id, ok, err := second.LoadMessageID(ctx, a.EventKey)
		if err != nil || !ok || id != a.MsgID {
			t.Fatalf("after reopen LoadMessageID(%q) = %q, %v, %v; want %q", a.EventKey, id, ok, err, a.MsgID)
		}
	}
	if n, err := second.HighestSeqInMinute(ctx, "20261012-1940"); err != nil || n != 3 {
		t.Fatalf("after reopen HighestSeqInMinute = %d, %v; want 3", n, err)
	}
}

// TestMessageIDWriterGivesUpOnlyAtShutdown pins the second half of the
// report: the give-up bound belongs to the shutdown path, is separate
// from the runtime retry policy and says so in the log. A shutdown must
// still be bounded — a database that no longer accepts writes must not
// turn Close into a hang.
func TestMessageIDWriterGivesUpOnlyAtShutdown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ids.db")
	ctx := context.Background()
	now := time.Date(2026, 10, 12, 19, 45, 0, 0, time.UTC)
	logs := &logCapture{}

	store, _, err := Open(path,
		WithMessageIDLog(logs.logf),
		WithMessageIDRetry([]time.Duration{time.Millisecond}))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	breakMessageIDTable(t, store)
	if err := store.SaveMessageID(ctx, core.MessageIDAssignment{
		EventKey: "imgw-meteo:13", MsgID: "WF-1012194501",
		Minute: "20261012-1945", Seq: 1, CreatedAt: now,
	}); err != nil {
		t.Fatalf("SaveMessageID: %v", err)
	}
	waitForMessageIDs(t, 5*time.Second, "the writer to retry the batch", func() bool {
		return logs.count("message id write failed") >= 5 || logs.count("abandoned") > 0
	})
	if n := logs.count("abandoned"); n != 0 {
		t.Fatalf("the writer gave up while the station was running (%d lines)\n%s",
			n, strings.Join(logs.lines, "\n"))
	}

	start := time.Now()
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(start); elapsed > messageIDShutdownWait+2*time.Second {
		t.Fatalf("Close took %s, want it bounded by %s", elapsed, messageIDShutdownWait)
	}
	// Close only returns once the writer is done, so the give-up line is
	// already there — and it names the shutdown, not the runtime policy.
	if n := logs.count("abandoned at shutdown"); n != 1 {
		t.Fatalf("shutdown abandonment logged %d times, want 1:\n%s", n, strings.Join(logs.lines, "\n"))
	}

	// Nothing was written: the assignment is gone with the process, which
	// is exactly what the loud log is for.
	second, _, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()
	if _, ok, err := second.LoadMessageID(ctx, "imgw-meteo:13"); err != nil || ok {
		t.Fatalf("LoadMessageID after shutdown = ok=%v, err=%v; want a missing assignment", ok, err)
	}
}
