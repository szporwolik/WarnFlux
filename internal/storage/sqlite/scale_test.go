package sqlite

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestScaleSanity runs only when WARNFLUX_SCALE=1 is set. It ingests a
// configurable number of unique events (WARNFLUX_SCALE_TOTAL, default
// 2000), updates a subset, delivers and ACKs the journal, then cleans it
// up — reporting rough durations and the database size. It is intentionally
// not a benchmark suite.
//
// Note: every ingest is a full-sync SQLite transaction by design, so large
// totals take minutes on non-tmpfs disks. Run with -timeout 30m for the
// default target of 10k events.
func TestScaleSanity(t *testing.T) {
	if os.Getenv("WARNFLUX_SCALE") == "" {
		t.Skip("set WARNFLUX_SCALE=1 to run the scale sanity check")
	}
	total := 2000
	if v := os.Getenv("WARNFLUX_SCALE_TOTAL"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			t.Fatalf("WARNFLUX_SCALE_TOTAL must be a positive integer, got %q", v)
		}
		total = n
	}
	path := t.TempDir() + "/scale.db"
	store, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < total; i++ {
		e := normEvent()
		e.SourceID = fmt.Sprintf("scale-%05d", i)
		if outcome, _ := ingestOne(t, store, e); outcome != storage.OutcomeNew {
			t.Fatalf("event %d: %v", i, outcome)
		}
	}
	ingestDur := time.Since(start)

	// Update a subset (content change → new journal record).
	start = time.Now()
	updates := total / 10
	if updates == 0 {
		updates = 1
	}
	for i := 0; i < updates; i++ {
		e := normEvent()
		e.SourceID = fmt.Sprintf("scale-%05d", i)
		e.Severity = "severe"
		if outcome, _ := ingestOne(t, store, e); outcome != storage.OutcomeUpdated {
			t.Fatalf("update %d: %v", i, outcome)
		}
	}
	updateDur := time.Since(start)

	// Journal delivery + ACK for one output (ack each batch, as the output
	// worker does, so the cursor advances and the loop terminates).
	start = time.Now()
	var lastID int64
	for {
		batch, err := store.PollChanges(ctx, "out-a", 256)
		if err != nil {
			t.Fatalf("PollChanges: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		lastID = batch[len(batch)-1].ID
		if err := store.AckChanges(ctx, "out-a", lastID); err != nil {
			t.Fatalf("AckChanges: %v", err)
		}
	}
	deliverDur := time.Since(start)

	// Cleanup after retention passes.
	start = time.Now()
	n, err := store.CleanupChanges(ctx, time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("CleanupChanges: %v", err)
	}
	cleanupDur := time.Since(start)

	info, err := os.Stat(path)
	var size int64
	if err == nil {
		size = info.Size()
	}

	t.Logf("events=%d updates=%d ingest=%s update=%s deliver+ack=%s cleanup(deleted=%d)=%s db=%d bytes",
		total, updates, ingestDur, updateDur, deliverDur, n, cleanupDur, size)
}
