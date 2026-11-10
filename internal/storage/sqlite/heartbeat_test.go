package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestHeartbeatStore pins the history the dashboard strip reads: one row per
// minute (a second tick rewrites, never duplicates), newest data behind the
// retention bound disappears, and a quiet minute is simply absent — that
// absence is what the strip renders as "the station was not running".
func TestHeartbeatStore(t *testing.T) {
	store, _, err := Open(filepath.Join(t.TempDir(), "heartbeat.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	ctx := context.Background()

	first := int64(1_700_000_000_000)
	for i := 0; i < 3; i++ {
		if err := store.RecordHeartbeat(ctx, storage.HeartbeatSample{
			MinuteMS: first + int64(i)*60_000, Level: "ok",
		}); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	// The same minute twice: the newest assessment wins, no second row.
	if err := store.RecordHeartbeat(ctx, storage.HeartbeatSample{
		MinuteMS: first, Level: "warn", Problems: []string{"receivers", "clock"},
	}); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	got, err := store.HeartbeatsSince(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("samples = %d, want 3 (one per minute)", len(got))
	}
	if got[0].Level != "warn" || fmt.Sprint(got[0].Problems) != "[receivers clock]" {
		t.Errorf("rewritten minute = %+v, want warn with both problems", got[0])
	}
	if got[2].MinuteMS != first+120_000 {
		t.Errorf("newest sample = %d, want %d", got[2].MinuteMS, first+120_000)
	}

	// A since filter just above the oldest minute drops it.
	got, err = store.HeartbeatsSince(ctx, first+1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("filtered samples = %d, want 2", len(got))
	}

	removed, err := store.PruneHeartbeats(ctx, first+60_000)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Errorf("pruned %d rows, want 1", removed)
	}
	got, err = store.HeartbeatsSince(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("after prune = %d samples, want 2", len(got))
	}
}
