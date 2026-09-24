package state

import (
	"testing"
	"time"
)

// TestSnapshotPrunesExpired verifies that active hazards whose expires_at
// has passed are pruned from the mirror when a snapshot is taken, while
// future-expiry and open-ended hazards stay.
func TestSnapshotPrunesExpired(t *testing.T) {
	s := New()
	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(time.Hour)

	add := func(key string, expires *time.Time) {
		t.Helper()
		if err := s.AddOrUpdateActive("r1", "warnflux/active/x/"+key, Hazard{
			EventKey:  key,
			Status:    "active",
			ExpiresAt: expires,
		}); err != nil {
			t.Fatal(err)
		}
	}
	add("past", &past)
	add("future", &future)
	add("open-ended", nil)

	snap := s.Snapshot()
	if snap.ActiveCount != 2 {
		t.Fatalf("ActiveCount = %d, want 2 (expired hazard must be pruned)", snap.ActiveCount)
	}
	for _, h := range snap.Hazards {
		if h.EventKey == "past" {
			t.Fatalf("expired hazard %q still present in the snapshot", h.EventKey)
		}
	}

	// Pruning is idempotent: the second snapshot must be identical.
	if again := s.Snapshot(); again.ActiveCount != 2 {
		t.Fatalf("second Snapshot ActiveCount = %d, want 2", again.ActiveCount)
	}
}

// TestSnapshotPruneSkipsNonActive documents that the prune only targets
// entries the mirror treats as active hazards; other statuses (which the
// receivers normally never mirror) are left alone.
func TestSnapshotPruneSkipsNonActive(t *testing.T) {
	s := New()
	past := time.Now().Add(-time.Minute)
	if err := s.AddOrUpdateActive("r1", "t/1", Hazard{
		EventKey:  "x:1",
		Status:    "cancelled",
		ExpiresAt: &past,
	}); err != nil {
		t.Fatal(err)
	}
	if got := s.Snapshot().ActiveCount; got != 1 {
		t.Fatalf("ActiveCount = %d, want 1 (prune must only drop status active)", got)
	}
}
