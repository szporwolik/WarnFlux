package trail

import (
	"testing"
	"time"
)

func at() time.Time { return time.Date(2026, 9, 23, 14, 32, 5, 0, time.Local) }

// TestRecorderLifecycle pins the basic append/read semantics.
func TestRecorderLifecycle(t *testing.T) {
	r := NewRecorder(10)
	r.Receive("imgw:1", "imgw", "severe", "Strong wind", "Gale warning", at())
	r.Add("imgw:1", StepMatched, "matched group Niepołomice", at())
	r.Add("imgw:1", StepDelivered, "delivered", at().Add(time.Second))
	r.SetOutcome("imgw:1", OutcomeDelivered)

	tr, ok := r.Get("imgw:1")
	if !ok {
		t.Fatal("trail not found")
	}
	if tr.Outcome != OutcomeDelivered || len(tr.Steps) != 3 {
		t.Fatalf("trail = %+v", tr)
	}
	if tr.Steps[0].Kind != StepReceived || tr.Steps[1].Kind != StepMatched ||
		tr.Steps[2].Kind != StepDelivered {
		t.Fatalf("steps = %+v", tr.Steps)
	}
	if tr.Steps[0].Seq >= tr.Steps[1].Seq || tr.Steps[1].Seq >= tr.Steps[2].Seq {
		t.Fatalf("step seqs not increasing: %+v", tr.Steps)
	}
	if tr.ReceivedAt == "" || tr.Source != "imgw" || tr.Severity != "severe" {
		t.Fatalf("header = %+v", tr)
	}

	// Unknown keys are ignored.
	r.Add("nope", StepSkipped, "x", at())
	if _, ok := r.Get("nope"); ok {
		t.Fatal("Add created a trail without Receive")
	}

	// Receive is idempotent: it never resets an existing trail.
	r.Receive("imgw:1", "imgw", "minor", "other", "other", at())
	tr, _ = r.Get("imgw:1")
	if tr.Severity != "severe" || len(tr.Steps) != 3 {
		t.Fatalf("Receive reset the trail: %+v", tr)
	}
}

// TestRecorderRing pins the bounded retention: oldest trails drop first.
func TestRecorderRing(t *testing.T) {
	r := NewRecorder(3)
	for i := 0; i < 5; i++ {
		r.Receive(string(rune('a'+i)), "imgw", "severe", "e", "h", at())
	}
	got := r.Recent(10)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if got[0].Key != "e" || got[2].Key != "c" {
		t.Fatalf("newest-first order = %q %q %q", got[0].Key, got[1].Key, got[2].Key)
	}
	if _, ok := r.Get("a"); ok {
		t.Fatal("oldest trail survived the cap")
	}
}

// TestRecorderNilSafe: a nil recorder never panics.
func TestRecorderNilSafe(t *testing.T) {
	var r *Recorder
	r.Receive("k", "s", "sev", "e", "h", at())
	r.Add("k", StepSkipped, "x", at())
	r.SetOutcome("k", OutcomeSkipped)
	if r.Recent(1) != nil {
		t.Fatal("nil recorder Recent must be nil")
	}
	if _, ok := r.Get("k"); ok {
		t.Fatal("nil recorder returned a trail")
	}
}
