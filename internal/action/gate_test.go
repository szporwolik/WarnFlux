package action

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
)

// fakeGateStore is the gate's storage seam, scripted per test.
type fakeGateStore struct {
	blocked    bool
	superseded bool
	verdict    storage.HazardVerdict
	err        error
	// asked records which lookups the gate performed.
	asked []string
}

func (f *fakeGateStore) LifecycleBlocks(context.Context, string, string, int64) (bool, error) {
	f.asked = append(f.asked, "blocks")
	return f.blocked, f.err
}

func (f *fakeGateStore) LifecycleSuperseded(context.Context, string, string, int64) (bool, error) {
	f.asked = append(f.asked, "superseded")
	return f.superseded, f.err
}

func (f *fakeGateStore) HazardActive(context.Context, string, string, time.Time) (storage.HazardVerdict, error) {
	f.asked = append(f.asked, "active")
	return f.verdict, f.err
}

func notifyRequest() ActionRequest {
	return ActionRequest{ID: "emcom:net/log", Event: dispatch.Event{
		Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{
			Key: "emcom:net", Source: "emcom", Publisher: "pub-1", ChangeID: 9,
			Type: dispatch.TransitionExpired, Notify: true,
			Hazard: dispatch.Hazard{EventKey: "emcom:net", Severity: "severe"},
		},
	}}
}

func hazardRequest() ActionRequest {
	return ActionRequest{ID: "imgw:1/log", Event: dispatch.Event{
		Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{
			Key: "imgw:1", Source: "imgw", Publisher: "pub-1", ChangeID: 3,
			Hazard: dispatch.Hazard{EventKey: "imgw:1"},
		},
	}}
}

// TestLifecycleGateStopsSupersededNotice pins the reported P1 on the
// gate itself: a queued stand-down notice must not go out once a NEWER
// version of the same message is recorded (the network was raised
// again), while the terminal status of the notice's own version — the
// whole reason it is being sent — must not hold it back.
func TestLifecycleGateStopsSupersededNotice(t *testing.T) {
	store := &fakeGateStore{superseded: true, verdict: storage.HazardInactive}
	gate := NewLifecycleGate(store, nil)

	if gate(context.Background(), notifyRequest()) {
		t.Fatal("a stand-down notice superseded by a newer version was allowed through")
	}
	for _, want := range []string{"superseded"} {
		found := false
		for _, got := range store.asked {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Errorf("the gate never asked for %q (asked %v)", want, store.asked)
		}
	}

	// The notice is the current version and terminal: it must go out,
	// even though the hazard itself is no longer active.
	fresh := &fakeGateStore{verdict: storage.HazardInactive}
	if !NewLifecycleGate(fresh, nil)(context.Background(), notifyRequest()) {
		t.Fatal("the current stand-down notice was suppressed")
	}
	for _, got := range fresh.asked {
		if got == "active" {
			t.Fatal("a Notify job must not be judged by the hazard's active state")
		}
	}
}

// TestLifecycleGateRules pins the alert path unchanged: superseded
// versions and terminal states are held back, an inactive hazard is held
// back, and every read failure fails open.
func TestLifecycleGateRules(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store *fakeGateStore
		want  bool
	}{
		{"active hazard passes", &fakeGateStore{verdict: storage.HazardActive}, true},
		{"inactive hazard is held back", &fakeGateStore{verdict: storage.HazardInactive}, false},
		{"unknown hazard passes", &fakeGateStore{verdict: storage.HazardUnknown}, true},
		{"superseded version is held back", &fakeGateStore{blocked: true}, false},
		{"read failure fails open", &fakeGateStore{err: errors.New("db down")}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := NewLifecycleGate(tc.store, nil)(context.Background(), hazardRequest()); got != tc.want {
				t.Fatalf("gate = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestLifecycleGateAgainstTheStore is the reported scenario end to end
// against the real ledger: a cancellation queued at version 9, then the
// network raised again at version 10. The queued notice is held back,
// and the very same notice sent while it is still the current version
// goes out.
func TestLifecycleGateAgainstTheStore(t *testing.T) {
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "gate.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	gate := NewLifecycleGate(store, nil)

	// Version 9: the network stands down. This is the notice in the queue.
	if err := store.RecordLifecycle(ctx, "pub-1", "emcom:net", 9, "cancelled"); err != nil {
		t.Fatal(err)
	}
	if !gate(ctx, notifyRequest()) {
		t.Fatal("the current stand-down notice (version 9) was suppressed")
	}

	// Version 10: the operator raises the network again. The queued
	// version 9 notice must now be held back.
	if err := store.RecordLifecycle(ctx, "pub-1", "emcom:net", 10, "active"); err != nil {
		t.Fatal(err)
	}
	if gate(ctx, notifyRequest()) {
		t.Fatal("the superseded stand-down notice reached the channels although version 10 is active")
	}

	// A notice that belongs to the newer version still goes out.
	next := notifyRequest()
	next.Event.Hazard.ChangeID = 11
	if err := store.RecordLifecycle(ctx, "pub-1", "emcom:net", 11, "cancelled"); err != nil {
		t.Fatal(err)
	}
	if !gate(ctx, next) {
		t.Fatal("the current stand-down notice (version 11) was suppressed")
	}
}
