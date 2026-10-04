package mqtt

import (
	"context"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/mqttpolicy"
	"github.com/szporwolik/WarnFlux/internal/trail"
)

// stepsOf returns the step kinds of the newest trail for one event key.
func stepsOf(t *testing.T, rec *trail.Recorder, key string) []string {
	t.Helper()
	for _, tr := range rec.Recent(16) {
		if tr.Key == key {
			out := make([]string, 0, len(tr.Steps))
			for _, s := range tr.Steps {
				out = append(out, string(s.Kind))
			}
			return out
		}
	}
	return nil
}

// TestTrailRecordsPublish pins the MQTT stream as a first-class
// notification path: a successful Handle adds a delivered step (events +
// active topics) to the per-alert audit trail.
func TestTrailRecordsPublish(t *testing.T) {
	oldMask := mqttpolicy.Mask()
	t.Cleanup(func() { mqttpolicy.Set(oldMask) })

	rec := trail.NewRecorder(trail.DefaultMaxTrails)
	fc := &fakeClient{connected: true}
	o := newTestOutput(fc)
	o.SetTrailRecorder(rec)

	ev := activeEvent()
	rec.Receive(ev.Key(), ev.Source, "severe", ev.Event, ev.Headline, time.Now())
	if err := o.Handle(context.Background(), core.EventChange{ID: 1, Type: core.ChangeNew, Event: ev}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	kinds := stepsOf(t, rec, ev.Key())
	if len(kinds) == 0 {
		t.Fatal("no trail steps recorded")
	}
	if kinds[len(kinds)-1] != string(trail.StepDelivered) {
		t.Fatalf("last step = %q, want delivered", kinds[len(kinds)-1])
	}
	// The delivered step names the published topics.
	for _, tr := range rec.Recent(16) {
		if tr.Key != ev.Key() {
			continue
		}
		last := tr.Steps[len(tr.Steps)-1]
		if want := "mqtt publish: warnflux/events + warnflux/active"; last.Text != want {
			t.Fatalf("delivered step text = %q, want %q", last.Text, want)
		}
	}
}

// TestTrailRecordsFailure: a failed publish leaves a failed step so the
// /notifications page answers "why did the MQTT path not deliver?".
func TestTrailRecordsFailure(t *testing.T) {
	oldMask := mqttpolicy.Mask()
	t.Cleanup(func() { mqttpolicy.Set(oldMask) })

	rec := trail.NewRecorder(trail.DefaultMaxTrails)
	fc := &fakeClient{connected: true}
	o := newTestOutput(fc)
	o.SetTrailRecorder(rec)

	ev := activeEvent()
	rec.Receive(ev.Key(), ev.Source, "severe", ev.Event, ev.Headline, time.Now())
	fc.setPublishErr(func(topic string) error {
		return errTestPublish
	})
	if err := o.Handle(context.Background(), core.EventChange{ID: 1, Type: core.ChangeNew, Event: ev}); err == nil {
		t.Fatal("Handle must fail when the broker rejects the publish")
	}

	kinds := stepsOf(t, rec, ev.Key())
	if len(kinds) == 0 || kinds[len(kinds)-1] != string(trail.StepFailed) {
		t.Fatalf("last step = %v, want failed", kinds)
	}
}

// TestTrailMaskedSilence: a fully masked publish is a deliberate policy
// silence, not a delivery — it must not add any step.
func TestTrailMaskedSilence(t *testing.T) {
	oldMask := mqttpolicy.Mask()
	t.Cleanup(func() { mqttpolicy.Set(oldMask) })

	rec := trail.NewRecorder(trail.DefaultMaxTrails)
	fc := &fakeClient{connected: true}
	o := newTestOutput(fc)
	o.SetTrailRecorder(rec)

	ev := activeEvent()
	rec.Receive(ev.Key(), ev.Source, "severe", ev.Event, ev.Headline, time.Now())
	mqttpolicy.Set(oldMask &^ uint32(mqttpolicy.CatEvents|mqttpolicy.CatActive))
	if err := o.Handle(context.Background(), core.EventChange{ID: 1, Type: core.ChangeNew, Event: ev}); err != nil {
		t.Fatalf("Handle(masked): %v", err)
	}

	if kinds := stepsOf(t, rec, ev.Key()); len(kinds) != 1 {
		t.Fatalf("masked publish recorded steps: %v, want only received", kinds)
	}
}

// TestTrailUnknownKeyIgnored: events without a routing trail never
// fabricate one — the recorder ignores unknown keys.
func TestTrailUnknownKeyIgnored(t *testing.T) {
	oldMask := mqttpolicy.Mask()
	t.Cleanup(func() { mqttpolicy.Set(oldMask) })

	rec := trail.NewRecorder(trail.DefaultMaxTrails)
	fc := &fakeClient{connected: true}
	o := newTestOutput(fc)
	o.SetTrailRecorder(rec)

	ev := activeEvent()
	if err := o.Handle(context.Background(), core.EventChange{ID: 1, Type: core.ChangeNew, Event: ev}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if got := rec.Recent(4); len(got) != 0 {
		t.Fatalf("trails = %d, want 0 for unknown keys", len(got))
	}
}

// errTestPublish is a shared fake-broker failure.
var errTestPublish = &testPublishError{}

type testPublishError struct{}

func (e *testPublishError) Error() string { return "broker refused publish" }
