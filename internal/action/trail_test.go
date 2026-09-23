package action_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/metrics"
	"github.com/szporwolik/WarnFlux/internal/trail"
)

// flakyPlugin fails the first N calls, then succeeds.
type flakyPlugin struct {
	failures atomic.Int64
	limit    int
}

func (p *flakyPlugin) Name() string { return "flaky" }

func (p *flakyPlugin) Execute(context.Context, action.ActionRequest) error {
	if int(p.failures.Add(1)) <= p.limit {
		return errors.New("smtp: timeout")
	}
	return nil
}

func (p *flakyPlugin) Close(context.Context) error { return nil }

func hazardRequest(key string) action.ActionRequest {
	return action.ActionRequest{
		ID: key + "/smtp-alerts",
		Event: dispatch.Event{
			Kind: dispatch.EventHazardTransition,
			Hazard: &dispatch.HazardTransition{
				Key: key,
			},
		},
	}
}

// TestInstanceRetryTrail pins the retry semantics and the audit steps:
// a transient SMTP timeout is retried and the trail records every
// attempt, the retry gap and the final delivery.
func TestInstanceRetryTrail(t *testing.T) {
	shortBackoff(t)
	rec := trail.NewRecorder(10)
	rec.Receive("imgw:1", "imgw", "severe", "Strong wind", "Gale", time.Now())
	met := metrics.New()

	reg := action.NewRegistry()
	flaky := &flakyPlugin{limit: 1}
	reg.Register("flaky", func(*yaml.Node) (action.Plugin, error) { return flaky, nil })
	m, err := action.NewManager([]config.Action{{
		ID: "smtp-alerts", Type: "flaky", Enabled: true,
		Runtime: config.ActionRuntime{
			QueueSize:   4,
			CallTimeout: 10 * time.Second,
			Retries:     1,
		},
	}}, reg, testLogger(), rec, met)
	if err != nil {
		t.Fatal(err)
	}
	m.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = m.Shutdown(ctx)
	})

	if err := m.Submit("smtp-alerts", hazardRequest("imgw:1")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		tr, _ := rec.Get("imgw:1")
		return tr.Outcome == trail.OutcomeDelivered
	}, "delivery outcome")

	tr, _ := rec.Get("imgw:1")
	var kinds []string
	var texts []string
	for _, s := range tr.Steps {
		kinds = append(kinds, string(s.Kind))
		texts = append(texts, s.Text)
	}
	want := []string{"received", "failed", "retry", "delivered"}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Fatalf("kinds = %v, want %v (texts: %q)", kinds, want, texts)
	}
	if !strings.Contains(texts[1], "attempt 1/2 failed") {
		t.Errorf("failure step text = %q", texts[1])
	}
	if !strings.Contains(texts[3], "delivered after 1 retry") {
		t.Errorf("delivery step text = %q", texts[3])
	}

	// Metrics: one failed attempt, one retry, one delivery.
	out := met.Render()
	for _, want := range []string{
		`warnflux_notifications_total{action="smtp-alerts",result="failed"} 1`,
		`warnflux_notifications_total{action="smtp-alerts",result="delivered"} 1`,
		`warnflux_notification_retry_total{action="smtp-alerts"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q:\n%s", want, out)
		}
	}
}

// TestInstanceRetriesExhaustedTrail pins the failed outcome when all
// attempts fail.
func TestInstanceRetriesExhaustedTrail(t *testing.T) {
	shortBackoff(t)
	rec := trail.NewRecorder(10)
	rec.Receive("imgw:2", "imgw", "severe", "e", "h", time.Now())

	reg := action.NewRegistry()
	reg.Register("flaky", func(*yaml.Node) (action.Plugin, error) { return &flakyPlugin{limit: 99}, nil })
	m, err := action.NewManager([]config.Action{{
		ID: "smtp-alerts", Type: "flaky", Enabled: true,
		Runtime: config.ActionRuntime{QueueSize: 4, CallTimeout: 5 * time.Second, Retries: 2},
	}}, reg, testLogger(), rec, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = m.Shutdown(ctx)
	})

	if err := m.Submit("smtp-alerts", hazardRequest("imgw:2")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		tr, _ := rec.Get("imgw:2")
		return tr.Outcome == trail.OutcomeFailed
	}, "failed outcome")

	tr, _ := rec.Get("imgw:2")
	var kinds []string
	for _, s := range tr.Steps {
		kinds = append(kinds, string(s.Kind))
	}
	want := "received,failed,retry,failed,retry,failed"
	if strings.Join(kinds, ",") != want {
		t.Fatalf("kinds = %v, want %v", kinds, want)
	}
}

// TestInstanceTrailWithoutRecorder pins nil-safety: no trail recorder,
// no panic, and the delivery still happens (with retries).
func TestInstanceTrailWithoutRecorder(t *testing.T) {
	reg := action.NewRegistry()
	reg.Register("flaky", func(*yaml.Node) (action.Plugin, error) { return &flakyPlugin{limit: 0}, nil })
	m, err := action.NewManager([]config.Action{{
		ID: "smtp-alerts", Type: "flaky", Enabled: true,
		Runtime: config.ActionRuntime{QueueSize: 4, CallTimeout: 5 * time.Second, Retries: 2},
	}}, reg, testLogger(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = m.Shutdown(ctx)
	})
	if err := m.Submit("smtp-alerts", hazardRequest("imgw:3")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return m.Statuses()[0].Handled == 1
	}, "handled")
}

// shortBackoff shrinks the retry gap so tests finish quickly.
func shortBackoff(t *testing.T) {
	t.Helper()
	orig := action.RetryBackoff
	action.RetryBackoff = 20 * time.Millisecond
	t.Cleanup(func() { action.RetryBackoff = orig })
}
