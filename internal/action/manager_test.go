package action_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type recordingPlugin struct {
	name    string
	handled atomic.Int64
	delay   time.Duration
	ignore  bool // ignore ctx cancellation (hung behavior)
	fail    bool
	panics  bool
	closeC  chan struct{}
}

func (p *recordingPlugin) Name() string { return p.name }

func (p *recordingPlugin) Execute(ctx context.Context, req action.ActionRequest) error {
	p.handled.Add(1)
	if p.panics {
		panic("boom")
	}
	if p.delay > 0 {
		select {
		case <-time.After(p.delay):
		case <-ctx.Done():
			if p.ignore {
				// Simulate a callback that ignores cancellation: block long.
				time.Sleep(2 * time.Second)
			}
			return ctx.Err()
		}
	}
	if p.fail {
		return errors.New("synthetic failure")
	}
	return nil
}

func (p *recordingPlugin) Close(ctx context.Context) error {
	if p.closeC != nil {
		close(p.closeC)
	}
	return nil
}

func nodeCfg(t *testing.T, level string) *yaml.Node {
	t.Helper()
	if level == "" {
		return nil
	}
	var n yaml.Node
	if err := yaml.Unmarshal([]byte("level: "+level), &n); err != nil {
		t.Fatal(err)
	}
	return &n
}

func newManager(t *testing.T, reg *action.Registry, cfgs []config.Action) *action.Manager {
	t.Helper()
	m, err := action.NewManager(cfgs, reg, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	m.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = m.Shutdown(ctx)
	})
	return m
}

func sampleEvent() dispatch.Event {
	return dispatch.Event{
		Kind:       dispatch.EventMQTTMessage,
		ReceivedAt: time.Now(),
		Origin:     dispatch.Origin{Type: "mqtt", ReceiverID: "local"},
		MQTT:       &dispatch.MQTTMessage{Topic: "club/alarm/door", Payload: []byte("OPEN")},
	}
}

// TestExplicitRoutingOnly is the mandatory proof that ActionPlugins are not
// OutputPlugins: submitting to logger-a executes ONLY logger-a.
func TestExplicitRoutingOnly(t *testing.T) {
	reg := action.NewRegistry()
	pa, pb := &recordingPlugin{name: "a"}, &recordingPlugin{name: "b"}
	reg.Register("testa", func(*yaml.Node) (action.Plugin, error) { return pa, nil })
	reg.Register("testb", func(*yaml.Node) (action.Plugin, error) { return pb, nil })

	m := newManager(t, reg, []config.Action{
		{ID: "logger-a", Type: "testa", Enabled: true},
		{ID: "logger-b", Type: "testb", Enabled: true},
	})

	if err := m.Submit("logger-a", action.ActionRequest{ID: "r1", Event: sampleEvent()}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitFor(t, func() bool { return pa.handled.Load() >= 1 }, "logger-a never executed")
	if pb.handled.Load() != 0 {
		t.Errorf("logger-b executed %d times, want 0 (explicit routing only)", pb.handled.Load())
	}
}

func TestSubmitUnknownActionFails(t *testing.T) {
	reg := action.NewRegistry()
	m := newManager(t, reg, nil)
	if err := m.Submit("missing", action.ActionRequest{ID: "r1"}); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Errorf("Submit unknown action error = %v", err)
	}
}

func TestSubmitDisabledActionFails(t *testing.T) {
	reg := action.NewRegistry()
	reg.Register("testa", func(*yaml.Node) (action.Plugin, error) { return &recordingPlugin{}, nil })
	m := newManager(t, reg, []config.Action{{ID: "logger-a", Type: "testa", Enabled: false}})
	if err := m.Submit("logger-a", action.ActionRequest{ID: "r1"}); err == nil {
		t.Error("Submit to disabled action succeeded, want error")
	}
}

func TestSubmitUnknownTypeFailsConstruction(t *testing.T) {
	reg := action.NewRegistry()
	if _, err := action.NewManager([]config.Action{{ID: "x", Type: "nope", Enabled: true}}, reg, testLogger(), nil); err == nil {
		t.Fatal("unknown type accepted")
	}
	// Even disabled entries with unknown types fail (config typo safety).
	if _, err := action.NewManager([]config.Action{{ID: "x", Type: "nope", Enabled: false}}, reg, testLogger(), nil); err == nil {
		t.Fatal("disabled unknown type accepted")
	}
}

func TestFullQueueReturnsClearError(t *testing.T) {
	reg := action.NewRegistry()
	slow := &recordingPlugin{delay: 200 * time.Millisecond}
	reg.Register("testa", func(*yaml.Node) (action.Plugin, error) { return slow, nil })
	m := newManager(t, reg, []config.Action{{
		ID: "logger-a", Type: "testa", Enabled: true,
		Runtime: config.ActionRuntime{QueueSize: 2, CallTimeout: time.Second, ShutdownTimeout: time.Second},
	}})

	// Fill the queue faster than the worker drains.
	var lastErr error
	for i := 0; i < 20; i++ {
		lastErr = m.Submit("logger-a", action.ActionRequest{ID: "r", Event: sampleEvent()})
		if lastErr != nil {
			break
		}
	}
	if lastErr == nil || !strings.Contains(lastErr.Error(), "queue full") {
		t.Fatalf("expected clear queue-full error, got %v", lastErr)
	}
}

func TestPanicIsolation(t *testing.T) {
	reg := action.NewRegistry()
	p := &recordingPlugin{panics: true}
	reg.Register("testa", func(*yaml.Node) (action.Plugin, error) { return p, nil })
	m := newManager(t, reg, []config.Action{{
		ID: "logger-a", Type: "testa", Enabled: true,
		Runtime: config.ActionRuntime{QueueSize: 4, CallTimeout: time.Second, ShutdownTimeout: time.Second},
	}})
	if err := m.Submit("logger-a", action.ActionRequest{ID: "r1", Event: sampleEvent()}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		st := m.Statuses()[0]
		return st.Failures >= 1 && st.State == action.StateDegraded
	}, "panic not recorded as failure")
	// The manager stays usable.
	if err := m.Submit("logger-a", action.ActionRequest{ID: "r2", Event: sampleEvent()}); err != nil {
		t.Fatalf("Submit after panic: %v", err)
	}
}

func TestTimeoutDisablesHungPluginPermanently(t *testing.T) {
	reg := action.NewRegistry()
	hung := &recordingPlugin{delay: 500 * time.Millisecond, ignore: true}
	reg.Register("testa", func(*yaml.Node) (action.Plugin, error) { return hung, nil })
	m := newManager(t, reg, []config.Action{{
		ID: "logger-a", Type: "testa", Enabled: true,
		Runtime: config.ActionRuntime{QueueSize: 4, CallTimeout: 100 * time.Millisecond, ShutdownTimeout: time.Second},
	}})
	if err := m.Submit("logger-a", action.ActionRequest{ID: "r1", Event: sampleEvent()}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		st := m.Statuses()[0]
		return st.State == action.StateDisabled && strings.Contains(st.Reason, "ignored cancellation")
	}, "hung plugin not disabled")
	// Subsequent submits fail clearly.
	if err := m.Submit("logger-a", action.ActionRequest{ID: "r2", Event: sampleEvent()}); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Errorf("submit to hung plugin error = %v", err)
	}
}

func TestShutdownBoundedDespiteBrokenAction(t *testing.T) {
	reg := action.NewRegistry()
	hung := &recordingPlugin{delay: time.Hour, ignore: true}
	reg.Register("testa", func(*yaml.Node) (action.Plugin, error) { return hung, nil })
	m, err := action.NewManager([]config.Action{{
		ID: "logger-a", Type: "testa", Enabled: true,
		Runtime: config.ActionRuntime{QueueSize: 4, CallTimeout: 50 * time.Millisecond, ShutdownTimeout: 100 * time.Millisecond},
	}}, reg, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	m.Start(context.Background())
	if err := m.Submit("logger-a", action.ActionRequest{ID: "r1", Event: sampleEvent()}); err != nil {
		t.Fatal(err)
	}
	// Let the call hit its deadline and disable the instance.
	waitFor(t, func() bool { return m.Statuses()[0].State == action.StateDisabled }, "not disabled")

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = m.Shutdown(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	if elapsed > 1500*time.Millisecond {
		t.Errorf("shutdown took %s; broken action extended it", elapsed)
	}
}

func TestErrorIsolationKeepsOtherActionsRunning(t *testing.T) {
	reg := action.NewRegistry()
	failer := &recordingPlugin{fail: true}
	good := &recordingPlugin{}
	reg.Register("testfail", func(*yaml.Node) (action.Plugin, error) { return failer, nil })
	reg.Register("testgood", func(*yaml.Node) (action.Plugin, error) { return good, nil })
	m := newManager(t, reg, []config.Action{
		{ID: "failer", Type: "testfail", Enabled: true},
		{ID: "good", Type: "testgood", Enabled: true},
	})
	if err := m.Submit("failer", action.ActionRequest{ID: "r1", Event: sampleEvent()}); err != nil {
		t.Fatal(err)
	}
	if err := m.Submit("good", action.ActionRequest{ID: "r2", Event: sampleEvent()}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return good.handled.Load() >= 1 }, "good action never ran")
	waitFor(t, func() bool { return m.Statuses()[0].Failures >= 1 }, "failure not recorded")
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
