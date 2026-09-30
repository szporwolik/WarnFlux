package action

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// runningStatus is the in-memory equivalent of the SQLite 'running'
// state (claimed, not yet settled).
const runningStatus storage.DeliveryStatus = 99

// memDeliveryStore mirrors the SQLite delivery-queue semantics in memory.
type memDeliveryStore struct {
	mu   sync.Mutex
	jobs map[string]*memJob
}

type memJob struct {
	job    storage.DeliveryJob
	status storage.DeliveryStatus
	next   time.Time
}

func newMemDeliveryStore() *memDeliveryStore {
	return &memDeliveryStore{jobs: map[string]*memJob{}}
}

func (m *memDeliveryStore) key(groupID int64, actionID, dedupKey string) string {
	return fmt.Sprintf("%d|%s|%s", groupID, actionID, dedupKey)
}

func (m *memDeliveryStore) EnqueueDelivery(ctx context.Context, job storage.DeliveryJob) (storage.DeliveryStatus, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := m.key(job.GroupID, job.ActionID, job.DedupKey)
	if j, ok := m.jobs[k]; ok {
		if j.status == storage.DeliveryFailed {
			j.job = job
			j.status = storage.DeliverySaved
			j.next = time.Time{}
			return storage.DeliverySaved, true, nil
		}
		if j.status == runningStatus {
			return storage.DeliverySaved, false, nil
		}
		return j.status, false, nil
	}
	m.jobs[k] = &memJob{job: job, status: storage.DeliverySaved}
	return storage.DeliverySaved, true, nil
}

func (m *memDeliveryStore) ClaimNextDelivery(ctx context.Context, actionID string, maxAttempts int, now time.Time) (storage.DeliveryJob, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	var best *memJob
	for _, j := range m.jobs {
		if j.job.ActionID != actionID {
			continue
		}
		if j.status != storage.DeliverySaved && j.status != storage.DeliveryFailed {
			continue
		}
		if !j.next.IsZero() && j.next.After(now) {
			continue
		}
		// Mirror the SQLite rule: the budget bounds retries, but a saved
		// job recovered from a crashed claim must still execute.
		if j.status != storage.DeliverySaved && j.job.Attempts >= maxAttempts {
			continue
		}
		if best == nil || j.next.Before(best.next) || (j.next.Equal(best.next) && j.job.FiredAt.Before(best.job.FiredAt)) {
			best = j
		}
	}
	if best == nil {
		return storage.DeliveryJob{}, false, nil
	}
	best.status = runningStatus
	best.job.Attempts++
	best.next = now.Add(5 * time.Minute) // claim lease
	return best.job, true, nil
}

func (m *memDeliveryStore) SettleDelivery(ctx context.Context, groupID int64, actionID, dedupKey string, stage storage.DeliveryStatus, nextAttempt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j, ok := m.jobs[m.key(groupID, actionID, dedupKey)]; ok {
		j.status = stage
		j.next = nextAttempt
	}
	return nil
}

func (m *memDeliveryStore) RecoverStaleClaims(ctx context.Context, now time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, j := range m.jobs {
		if j.status == runningStatus && j.next.Before(now) {
			j.status = storage.DeliverySaved
			n++
		}
	}
	return n, nil
}

func (m *memDeliveryStore) PendingDeliveries(ctx context.Context, actionID string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, j := range m.jobs {
		if j.job.ActionID != actionID {
			continue
		}
		if j.status != storage.DeliveryAccepted && j.status != storage.DeliveryConfirmed {
			n++
		}
	}
	return n, nil
}

// statusOf reports the settled status of one job (running maps to saved
// for readability in assertions).
func (m *memDeliveryStore) statusOf(groupID int64, actionID, dedupKey string) storage.DeliveryStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j, ok := m.jobs[m.key(groupID, actionID, dedupKey)]; ok {
		if j.status == runningStatus {
			return storage.DeliverySaved
		}
		return j.status
	}
	return -1
}

// plantRunningExpired leaves a job claimed by a "dead process": its
// claim lease has expired.
func (m *memDeliveryStore) plantRunningExpired(actionID string, payload []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs["1|"+actionID+"|c:1"] = &memJob{
		job:    storage.DeliveryJob{GroupID: 1, ActionID: actionID, EventKey: "imgw:1", DedupKey: "c:1", Payload: payload, Attempts: 1, FiredAt: time.Now()},
		status: runningStatus,
		next:   time.Now().Add(-time.Minute),
	}
}

type deliveryPlugin struct {
	handled atomic.Int64
	mu      sync.Mutex
	last    ActionRequest
	fail    bool
	stage   DeliveryStage
	stageOK bool
}

func (p *deliveryPlugin) Name() string { return "logger" }

func (p *deliveryPlugin) Execute(ctx context.Context, req ActionRequest) error {
	p.handled.Add(1)
	p.mu.Lock()
	p.last = req
	p.mu.Unlock()
	if p.fail {
		return errors.New("synthetic failure")
	}
	return nil
}

func (p *deliveryPlugin) ExecuteStage(ctx context.Context, req ActionRequest) (DeliveryStage, error) {
	if p.stageOK {
		p.handled.Add(1)
		p.mu.Lock()
		p.last = req
		p.mu.Unlock()
		return p.stage, nil
	}
	return StageAccepted, p.Execute(ctx, req)
}

func (p *deliveryPlugin) Close(ctx context.Context) error { return nil }

func waitForDelivery(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// newDeliveryInstance builds an instance with the durable queue attached
// and a running worker. Cleanup stops the worker.
func newDeliveryInstance(t *testing.T, p Plugin, retries int) (*Instance, *memDeliveryStore) {
	t.Helper()
	oldPoll, oldBackoff := DeliveryPollInterval, RetryBackoff
	DeliveryPollInterval, RetryBackoff = 5*time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { DeliveryPollInterval, RetryBackoff = oldPoll, oldBackoff })

	ms := newMemDeliveryStore()
	inst := NewInstance("log", "logger", p, 4, 2*time.Second, 2*time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)), nil, retries, nil)
	inst.setDeliveryStore(ms)
	ctx, cancel := context.WithCancel(context.Background())
	inst.Start(ctx)
	t.Cleanup(func() {
		cancel()
		inst.wg.Wait()
	})
	return inst, ms
}

func enqueueOne(t *testing.T, ms *memDeliveryStore, payload []byte) {
	t.Helper()
	st, queued, err := ms.EnqueueDelivery(context.Background(), storage.DeliveryJob{
		GroupID: 1, ActionID: "log", EventKey: "imgw:1", DedupKey: "c:1",
		Payload: payload, FiredAt: time.Now(),
	})
	if err != nil || st != storage.DeliverySaved || !queued {
		t.Fatalf("enqueue = (%v, %v, %v), want (saved, true, nil)", st, queued, err)
	}
}

// TestInstanceDurableDelivery pins the worker contract: a job persisted
// with its payload is claimed from the queue, executed, and settled as
// accepted only AFTER the execution succeeded.
func TestInstanceDurableDelivery(t *testing.T) {
	p := &deliveryPlugin{}
	_, ms := newDeliveryInstance(t, p, 1)

	ev := dispatch.Event{Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{Key: "imgw:1", Source: "imgw"}}
	payload, err := json.Marshal(ActionRequest{ID: "imgw:1/log", Event: ev, Bcc: []string{"a@example.net"}})
	if err != nil {
		t.Fatal(err)
	}
	enqueueOne(t, ms, payload)

	waitForDelivery(t, func() bool {
		return p.handled.Load() == 1 && ms.statusOf(1, "log", "c:1") == storage.DeliveryAccepted
	}, "durable job executed and settled")
	if st := ms.statusOf(1, "log", "c:1"); st != storage.DeliveryAccepted {
		t.Fatalf("settled = %v, want accepted", st)
	}
	p.mu.Lock()
	got := p.last
	p.mu.Unlock()
	if got.Event.Hazard == nil || got.Event.Hazard.Key != "imgw:1" {
		t.Errorf("executed payload event = %+v, want hazard key imgw:1", got.Event.Hazard)
	}
	if len(got.Bcc) != 1 || got.Bcc[0] != "a@example.net" {
		t.Errorf("executed payload Bcc = %v, want [a@example.net]", got.Bcc)
	}
}

// TestInstanceDurableRetryBudget pins the burned-budget contract: every
// failed attempt is settled with the next-attempt deadline, the budget
// caps the claims, and after the terminal failure the job stays failed
// (a replay re-arms it — see the storage tests).
func TestInstanceDurableRetryBudget(t *testing.T) {
	p := &deliveryPlugin{fail: true}
	_, ms := newDeliveryInstance(t, p, 2) // 3 attempts total

	ev := dispatch.Event{Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{Key: "imgw:1", Source: "imgw"}}
	payload, _ := json.Marshal(ActionRequest{ID: "imgw:1/log", Event: ev})
	enqueueOne(t, ms, payload)

	waitForDelivery(t, func() bool {
		return p.handled.Load() == 3 && ms.statusOf(1, "log", "c:1") == storage.DeliveryFailed
	}, "retry budget exhausted with terminal failure")

	// The terminal failure must not be claimed again.
	time.Sleep(50 * time.Millisecond)
	if got := p.handled.Load(); got != 3 {
		t.Errorf("executions = %d, want 3 (terminal failure must not re-run)", got)
	}
}

// TestInstanceDurableConfirmedStage pins the stage separation: a channel
// that can prove recipient-level confirmation reports it and the job is
// settled as confirmed, not just accepted.
func TestInstanceDurableConfirmedStage(t *testing.T) {
	p := &deliveryPlugin{stage: StageConfirmed, stageOK: true}
	_, ms := newDeliveryInstance(t, p, 0)

	ev := dispatch.Event{Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{Key: "imgw:1", Source: "imgw"}}
	payload, _ := json.Marshal(ActionRequest{ID: "imgw:1/log", Event: ev})
	enqueueOne(t, ms, payload)

	waitForDelivery(t, func() bool {
		return p.handled.Load() == 1 && ms.statusOf(1, "log", "c:1") == storage.DeliveryConfirmed
	}, "confirmed stage settled")
}

// TestInstanceLegacyEmptyPayload pins the migration seam: a pre-payload
// 'running' job recovered from an old database settles as accepted
// (its claim already stood for delivery) and never invokes the plugin.
func TestInstanceLegacyEmptyPayload(t *testing.T) {
	p := &deliveryPlugin{}
	_, ms := newDeliveryInstance(t, p, 1)
	enqueueOne(t, ms, nil)

	waitForDelivery(t, func() bool {
		return ms.statusOf(1, "log", "c:1") == storage.DeliveryAccepted
	}, "empty payload settled as accepted")
	if got := p.handled.Load(); got != 0 {
		t.Errorf("plugin invoked %d times, want 0 (no payload to execute)", got)
	}
}

// TestManagerStaleClaimRecovery pins the crash window end to end: a job
// left running by a dead process is re-queued by the manager and
// executed by the worker after the claim lease expires.
func TestManagerStaleClaimRecovery(t *testing.T) {
	oldPoll, oldRecovery := DeliveryPollInterval, DeliveryRecoveryInterval
	DeliveryPollInterval, DeliveryRecoveryInterval = 5*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { DeliveryPollInterval, DeliveryRecoveryInterval = oldPoll, oldRecovery })

	p := &deliveryPlugin{}
	ms := newMemDeliveryStore()
	ev := dispatch.Event{Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{Key: "imgw:1", Source: "imgw"}}
	payload, _ := json.Marshal(ActionRequest{ID: "imgw:1/log", Event: ev})
	ms.plantRunningExpired("log", payload)

	reg := NewRegistry()
	if err := reg.Register("logger", func(node *yaml.Node) (Plugin, error) { return p, nil }); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager([]config.Action{{
		ID: "log", Type: "logger", Enabled: true,
		Runtime: config.ActionRuntime{QueueSize: 1, CallTimeout: time.Second, ShutdownTimeout: time.Second},
	}}, reg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	m.SetDeliveryStore(ms)
	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	t.Cleanup(func() {
		cancel()
		_ = m.Shutdown(context.Background())
	})

	waitForDelivery(t, func() bool {
		return p.handled.Load() == 1 && ms.statusOf(1, "log", "c:1") == storage.DeliveryAccepted
	}, "stale claim recovered and executed")
}
