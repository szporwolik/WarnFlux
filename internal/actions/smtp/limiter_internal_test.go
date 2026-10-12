package smtp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
)

// TestRateLimiterReleasesCancelledReservations pins the reported P1: the
// limiter advanced the next-send time BEFORE it looked at the context and
// kept that reservation when the wait was cancelled, so a broadcast that
// timed out spent the shared budget for recipients it never mailed — the
// next alarm was pushed minutes into the future.
func TestRateLimiterReleasesCancelledReservations(t *testing.T) {
	l := newRateLimiter(600) // one slot every 100 ms

	// An already cancelled request reserves nothing.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait with a cancelled context = %v, want context.Canceled", err)
	}
	if next := limiterNext(l); !next.IsZero() {
		t.Fatalf("a cancelled request reserved a slot at %s", next)
	}

	// The send that happens takes its slot ...
	if err := l.wait(context.Background()); err != nil {
		t.Fatalf("first wait: %v", err)
	}
	reserved := limiterNext(l)
	if reserved.IsZero() {
		t.Fatal("a completed wait did not reserve the next slot")
	}

	// ... and a request cancelled WHILE IT WAITS gives its slot back: the
	// budget is charged when the turn comes, so a waiter that gives up
	// leaves nothing behind.
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelWait()
	if err := l.wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled wait = %v, want the context error", err)
	}
	if next := limiterNext(l); !next.Equal(reserved) {
		t.Fatalf("the cancelled wait kept a reservation: next = %s, want %s", next, reserved)
	}
	if n := limiterQueue(l); n != 0 {
		t.Fatalf("the cancelled waiter stayed in the queue: %d waiters", n)
	}

	// The reported reproduction: a hundred cancelled calls of one
	// broadcast must not reserve a hundred slots.
	spent, cancelSpent := context.WithCancel(context.Background())
	cancelSpent()
	beforeBurst := limiterNext(l)
	for i := 0; i < 100; i++ {
		if err := l.wait(spent); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled call %d = %v, want context.Canceled", i, err)
		}
	}
	if next := limiterNext(l); !next.Equal(beforeBurst) {
		gap := next.Sub(beforeBurst)
		t.Fatalf("100 cancelled calls reserved %s of the shared budget", gap)
	}

	// The next alarm therefore waits only for the interval of the send
	// that DID happen instead of the two intervals a kept reservation
	// would have charged.
	start := time.Now()
	if err := l.wait(context.Background()); err != nil {
		t.Fatalf("wait after the cancellation: %v", err)
	}
	if waited := time.Since(start); waited > 600*time.Millisecond {
		t.Fatalf("the next send waited %s; the cancelled reservation was not released", waited)
	}
}

// limiterNext reads the limiter's next slot under its lock.
func limiterNext(l *rateLimiter) time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.next
}

// limiterQueue reports how many senders are waiting for a slot.
func limiterQueue(l *rateLimiter) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.queue)
}

// receiveWithin reads one admission time, failing the test instead of
// hanging when the queue stalled.
func receiveWithin(t *testing.T, ch chan time.Time, what string) time.Time {
	t.Helper()
	select {
	case ts := <-ch:
		return ts
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return time.Time{}
	}
}

// waitForLimiterQueue waits until exactly n senders are queued.
func waitForLimiterQueue(t *testing.T, l *rateLimiter, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if limiterQueue(l) == n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("queue = %d waiters, want %d", limiterQueue(l), n)
}

// TestCancelledWaitersInAnyOrderLeaveNoBudgetBehind pins the reported P2:
// only the LAST reservation used to be returned, so ten waiters cancelled
// ahead of it kept ten slots of the shared budget — the next send had to
// wait about ten seconds at 60/min even though nothing was sent.
func TestCancelledWaitersInAnyOrderLeaveNoBudgetBehind(t *testing.T) {
	l := newRateLimiter(600) // one slot per 100 ms

	// The send that happens takes its slot.
	if err := l.wait(context.Background()); err != nil {
		t.Fatalf("first wait: %v", err)
	}
	before := limiterNext(l)

	// Ten more queue up behind it and are all cancelled before their turn.
	cancels := make([]context.CancelFunc, 0, 10)
	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		go func() { done <- l.wait(ctx) }()
	}
	waitForLimiterQueue(t, l, 10)

	// Cancel them in a scrambled order: first, middle, last.
	order := []int{4, 0, 9, 2, 7, 1, 8, 3, 6, 5}
	for _, i := range order {
		cancels[i]()
	}
	for i := 0; i < 10; i++ {
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled waiter %d = %v, want context.Canceled", i, err)
		}
	}
	waitForLimiterQueue(t, l, 0)
	if next := limiterNext(l); !next.Equal(before) {
		t.Fatalf("the cancelled waiters charged the budget: next = %s, want %s", next, before)
	}

	// The next send waits for the slot of the message that DID go out —
	// one interval — instead of the ten that never did.
	start := time.Now()
	if err := l.wait(context.Background()); err != nil {
		t.Fatalf("wait after the cancellations: %v", err)
	}
	if waited := time.Since(start); waited > 600*time.Millisecond {
		t.Fatalf("the next send waited %s; the cancelled waiters left budget behind", waited)
	}
}

// yamlNode builds a config node from YAML text for the construction path.
func yamlNode(t *testing.T, text string) *yaml.Node {
	t.Helper()
	var n yaml.Node
	if err := yaml.Unmarshal([]byte(text), &n); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	return &n
}

// limiterIntervalFor reads the shared limiter's pace for one account.
func limiterIntervalFor(cfg Config) time.Duration {
	l := sharedLimiter(cfg)
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.interval
}

// TestPanelSendKeepsTheDefaultRateLimit pins the reported P2: the instance
// kept a copy of its configuration made BEFORE the unset rate limit was
// replaced by the 30/min default, so the panel's first direct send handed
// rate 0 to the shared limiter and zeroed the pace the routed alarms rely
// on (the reproduction measured 2 s → 0).
func TestPanelSendKeepsTheDefaultRateLimit(t *testing.T) {
	ResetLimitersForTest()
	// No rate_limit_per_minute: the default must apply everywhere.
	p, err := New(yamlNode(t, "host: 127.0.0.1\nport: 1\nfrom: wf@example.net\nto: [visible@example.net]\n"))
	if err != nil {
		t.Fatal(err)
	}
	a := p.(*emailAction)
	if a.cfg.RateLimitPerMinute != defaultRateLimitPerMinute {
		t.Fatalf("the instance kept rate %d, want the %d/min default",
			a.cfg.RateLimitPerMinute, defaultRateLimitPerMinute)
	}
	if got := limiterIntervalFor(a.cfg); got != time.Minute/defaultRateLimitPerMinute {
		t.Fatalf("interval after construction = %s, want %s", got, time.Minute/defaultRateLimitPerMinute)
	}

	// The panel sends through the instance's configuration: the dial fails
	// (port 1), but the limiter lookup already happened.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.SendNotice(ctx, []string{"one@example.net"}, "s", "b", action.AppInfo{}, "pl"); err == nil {
		t.Fatal("send against a closed port reported success")
	}
	if got := limiterIntervalFor(a.cfg); got != time.Minute/defaultRateLimitPerMinute {
		t.Fatalf("interval after a panel send = %s, want %s (the send disabled the limit)",
			got, time.Minute/defaultRateLimitPerMinute)
	}
}

// TestASendCannotChangeTheAccountPace pins the second half of the report:
// handing a raw configuration to a direct send must not re-pace the
// shared limiter, whatever that configuration says.
func TestASendCannotChangeTheAccountPace(t *testing.T) {
	ResetLimitersForTest()
	configured := Config{Host: "mail.example.net", Port: 587, RateLimitPerMinute: 60}
	configuredLimiter(configured) // what an instance does at construction

	raw := configured // same account, rate unset as a caller would pass it
	raw.RateLimitPerMinute = 0
	if got := limiterIntervalFor(raw); got != time.Second {
		t.Fatalf("interval after a raw lookup = %s, want 1s (60/min)", got)
	}
	// A reconfiguration (a reload) still applies.
	reloaded := configured
	reloaded.RateLimitPerMinute = 30
	configuredLimiter(reloaded)
	if got := limiterIntervalFor(reloaded); got != 2*time.Second {
		t.Fatalf("interval after a reload = %s, want 2s (30/min)", got)
	}
}

// TestReenablingTheAccountLimit pins the reported P2: a configuration with
// a negative limit stored NO limiter for the account, so a later
// configuration with a positive limit found the entry, called nothing on
// it and left every sender unlimited.
func TestReenablingTheAccountLimit(t *testing.T) {
	ResetLimitersForTest()
	off := Config{Host: "mail.example.net", Port: 587, RateLimitPerMinute: -1}
	if l := configuredLimiter(off); l == nil {
		t.Fatal("a negative limit stored no limiter at all; the limit could never come back")
	}
	if got := limiterIntervalFor(off); got != 0 {
		t.Fatalf("interval with a negative limit = %s, want 0 (no pacing)", got)
	}

	// The same account is configured with a positive limit again.
	on := off
	on.RateLimitPerMinute = 600 // 100 ms, short enough to observe
	configuredLimiter(on)
	if got := limiterIntervalFor(on); got != 100*time.Millisecond {
		t.Fatalf("interval after -1 → 600 = %s, want 100ms (the limit was not switched back on)", got)
	}

	// It really paces again: the second send waits for its slot.
	ctx := context.Background()
	l := configuredLimiter(on)
	if err := l.wait(ctx); err != nil {
		t.Fatalf("first wait: %v", err)
	}
	start := time.Now()
	if err := l.wait(ctx); err != nil {
		t.Fatalf("second wait: %v", err)
	}
	if waited := time.Since(start); waited < 50*time.Millisecond {
		t.Fatalf("the second send waited %s although the limit was re-enabled", waited)
	}
}

// TestConcurrentWaitersKeepTheirSpacing pins the reported P2: the queue
// identified waiters by the ADDRESS of an empty struct, and such addresses
// can be equal — several waiters then believed they were first in line and
// the pace vanished (the report measured ~3 µs where 100 ms was due).
func TestConcurrentWaitersKeepTheirSpacing(t *testing.T) {
	const interval = 50 * time.Millisecond
	l := newRateLimiter(int(time.Minute / interval)) // 1200/min = 50 ms

	const senders = 4
	// Each sender reports how long it waited for its slot, measured from
	// the same starting point: the SPAN of the admissions is what the pace
	// shows, and it is immune to the order in which the goroutines report.
	waited := make(chan time.Duration, senders)
	release := make(chan struct{})
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-release // start together, so they really race for the queue
			if err := l.wait(context.Background()); err != nil {
				t.Errorf("wait: %v", err)
				return
			}
			waited <- time.Since(start)
		}()
	}
	close(release)
	wg.Wait()
	close(waited)

	var longest time.Duration
	n := 0
	for w := range waited {
		n++
		if w > longest {
			longest = w
		}
	}
	if n != senders {
		t.Fatalf("admitted %d senders, want %d", n, senders)
	}
	// The last sender waited for its place in the queue: about three
	// intervals. Without the pace every one of them would be admitted at
	// once (the report measured microseconds where 100 ms was due).
	want := time.Duration(senders-1) * interval
	if longest < want-interval/2 {
		t.Fatalf("the last of %d senders was admitted after %s, want about %s — the pace was skipped",
			senders, longest, want)
	}
	if longest > want+5*time.Second {
		t.Fatalf("the last sender was admitted after %s, want about %s", longest, want)
	}
}

// TestCancellingTheMiddleWaiterKeepsTheQueue pins the other half: a waiter
// cancelled IN THE MIDDLE of the queue must leave it without stalling the
// senders behind it, and its slot must not be charged.
func TestCancellingTheMiddleWaiterKeepsTheQueue(t *testing.T) {
	const interval = 50 * time.Millisecond
	l := newRateLimiter(int(time.Minute / interval))
	sequenceStart := time.Now()

	// The send that happens takes its slot.
	if err := l.wait(context.Background()); err != nil {
		t.Fatalf("first wait: %v", err)
	}
	before := limiterNext(l)

	ctxs := make([]context.CancelFunc, 3)
	done := make([]chan error, 3)
	admitted := make([]chan time.Time, 3)
	// Register them one at a time, so the queue order is known: the second
	// one really is the middle element.
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		ctxs[i] = cancel
		done[i] = make(chan error, 1)
		admitted[i] = make(chan time.Time, 1)
		go func(i int) {
			if err := l.wait(ctx); err != nil {
				done[i] <- err
				return
			}
			admitted[i] <- time.Now()
			done[i] <- nil
		}(i)
		waitForLimiterQueue(t, l, i+1)
	}

	// Cancel the MIDDLE one.
	ctxs[1]()
	if err := <-done[1]; !errors.Is(err, context.Canceled) {
		t.Fatalf("middle waiter = %v, want context.Canceled", err)
	}
	waitForLimiterQueue(t, l, 2)

	// The two behind it are still served (a stall must fail, not hang),
	// and one at a time: while the waiter that was ahead of the cancelled
	// one holds the slot, the other one is still queued.
	_ = receiveWithin(t, admitted[0], "the waiter ahead of the cancelled one")
	if n := limiterQueue(l); n != 1 {
		t.Fatalf("queue = %d waiters after the first admission, want 1 (the pace was skipped)", n)
	}
	_ = receiveWithin(t, admitted[2], "the waiter behind the cancelled one")
	if err := <-done[0]; err != nil {
		t.Fatalf("first waiter: %v", err)
	}
	if err := <-done[2]; err != nil {
		t.Fatalf("last waiter: %v", err)
	}
	// The two admissions were spaced, so the whole sequence took at least
	// one interval of the two slots it used (nothing is measured across
	// goroutines, so the assertion cannot be inverted by scheduling).
	if elapsed := time.Since(sequenceStart); elapsed < interval*3/4 {
		t.Fatalf("both senders were admitted after %s, want at least %s — the pace was skipped",
			elapsed, interval*3/4)
	}
	// The cancelled waiter did not consume the slot it never used: the
	// budget moved by the two sends that happened (plus the first one).
	l.mu.Lock()
	next := l.next
	l.mu.Unlock()
	if want := before.Add(2 * interval); next.After(want.Add(interval)) {
		t.Fatalf("next slot = %s, want about %s (the cancelled waiter charged the budget)", next, want)
	}
	waitForLimiterQueue(t, l, 0)
}
