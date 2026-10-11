package discord

// The periodic notices are exercised against the same fake server as the
// inbound path: the posts land on its /webhook handler. A notice is a
// CHANNEL message — the tests also pin that nothing is sent privately.

import (
	"context"
	"strings"
	"testing"
	"time"
)

// configureNotices wires the fixture's notices and makes both due now.
func (f *listenerFixture) configureNotices() {
	f.action.ConfigureNotices(NoticesConfig{
		Identity: "WarnFlux v0.7.4 - SOSNA - sosna.sp9moa.pl",
		Site:     "https://sosna.sp9moa.pl",
		Uptime:   func() string { return f.uptime },
		Summary:  func() HazardSummary { return f.summary },
	})
}

func TestNoticesHazardStatusOnChannel(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.summary = HazardSummary{Severe: 1, Other: 2}
	f.configureNotices()

	f.action.notices.hazardsTick(context.Background())

	posts := g.waitWebhook(1)
	// The status line carries the same "/hazard" hint the Meshtastic
	// channel appends, so both surfaces read alike.
	want := "WarnFlux: 1 severe or more, 2 other active — https://sosna.sp9moa.pl · " + hazardListHint
	if posts[0].content != want {
		t.Errorf("channel post = %q, want %q", posts[0].content, want)
	}
	if !strings.Contains(posts[0].content, "/hazard") {
		t.Error("the status line must tell the readers how to get the list")
	}

	// A periodic line is information for the channel: nothing is sent to
	// a person, not even to a registered operator.
	g.expectNoSend(0, 200*time.Millisecond)
	if n := g.openCount(); n != 0 {
		t.Errorf("opened %d direct-message channels, want 0", n)
	}

	// The cadence is honoured: the next tick is not due yet.
	f.action.notices.hazardsTick(context.Background())
	if n := len(g.webhookPosts()); n != 1 {
		t.Errorf("channel posts = %d, want 1 (not due yet)", n)
	}

	// Past the interval the line goes out again.
	f.setNow(f.at().Add(defaultHazardsInterval + time.Minute))
	f.action.notices.hazardsTick(context.Background())
	g.waitWebhook(2)
}

// TestNoticesSkipQuietHazardStatus pins the severe-or-higher rule: an
// hourly "0 severe or more, 12 other active" line trains the readers to
// ignore the line that matters, so only hazards at the alert bar or above
// reach the channel.
func TestNoticesSkipQuietHazardStatus(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.summary = HazardSummary{}
	f.configureNotices()

	f.action.notices.hazardsTick(context.Background())
	// Nothing while no hazard is active at all.
	if n := len(g.webhookPosts()); n != 0 {
		t.Fatalf("channel posts = %d, want 0 while nothing is active", n)
	}

	// Below the bar only: still nothing. The cadence was consumed by the
	// skipped window, so the next one has to come round first.
	f.summary = HazardSummary{Other: 12}
	f.setNow(f.at().Add(defaultHazardsInterval + time.Minute))
	f.action.notices.hazardsTick(context.Background())
	if n := len(g.webhookPosts()); n != 0 {
		t.Fatalf("channel posts = %d, want 0 for below-severe hazards only", n)
	}

	// One severe hazard and the line goes out, counters and hint alike.
	f.summary = HazardSummary{Severe: 1, Other: 12}
	f.setNow(f.at().Add(defaultHazardsInterval + time.Minute))
	f.action.notices.hazardsTick(context.Background())
	posts := g.waitWebhook(1)
	if !strings.Contains(posts[0].content, "1 severe or more, 12 other active") ||
		!strings.Contains(posts[0].content, "/hazard") {
		t.Errorf("channel post = %q, want the counters plus the hint", posts[0].content)
	}
}

func TestNoticesUptimeLine(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.uptime = "4h12m"
	f.configureNotices()

	f.action.notices.uptimeTick(context.Background())

	posts := g.waitWebhook(1)
	want := "WarnFlux v0.7.4 - SOSNA - sosna.sp9moa.pl | up 4h12m"
	if posts[0].content != want {
		t.Errorf("channel post = %q, want %q", posts[0].content, want)
	}
	g.expectNoSend(0, 200*time.Millisecond)

	// Once per interval.
	f.action.notices.uptimeTick(context.Background())
	if n := len(g.webhookPosts()); n != 1 {
		t.Errorf("channel posts = %d, want 1 (not due yet)", n)
	}
	f.setNow(f.at().Add(defaultUptimeInterval + time.Minute))
	f.action.notices.uptimeTick(context.Background())
	g.waitWebhook(2)
}

func TestNoticesNeedNoBotToken(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.action.token = ""
	f.action.dm = nil
	f.summary = HazardSummary{Severe: 3, Other: 0}
	f.configureNotices()

	f.action.notices.hazardsTick(context.Background())
	posts := g.waitWebhook(1)
	if !strings.Contains(posts[0].content, "3 severe or more, 0 other active") {
		t.Errorf("channel post = %q", posts[0].content)
	}
	// A webhook alone cannot reach a person — and the notices never try.
	g.expectNoSend(0, 200*time.Millisecond)
}

func TestNoticeIntervals(t *testing.T) {
	cases := []struct {
		name    string
		cfg     map[string]any
		wantErr string
		hazards time.Duration
		uptime  time.Duration
	}{
		{"defaults", map[string]any{}, "", defaultHazardsInterval, defaultUptimeInterval},
		{"explicit", map[string]any{"hazards_interval": "30m", "uptime_interval": "12h"}, "", 30 * time.Minute, 12 * time.Hour},
		{"disabled", map[string]any{"hazards_interval": "0s", "uptime_interval": "0s"}, "", 0, 0},
		{"too short", map[string]any{"hazards_interval": "30s"}, "must be between", 0, 0},
		{"too long", map[string]any{"uptime_interval": "1000h"}, "must be between", 0, 0},
	}
	for _, c := range cases {
		cfg := map[string]any{"url": "http://example.invalid/webhook"}
		for k, v := range c.cfg {
			cfg[k] = v
		}
		p, err := New(node(t, cfg))
		if c.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("%s: err = %v, want mention of %q", c.name, err, c.wantErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		a := p.(*discordAction)
		if a.notices.hazardsInterval != c.hazards || a.notices.uptimeInterval != c.uptime {
			t.Errorf("%s: intervals = %s/%s, want %s/%s", c.name,
				a.notices.hazardsInterval, a.notices.uptimeInterval, c.hazards, c.uptime)
		}
	}
}

func TestNoticesRunStopsOnCancel(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.action.notices.hazardsInterval = 0
	f.action.notices.uptimeInterval = 0
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.action.RunNotices(ctx)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunNotices did not return after the context was cancelled")
	}
}
