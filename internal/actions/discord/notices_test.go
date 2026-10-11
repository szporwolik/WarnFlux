package discord

// The periodic notices are exercised against the same fake server as the
// inbound path: the webhook posts land on its /webhook handler and the
// direct-message copies on the REST channel endpoint, so one test sees
// both surfaces of one notice.

import (
	"context"
	"strings"
	"testing"
	"time"
)

// configureNotices wires the fixture's notices and makes both due now.
func (f *listenerFixture) configureNotices() {
	f.action.ConfigureNotices(NoticesConfig{
		Identity:   "WarnFlux v0.7.4 - SOSNA - sosna.sp9moa.pl",
		Site:       "https://sosna.sp9moa.pl",
		Uptime:     func() string { return f.uptime },
		Summary:    func() HazardSummary { return f.summary },
		Recipients: func(context.Context) ([]string, error) { return f.recipients, nil },
	})
}

func TestNoticesHazardStatusOnChannelAndDirectMessage(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.summary = HazardSummary{Severe: 1, Other: 2}
	f.recipients = []string{"42"}
	f.configureNotices()

	f.action.notices.hazardsTick(context.Background())

	posts := g.waitWebhook(1)
	want := "WarnFlux: 1 severe or more, 2 other active — https://sosna.sp9moa.pl"
	if posts[0].content != want {
		t.Errorf("channel post = %q, want %q", posts[0].content, want)
	}
	if strings.Contains(posts[0].content, "DM me") {
		t.Error("the channel copy must not advertise a command")
	}

	// The direct-message copy carries the same numbers plus the hint,
	// because commands live in direct messages.
	dm := g.waitSent(0)
	if wantDM := want + " · " + hazardHint; dm.content != wantDM {
		t.Errorf("direct message = %q, want %q", dm.content, wantDM)
	}
	if dm.channel != "dm-opened" {
		t.Errorf("direct message channel = %q, want the opened one", dm.channel)
	}
	if n := g.openCount(); n != 1 {
		t.Errorf("opened %d direct-message channels, want 1", n)
	}

	// The cadence is honoured: the next tick is not due yet.
	f.action.notices.hazardsTick(context.Background())
	if n := len(g.webhookPosts()); n != 1 {
		t.Errorf("channel posts = %d, want 1 (not due yet)", n)
	}
	if n := g.sentCount(); n != 1 {
		t.Errorf("direct messages = %d, want 1 (not due yet)", n)
	}

	// Past the interval the line goes out again.
	f.setNow(f.at().Add(defaultHazardsInterval + time.Minute))
	f.action.notices.hazardsTick(context.Background())
	g.waitWebhook(2)
}

func TestNoticesSkipEmptyHazardStatus(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.summary = HazardSummary{}
	f.recipients = []string{"42"}
	f.configureNotices()

	f.action.notices.hazardsTick(context.Background())
	// Nothing while no hazard is active — neither surface.
	g.expectNoSend(0, 200*time.Millisecond)
	if n := len(g.webhookPosts()); n != 0 {
		t.Fatalf("channel posts = %d, want 0 while nothing is active", n)
	}
	// "0 severe, 12 other" IS worth posting: it says nothing sits above
	// the alert bar. The cadence was consumed by the skipped window, so
	// the next one has to come round first.
	f.summary = HazardSummary{Other: 12}
	f.setNow(f.at().Add(defaultHazardsInterval + time.Minute))
	f.action.notices.hazardsTick(context.Background())
	posts := g.waitWebhook(1)
	if !strings.Contains(posts[0].content, "0 severe or more, 12 other active") {
		t.Errorf("channel post = %q", posts[0].content)
	}
}

func TestNoticesUptimeLine(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.uptime = "4h12m"
	f.recipients = []string{"42"}
	f.configureNotices()

	f.action.notices.uptimeTick(context.Background())

	posts := g.waitWebhook(1)
	want := "WarnFlux v0.7.4 - SOSNA - sosna.sp9moa.pl | up 4h12m"
	if posts[0].content != want {
		t.Errorf("channel post = %q, want %q", posts[0].content, want)
	}
	dm := g.waitSent(0)
	if dm.content != want {
		t.Errorf("direct message = %q, want %q", dm.content, want)
	}

	// Once per interval.
	f.action.notices.uptimeTick(context.Background())
	if n := len(g.webhookPosts()); n != 1 {
		t.Errorf("channel posts = %d, want 1 (not due yet)", n)
	}
	f.setNow(f.at().Add(defaultUptimeInterval + time.Minute))
	f.action.notices.uptimeTick(context.Background())
	g.waitWebhook(2)
}

func TestNoticesWithoutTokenPostChannelOnly(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.action.token = ""
	f.action.dm = nil
	f.summary = HazardSummary{Severe: 3, Other: 0}
	f.recipients = []string{"42"}
	f.configureNotices()

	f.action.notices.hazardsTick(context.Background())
	posts := g.waitWebhook(1)
	if !strings.Contains(posts[0].content, "3 severe or more, 0 other active") {
		t.Errorf("channel post = %q", posts[0].content)
	}
	// A webhook alone cannot reach a person: nothing is attempted.
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
