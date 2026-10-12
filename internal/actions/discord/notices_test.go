package discord

// The periodic notices are exercised against the same fake server as the
// inbound path: the posts land on its /webhook handler. A notice is a
// CHANNEL message — the tests also pin that nothing is sent privately.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/schedule"
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

// TestNoticesUptimeDailyAt pins the presence line's daily anchor: the
// default is 06:00 station time, exactly like the Meshtastic beacon, so a
// restart posts nothing until the clock comes round and the line never
// repeats within a day.
func TestNoticesUptimeDailyAt(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.uptime = "1d2h"
	f.configureNotices()

	// The fixture's clock sits at noon: the line is armed for tomorrow.
	f.action.notices.uptimeTick(context.Background())
	if n := len(g.webhookPosts()); n != 0 {
		t.Fatalf("presence line posted before its anchor (%d posts)", n)
	}

	// 06:00 the next morning: it goes out, on the channel only.
	f.setNow(time.Date(2026, 10, 12, 6, 0, 0, 0, time.UTC))
	f.action.notices.uptimeTick(context.Background())
	posts := g.waitWebhook(1)
	want := "WarnFlux v0.7.4 - SOSNA - sosna.sp9moa.pl | up 1d2h"
	if posts[0].content != want {
		t.Errorf("channel post = %q, want %q", posts[0].content, want)
	}
	g.expectNoSend(0, 200*time.Millisecond)

	// Later the same day: nothing (the anchor re-armed for tomorrow).
	f.setNow(time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC))
	f.action.notices.uptimeTick(context.Background())
	if n := len(g.webhookPosts()); n != 1 {
		t.Fatalf("presence line repeated within the day (%d posts)", n)
	}

	// The next morning it comes round again.
	f.setNow(time.Date(2026, 10, 13, 6, 0, 0, 0, time.UTC))
	f.action.notices.uptimeTick(context.Background())
	g.waitWebhook(2)
}

// TestNoticesUptimeSpacing pins the alternative cadence: an empty anchor
// plus a spacing posts right away and then every interval.
func TestNoticesUptimeSpacing(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixtureCfg(t, g, map[string]any{"uptime_at": "", "uptime_interval": "12h"})
	f.uptime = "3h"
	f.configureNotices()

	f.action.notices.uptimeTick(context.Background())
	posts := g.waitWebhook(1)
	if !strings.HasSuffix(posts[0].content, "| up 3h") {
		t.Errorf("channel post = %q", posts[0].content)
	}
	g.expectNoSend(0, 200*time.Millisecond)

	// Not due yet, then due one interval later.
	f.setNow(f.at().Add(6 * time.Hour))
	f.action.notices.uptimeTick(context.Background())
	if n := len(g.webhookPosts()); n != 1 {
		t.Fatalf("presence line posted early (%d posts)", n)
	}
	f.setNow(f.at().Add(7 * time.Hour))
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
		name     string
		cfg      map[string]any
		wantErr  string
		hazards  time.Duration
		uptime   time.Duration
		uptimeAt string
	}{
		// Neither a time nor a spacing: the daily default anchor.
		{"defaults", map[string]any{}, "", defaultHazardsInterval, 0, schedule.AtDefault},
		// An explicit spacing is an alternative to the anchor.
		{"spacing", map[string]any{"hazards_interval": "30m", "uptime_interval": "12h"}, "", 30 * time.Minute, 12 * time.Hour, ""},
		// An explicit anchor.
		{"anchor", map[string]any{"uptime_at": "21:30"}, "", defaultHazardsInterval, 0, "21:30"},
		// An explicitly empty anchor: no daily notice, the spacing applies.
		{"no anchor", map[string]any{"uptime_at": ""}, "", defaultHazardsInterval, defaultUptimeInterval, ""},
		// 0 disables the line (what dev uses so only prod posts).
		{"disabled", map[string]any{"hazards_interval": "0s", "uptime_interval": "0s"}, "", 0, 0, ""},
		{"both modes", map[string]any{"uptime_at": "06:00", "uptime_interval": "12h"}, "not both", 0, 0, ""},
		{"bad anchor", map[string]any{"uptime_at": "6 rano"}, "clock time", 0, 0, ""},
		{"too short", map[string]any{"hazards_interval": "30s"}, "must be between", 0, 0, ""},
		{"too long", map[string]any{"uptime_interval": "1000h"}, "must be between", 0, 0, ""},
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
		if a.notices.hazardsInterval != c.hazards || a.notices.uptimeInterval != c.uptime ||
			a.notices.uptimeAt != c.uptimeAt {
			t.Errorf("%s: cadence = hazards %s / uptime %s at %q, want %s / %s at %q", c.name,
				a.notices.hazardsInterval, a.notices.uptimeInterval, a.notices.uptimeAt,
				c.hazards, c.uptime, c.uptimeAt)
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

// TestNoticesSetCadenceLive pins the Config-page path: a running notifier
// takes a new cadence without a restart. A spacing switched on becomes due
// at once, the daily anchor re-arms to the next occurrence, and a channel
// silenced completely is left silenced.
func TestNoticesSetCadenceLive(t *testing.T) {
	g := newFakeGateway(t)
	// The defaults both surfaces ship: the status line every hour and the
	// presence line anchored at 06:00.
	f := newListenerFixture(t, g)
	f.summary = HazardSummary{Severe: 2, Other: 1}
	f.uptime = "5m"
	f.configureNotices()

	// The status line is due at the start, the presence line waits for the
	// anchor the fixture's clock has already passed.
	f.action.notices.hazardsTick(context.Background())
	f.action.notices.uptimeTick(context.Background())
	posts := g.waitWebhook(1)
	if !strings.Contains(posts[0].content, "2 severe or more") {
		t.Errorf("status line = %q", posts[0].content)
	}
	if n := len(g.webhookPosts()); n != 1 {
		t.Fatalf("presence line posted before its anchor (%d posts)", n)
	}

	// The panel moves the spacing and the anchor: the status line holds
	// for its new window, the presence line moves to tomorrow's hour.
	f.action.SetCadence(30*time.Minute, 0, "05:00")
	f.setNow(f.at().Add(20 * time.Minute))
	f.action.notices.hazardsTick(context.Background())
	if n := len(g.webhookPosts()); n != 1 {
		t.Fatalf("status line repeated inside its new window (%d posts)", n)
	}
	f.setNow(f.at().Add(20 * time.Minute))
	f.action.notices.hazardsTick(context.Background())
	g.waitWebhook(2)

	// A spacing replaces the anchor, and the line it moves is the morning
	// one: it goes out at once instead of waiting another day.
	f.action.SetCadence(30*time.Minute, 12*time.Hour, "")
	f.action.notices.uptimeTick(context.Background())
	posts = g.waitWebhook(3)
	if !strings.HasSuffix(posts[2].content, "| up 5m") {
		t.Errorf("presence line = %q", posts[2].content)
	}
	f.setNow(f.at().Add(6 * time.Hour))
	f.action.notices.uptimeTick(context.Background())
	if n := len(g.webhookPosts()); n != 3 {
		t.Fatalf("presence line repeated inside its window (%d posts)", n)
	}

	// A channel silenced completely — the development arrangement, where
	// production owns the webhook — stays silent whatever the panel sends.
	silent := newListenerFixtureCfg(t, g, map[string]any{
		"hazards_interval": "0s", "uptime_at": "", "uptime_interval": "0s",
	})
	silent.summary = HazardSummary{Severe: 2}
	silent.configureNotices()
	silent.action.SetCadence(time.Minute, time.Minute, "06:00")
	silent.action.notices.hazardsTick(context.Background())
	silent.action.notices.uptimeTick(context.Background())
	if n := len(g.webhookPosts()); n != 3 {
		t.Fatalf("a silenced channel started announcing (%d posts)", n)
	}
}
