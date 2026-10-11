package discord

// The periodic notices mirror the Meshtastic group channel: an hourly
// hazard status line (counters only, nothing while no hazard is active)
// and a presence line carrying the installation identity with the station
// uptime every few hours. They are CHANNEL posts, never direct messages:
// the channel is the shared surface that shows what the station sees,
// while a private message stays reserved for what concerns that one
// operator (their alerts, the mass notices and their command answers).
//
// The status line carries the same "DM me /hazard for the list" hint the
// Meshtastic channel appends, so the readers of both surfaces learn the
// same way how to pull the details.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/schedule"
)

// Notice defaults: the same cadence the Meshtastic channel runs, on
// purpose — an operator reading both surfaces sees the same rhythm.
const (
	defaultHazardsInterval = time.Hour
	// defaultUptimeInterval is the presence-line spacing used when the
	// configuration asks for a spacing instead of the daily anchor.
	defaultUptimeInterval = 24 * time.Hour
	minNoticeInterval     = time.Minute
	maxNoticeInterval     = 720 * time.Hour
	// noticeTick is how often the notifier checks whether a notice is due.
	noticeTick = time.Minute
)

// HazardSummary counts the active hazards the status line reports: the
// severe-and-above ones (each of them already went out as its own alert)
// against everything below that bar.
type HazardSummary struct {
	Severe int
	Other  int
}

// Total is the number of active hazards the summary covers.
func (s HazardSummary) Total() int { return s.Severe + s.Other }

// Alarming reports whether the status line is worth posting at all: the
// channel hears about hazards that sit at severe or higher, the level
// that already rang the devices.
func (s HazardSummary) Alarming() bool { return s.Severe > 0 }

// hazardListHint tells channel readers how to ask for the full list: the
// status line carries counters only. It is the same wording the
// Meshtastic channel uses, so both surfaces read alike.
const hazardListHint = "DM me /hazard for the list"

// NoticesConfig wires the periodic notices to the station: who the
// station is and where the details are published.
type NoticesConfig struct {
	// Identity is the one-line installation banner (version, name,
	// domain) the presence line carries — the same string the radio
	// channels use.
	Identity string
	// Site is the public link the hazard status line points at.
	Site string
	// Uptime renders how long the process has been running ("4h12m").
	Uptime func() string
	// Summary reports the active hazard counters.
	Summary func() HazardSummary
}

// notifier runs the two periodic notices of one instance.
type notifier struct {
	action *discordAction
	logger *slog.Logger
	now    func() time.Time

	hazardsInterval time.Duration
	uptimeInterval  time.Duration
	// uptimeAt is the daily anchor of the presence line ("" = the spacing
	// mode; both empty = the line is off).
	uptimeAt string

	mu          sync.Mutex
	cfg         NoticesConfig
	hazardsNext time.Time
	uptimeNext  time.Time
}

func newNotifier(a *discordAction, logger *slog.Logger, hazards, uptime time.Duration, uptimeAt string) *notifier {
	if logger == nil {
		logger = slog.Default()
	}
	return &notifier{
		action:          a,
		logger:          logger,
		now:             time.Now,
		hazardsInterval: hazards,
		uptimeInterval:  uptime,
		uptimeAt:        uptimeAt,
	}
}

// uptimeArm returns when the presence line is next due: the daily anchor
// (today or tomorrow, station local time) in anchor mode, or now in
// spacing mode — a restart then posts it right away instead of waiting a
// whole interval. Both zero/false values mean the line is off.
func uptimeArm(now time.Time, at string, interval time.Duration, after bool) time.Time {
	switch {
	case at != "":
		if after {
			// Re-arm after a post: the next day, never the same moment.
			return schedule.NextDailyAfter(now, at)
		}
		return schedule.NextDaily(now, at)
	case interval > 0:
		if after {
			// Re-arm one spacing ahead: the next line is a full interval
			// away, never a repeat at the same moment.
			return now.Add(interval)
		}
		return now
	default:
		return time.Time{}
	}
}

// ConfigureNotices installs the station seams. Both notices become due
// immediately, so a restart resyncs the surface instead of waiting a
// full interval — the same rule the Meshtastic beacon follows.
func (a *discordAction) ConfigureNotices(cfg NoticesConfig) {
	if a.notices == nil {
		return
	}
	now := a.notices.now()
	a.notices.mu.Lock()
	defer a.notices.mu.Unlock()
	a.notices.cfg = cfg
	if a.notices.hazardsInterval > 0 {
		a.notices.hazardsNext = now
	}
	if next := uptimeArm(now, a.notices.uptimeAt, a.notices.uptimeInterval, false); !next.IsZero() {
		a.notices.uptimeNext = next
	}
}

// RunNotices keeps the periodic notices going until ctx is cancelled.
func (a *discordAction) RunNotices(ctx context.Context) {
	if a.notices == nil {
		return
	}
	a.notices.run(ctx)
}

// NotifierOf reports the periodic-notice capability of one plugin
// instance. Unlike the direct-message command path, it needs no bot
// token: the channel notice rides the webhook alone (the direct-message
// copies are skipped without a token).
func NotifierOf(p action.Plugin) (Notifier, bool) {
	a, ok := p.(*discordAction)
	if !ok || a.notices == nil {
		return nil, false
	}
	return a, true
}

// Info describes what one Discord instance offers, for the pages that
// explain the station to its users.
type Info struct {
	// HazardsInterval is the hazard status line spacing (0 = disabled).
	HazardsInterval time.Duration
	// UptimeInterval is the presence line spacing (0 = the line follows
	// UptimeAt, or is off when that is empty too).
	UptimeInterval time.Duration
	// UptimeAt is the daily anchor of the presence line ("" = spacing mode).
	UptimeAt string
	// DirectMessages reports a bot token: the station can message people
	// privately and answer commands in direct messages.
	DirectMessages bool
}

// InfoOf reports the capabilities of one Discord instance (ok is false
// for another plugin type).
func InfoOf(p action.Plugin) (Info, bool) {
	a, ok := p.(*discordAction)
	if !ok {
		return Info{}, false
	}
	info := Info{DirectMessages: a.token != ""}
	if a.notices != nil {
		info.HazardsInterval = a.notices.hazardsInterval
		info.UptimeInterval = a.notices.uptimeInterval
		info.UptimeAt = a.notices.uptimeAt
	}
	return info, true
}

// Notifier is the periodic capability of a Discord instance.
type Notifier interface {
	// ConfigureNotices installs the identity, the hazard summary, the
	// uptime source and the direct-message recipients; it must run
	// before RunNotices.
	ConfigureNotices(cfg NoticesConfig)
	// RunNotices blocks until ctx is cancelled, emitting the notices.
	RunNotices(ctx context.Context)
}

// run checks the two cadences until ctx is cancelled.
func (n *notifier) run(ctx context.Context) {
	if n.hazardsInterval <= 0 && n.uptimeInterval <= 0 && n.uptimeAt == "" {
		return
	}
	ticker := time.NewTicker(noticeTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		n.hazardsTick(ctx)
		n.uptimeTick(ctx)
	}
}

// hazardsTick posts the hazard status line when its cadence is due.
// Nothing goes out while nothing sits at severe or higher — the hourly
// line is there to point at what rang the devices, not to report a quiet
// channel.
func (n *notifier) hazardsTick(ctx context.Context) {
	if n.hazardsInterval <= 0 {
		return
	}
	now := n.now()
	n.mu.Lock()
	due := !n.hazardsNext.IsZero() && !now.Before(n.hazardsNext)
	if due {
		// Advance the cadence BEFORE reading the source: a slow callback
		// or a failed delivery must never fire the line twice per window.
		n.hazardsNext = now.Add(n.hazardsInterval)
	}
	cfg := n.cfg
	n.mu.Unlock()
	if !due || cfg.Summary == nil {
		return
	}
	sum := cfg.Summary()
	if !sum.Alarming() {
		// Nothing at severe or higher: the channel stays quiet instead of
		// reading "0 severe" every hour.
		n.logger.Debug("discord: hazard status skipped", "reason", "nothing at severe or higher")
		return
	}
	n.post(ctx, "hazards", n.hazardStatusText(cfg, sum))
}

// uptimeTick posts the presence line when its cadence is due: once a day at
// the anchor (re-armed for tomorrow, so a slow or failed post never repeats
// the line the same day), or every interval in spacing mode.
func (n *notifier) uptimeTick(ctx context.Context) {
	if n.uptimeAt == "" && n.uptimeInterval <= 0 {
		return
	}
	now := n.now()
	n.mu.Lock()
	due := !n.uptimeNext.IsZero() && !now.Before(n.uptimeNext)
	if due {
		n.uptimeNext = uptimeArm(now, n.uptimeAt, n.uptimeInterval, true)
	}
	cfg := n.cfg
	n.mu.Unlock()
	if !due {
		return
	}
	n.post(ctx, "uptime", n.uptimeText(cfg))
}

// post publishes one notice on the webhook channel, bounded to Discord's
// content limit. Nothing is ever sent privately here: a periodic line is
// information for the channel, not a notification about one operator.
// kind names which notice went out ("hazards" or "uptime"), so the log
// tells the two apart at a glance.
func (n *notifier) post(ctx context.Context, kind, text string) {
	if err := n.action.post(ctx, truncateRunes(text, maxContentRunes), "WarnFlux"); err != nil {
		n.logger.Warn("discord: periodic notice failed", "kind", kind, "error", err)
		return
	}
	n.logger.Info("discord: periodic notice posted", "kind", kind)
}

// hazardStatusText renders the status line: the counters plus, when one is
// configured, the public site holding the details, and the same hint the
// Meshtastic channel appends (dropped only if the line would not fit —
// on Discord it always does). It is only ever called for a summary with
// something at severe or higher.
func (n *notifier) hazardStatusText(cfg NoticesConfig, sum HazardSummary) string {
	line := fmt.Sprintf("WarnFlux: %d severe or more, %d other active", sum.Severe, sum.Other)
	if site := strings.TrimSpace(cfg.Site); site != "" {
		line += " — " + site
	}
	if hint := " · " + hazardListHint; len([]rune(line))+len([]rune(hint)) <= maxContentRunes {
		line += hint
	}
	return line
}

// uptimeText renders the presence line: the installation identity plus
// how long the station has been running.
func (n *notifier) uptimeText(cfg NoticesConfig) string {
	line := strings.TrimSpace(cfg.Identity)
	if line == "" {
		line = "WarnFlux"
	}
	if cfg.Uptime != nil {
		if up := strings.TrimSpace(cfg.Uptime()); up != "" {
			line += " | up " + up
		}
	}
	return line
}
