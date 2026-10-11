package discord

// The periodic notices mirror what the Meshtastic channel does, so both
// surfaces read alike: an hourly hazard status line (counters only,
// nothing while no hazard is active) and a presence line carrying the
// installation identity with the station uptime every few hours. On
// Discord each notice goes to the webhook CHANNEL and as a DIRECT
// MESSAGE to every registered operator — the channel is the shared
// surface, the direct message the personal one.
//
// Commands stay a direct-message feature: the channel copy never
// advertises one, the direct-message copy does.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
)

// Notice defaults: the same cadence the Meshtastic channel runs, on
// purpose — an operator reading both surfaces sees the same rhythm.
const (
	defaultHazardsInterval = time.Hour
	defaultUptimeInterval  = 4 * time.Hour
	minNoticeInterval      = time.Minute
	maxNoticeInterval      = 720 * time.Hour
	// noticeTick is how often the notifier checks whether a notice is due.
	noticeTick = time.Minute
)

// hazardHint tells direct-message readers how to ask for the full list:
// the status line carries counters only, so the details need a command —
// which lives in direct messages.
const hazardHint = "DM me /hazard for the list"

// HazardSummary counts the active hazards the status line reports: the
// severe-and-above ones (each of them already went out as its own alert)
// against everything below that bar.
type HazardSummary struct {
	Severe int
	Other  int
}

// Total is the number of active hazards the summary covers.
func (s HazardSummary) Total() int { return s.Severe + s.Other }

// NoticesConfig wires the periodic notices to the station: who the
// station is, where the details are published and who receives the
// direct-message copies.
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
	// Recipients lists the Discord user ids that receive the
	// direct-message copies (the directory's registered ids).
	Recipients func(ctx context.Context) ([]string, error)
}

// notifier runs the two periodic notices of one instance.
type notifier struct {
	action *discordAction
	logger *slog.Logger
	now    func() time.Time

	hazardsInterval time.Duration
	uptimeInterval  time.Duration

	mu          sync.Mutex
	cfg         NoticesConfig
	hazardsNext time.Time
	uptimeNext  time.Time
}

func newNotifier(a *discordAction, logger *slog.Logger, hazards, uptime time.Duration) *notifier {
	if logger == nil {
		logger = slog.Default()
	}
	return &notifier{
		action:          a,
		logger:          logger,
		now:             time.Now,
		hazardsInterval: hazards,
		uptimeInterval:  uptime,
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
	if a.notices.uptimeInterval > 0 {
		a.notices.uptimeNext = now
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
	// UptimeInterval is the presence line spacing (0 = disabled).
	UptimeInterval time.Duration
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
	if n.hazardsInterval <= 0 && n.uptimeInterval <= 0 {
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

// hazardsTick sends the hazard status line when its cadence is due.
// Nothing goes out while no hazard is active: an empty status every hour
// would train the readers to ignore it.
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
	if sum.Total() == 0 {
		n.logger.Debug("discord: hazard status skipped", "reason", "no active hazard")
		return
	}
	n.deliver(ctx, cfg, n.hazardStatusText(cfg, sum, false), n.hazardStatusText(cfg, sum, true))
}

// uptimeTick sends the presence line when its cadence is due.
func (n *notifier) uptimeTick(ctx context.Context) {
	if n.uptimeInterval <= 0 {
		return
	}
	now := n.now()
	n.mu.Lock()
	due := !n.uptimeNext.IsZero() && !now.Before(n.uptimeNext)
	if due {
		n.uptimeNext = now.Add(n.uptimeInterval)
	}
	cfg := n.cfg
	n.mu.Unlock()
	if !due {
		return
	}
	text := n.uptimeText(cfg)
	n.deliver(ctx, cfg, text, text)
}

// deliver posts one notice on the webhook channel and sends the copy to
// every registered operator. The two texts differ only in the command
// hint; both are bounded to Discord's content limit.
func (n *notifier) deliver(ctx context.Context, cfg NoticesConfig, channelText, dmText string) {
	if err := n.action.post(ctx, truncateRunes(channelText, maxContentRunes), "WarnFlux"); err != nil {
		n.logger.Warn("discord: periodic notice failed", "error", err)
	}
	if n.action.token == "" || cfg.Recipients == nil {
		return
	}
	ids, err := cfg.Recipients(ctx)
	if err != nil {
		n.logger.Warn("discord: notice recipients unavailable", "error", err)
		return
	}
	var sent, failed int
	for _, id := range ids {
		if err := n.action.SendDM(ctx, id, dmText); err != nil {
			failed++
			n.logger.Warn("discord: notice direct message failed", "user", id, "error", err)
			continue
		}
		sent++
	}
	n.logger.Info("discord: periodic notice delivered", "users", sent, "failures", failed)
}

// hazardStatusText renders the status line: the counters plus, when one
// is configured, the public site holding the details. "0 severe" is
// worth posting — it tells the readers that nothing above the alert bar
// is running. hint adds the command hint (direct messages only).
func (n *notifier) hazardStatusText(cfg NoticesConfig, sum HazardSummary, hint bool) string {
	line := fmt.Sprintf("WarnFlux: %d severe or more, %d other active", sum.Severe, sum.Other)
	if site := strings.TrimSpace(cfg.Site); site != "" {
		line += " — " + site
	}
	if hint {
		line += " · " + hazardHint
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
