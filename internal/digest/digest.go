// Package digest sends the operator a daily system-status mail: what the
// station did in the last 24 hours, which services are healthy, and where it
// stopped working. It is the once-a-day summary that saves reading the log.
package digest

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/schedule"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// Window is the reporting period: the 24 hours before the anchor.
type Window struct {
	From time.Time
	To   time.Time
}

// Counters is the traffic of one window.
type Counters = storage.DigestCounters

// Store is the persistence the digest reads.
type Store interface {
	DigestCounters(ctx context.Context, from, to time.Time) (Counters, error)
	AdminEmails(ctx context.Context) ([]string, error)
}

// Mailer sends one branded notice.
type Mailer func(ctx context.Context, to []string, subject, body string) error

// Sources is everything the digest needs, injected so the package stays
// testable without a database, a mail server or a plugin manager.
type Sources struct {
	Store      Store
	Statuses   func() []plugin.PluginStatus
	Recipients func(ctx context.Context) ([]string, error)
	Mailer     Mailer
	Station    string
	Version    string
	Commit     string
	StartedAt  time.Time
	Now        func() time.Time
}

// Report is one day's status, rendered into the mail body.
type Report struct {
	Window    Window
	Counters  Counters
	Plugins   []plugin.PluginStatus
	Station   string
	Version   string
	Commit    string
	StartedAt time.Time
	Generated time.Time
}

// Verdict is the one word the subject line carries: it answers "do I need to
// read this mail carefully?" before the mail is opened.
func (r Report) Verdict() string {
	for _, p := range r.Plugins {
		switch p.State {
		case plugin.StateDegraded, plugin.StateStopped, plugin.StateSuspended:
			return "DEGRADED"
		}
	}
	return "OK"
}

// Subject is the mail subject: station, verdict and the date.
func (r Report) Subject() string {
	return fmt.Sprintf("%s — daily status %s (%s)",
		r.Station, r.Generated.Format("2006-01-02"), strings.ToLower(r.Verdict()))
}

// Render builds the markdown body. It deliberately uses only the subset the
// notice mail renders (headings, list items, bold), because that is what the
// branded template understands.
func (r Report) Render() string {
	var b strings.Builder
	write := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }

	write("## System status — %s\n\n", r.Station)
	write("**Verdict:** %s  \n", r.Verdict())
	write("**Version:** %s", r.Version)
	if r.Commit != "" {
		write(" (%s)", shortCommit(r.Commit))
	}
	write("  \n**Uptime:** %s  \n", roundDuration(r.Generated.Sub(r.StartedAt)))
	write("**Window:** %s → %s  \n", stamp(r.Window.From), stamp(r.Window.To))
	write("**Generated:** %s\n\n", stamp(r.Generated))

	write("### Traffic in the last 24 hours\n\n")
	write("- SMS: %d received · %d sent\n", r.Counters.SMSReceived, r.Counters.SMSSent)
	write("- Meshtastic: %d received · %d sent\n", r.Counters.MeshReceived, r.Counters.MeshSent)
	write("- APRS: %d received · %d sent\n", r.Counters.APRSReceived, r.Counters.APRSSent)
	write("- Hazards: %d new · %d active now · %d ended\n",
		r.Counters.HazardsNew, r.Counters.HazardsActive, r.Counters.HazardsClosed)
	write("- Notifications: %d dispatched · %d failed\n\n", r.Counters.Deliveries, r.Counters.DeliveriesFailed)

	write("### Services\n\n")
	if len(r.Plugins) == 0 {
		write("- no plugin reported a status\n\n")
	} else {
		for _, p := range r.sortedPlugins() {
			write("- **%s** (%s) — %s", p.ID, p.Type, p.State)
			if p.LastSummary != "" && p.State != plugin.StateDisabled {
				write(" · %s", p.LastSummary)
			}
			if p.LastError != "" && (p.State == plugin.StateDegraded || p.State == plugin.StateStopped) {
				write(" · last error: %s", oneLine(p.LastError))
			}
			if p.ConsecutiveFailures > 1 {
				write(" · %d consecutive failures", p.ConsecutiveFailures)
			}
			write("\n")
		}
		b.WriteString("\n")
	}

	b.WriteString("---\n\n")
	b.WriteString("This mail is sent once a day by WarnFlux. Every number above covers the window shown at the top; ")
	b.WriteString("the services section is a live snapshot taken when the mail was built.\n")
	return b.String()
}

// sortedPlugins orders the rows the way the operator scans them: problem
// states first, then running, then switched off.
func (r Report) sortedPlugins() []plugin.PluginStatus {
	out := append([]plugin.PluginStatus(nil), r.Plugins...)
	rank := func(s plugin.PluginState) int {
		switch s {
		case plugin.StateDegraded, plugin.StateStopped, plugin.StateSuspended:
			return 0
		case plugin.StateRunning, plugin.StateStarting, plugin.StateStopping:
			return 1
		default:
			return 2
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if ri, rj := rank(out[i].State), rank(out[j].State); ri != rj {
			return ri < rj
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// Run sends the digest every day at at (a station-local clock time). A ticker
// is not enough: the anchor has to survive DST, and the mail is armed for the
// next occurrence every time, so a station that was restarted still delivers
// the report.
func Run(ctx context.Context, src Sources, at string) {
	if !schedule.Valid(at) {
		slog.Error("admin digest: invalid clock time, digest disabled", "at", at)
		return
	}
	for {
		next := schedule.NextDailyAfter(nowOrDefault(src), at)
		if next.IsZero() {
			slog.Error("admin digest: cannot arm the daily report", "at", at)
			return
		}
		slog.Info("admin digest armed", "at", at, "next", next.Format(time.RFC3339))
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if err := Send(ctx, src); err != nil {
			slog.Error("admin digest not sent", "error", err)
		}
	}
}

// Send builds the report for the last 24 hours and mails it. It is exported so
// the admin panel can trigger the same report on demand.
func Send(ctx context.Context, src Sources) error {
	now := nowOrDefault(src)
	report := Report{
		Window:    Window{From: now.Add(-24 * time.Hour), To: now},
		Station:   src.Station,
		Version:   src.Version,
		Commit:    src.Commit,
		StartedAt: src.StartedAt,
		Generated: now,
	}
	if src.Statuses != nil {
		report.Plugins = src.Statuses()
	}
	if src.Store != nil {
		counters, err := src.Store.DigestCounters(ctx, report.Window.From, report.Window.To)
		if err != nil {
			// A broken counter query must not cost the operator the rest of
			// the report: the mail still goes out.
			slog.Warn("admin digest: counters unavailable", "error", err)
		} else {
			report.Counters = counters
		}
	}
	to, err := recipients(ctx, src)
	if err != nil {
		return fmt.Errorf("recipients: %w", err)
	}
	if len(to) == 0 {
		slog.Warn("admin digest: no administrator has an e-mail address")
		return nil
	}
	if src.Mailer == nil {
		return fmt.Errorf("no mailer configured")
	}
	return src.Mailer(ctx, to, report.Subject(), report.Render())
}

// recipients prefers the addresses from the configuration and falls back to
// every administrator account that has an e-mail address.
func recipients(ctx context.Context, src Sources) ([]string, error) {
	if src.Recipients != nil {
		to, err := src.Recipients(ctx)
		if err != nil {
			return nil, err
		}
		if len(to) > 0 {
			return to, nil
		}
	}
	if src.Store == nil {
		return nil, nil
	}
	return src.Store.AdminEmails(ctx)
}

// nowOrDefault keeps the clock injectable without making every caller set it.
func nowOrDefault(src Sources) time.Time {
	if src.Now != nil {
		return src.Now()
	}
	return time.Now()
}

// shortCommit trims a full SHA to the seven characters every panel shows.
func shortCommit(commit string) string {
	if len(commit) > 7 {
		return commit[:7]
	}
	return commit
}

// oneLine keeps a provider error from turning one bullet into a paragraph.
func oneLine(err string) string {
	err = strings.Join(strings.Fields(err), " ")
	const cap = 120
	if len(err) > cap {
		err = err[:cap] + "…"
	}
	return err
}

// stamp renders a time the way the panels do: local zone, minute precision.
func stamp(t time.Time) string { return t.Format("2006-01-02 15:04") }

// roundDuration prints an uptime the operator can scan ("2d 4h 12m").
func roundDuration(d time.Duration) string {
	if d < time.Minute {
		return d.Round(time.Second).String()
	}
	d = d.Round(time.Minute)
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60
	switch {
	case days > 0:
		return fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
	case hours > 0:
		return fmt.Sprintf("%dh %dm", hours, minutes)
	default:
		return fmt.Sprintf("%dm", minutes)
	}
}
