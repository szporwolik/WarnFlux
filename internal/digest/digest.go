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

	"github.com/szporwolik/WarnFlux/internal/health"
	"github.com/szporwolik/WarnFlux/internal/i18n"
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
	Store Store
	// Health returns the inputs of the SHARED system-health assessment (see
	// internal/health): the digest must judge the station the way the
	// dashboard card does, not by source plugins alone. nil = the assessment
	// is unavailable and the report then says so instead of claiming OK.
	Health     func() health.Inputs
	Statuses   func() []plugin.PluginStatus
	Recipients func(ctx context.Context) ([]string, error)
	Mailer     Mailer
	Station    string
	Version    string
	Commit     string
	StartedAt  time.Time
	Now        func() time.Time
	// Lang returns the system notification language and Zone/Loc are the
	// station's display zone. Lang is a function because the admin Config
	// page switches the language without restarting the station.
	Lang func() string
	Zone string
	Loc  *time.Location
}

// Report is one day's status, rendered into the mail body.
type Report struct {
	Window   Window
	Counters Counters
	// CountersReady is false when the counter query failed: the traffic
	// section then says "unavailable" instead of printing zeros that look
	// like a quiet day.
	CountersReady bool
	// Health is the shared assessment of every subsystem.
	Health  health.Report
	Plugins []plugin.PluginStatus
	// Station is the installation name (web.header1), Version and Commit
	// identify the running build.
	Station   string
	Version   string
	Commit    string
	StartedAt time.Time
	Generated time.Time
	// Lang is the system notification language and Zone/Loc the station's
	// display zone: the routed mails always name the zone next to a time, so
	// the digest does too.
	Lang string
	Zone string
	Loc  *time.Location
}

// Verdict is the one word the subject line carries: it answers "do I need to
// read this mail carefully?" before the mail is opened.
func (r Report) Verdict() string {
	lang := i18n.Effective(r.Lang)
	switch r.Health.FindingsLevel() {
	case health.LevelBad, health.LevelWarn:
		return i18n.T(lang, "digest.degraded")
	}
	// Nothing is wrong — but a report that could not read part of the station
	// must not claim OK either: "I could not look" is not "all good".
	if r.Health.Incomplete() {
		return i18n.T(lang, "digest.incomplete")
	}
	return i18n.T(lang, "digest.ok")
}

// Subject follows the routed notification mails: the bracketed installation
// name first, then what the message is, then the verdict — so the inbox list
// already separates an alert from the daily report.
func (r Report) Subject() string {
	lang := i18n.Effective(r.Lang)
	base := fmt.Sprintf("%s %s — %s",
		i18n.T(lang, "digest.mail_subject"), r.date(), r.Verdict())
	if station := strings.TrimSpace(r.Station); station != "" {
		return "[" + station + "] " + base
	}
	return base
}

// Render builds the markdown body. It uses only the subset the notice mail
// renders (headings, list items, bold) and the same label vocabulary as the
// routed notification mails. The envelope already prints the installation
// name and the "daily system status" label, so the body starts with the
// verdict instead of repeating the title.
func (r Report) Render() string {
	lang := i18n.Effective(r.Lang)
	t := func(key string) string { return i18n.T(lang, key) }
	var b strings.Builder
	write := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }

	write("**%s:** **%s**  \n", t("digest.verdict"), r.Verdict())
	write("**%s:** %s", t("digest.version"), r.Version)
	if r.Commit != "" {
		write(" (%s)", shortCommit(r.Commit))
	}
	write("  \n**%s:** %s  \n", t("digest.uptime"), roundDuration(r.Generated.Sub(r.StartedAt)))
	write("**%s:** %s → %s  \n", t("digest.window"), r.stamp(r.Window.From), r.stamp(r.Window.To))
	write("**%s:** %s\n\n", t("digest.generated"), r.stamp(r.Generated))

	write("### %s\n\n", t("digest.traffic"))
	if r.CountersReady {
		write("- %s: %d %s · %d %s\n", t("digest.sms"),
			r.Counters.SMSReceived, t("digest.received"), r.Counters.SMSSent, t("digest.sent"))
		write("- %s: %d %s · %d %s\n", t("digest.meshtastic"),
			r.Counters.MeshReceived, t("digest.received"), r.Counters.MeshSent, t("digest.sent"))
		write("- %s: %d %s · %d %s\n", t("digest.aprs"),
			r.Counters.APRSReceived, t("digest.received"), r.Counters.APRSSent, t("digest.sent"))
		write("- %s: %d %s · %d %s · %d %s\n", t("digest.hazards"),
			r.Counters.HazardsNew, t("digest.new"),
			r.Counters.HazardsActive, t("digest.active"),
			r.Counters.HazardsClosed, t("digest.ended"))
		write("- %s: %d %s · %d %s\n\n", t("digest.notifications"),
			r.Counters.Deliveries, t("digest.dispatched"),
			r.Counters.DeliveriesFailed, t("digest.failed"))
	} else {
		// Zeros would read as "a quiet day"; the truth is that the numbers
		// could not be read at all (reported P2). The section keeps one line
		// that says so instead of a table of made-up values.
		write("- %s: %s\n\n", t("digest.counter_source"), t("digest.unavailable"))
	}

	write("### %s\n\n", t("digest.health"))
	write("*%s*\n\n", t("digest.health_scope"))
	if findings := r.Health.Findings; len(findings) > 0 {
		for _, f := range findings {
			subject := f.Subject
			if subject == "" {
				subject = f.Subsystem
			}
			line := fmt.Sprintf("- **%s** — %s", f.Subsystem, subject)
			if f.Detail != "" {
				line += " · " + oneLine(f.Detail)
			}
			write("%s\n", line)
		}
	} else {
		write("- %s\n", t("digest.health_ok"))
	}
	if missing := r.Health.Missing; len(missing) > 0 {
		write("- %s\n", fmt.Sprintf(t("digest.unavailable_hint"), strings.Join(missing, ", ")))
	}
	if r.Health.Pending > 0 {
		write("- %s: %d\n", t("digest.pending"), r.Health.Pending)
	}
	b.WriteString("\n")

	write("### %s\n\n", t("digest.services"))
	if len(r.Plugins) == 0 {
		write("- %s\n\n", t("digest.no_services"))
	} else {
		for _, p := range r.sortedPlugins() {
			write("- **%s** (%s) — %s", p.ID, p.Type, p.State)
			if p.LastSummary != "" && p.State != plugin.StateDisabled {
				write(" · %s", p.LastSummary)
			}
			if p.LastError != "" && (p.State == plugin.StateDegraded || p.State == plugin.StateStopped) {
				write(" · %s: %s", t("digest.last_error"), oneLine(p.LastError))
			}
			if p.ConsecutiveFailures > 1 {
				write(" · "+t("digest.failures"), p.ConsecutiveFailures)
			}
			write("\n")
		}
		b.WriteString("\n")
	}

	write("---\n\n%s\n", t("digest.footer"))
	return b.String()
}

// date is the report's own date in the station zone.
func (r Report) date() string {
	return r.display(r.Generated).Format("2006-01-02")
}

// stamp renders a time the way the notification mails do: the station's zone,
// minute precision, and the zone named once next to the report's own
// timestamp.
func (r Report) stamp(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	return r.display(t).Format("2006-01-02 15:04")
}

// display converts t into the station's zone (UTC when the caller passed none).
func (r Report) display(t time.Time) time.Time {
	if r.Loc == nil {
		return t.UTC()
	}
	return t.In(r.Loc)
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
		Zone:      src.Zone,
		Loc:       src.Loc,
	}
	if src.Lang != nil {
		report.Lang = src.Lang()
	}
	if src.Statuses != nil {
		report.Plugins = src.Statuses()
	}
	// The verdict covers the WHOLE station: plugins, receivers, actions,
	// hardware, the database and the dispatch queue (see internal/health).
	inputs := health.Inputs{}
	if src.Health != nil {
		inputs = src.Health()
	} else {
		// Without the shared source the digest can still judge the plugins it
		// was given, but the rest of the station is unknown — and an unknown
		// station is never "OK".
		inputs.Missing = append(inputs.Missing, "system health")
		inputs.Plugins = report.Plugins
	}
	if src.Store != nil {
		counters, err := src.Store.DigestCounters(ctx, report.Window.From, report.Window.To)
		if err != nil {
			// A broken counter query must not cost the operator the rest of
			// the report: the mail still goes out, marked as incomplete.
			slog.Warn("admin digest: counters unavailable", "error", err)
			inputs.Missing = append(inputs.Missing, "counters")
		} else {
			report.Counters = counters
			report.CountersReady = true
		}
	} else {
		inputs.Missing = append(inputs.Missing, "counters")
	}
	report.Health = health.Assess(inputs)
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
