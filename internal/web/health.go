package web

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/szporwolik/WarnFlux/internal/health"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/ingesthttp"
	"github.com/szporwolik/WarnFlux/internal/preflight"
)

// ingestProbe is the optional surface a public ingest endpoint exposes to
// the health page (satisfied by *ingesthttp.Instance).
type ingestProbe interface {
	ID() string
	Counters() ingesthttp.Counters
	Connected() bool
	Started() bool
}

// dbPinger is the optional storage surface the health page needs for the
// DB row (satisfied by *sqlite.Store; fakes may skip it).
type dbPinger interface {
	Ping(ctx context.Context) error
}

// diskProbe is the optional storage surface for the low-disk alarm
// (satisfied by *sqlite.Store): free bytes on the database filesystem.
type diskProbe interface {
	FreeBytes(ctx context.Context) (int64, error)
}

// healthRow is one subsystem line: name, verdict badge and a one-line
// operational detail. BadgeClass is "ok" (green), "bad" (red — something
// that actually matters), "warn" (amber) or "muted" (config-disabled).
type healthRow struct {
	Name       string
	BadgeClass string
	BadgeText  string
	Detail     string
}

// healthView is the system-health card model. The card lives on the
// dashboard (the former /health page moved there); the plugins, MQTT and
// actions cards already carry the per-component detail.
type healthView struct {
	Lang string
	// Overall is the verdict as a badge class (badges.go): "ok" (green),
	// "warn" (amber — something is limited or held) or "bad" (red —
	// something that should run does not). OverallText is that class
	// spelled out for the operator.
	Overall     string
	OverallText string
	DB          healthRow
	Queue       healthRow
	Pending     healthRow
	// Hardware lists the configured devices the station cannot use right
	// now (unplugged modem, missing AIOC cable). Empty means every probed
	// dependency is present — the card then says so instead of hiding the
	// section, so "is the hardware fine?" has a visible answer.
	Hardware []healthRow
	// HardwareOK is true when at least one dependency was probed and all of
	// them are present.
	HardwareOK bool
	// Zone names the zone every human-facing timestamp of THIS panel uses
	// (and, when it is UTC, says so instead of leaving the operator to
	// guess). Storage and the log stream are always UTC.
	Zone healthRow
}

// worse returns the more severe of two badge classes: the health verdict
// is the worst finding, never an average.
func worse(a, b string) string {
	rank := func(class string) int {
		switch class {
		case badgeBad:
			return 3
		case badgeWarn:
			return 2
		case badgeOK:
			return 1
		default:
			return 0
		}
	}
	if rank(b) > rank(a) {
		return b
	}
	return a
}

// verdictText spells a verdict out in the page language.
func verdictText(lang, class string) string {
	switch class {
	case badgeBad:
		return i18n.T(lang, "health.badge.fault")
	case badgeWarn:
		return i18n.T(lang, "health.badge.degraded")
	default:
		return i18n.T(lang, "health.badge.ok")
	}
}

// handleHealthLegacy keeps the old /health URL working: the page moved
// onto the dashboard, so every signed-in role lands on its own page.
func (s *Server) handleHealthLegacy(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessions.currentSession(r); sess != nil {
		http.Redirect(w, r, landingForRole(sess.role), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// handlePartialHealth serves the refreshable health section.
func (s *Server) handlePartialHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "health", s.buildHealthView(s.langFor(r)))
}

// HealthInputs collects the inputs of the SHARED health assessment
// (internal/health): the same ones the dashboard card classifies, so the panel
// and the daily digest always agree about the station.
func (s *Server) HealthInputs() health.Inputs {
	in := health.Inputs{}
	if s.router != nil {
		in.Plugins = s.router.Statuses()
	}
	if s.receivers != nil {
		in.Receivers = s.receivers.Statuses()
	}
	if s.actions != nil {
		in.Actions = s.actions.Statuses()
	}
	if s.hardware != nil {
		in.Hardware = s.hardware.Active()
	}
	// A probe this station does not have is simply absent: the panel keeps
	// reporting the subsystems it can see (a construction without a storage
	// handle is a test fixture, not a station that cannot check its database).
	in.Database = &health.Database{}
	if pinger, ok := s.users.(dbPinger); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := pinger.Ping(ctx)
		cancel()
		if err != nil {
			in.Database.Err = err.Error()
		}
	}
	if s.ingress != nil {
		_, droppedFull, _, depth, capacity := s.ingress.Stats()
		in.Queue = &health.QueueState{
			Depth:     depth,
			Capacity:  capacity,
			Dropped:   droppedFull,
			Emergency: s.ingress.EmergencyAccepted(),
		}
	}
	return in
}

// badgeForLevel renders a shared verdict level as the panel's badge class.
func badgeForLevel(level health.Level) string {
	switch level {
	case health.LevelBad:
		return badgeBad
	case health.LevelWarn:
		return badgeWarn
	default:
		return badgeOK
	}
}

// buildHealthView assembles the rows and the overall verdict. The verdict
// is the worst finding, in the vocabulary every other card uses: red when
// something that should run does not (a stopped plugin, a disconnected
// receiver, an unhealthy action, a DB failure, a full queue), amber when
// something is limited or held (a degraded or suspended plugin, dispatch
// drops), green otherwise. Switched-off subsystems stay gray and never
// colour the verdict — the operator turned them off on purpose. The
// per-component rows live in the plugin, MQTT and action cards next to
// this one.
func (s *Server) buildHealthView(lang string) healthView {
	v := healthView{}
	// One assessment, shared with the daily digest: plugins, receivers,
	// actions, hardware, the database and the dispatch queue. It also carries
	// the database probe, so the card does not ping twice.
	inputs := s.HealthInputs()
	assessment := health.Assess(inputs)
	verdict := badgeForLevel(assessment.Level())
	pending := assessment.Pending

	// Display time zone: the panel never shows a nameless clock. A named
	// IANA zone is quoted as it is; an unnamed process zone reports its
	// offset instead of pretending to be something it may not be.
	zoneName := s.displayLoc().String()
	if zoneName == "Local" {
		_, offset := time.Now().In(s.displayLoc()).Zone()
		sign := "+"
		if offset < 0 {
			sign, offset = "-", -offset
		}
		zoneName = fmt.Sprintf("Local (UTC%s%02d:%02d)", sign, offset/3600, (offset%3600)/60)
	}
	v.Zone = healthRow{
		Name:       i18n.T(lang, "dash.timezone"),
		BadgeClass: badgeOK,
		BadgeText:  zoneName,
		Detail:     i18n.T(lang, "dash.timezone.detail") + " " + zoneName,
	}

	// Database.
	v.DB = healthRow{Name: i18n.T(lang, "dash.database"), BadgeClass: badgeOK, BadgeText: "OK", Detail: i18n.T(lang, "dash.ready")}
	if inputs.Database != nil && inputs.Database.Err != "" {
		v.DB.BadgeClass, v.DB.BadgeText = "bad", i18n.T(lang, "health.badge.error")
		v.DB.Detail = inputs.Database.Err
	}
	// Low-disk alarm: below the configured threshold the DB row turns red
	// and the operator sees exactly how much room is left on the card.
	minFree := s.minFreeBytes.Load()
	if minFree > 0 {
		if dp, ok := s.users.(diskProbe); ok {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			free, err := dp.FreeBytes(ctx)
			cancel()
			if err != nil {
				s.logger.Warn("health: free space check failed", "error", err)
			} else if free < minFree {
				v.DB.BadgeClass, v.DB.BadgeText = "bad", i18n.T(lang, "health.badge.low_disk")
				v.DB.Detail += " · " + fmt.Sprintf(i18n.T(lang, "health.db_free"), free>>20, minFree>>20)
				verdict = worse(verdict, badgeBad)
			}
		}
	}

	// Hardware: every configured device the station currently cannot use.
	// A missing device never stops the station (it starts, logs and says so
	// here), so this section is the operator's answer to "why is SMS dead?"
	// without reading the log.
	problems := []preflight.Condition(nil)
	if s.hardware != nil {
		problems = s.hardware.Active()
	}
	for _, cond := range problems {
		row := healthRow{
			Name:       hardwareName(lang, cond.Key),
			BadgeClass: cond.Severity,
			BadgeText:  i18n.T(lang, "health.hardware.missing"),
			Detail:     cond.Detail + " · " + i18n.T(lang, "health.hardware.since") + " " + cond.Since.In(s.displayLoc()).Format("2006-01-02 15:04"),
		}
		if row.BadgeClass != badgeBad && row.BadgeClass != badgeWarn {
			row.BadgeClass = badgeWarn
		}
		v.Hardware = append(v.Hardware, row)
	}
	v.HardwareOK = len(v.Hardware) == 0 && s.hardware != nil && s.hardware.Len() > 0

	// Dispatch queue.
	received, droppedFull, _, depth, cap := s.ingress.Stats()
	v.Queue = healthRow{
		Name:       i18n.T(lang, "dash.dispatch_queue"),
		BadgeClass: badgeOK,
		BadgeText:  "OK",
		Detail:     fmt.Sprintf(i18n.T(lang, "health.queue_detail"), depth, cap, received, droppedFull),
	}
	if emergency := s.ingress.EmergencyAccepted(); emergency > 0 {
		// Emergency acceptance is the explicit, visible fallback: the
		// events were delivered but live only in RAM until the next
		// restart. The badge must never stay green while it happens.
		v.Queue.Detail += " · " + fmt.Sprintf(i18n.T(lang, "health.accept_emergency"), emergency)
		if v.Queue.BadgeClass == "ok" {
			v.Queue.BadgeClass, v.Queue.BadgeText = "warn", i18n.T(lang, "health.badge.emergency")
		}
		verdict = worse(verdict, badgeWarn)
	}
	if depth >= cap {
		v.Queue.BadgeClass, v.Queue.BadgeText = "bad", i18n.T(lang, "health.badge.full")
		verdict = worse(verdict, badgeBad)
	} else if droppedFull > 0 {
		v.Queue.BadgeClass, v.Queue.BadgeText = "warn", i18n.T(lang, "health.badge.drops")
		verdict = worse(verdict, badgeWarn)
	}

	// Pending notifications: queued action requests waiting for a worker.
	v.Pending = healthRow{
		Name:       i18n.T(lang, "health.pending"),
		BadgeClass: badgeOK,
		BadgeText:  fmt.Sprintf("%d", pending),
		Detail:     i18n.T(lang, "health.pending_detail"),
	}
	if pending > 0 {
		v.Pending.BadgeClass = "warn"
	}

	// The row badges describe their own subsystem; the verdict is the
	// worst *problem* among them. Pending work stays out of it on purpose:
	// a station with something in flight is busy, not broken.
	v.Overall = verdict
	v.OverallText = verdictText(lang, verdict)
	return v
}

// ingestHealthRows renders one health row per public HTTP ingest
// endpoint. The rows live in the Sources & Outputs card on the
// dashboard (they are sources after all), not in the health card.
func (s *Server) ingestHealthRows() []healthRow {
	var ids []string
	for id := range s.ingest {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var out []healthRow
	for _, id := range ids {
		probe, ok := s.ingest[id].(ingestProbe)
		if !ok {
			continue
		}
		row := healthRow{Name: id + " (ingest)"}
		c := probe.Counters()
		row.Detail = fmt.Sprintf("accepted=%d rejected=%d auth_failed=%d rate_limited=%d forbidden=%d",
			c.Accepted, c.Rejected, c.AuthFailed, c.RateLimited, c.Forbidden)
		switch {
		case probe.Connected():
			row.BadgeClass, row.BadgeText = "ok", "OK"
		case probe.Started():
			row.BadgeClass, row.BadgeText = "bad", i18n.T(i18n.LangEN, "health.badge.no_broker")
		default:
			row.BadgeClass, row.BadgeText = "muted", i18n.T(i18n.LangEN, "health.badge.not_started")
		}
		out = append(out, row)
	}
	return out
}

// hardwareName maps a preflight finding key onto the operator-facing device
// name. An unknown key is shown as it is: a new probe must be readable before
// anyone translates it.
func hardwareName(lang, key string) string {
	if name := i18n.T(lang, "preflight."+key); name != "preflight."+key {
		return name
	}
	return key
}
