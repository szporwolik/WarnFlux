package web

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/ingesthttp"
	"github.com/szporwolik/WarnFlux/internal/plugin"
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
	Lang    string
	Overall string // ok | degraded
	DB      healthRow
	Queue   healthRow
	Pending healthRow
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

// buildHealthView assembles the rows and the overall verdict. Only
// genuinely operational problems are red: a source in a degraded state, a
// disconnected receiver, an unhealthy action, a DB failure or a full
// dispatch queue. Config-disabled subsystems stay gray.
func (s *Server) buildHealthView(lang string) healthView {
	v := healthView{Overall: "ok"}
	bad := false

	// Degraded sources and disconnected receivers still flip the overall
	// verdict; their per-component rows live in the plugins and MQTT
	// cards next to this one.
	for _, st := range s.router.Statuses() {
		if st.Kind != plugin.KindSource {
			continue
		}
		switch st.State {
		case plugin.StateDegraded, plugin.StateStarting, plugin.StateStopping:
			bad = true
		}
	}
	for _, rs := range s.receivers.Statuses() {
		if rs.Enabled && !rs.Connected {
			bad = true
		}
	}

	// Actions (SMTP, …) and pending-notification pressure.
	pending := 0
	for _, as := range s.actions.Statuses() {
		pending += as.QueueDepth
		if as.State == action.StateDegraded {
			bad = true
		}
	}

	// Database.
	v.DB = healthRow{Name: i18n.T(lang, "dash.database"), BadgeClass: "ok", BadgeText: "OK", Detail: i18n.T(lang, "dash.ready")}
	if pinger, ok := s.users.(dbPinger); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := pinger.Ping(ctx)
		cancel()
		if err != nil {
			v.DB.BadgeClass, v.DB.BadgeText = "bad", i18n.T(lang, "health.badge.error")
			v.DB.Detail = err.Error()
			bad = true
		}
	}
	// Low-disk alarm: below the configured threshold the DB row turns red
	// and the operator sees exactly how much room is left on the card.
	if s.minFreeBytes > 0 {
		if dp, ok := s.users.(diskProbe); ok {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			free, err := dp.FreeBytes(ctx)
			cancel()
			if err != nil {
				s.logger.Warn("health: free space check failed", "error", err)
			} else if free < s.minFreeBytes {
				v.DB.BadgeClass, v.DB.BadgeText = "bad", i18n.T(lang, "health.badge.low_disk")
				v.DB.Detail += " · " + fmt.Sprintf(i18n.T(lang, "health.db_free"), free>>20, s.minFreeBytes>>20)
				bad = true
			}
		}
	}

	// Dispatch queue.
	received, droppedFull, _, depth, cap := s.ingress.Stats()
	v.Queue = healthRow{
		Name:       i18n.T(lang, "dash.dispatch_queue"),
		BadgeClass: "ok",
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
		bad = true
	}
	if depth >= cap {
		v.Queue.BadgeClass, v.Queue.BadgeText = "bad", i18n.T(lang, "health.badge.full")
		bad = true
	} else if droppedFull > 0 {
		v.Queue.BadgeClass, v.Queue.BadgeText = "warn", i18n.T(lang, "health.badge.drops")
	}

	// Pending notifications: queued action requests waiting for a worker.
	v.Pending = healthRow{
		Name:       i18n.T(lang, "health.pending"),
		BadgeClass: "ok",
		BadgeText:  fmt.Sprintf("%d", pending),
		Detail:     i18n.T(lang, "health.pending_detail"),
	}
	if pending > 0 {
		v.Pending.BadgeClass = "warn"
	}

	if bad {
		v.Overall = "degraded"
	}
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
