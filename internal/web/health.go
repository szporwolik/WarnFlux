package web

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
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

// healthRow is one subsystem line: name, verdict badge and a one-line
// operational detail. BadgeClass is "ok" (green), "bad" (red — something
// that actually matters), "warn" (amber) or "muted" (config-disabled).
type healthRow struct {
	Name       string
	BadgeClass string
	BadgeText  string
	Detail     string
}

// healthView is the /health page model.
type healthView struct {
	AppTitle string
	Name     string
	Header1  string
	Header2  string
	Tagline  string
	Version  string
	Commit   string
	RepoURL  string
	CSRF     string
	Username string

	Overall string // ok | degraded
	Sources []healthRow
	MQTT    []healthRow
	Actions []healthRow
	Ingest  []healthRow
	DB      healthRow
	Queue   healthRow
	Pending healthRow

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavTest          bool
	NavLogs          bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
	NavCompose       bool
}

// handleHealthPage renders the system health dashboard.
func (s *Server) handleHealthPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := s.buildHealthView()
	view.CSRF = sess.csrf
	view.Username = sess.username
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "healthpage", view)
}

// handlePartialHealth serves the refreshable health section.
func (s *Server) handlePartialHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "health", s.buildHealthView())
}

// buildHealthView assembles the rows and the overall verdict. Only
// genuinely operational problems are red: a source in a degraded state, a
// disconnected receiver, an unhealthy action, a DB failure or a full
// dispatch queue. Config-disabled subsystems stay gray.
func (s *Server) buildHealthView() healthView {
	v := healthView{
		AppTitle:  s.cfg.Title,
		Name:      s.displayName(),
		Header1:   s.displayHeader1(),
		Header2:   s.cfg.Header2,
		Tagline:   s.cfg.Tagline,
		Version:   s.version,
		Commit:    s.commit,
		RepoURL:   repoURL,
		Overall:   "ok",
		NavHealth: true,
	}
	bad := false

	// Sources (IMGW, RSO, openmeteo, …).
	for _, st := range s.router.Statuses() {
		if st.Kind != plugin.KindSource {
			continue
		}
		row := healthRow{Name: st.ID, Detail: sourceDetail(st)}
		switch st.State {
		case plugin.StateRunning:
			row.BadgeClass, row.BadgeText = "ok", "OK"
			if st.LastSummary != "" {
				row.Detail = st.LastSummary + " · " + row.Detail
			}
		case plugin.StateDegraded, plugin.StateSuspended, plugin.StateStarting, plugin.StateStopping:
			row.BadgeClass, row.BadgeText = "bad", strings.ToUpper(string(st.State))
			bad = true
			if st.LastError != "" {
				row.Detail = st.LastError + " · " + row.Detail
			}
		default:
			row.BadgeClass, row.BadgeText = "muted", "disabled"
		}
		v.Sources = append(v.Sources, row)
	}

	// MQTT receivers.
	for _, rs := range s.receivers.Statuses() {
		row := healthRow{Name: rs.ID}
		if !rs.Enabled {
			row.BadgeClass, row.BadgeText = "muted", "disabled"
			row.Detail = "not enabled"
		} else if rs.Connected {
			row.BadgeClass, row.BadgeText = "ok", "OK"
			row.Detail = "connected · " + rs.Broker
			if !rs.LastMessage.IsZero() {
				row.Detail += " · last message " + ageText(time.Since(rs.LastMessage))
			}
		} else {
			row.BadgeClass, row.BadgeText = "bad", "DISCONNECTED"
			bad = true
			row.Detail = "not connected"
			if rs.LastError != "" {
				row.Detail += " · " + rs.LastError
			}
		}
		v.MQTT = append(v.MQTT, row)
	}

	// Actions (SMTP, …) and pending-notification pressure.
	pending := 0
	for _, as := range s.actions.Statuses() {
		row := healthRow{Name: as.ID}
		pending += as.QueueDepth
		switch as.State {
		case action.StateHealthy:
			row.BadgeClass, row.BadgeText = "ok", "OK"
			row.Detail = "healthy"
			if !as.LastSuccess.IsZero() {
				row.Detail += " · last success " + ageText(time.Since(as.LastSuccess))
			}
			if as.Failures > 0 {
				row.Detail += fmt.Sprintf(" · %d failures since start", as.Failures)
			}
		case action.StateDegraded:
			row.BadgeClass, row.BadgeText = "bad", "DEGRADED"
			bad = true
			if as.LastErrorText != "" {
				row.Detail = as.LastErrorText
			}
		default:
			row.BadgeClass, row.BadgeText = "muted", "disabled"
			row.Detail = as.Reason
		}
		v.Actions = append(v.Actions, row)
	}

	// Public HTTP ingest endpoints.
	var ingestIDs []string
	for id := range s.ingest {
		ingestIDs = append(ingestIDs, id)
	}
	sort.Strings(ingestIDs)
	for _, id := range ingestIDs {
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
			row.BadgeClass, row.BadgeText = "bad", "NO BROKER"
			bad = true
		default:
			row.BadgeClass, row.BadgeText = "muted", "not started"
		}
		v.Ingest = append(v.Ingest, row)
	}

	// Database.
	v.DB = healthRow{Name: "Database", BadgeClass: "ok", BadgeText: "OK", Detail: "ready"}
	if pinger, ok := s.users.(dbPinger); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := pinger.Ping(ctx)
		cancel()
		if err != nil {
			v.DB.BadgeClass, v.DB.BadgeText = "bad", "ERROR"
			v.DB.Detail = err.Error()
			bad = true
		}
	}

	// Dispatch queue.
	received, droppedFull, _, depth, cap := s.ingress.Stats()
	v.Queue = healthRow{
		Name:       "Dispatch queue",
		BadgeClass: "ok",
		BadgeText:  "OK",
		Detail:     fmt.Sprintf("%d / %d in flight · %d processed · %d dropped (full)", depth, cap, received, droppedFull),
	}
	if depth >= cap {
		v.Queue.BadgeClass, v.Queue.BadgeText = "bad", "FULL"
		bad = true
	} else if droppedFull > 0 {
		v.Queue.BadgeClass, v.Queue.BadgeText = "warn", "DROPS"
	}

	// Pending notifications: queued action requests waiting for a worker.
	v.Pending = healthRow{
		Name:       "Pending notifications",
		BadgeClass: "ok",
		BadgeText:  fmt.Sprintf("%d", pending),
		Detail:     "queued action requests",
	}
	if pending > 0 {
		v.Pending.BadgeClass = "warn"
	}

	if bad {
		v.Overall = "degraded"
	}
	return v
}

// sourceDetail describes the last poll of a source plugin.
func sourceDetail(st plugin.PluginStatus) string {
	if st.LastSuccessAt == nil {
		if st.LastError != "" {
			return st.LastError
		}
		return "no successful poll yet"
	}
	return "last poll " + ageText(time.Since(*st.LastSuccessAt))
}

// ageText renders a duration like the template "age" helper, for views
// built in Go rather than templates.
func ageText(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Second:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
}
