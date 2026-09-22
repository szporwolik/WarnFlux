package web

import (
	"net/http"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// routerHeartbeatStaleAfter marks a router heartbeat stale when no status
// ping has been received for 3 × WarnFlux's default heartbeat interval
// (30s). A single missed ping therefore never alarms.
const routerHeartbeatStaleAfter = 90 * time.Second

// ---- system view ---------------------------------------------------------

type statusView struct {
	Title       string
	Version     string
	Commit      string
	Uptime      time.Duration
	Receivers   int
	Connected   int
	Messages    int64
	Malformed   int64
	Oversized   int64
	RouterDocs  int
	QueueDepth  int
	QueueCap    int
	DroppedFull int64
}

// ---- MQTT connections view ----------------------------------------------

type receiverRow struct {
	ID            string
	Enabled       bool
	Broker        string
	Connected     bool
	HREnabled     bool
	HRPrefix      string
	Subscriptions int
	LastConnect   time.Time
	LastMessage   time.Time
	LastError     string
	Messages      int64
	Malformed     int64
	Oversized     int64
	Dropped       int64
	RouterState   string
	Heartbeat     string
}

type mqttView struct {
	Receivers []receiverRow
}

// ---- weather view --------------------------------------------------------

type weatherEntry struct {
	Key          string
	Location     string
	Receiver     string
	Temperature  *float64
	Condition    string
	Humidity     *float64
	WindSpeed    *float64
	WindDir      *float64
	WindGusts    *float64
	Pressure     *float64
	GeneratedAt  time.Time
	ProviderName string
}

type weatherView struct {
	Entries []weatherEntry
}

// ---- warnings view -------------------------------------------------------

type hazardView struct {
	Severity    string
	Headline    string
	Event       string
	Source      string
	Receiver    string
	Status      string
	EffectiveAt *time.Time
	ExpiresAt   *time.Time
	Areas       string
	Description string
	UpdatedAt   time.Time
}

type warningsView struct {
	Hazards []hazardView
	Count   int
}

// ---- router plugins view (sources + outputs) -----------------------------

type pluginStatusView struct {
	ID                  string
	Type                string
	State               plugin.PluginState
	StartedAt           time.Time
	LastSuccessAt       *time.Time
	LastErrorAt         *time.Time
	LastError           string
	ConsecutiveFailures int
	RestartCount        int
}

type pluginsView struct {
	Sources []pluginStatusView
	Outputs []pluginStatusView
}

// ---- actions view --------------------------------------------------------

type actionStatusView struct {
	ID            string
	Type          string
	State         action.InstanceState
	Reason        string
	QueueDepth    int
	QueueCapacity int
	Handled       int64
	Failures      int64
	LastSuccess   time.Time
	LastError     time.Time
	LastErrorText string
}

type actionsView struct {
	Actions []actionStatusView
}

type pageView struct {
	AppTitle string
	RepoURL  string
	Status   statusView
	MQTT     mqttView
	Weather  weatherView
	Warnings warningsView
	Plugins  pluginsView
	Actions  actionsView
	CSRF     string
}

// ---- view builders -------------------------------------------------------

func (s *Server) buildStatusView() statusView {
	_, droppedFull, _, depth, cap := s.ingress.Stats()
	rs := s.receivers.Statuses()
	view := statusView{
		Title:       s.cfg.Title,
		Version:     s.version,
		Commit:      s.commit,
		Uptime:      time.Since(s.startedAt),
		Receivers:   len(rs),
		QueueDepth:  depth,
		QueueCap:    cap,
		DroppedFull: droppedFull,
	}
	for _, r := range rs {
		if r.Connected {
			view.Connected++
		}
		view.Messages += r.Messages
		view.Malformed += r.Malformed
		view.Oversized += r.Oversized
	}
	view.RouterDocs = len(s.st.Snapshot().Router)
	return view
}

func (s *Server) buildMQTTView(snap state.Snapshot) mqttView {
	view := mqttView{}
	for _, r := range s.receivers.Statuses() {
		row := receiverRow{
			ID:            r.ID,
			Enabled:       r.Enabled,
			Broker:        r.Broker,
			Connected:     r.Connected,
			HREnabled:     r.HREnabled,
			HRPrefix:      r.HRPrefix,
			Subscriptions: r.Subscriptions,
			LastConnect:   r.LastConnect,
			LastMessage:   r.LastMessage,
			LastError:     r.LastError,
			Messages:      r.Messages,
			Malformed:     r.Malformed,
			Oversized:     r.Oversized,
			Dropped:       r.Dropped,
		}
		if rs, ok := snap.Router[r.ID]; ok {
			row.RouterState = rs.State
			row.Heartbeat = heartbeatState(rs)
		} else {
			row.Heartbeat = "unknown"
		}
		view.Receivers = append(view.Receivers, row)
	}
	return view
}

// heartbeatState classifies one router's alive-ping freshness from the last
// received retained status message.
func heartbeatState(rs state.RouterStatus) string {
	if !rs.Valid || rs.ReceivedAt.IsZero() {
		return "unknown"
	}
	if time.Since(rs.ReceivedAt) < routerHeartbeatStaleAfter {
		return "fresh"
	}
	return "stale"
}

func buildWeatherView(snap state.Snapshot) weatherView {
	v := weatherView{Entries: make([]weatherEntry, 0, len(snap.Weather))}
	for _, e := range snap.Weather {
		w := e.Weather
		entry := weatherEntry{
			Key:          e.Key,
			Location:     w.LocationName,
			Receiver:     e.ReceiverID,
			Temperature:  w.TemperatureC,
			Condition:    w.Condition,
			Humidity:     w.HumidityPct,
			WindSpeed:    w.WindSpeedKmh,
			WindDir:      w.WindDirectionDeg,
			WindGusts:    w.WindGustsKmh,
			Pressure:     w.PressureMSLHpa,
			GeneratedAt:  w.GeneratedAt,
			ProviderName: w.ProviderName,
		}
		if entry.Location == "" {
			entry.Location = w.LocationID
		}
		v.Entries = append(v.Entries, entry)
	}
	return v
}

func buildWarningsView(snap state.Snapshot) warningsView {
	v := warningsView{Hazards: make([]hazardView, 0, len(snap.Hazards))}
	for _, h := range snap.Hazards {
		hv := hazardView{
			Severity:    h.Severity,
			Headline:    h.Headline,
			Event:       h.Event,
			Source:      h.Source,
			Receiver:    h.ReceiverID,
			Status:      h.Status,
			EffectiveAt: h.EffectiveAt,
			ExpiresAt:   h.ExpiresAt,
			Areas:       strings.Join(h.Areas, ", "),
			Description: h.Description,
			UpdatedAt:   h.UpdatedAt,
		}
		if hv.Headline == "" {
			hv.Headline = hv.Event
		}
		if hv.Severity == "" {
			hv.Severity = "unknown"
		}
		v.Hazards = append(v.Hazards, hv)
	}
	v.Count = len(v.Hazards)
	return v
}

func (s *Server) buildPluginsView() pluginsView {
	v := pluginsView{}
	for _, st := range s.router.Statuses() {
		row := pluginStatusView{
			ID:                  st.ID,
			Type:                st.Type,
			State:               st.State,
			StartedAt:           st.StartedAt,
			LastSuccessAt:       st.LastSuccessAt,
			LastErrorAt:         st.LastErrorAt,
			LastError:           st.LastError,
			ConsecutiveFailures: st.ConsecutiveFailures,
			RestartCount:        st.RestartCount,
		}
		switch st.Kind {
		case plugin.KindSource:
			v.Sources = append(v.Sources, row)
		case plugin.KindOutput:
			v.Outputs = append(v.Outputs, row)
		}
	}
	return v
}

func (s *Server) buildActionsView() actionsView {
	v := actionsView{}
	for _, st := range s.actions.Statuses() {
		v.Actions = append(v.Actions, actionStatusView{
			ID:            st.ID,
			Type:          st.Type,
			State:         st.State,
			Reason:        st.Reason,
			QueueDepth:    st.QueueDepth,
			QueueCapacity: st.QueueCapacity,
			Handled:       st.Handled,
			Failures:      st.Failures,
			LastSuccess:   st.LastSuccess,
			LastError:     st.LastError,
			LastErrorText: st.LastErrorText,
		})
	}
	return v
}

// ---- handlers ------------------------------------------------------------

func (s *Server) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	// Already authenticated: go straight to the dashboard.
	if s.sessions.currentSession(r) != nil {
		http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
		return
	}
	csrf, err := newCSRFCookie(w, s.cfg.Auth.SecureCookie)
	if err != nil {
		s.logger.Error("web: csrf token generation failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "login", map[string]any{
		"AppTitle": s.cfg.Title,
		"Version":  s.version,
		"Commit":   s.commit,
		"RepoURL":  repoURL,
		"CSRF":     csrf,
		"Error":    "",
	})
}

func (s *Server) handleLoginSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	// Double-submit CSRF check for the login form.
	cookie, _ := r.Cookie(csrfCookie)
	if cookie == nil || cookie.Value == "" || r.PostFormValue("csrf") != cookie.Value {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}

	username := r.PostFormValue("username")
	password := r.PostFormValue("password")
	valid := checkUsername(username, s.cfg.Auth.Username) &&
		checkPassword(password, s.cfg.Auth.Password)

	if !valid {
		s.logger.Warn("web: failed login attempt", "remote", r.RemoteAddr)
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusUnauthorized)
		s.render(w, "login", map[string]any{
			"AppTitle": s.cfg.Title,
			"Version":  s.version,
			"Commit":   s.commit,
			"RepoURL":  repoURL,
			"CSRF":     cookie.Value,
			"Error":    "Invalid username or password",
		})
		return
	}

	token, _, err := s.sessions.newSession(username)
	if err != nil {
		s.logger.Error("web: session creation failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	s.sessions.setSessionCookie(w, token)
	http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if sess == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	// Session-bound CSRF token protects the state-changing logout route.
	if r.PostFormValue("csrf") == "" || r.PostFormValue("csrf") != sess.csrf {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	if cookie, err := r.Cookie(sessionCookie); err == nil {
		s.sessions.delete(cookie.Value)
	}
	s.sessions.clearSessionCookie(w)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	snap := s.st.Snapshot()
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "page", pageView{
		AppTitle: s.cfg.Title,
		RepoURL:  repoURL,
		Status:   s.buildStatusView(),
		MQTT:     s.buildMQTTView(snap),
		Weather:  buildWeatherView(snap),
		Warnings: buildWarningsView(snap),
		Plugins:  s.buildPluginsView(),
		Actions:  s.buildActionsView(),
		CSRF:     sess.csrf,
	})
}

func (s *Server) handlePartialStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "status", s.buildStatusView())
}

func (s *Server) handlePartialMQTT(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "mqtt", s.buildMQTTView(s.st.Snapshot()))
}

func (s *Server) handlePartialWeather(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "weather", buildWeatherView(s.st.Snapshot()))
}

func (s *Server) handlePartialWarnings(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "warnings", buildWarningsView(s.st.Snapshot()))
}

func (s *Server) handlePartialPlugins(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "plugins", s.buildPluginsView())
}

func (s *Server) handlePartialActions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "actions", s.buildActionsView())
}

// handleHealthz reports process liveness. It never fails because MQTT is
// disconnected: that is operational state, not process health.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleReadyz reports readiness: config valid, database open/migrated,
// HTTP initialized. Transient MQTT outages do not affect readiness.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if s.ready.Load() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("not ready\n"))
}
