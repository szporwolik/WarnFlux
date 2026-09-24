package web

import (
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/severity"
)

// composeSource is the fixed source id stamped on every communication
// issued through the compose module. The "issued communications" list
// filters the mirrored active state by this source. It is generic and
// installation-agnostic on purpose.
const composeSource = "compose"

// composeEventKeyRe validates module-generated event keys
// ("compose:<id>"). Keys from the form are always module-generated;
// this is a defensive check at the HTTP boundary.
var composeEventKeyRe = regexp.MustCompile(`^compose:[a-z0-9.-]{1,64}$`)

// Compose form bounds (generous, like the wire).
const (
	maxComposeHeadline = 200
	maxComposeEvent    = 120
	maxComposeText     = 4000
	maxComposeAreasLen = 500
	maxComposeAreas    = 32
)

// composePublisher issues and removes active-hazard documents on the
// WarnFlux broker. *mqttreceiver.Manager implements it.
type composePublisher interface {
	PublishActive(source string, h state.Hazard) error
	ExpireActive(source string, eventKey string) error
}

// composeStatuses are the allowed document states for the form.
var composeStatuses = []option{
	{Value: "active", Label: "active"},
	{Value: "expired", Label: "expired"},
}

// composeForm carries the submitted (or prefilled) communication values.
type composeForm struct {
	EventKey    string
	Event       string
	Severity    string
	Urgency     string
	Certainty   string
	Status      string
	Headline    string
	Description string
	Instruction string
	Areas       string
	EffectiveAt string
	ExpiresAt   string
	ReceivedAt  string
}

// composeItem is one module-issued communication in the list.
type composeItem struct {
	EventKey    string
	Event       string
	Severity    string
	Urgency     string
	Certainty   string
	Status      string
	Headline    string
	Areas       string
	EffectiveAt *time.Time
	ExpiresAt   *time.Time
	UpdatedAt   time.Time
}

// composeView is the /compose page model.
type composeView struct {
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
	Role     string

	Source string
	Form   composeForm
	Items  []composeItem
	Msg    string
	Error  string

	Severities  []option
	Urgencies   []option
	Certainties []option
	Statuses    []option

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavTest          bool
	NavCompose       bool
	NavLogs          bool
	NavAudit         bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
}

// composeFlash maps the post-action redirect marker to a banner message.
var composeFlash = map[string]string{
	"published": "Communication published on the broker.",
	"updated":   "Communication updated on the broker.",
	"expired":   "Communication expired and removed from the broker.",
}

// handleComposePage renders the compose form and the issued list.
func (s *Server) handleComposePage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	form := composeForm{Severity: "moderate", Status: "active"}

	if key := r.URL.Query().Get("edit"); key != "" {
		h, ok := s.composeHazard(key)
		if !ok {
			form = composeForm{Severity: "moderate", Status: "active"}
		} else {
			form = composeFormFromHazard(h)
		}
	}

	view := s.buildComposeView(form)
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	view.Msg = composeFlash[r.URL.Query().Get("msg")]
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "compose", view)
}

// handleComposeSave validates the form and publishes the communication
// as a retained active-hazard document. Existing event keys update the
// same topic; status "expired" removes it (same protocol effect as the
// explicit expire action).
func (s *Server) handleComposeSave(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || r.PostFormValue("csrf") == "" || r.PostFormValue("csrf") != sess.csrf {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}

	form := composeForm{
		EventKey:    strings.TrimSpace(r.PostFormValue("event_key")),
		Event:       strings.TrimSpace(r.PostFormValue("event")),
		Severity:    strings.ToLower(strings.TrimSpace(r.PostFormValue("severity"))),
		Urgency:     strings.ToLower(strings.TrimSpace(r.PostFormValue("urgency"))),
		Certainty:   strings.ToLower(strings.TrimSpace(r.PostFormValue("certainty"))),
		Status:      strings.ToLower(strings.TrimSpace(r.PostFormValue("status"))),
		Headline:    strings.TrimSpace(r.PostFormValue("headline")),
		Description: strings.TrimSpace(r.PostFormValue("description")),
		Instruction: strings.TrimSpace(r.PostFormValue("instruction")),
		Areas:       strings.TrimSpace(r.PostFormValue("areas")),
		EffectiveAt: strings.TrimSpace(r.PostFormValue("effective_at")),
		ExpiresAt:   strings.TrimSpace(r.PostFormValue("expires_at")),
		ReceivedAt:  strings.TrimSpace(r.PostFormValue("received_at")),
	}
	if form.Status == "" {
		form.Status = "active"
	}

	if msg := validateComposeForm(form); msg != "" {
		s.renderComposeError(w, r, http.StatusUnprocessableEntity, form, msg)
		return
	}

	h := composeHazardFromForm(form, time.Now())

	if s.pub == nil {
		s.logger.Warn("compose: publish skipped, no broker publisher configured")
		s.renderComposeError(w, r, http.StatusServiceUnavailable, form,
			"publishing is unavailable: no broker publisher is configured")
		return
	}
	if err := s.pub.PublishActive(composeSource, h); err != nil {
		s.logger.Warn("compose: publish failed", "event_key", h.EventKey, "error", err)
		s.renderComposeError(w, r, http.StatusServiceUnavailable, form,
			"publish failed: "+err.Error())
		return
	}
	s.logger.Info("compose: communication published",
		"event_key", h.EventKey, "severity", h.Severity, "status", h.Status)

	// The communication also flows through the canonical dispatch ingress
	// so the router's per-group rules (severity thresholds + assigned
	// actions) fire exactly like for any other source.
	typ := dispatch.TransitionNew
	if form.EventKey != "" {
		typ = dispatch.TransitionUpdated
	}
	if form.Status == "expired" {
		typ = dispatch.TransitionExpired
	}
	if !s.ingress.Enqueue(composeTransition(h, typ)) {
		s.logger.Warn("compose: dispatch queue full, transition dropped", "event_key", h.EventKey)
	}

	flash := "published"
	if form.EventKey != "" {
		flash = "updated"
	}
	if form.Status == "expired" {
		flash = "expired"
	}
	s.audit(sess.username, "compose-"+flash, h.EventKey)
	http.Redirect(w, r, "/compose?msg="+flash, http.StatusSeeOther)
}

// handleComposeExpire removes one module-issued communication by
// publishing an empty retained payload on its topic.
func (s *Server) handleComposeExpire(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || r.PostFormValue("csrf") == "" || r.PostFormValue("csrf") != sess.csrf {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	key := strings.TrimSpace(r.PostFormValue("event_key"))
	h, ok := s.composeHazard(key)
	if !ok {
		http.Error(w, "unknown communication", http.StatusNotFound)
		return
	}
	if s.pub == nil {
		s.logger.Warn("compose: expire skipped, no broker publisher configured", "event_key", key)
		http.Error(w, "publishing is unavailable: no broker publisher is configured", http.StatusServiceUnavailable)
		return
	}
	if err := s.pub.ExpireActive(composeSource, key); err != nil {
		s.logger.Warn("compose: expire failed", "event_key", key, "error", err)
		http.Error(w, "expire failed: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	s.logger.Info("compose: communication expired", "event_key", key)
	s.audit(sess.username, "compose-expire", key)

	// Group routing sees the expiry too (like the sources' cancelled /
	// expired transitions on the /events stream).
	if !s.ingress.Enqueue(composeTransition(h, dispatch.TransitionExpired)) {
		s.logger.Warn("compose: dispatch queue full, expiry transition dropped", "event_key", key)
	}
	http.Redirect(w, r, "/compose?msg=expired", http.StatusSeeOther)
}

// composeTransition builds the canonical ingress event for one compose
// action (new / updated / expired).
func composeTransition(h state.Hazard, typ dispatch.TransitionType) dispatch.Event {
	now := time.Now()
	return dispatch.Event{
		Kind:       dispatch.EventHazardTransition,
		ReceivedAt: now,
		Origin:     dispatch.Origin{Type: "web", ReceiverID: "compose"},
		Hazard: &dispatch.HazardTransition{
			Type:      typ,
			Key:       h.EventKey,
			Source:    composeSource,
			Timestamp: now,
			Hazard: dispatch.Hazard{
				EventKey:    h.EventKey,
				Source:      h.Source,
				SourceID:    h.SourceID,
				Event:       h.Event,
				Severity:    h.Severity,
				Urgency:     h.Urgency,
				Certainty:   h.Certainty,
				Headline:    h.Headline,
				Areas:       h.Areas,
				EffectiveAt: h.EffectiveAt,
				ExpiresAt:   h.ExpiresAt,
				ReceivedAt:  h.ReceivedAt,
				UpdatedAt:   h.UpdatedAt,
			},
		},
	}
}

// validateComposeForm returns a user-facing message for invalid input.
func validateComposeForm(form composeForm) string {
	if form.EventKey != "" && !composeEventKeyRe.MatchString(form.EventKey) {
		return "invalid event key"
	}
	if form.Event == "" {
		return "event must not be empty"
	}
	if len(form.Event) > maxComposeEvent {
		return fmt.Sprintf("event is too long (maximum %d characters)", maxComposeEvent)
	}
	if form.Headline == "" {
		return "headline must not be empty"
	}
	if len(form.Headline) > maxComposeHeadline {
		return fmt.Sprintf("headline is too long (maximum %d characters)", maxComposeHeadline)
	}
	if !severity.Valid(form.Severity) {
		return "invalid severity"
	}
	if form.Urgency != "" && !oneOf(form.Urgency, testUrgencies) {
		return "invalid urgency"
	}
	if form.Certainty != "" && !oneOf(form.Certainty, testCertainties) {
		return "invalid certainty"
	}
	if form.Status != "active" && form.Status != "expired" {
		return "invalid status"
	}
	if len(form.Description) > maxComposeText || len(form.Instruction) > maxComposeText {
		return fmt.Sprintf("description and instruction are limited to %d characters", maxComposeText)
	}
	if len(form.Areas) > maxComposeAreasLen {
		return fmt.Sprintf("areas are too long (maximum %d characters)", maxComposeAreasLen)
	}
	if form.EffectiveAt != "" && parseComposeTime(form.EffectiveAt) == nil {
		return "invalid effective time"
	}
	if form.ExpiresAt != "" && parseComposeTime(form.ExpiresAt) == nil {
		return "invalid expires time"
	}
	return ""
}

// parseComposeTime parses a datetime-local form value in the server's
// local zone; RFC 3339 strings (hidden round-trips) are accepted too.
func parseComposeTime(v string) *time.Time {
	if t, err := time.ParseInLocation("2006-01-02T15:04", v, time.Local); err == nil {
		return &t
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return &t
	}
	return nil
}

// composeTimeValue formats an optional time for the datetime-local input.
func composeTimeValue(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Local().Format("2006-01-02T15:04")
}

// composeAreas splits the comma/semicolon-separated areas field.
func composeAreas(raw string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ';' })
	areas := make([]string, 0, len(parts))
	for _, p := range parts {
		if a := strings.TrimSpace(p); a != "" {
			areas = append(areas, a)
		}
	}
	if len(areas) > maxComposeAreas {
		areas = areas[:maxComposeAreas]
	}
	return areas
}

// composeHazardFromForm assembles the mirror hazard for publication.
func composeHazardFromForm(form composeForm, now time.Time) state.Hazard {
	key := form.EventKey
	if key == "" {
		key = fmt.Sprintf("%s:%d", composeSource, now.UnixMilli())
	}
	received := parseComposeTime(form.ReceivedAt)
	if received == nil {
		received = &now
	}
	return state.Hazard{
		EventKey:    key,
		Source:      composeSource,
		SourceID:    "ops",
		Event:       form.Event,
		Severity:    form.Severity,
		Urgency:     form.Urgency,
		Certainty:   form.Certainty,
		Headline:    form.Headline,
		Description: form.Description,
		Instruction: form.Instruction,
		Areas:       composeAreas(form.Areas),
		Status:      form.Status,
		EffectiveAt: parseComposeTime(form.EffectiveAt),
		ExpiresAt:   parseComposeTime(form.ExpiresAt),
		ReceivedAt:  *received,
		UpdatedAt:   now,
	}
}

// composeFormFromHazard prefills the form for an edit.
func composeFormFromHazard(h state.Hazard) composeForm {
	return composeForm{
		EventKey:    h.EventKey,
		Event:       h.Event,
		Severity:    h.Severity,
		Urgency:     h.Urgency,
		Certainty:   h.Certainty,
		Status:      h.Status,
		Headline:    h.Headline,
		Description: h.Description,
		Instruction: h.Instruction,
		Areas:       strings.Join(h.Areas, ", "),
		EffectiveAt: composeTimeValue(h.EffectiveAt),
		ExpiresAt:   composeTimeValue(h.ExpiresAt),
		ReceivedAt:  h.ReceivedAt.Format(time.RFC3339),
	}
}

// composeHazard returns one module-issued hazard from the mirror.
func (s *Server) composeHazard(eventKey string) (state.Hazard, bool) {
	for _, h := range s.st.Snapshot().Hazards {
		if h.Source == composeSource && h.EventKey == eventKey {
			return h, true
		}
	}
	return state.Hazard{}, false
}

// composeItems returns the module-issued communications, newest first.
func (s *Server) composeItems() []composeItem {
	hazards := s.st.Snapshot().Hazards
	items := make([]composeItem, 0, len(hazards))
	for _, h := range hazards {
		if h.Source != composeSource {
			continue
		}
		items = append(items, composeItem{
			EventKey:    h.EventKey,
			Event:       h.Event,
			Severity:    h.Severity,
			Urgency:     h.Urgency,
			Certainty:   h.Certainty,
			Status:      h.Status,
			Headline:    h.Headline,
			Areas:       strings.Join(h.Areas, ", "),
			EffectiveAt: h.EffectiveAt,
			ExpiresAt:   h.ExpiresAt,
			UpdatedAt:   h.UpdatedAt,
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].UpdatedAt.After(items[j].UpdatedAt) })
	return items
}

// buildComposeView assembles the page model.
func (s *Server) buildComposeView(form composeForm) composeView {
	return composeView{
		AppTitle:    s.cfg.Title,
		Name:        s.displayName(),
		Header1:     s.displayHeader1(),
		Header2:     s.cfg.Header2,
		Tagline:     s.cfg.Tagline,
		Version:     s.version,
		Commit:      s.commit,
		RepoURL:     repoURL,
		Source:      composeSource,
		Form:        form,
		Items:       s.composeItems(),
		Severities:  testSeverities,
		Urgencies:   testUrgencies,
		Certainties: testCertainties,
		Statuses:    composeStatuses,
		NavCompose:  true,
	}
}

// renderComposeError re-renders the page with an error banner, preserving
// the submitted form values.
func (s *Server) renderComposeError(w http.ResponseWriter, r *http.Request, status int, form composeForm, msg string) {
	sess := s.sessions.currentSession(r)
	view := s.buildComposeView(form)
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	view.Error = msg
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	s.render(w, "compose", view)
}
