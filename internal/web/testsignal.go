// Package web — the test signal page: a logged-in user emits a synthetic
// hazard transition (or generic MQTT message) into the canonical dispatch
// ingress, so the whole routing path (rule engine → group severity
// thresholds → assigned actions and outputs) can be exercised without an
// upstream WarnFlux instance.
package web

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/severity"
)

// option is one select option for the test signal form.
type option struct {
	Value string
	Label string
}

var (
	testTransitions = []option{
		{Value: "hazard_new", Label: "new"},
		{Value: "hazard_updated", Label: "updated"},
		{Value: "hazard_cancelled", Label: "cancelled"},
		{Value: "hazard_expired", Label: "expired"},
	}
	testSeverities = []option{
		{Value: "unknown", Label: "unknown"},
		{Value: "minor", Label: "minor"},
		{Value: "moderate", Label: "moderate"},
		{Value: "severe", Label: "severe"},
		{Value: "extreme", Label: "extreme"},
	}
	testUrgencies = []option{
		{Value: "", Label: "—"},
		{Value: "unknown", Label: "unknown"},
		{Value: "immediate", Label: "immediate"},
		{Value: "expected", Label: "expected"},
		{Value: "future", Label: "future"},
		{Value: "past", Label: "past"},
	}
	testCertainties = []option{
		{Value: "", Label: "—"},
		{Value: "unknown", Label: "unknown"},
		{Value: "observed", Label: "observed"},
		{Value: "likely", Label: "likely"},
		{Value: "possible", Label: "possible"},
		{Value: "unlikely", Label: "unlikely"},
	}
)

// testSignalForm carries the submitted test signal values.
type testSignalForm struct {
	Transition string
	Source     string
	SourceID   string
	Event      string
	Severity   string
	Urgency    string
	Certainty  string
	Headline   string
	Areas      string
}

// testView is the full /test page model.
type testView struct {
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

	Transitions []option
	Severities  []option
	Urgencies   []option
	Certainties []option

	Form    testSignalForm
	Emitted bool
	Summary string
	Error   string

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavTest          bool
	NavLogs          bool
	NavAudit         bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
	NavCompose       bool
}

// handleTestPage renders the test signal form.
func (s *Server) handleTestPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := s.buildTestView(testSignalForm{
		Transition: "hazard_new",
		Source:     "test-signal",
		Severity:   "moderate",
	})
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "test", view)
}

// handleTestEmit validates the form and enqueues the synthetic event into
// the dispatch ingress. From there the rule engine treats it exactly like
// an event received from an upstream WarnFlux instance.
func (s *Server) handleTestEmit(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || r.PostFormValue("csrf") == "" || r.PostFormValue("csrf") != sess.csrf {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}

	form := testSignalForm{
		Transition: strings.ToLower(strings.TrimSpace(r.PostFormValue("transition"))),
		Source:     strings.ToLower(strings.TrimSpace(r.PostFormValue("source"))),
		SourceID:   strings.TrimSpace(r.PostFormValue("source_id")),
		Event:      strings.TrimSpace(r.PostFormValue("event")),
		Severity:   strings.ToLower(strings.TrimSpace(r.PostFormValue("severity"))),
		Urgency:    strings.ToLower(strings.TrimSpace(r.PostFormValue("urgency"))),
		Certainty:  strings.ToLower(strings.TrimSpace(r.PostFormValue("certainty"))),
		Headline:   strings.TrimSpace(r.PostFormValue("headline")),
		Areas:      strings.TrimSpace(r.PostFormValue("areas")),
	}

	if msg := validateTestForm(form); msg != "" {
		s.renderTestError(w, r, http.StatusUnprocessableEntity, form, msg)
		return
	}

	ev, summary := buildTestEvent(form, time.Now())
	if !s.ingress.Enqueue(ev) {
		s.renderTestError(w, r, http.StatusTooManyRequests, form,
			"dispatch queue is full; the signal was dropped — retry in a moment")
		return
	}

	view := s.buildTestView(form)
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	view.Emitted = true
	view.Summary = summary
	s.audit(sess.username, "test-emit", summary)
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "test", view)
}

// validateTestForm returns a user-facing message for invalid input.
func validateTestForm(form testSignalForm) string {
	switch form.Transition {
	case "hazard_new", "hazard_updated", "hazard_cancelled", "hazard_expired":
	default:
		return "invalid transition type"
	}
	if form.Source == "" {
		return "source must not be empty"
	}
	if len(form.Source) > 64 {
		return "source is too long (maximum 64 characters)"
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
	return ""
}

func oneOf(v string, opts []option) bool {
	for _, o := range opts {
		if o.Value == v {
			return true
		}
	}
	return false
}

// buildTestEvent constructs the canonical dispatch event for the submitted
// form and a human-readable summary of what was emitted.
func buildTestEvent(form testSignalForm, now time.Time) (dispatch.Event, string) {
	sourceID := form.SourceID
	if sourceID == "" {
		sourceID = fmt.Sprintf("test-%d", now.UnixMilli())
	}
	event := form.Event
	if event == "" {
		event = "Test signal"
	}
	key := core.EventKey(form.Source, sourceID)

	var typ dispatch.TransitionType
	switch form.Transition {
	case "hazard_updated":
		typ = dispatch.TransitionUpdated
	case "hazard_cancelled":
		typ = dispatch.TransitionCancelled
	case "hazard_expired":
		typ = dispatch.TransitionExpired
	default:
		typ = dispatch.TransitionNew
	}

	areas := strings.FieldsFunc(form.Areas, func(r rune) bool { return r == ',' || r == ';' })
	for i := range areas {
		areas[i] = strings.TrimSpace(areas[i])
	}

	ev := dispatch.Event{
		Kind:       dispatch.EventHazardTransition,
		ReceivedAt: now,
		Origin:     dispatch.Origin{Type: "web", ReceiverID: "test-signal"},
		Hazard: &dispatch.HazardTransition{
			Type:      typ,
			Key:       key,
			Source:    form.Source,
			Timestamp: now,
			Hazard: dispatch.Hazard{
				EventKey:  key,
				Source:    form.Source,
				SourceID:  sourceID,
				Event:     event,
				Severity:  form.Severity,
				Urgency:   form.Urgency,
				Certainty: form.Certainty,
				Headline:  form.Headline,
				Areas:     areas,
				UpdatedAt: now,
			},
		},
	}
	summary := fmt.Sprintf("%s · %s · %s", form.Transition, form.Severity, event)
	if form.Headline != "" {
		summary += " — " + form.Headline
	}
	return ev, summary
}

// buildTestView assembles the page model.
func (s *Server) buildTestView(form testSignalForm) testView {
	return testView{
		AppTitle:    s.cfg.Title,
		Name:        s.displayName(),
		Header1:     s.displayHeader1(),
		Header2:     s.cfg.Header2,
		Tagline:     s.cfg.Tagline,
		Version:     s.version,
		Commit:      s.commit,
		RepoURL:     repoURL,
		Transitions: testTransitions,
		Severities:  testSeverities,
		Urgencies:   testUrgencies,
		Certainties: testCertainties,
		Form:        form,
		NavTest:     true,
	}
}

// renderTestError re-renders the page with an error banner, preserving
// the submitted form values.
func (s *Server) renderTestError(w http.ResponseWriter, r *http.Request, status int, form testSignalForm, msg string) {
	sess := s.sessions.currentSession(r)
	view := s.buildTestView(form)
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	view.Error = msg
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	s.render(w, "test", view)
}
