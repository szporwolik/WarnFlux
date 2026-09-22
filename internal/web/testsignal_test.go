package web_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

func TestTestSignalEmit(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	csrf := env.csrfFromPage("/test")

	resp, html := env.postForm("/test", url.Values{
		"csrf":       {csrf},
		"transition": {"hazard_new"},
		"severity":   {"severe"},
		"urgency":    {"immediate"},
		"certainty":  {"observed"},
		"source":     {"test-source"},
		"event":      {"Testowy alert"},
		"headline":   {"Silny wiatr"},
		"areas":      {"małopolskie, śląskie"},
	})
	if resp.StatusCode != http.StatusOK || !strings.Contains(html, "Signal queued") {
		t.Fatalf("emit = %d, html: %s", resp.StatusCode, html)
	}
	if !strings.Contains(html, "hazard_new · severe · Testowy alert") {
		t.Errorf("summary missing: %s", html)
	}

	select {
	case ev := <-env.ingress.Events():
		if ev.Kind != dispatch.EventHazardTransition || ev.Hazard == nil {
			t.Fatalf("enqueued event = %+v, want hazard transition", ev)
		}
		h := ev.Hazard
		if h.Type != dispatch.TransitionNew {
			t.Errorf("type = %q, want hazard_new", h.Type)
		}
		if h.Hazard.Severity != "severe" || h.Hazard.Urgency != "immediate" || h.Hazard.Certainty != "observed" {
			t.Errorf("sev/urg/cer = %q/%q/%q, want severe/immediate/observed",
				h.Hazard.Severity, h.Hazard.Urgency, h.Hazard.Certainty)
		}
		if h.Hazard.Event != "Testowy alert" || h.Hazard.Headline != "Silny wiatr" {
			t.Errorf("event/headline = %q/%q", h.Hazard.Event, h.Hazard.Headline)
		}
		if len(h.Hazard.Areas) != 2 || h.Hazard.Areas[0] != "małopolskie" || h.Hazard.Areas[1] != "śląskie" {
			t.Errorf("areas = %v", h.Hazard.Areas)
		}
		if h.Source != "test-source" || !strings.HasPrefix(h.Key, "test-source:") {
			t.Errorf("source/key = %q/%q", h.Source, h.Key)
		}
		if ev.Origin.ReceiverID != "test-signal" {
			t.Errorf("origin = %+v", ev.Origin)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event enqueued into the ingress")
	}
}

func TestTestSignalValidation(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	csrf := env.csrfFromPage("/test")

	cases := []url.Values{
		{"csrf": {csrf}, "severity": {"orange"}},
		{"csrf": {csrf}, "severity": {"severe"}, "transition": {"hazard_bogus"}},
		{"csrf": {csrf}, "severity": {"severe"}, "urgency": {"maybe"}},
		{"csrf": {csrf}, "severity": {"severe"}, "certainty": {"definitely"}},
		{"csrf": {csrf}, "severity": {"severe"}, "source": {""}},
	}
	for i, form := range cases {
		resp, html := env.postForm("/test", form)
		if resp.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(html, "login-error") {
			t.Errorf("case %d = %d, want 422 with error banner; html: %s", i, resp.StatusCode, html)
		}
	}

	// Nothing invalid may have been enqueued.
	select {
	case ev := <-env.ingress.Events():
		t.Fatalf("invalid form still enqueued %+v", ev)
	default:
	}
}

func TestTestSignalPageRequiresLogin(t *testing.T) {
	env := newTestEnv(t)
	resp, _ := env.get("/test")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /test unauthenticated = %d %q, want redirect to /login", resp.StatusCode, resp.Header.Get("Location"))
	}
}

func TestTestSignalInNav(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	_, html := env.get("/test")
	for _, want := range []string{"Test signal", "Emit signal", "name=\"severity\"", "name=\"urgency\"", "name=\"certainty\""} {
		if !strings.Contains(html, want) {
			t.Errorf("/test page missing %q", want)
		}
	}
}
