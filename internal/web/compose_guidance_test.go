package web_test

import (
	"net/http"
	"strings"
	"testing"
)

// TestComposeFormGuidesTheEventType pins the guidance the production incident
// asked for: the event field offers the canonical vocabulary as suggestions (a
// suggestion is a name every channel translates, a typed one is not), explains
// itself, and the operator sees the notification line BEFORE sending it — a
// message reading "[SOSNA] POWAŻNE: Brak — Mup" must not be a surprise.
func TestComposeFormGuidesTheEventType(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	resp, html := env.get("/compose")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /compose = %d, want 200", resp.StatusCode)
	}
	for _, want := range []string{
		`list="compose-event-choices"`,
		`<datalist id="compose-event-choices">`,
		`<option value="Thunderstorm warning">`,
		`<option value="Weather alert">`,
		`class="compose-preview"`,
		`id="compose-preview-line"`,
		`data-prefix="`,
		"Notifications will read:",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("compose form is missing %s", want)
		}
	}
	// The field stays free text: the suggestions are hints, not a whitelist.
	if !strings.Contains(html, `name="event" value="" required`) {
		t.Errorf("the event field is no longer a (prefilled) text input: %.400s", html)
	}
}
