package web_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/radiocli"
)

// TestHelpPage pins the operator manual: every signed-in user sees it,
// the sections render in the page language and the sidebar carries the
// Help entry.
func TestHelpPage(t *testing.T) {
	env := newTestEnv(t)
	if _, err := env.users.CreateUser("member1", "", "", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}

	// Unauthenticated: redirect to login.
	resp, _ := env.get("/help")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /help unauthenticated = %d %q, want 303 /login", resp.StatusCode, resp.Header.Get("Location"))
	}

	env.login()
	resp, html := env.get("/help")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /help = %d", resp.StatusCode)
	}
	for _, want := range []string{
		"Getting started", "How it works", "EMCOM networks", "/emcom &lt;network-id&gt;",
		"New message", "Admin", "Alert RCB",
		"group channel", "delivery channels",
		// The REST API section documents the public ingest endpoint.
		"REST API (integrations)", "POST /api/v1/ingest/",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("help page missing %q", want)
		}
	}
	if !strings.Contains(html, `<span class="nav-label">Help</span>`) {
		t.Error("sidebar missing the Help entry")
	}

	// The manual is available to every signed-in user.
	env.logout()
	_, html = env.get("/login")
	resp, _ = env.postForm("/login", map[string][]string{
		"csrf": {extractCSRF(t, html)}, "username": {"member1"}, "password": {"password123"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("member login = %d, want 303", resp.StatusCode)
	}
	resp, html = env.get("/help")
	if resp.StatusCode != http.StatusOK || !strings.Contains(html, "How it works") {
		t.Fatalf("member GET /help = %d, want the manual", resp.StatusCode)
	}
}

// TestHelpPageRadioCommands pins the command reference: the manual lists
// exactly what the shared interpreter answers (nothing invented), marks
// the restricted commands, translates both languages and stays absent
// when no interpreter is wired.
func TestHelpPageRadioCommands(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	// No interpreter wired: the manual renders without a command list.
	resp, html := env.get("/help")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /help = %d", resp.StatusCode)
	}
	if strings.Contains(html, "Radio commands") {
		t.Error("command section rendered without an interpreter")
	}
	// The "join our Meshtastic channel" card is bound to the station's
	// own channel: with no hub (and no channel key) it must not render,
	// so the page never shows a scan code nobody can use.
	if strings.Contains(html, "Meshtastic group channel") || strings.Contains(html, "data:image/png;base64,") {
		t.Error("mesh channel card rendered without a known channel")
	}

	bot := radiocli.New("WarnFlux test")
	bot.Register("weather", "weather", func(string, radiocli.Sender) radiocli.Result {
		return radiocli.Result{Handled: true}
	})
	bot.RegisterRestricted("alert", "alert", func(string, radiocli.Sender) radiocli.Result {
		return radiocli.Result{Handled: true}
	})
	env.server.SetRadioCLI(bot)

	resp, html = env.get("/help")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /help with interpreter = %d", resp.StatusCode)
	}
	for _, want := range []string{
		"Radio commands", "/help", "/weather", "/alert &lt;text&gt;",
		"everyone", "authorised only", "direct message",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("command section missing %q", want)
		}
	}

	// The section follows the UI language like the rest of the manual.
	u, err := url.Parse(env.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	env.client.Jar.SetCookies(u, []*http.Cookie{{Name: "wf_lang", Value: "pl"}})
	resp, html = env.get("/help")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /help (pl) = %d", resp.StatusCode)
	}
	for _, want := range []string{"Komendy radiowe", "/alert &lt;treść&gt;", "Średnia regionalna", "dla wszystkich", "tylko uprawnieni"} {
		if !strings.Contains(html, want) {
			t.Errorf("Polish command section missing %q", want)
		}
	}
	if strings.Contains(html, "Region average") {
		t.Error("Polish command section leaked the English description")
	}
}
