package web_test

import (
	"net/http"
	"strings"
	"testing"
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
		"How it works", "EMCOM networks", "/emcom &lt;network-id&gt;",
		"New message", "Admin", "Alert RCB",
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
