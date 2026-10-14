package web_test

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestEmailLoginKeepsTheAccountAccess pins the ticket: authenticating by the
// e-mail ADDRESS must not cost the account its own features. The session used
// to keep what was typed, while every authorization step and the account page
// look the user up by the canonical name — so a group member logging in by
// address reached neither the communications, the EMCOM panel nor their own
// account.
func TestEmailLoginKeepsTheAccountAccess(t *testing.T) {
	env := newTestEnv(t)
	env.login() // admin: build the operator and its authorizations
	ops, err := env.users.CreateGroup("ops")
	if err != nil {
		t.Fatal(err)
	}
	ember, err := env.users.CreateUser("sq9mup", "", "sq9mup@example.com", "", "member", "password123")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.users.SetUserGroups(ember.ID, []int64{ops.ID}); err != nil {
		t.Fatal(err)
	}
	slug := seedEmcomNetwork(t, env)
	gs, ok := env.users.(storage.GroupStore)
	if !ok {
		t.Fatal("fake users store is not a GroupStore")
	}
	if err := gs.SetEmcomNetworkGroups(context.Background(), slug, []int64{ops.ID}); err != nil {
		t.Fatal(err)
	}
	env.logout()

	// The same credentials through the two identifiers: the SAME access.
	access := func(login string) (map[string]int, string) {
		t.Helper()
		_, html := env.get("/login")
		csrf := extractCSRF(t, html)
		resp, _ := env.postForm("/login", url.Values{
			"csrf": {csrf}, "username": {login}, "password": {"password123"},
		})
		if resp.StatusCode != http.StatusSeeOther {
			t.Fatalf("login as %q = %d, want 303", login, resp.StatusCode)
		}
		out := map[string]int{}
		for _, path := range []string{"/account", "/compose", "/emcom", "/dashboard"} {
			out[path] = env.getStatus(path)
		}
		shown := env.usernameShown(t)
		env.logout()
		return out, shown
	}

	byName, nameShown := access("sq9mup")
	byEmail, emailShown := access("sq9mup@example.com")

	for path, want := range map[string]int{"/account": http.StatusOK, "/compose": http.StatusOK, "/emcom": http.StatusOK} {
		if got := byName[path]; got != want {
			t.Fatalf("login by username: GET %s = %d, want %d", path, got, want)
		}
		if got := byEmail[path]; got != want {
			t.Fatalf("login by e-mail: GET %s = %d, want %d (the session must carry the account, not the typed address)", path, got, want)
		}
	}
	if nameShown != "sq9mup" || emailShown != "sq9mup" {
		t.Fatalf("the session names the account as %q (username) / %q (e-mail), want %q",
			nameShown, emailShown, "sq9mup")
	}
}

// getStatus returns the status code of one GET without following redirects.
func (e *testEnv) getStatus(path string) int {
	e.t.Helper()
	resp, _ := e.get(path)
	return resp.StatusCode
}

// usernameShown reads the account name the panel renders in its account menu
// (`<strong>{{ .Username }}</strong>`) — the session's own view of who is
// signed in.
func (e *testEnv) usernameShown(t *testing.T) string {
	t.Helper()
	_, html := e.get("/dashboard")
	const marker = `<div class="user-menu-head">`
	start := strings.Index(html, marker)
	if start < 0 {
		t.Fatalf("the panel does not render its account menu: %.300s", html)
	}
	rest := html[start:]
	if open := strings.Index(rest, "<strong>"); open >= 0 {
		rest = rest[open+len("<strong>"):]
		if close := strings.Index(rest, "</strong>"); close >= 0 {
			return strings.TrimSpace(rest[:close])
		}
	}
	t.Fatalf("could not read the account name from the account menu: %.300s", html)
	return ""
}
