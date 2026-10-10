package web_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestMassInfoPage pins the admin-only Mass info page: form rendering,
// validation errors and the access gate.
func TestMassInfoPage(t *testing.T) {
	env := newTestEnv(t)
	g1, err := env.users.CreateGroup("ops")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.users.CreateUser("alice", "", "alice@example.com", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}
	alice, err := env.users.GetUserByUsername("alice")
	if err != nil {
		t.Fatal(err)
	}
	_ = g1
	_ = alice

	// Non-admin sessions never reach the page.
	if _, err := env.users.CreateUser("member1", "", "", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}
	_, html := env.get("/login")
	resp, _ := env.postForm("/login", map[string][]string{
		"csrf": {extractCSRF(t, html)}, "username": {"member1"}, "password": {"password123"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("member login = %d", resp.StatusCode)
	}
	resp, _ = env.get("/mass")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
		t.Fatalf("member GET /mass = %d %q, want 303 /dashboard", resp.StatusCode, resp.Header.Get("Location"))
	}
	env.logout()

	env.login()
	resp, html = env.get("/mass")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /mass = %d", resp.StatusCode)
	}
	for _, want := range []string{`name="message"`, `name="groups"`, `name="users"`, `name="networks"`, `name="channels"`} {
		if !strings.Contains(html, want) {
			t.Errorf("mass page missing %s", want)
		}
	}
	if !strings.Contains(html, `<span class="nav-label">Mass info</span>`) {
		t.Error("admin sidebar missing the Mass info entry")
	}
	csrf := extractCSRF(t, html)

	// Validation: empty message, overlong message, no channels, no targets.
	resp, html = env.postForm("/mass", url.Values{"csrf": {csrf}, "channels": {"aprs"}})
	if resp.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(html, "Write the message first") {
		t.Fatalf("empty message = %d, want 422 with the error", resp.StatusCode)
	}
	resp, _ = env.postForm("/mass", url.Values{"csrf": {csrf}, "message": {strings.Repeat("x", 51)}, "channels": {"aprs"}})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("51-char message = %d, want 422", resp.StatusCode)
	}
	resp, _ = env.postForm("/mass", url.Values{"csrf": {csrf}, "message": {"hello"}})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("no channels = %d, want 422", resp.StatusCode)
	}
	resp, _ = env.postForm("/mass", url.Values{"csrf": {csrf}, "message": {"hello"}, "channels": {"aprs"}})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("no targets = %d, want 422", resp.StatusCode)
	}

	// A validation error keeps the operator's picks: the re-rendered form
	// marks the submitted groups, users and channels as checked.
	resp, html = env.postForm("/mass", url.Values{
		"csrf": {csrf}, "message": {strings.Repeat("x", 51)},
		"groups": {idStr(g1.ID)}, "users": {idStr(alice.ID)},
		"channels": {"aprs", "sms"},
	})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("overlong message with picks = %d, want 422", resp.StatusCode)
	}
	for _, want := range []string{
		fmt.Sprintf(`name="groups" value="%d" checked`, g1.ID),
		fmt.Sprintf(`name="users" value="%d" checked`, alice.ID),
		`name="channels" value="aprs" checked`,
		`name="channels" value="sms" checked`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("form lost the submitted selection %q", want)
		}
	}

	// A valid send with no attached transports still redirects with the
	// zero-count flash (recipients were resolved and deduped: alice plus
	// the admin, who belongs to ops in the fake directory).
	resp, _ = env.postForm("/mass", url.Values{
		"csrf": {csrf}, "message": {"hello"}, "channels": {"aprs", "email"},
		"groups": {idStr(g1.ID)}, "users": {idStr(alice.ID)},
	})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/mass?msg="+url.QueryEscape(
		"Sent to 2 users — APRS 0, SMS 0, e-mail 0, Discord 0, Meshtastic 0.") {
		t.Fatalf("valid send = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
}

// TestMassRecipientsDedup pins the recipient resolution: every user is
// delivered once across overlapping groups, networks and direct picks.
func TestMassRecipientsDedup(t *testing.T) {
	env := newTestEnv(t)
	g1, err := env.users.CreateGroup("ops")
	if err != nil {
		t.Fatal(err)
	}
	g2, err := env.users.CreateGroup("hams")
	if err != nil {
		t.Fatal(err)
	}
	mk := func(name string, groups ...int64) int64 {
		t.Helper()
		if _, err := env.users.CreateUser(name, "", "", "", "member", ""); err != nil {
			t.Fatal(err)
		}
		u, err := env.users.GetUserByUsername(name)
		if err != nil {
			t.Fatal(err)
		}
		if len(groups) > 0 {
			if err := env.users.SetUserGroups(u.ID, groups); err != nil {
				t.Fatal(err)
			}
		}
		return u.ID
	}
	u1 := mk("u1", g1.ID)
	u2 := mk("u2", g1.ID, g2.ID)
	u3 := mk("u3", g2.ID)

	// Both groups cover u1, u2 (twice) and u3 — exactly three users.
	got := env.server.MassRecipientsForTest(context.Background(), nil, []int64{g1.ID, g2.ID}, nil)
	if len(got) != 3 || got[0].ID != u1 || got[1].ID != u2 || got[2].ID != u3 {
		t.Fatalf("group dedup = %+v", got)
	}

	// The network scope adds the members of its assigned groups; a
	// direct user pick merges in without duplicates.
	gs, ok := env.users.(storage.GroupStore)
	if !ok {
		t.Fatal("fake users store is not a GroupStore")
	}
	if err := gs.SetEmcomNetworkGroups(context.Background(), "net-a", []int64{g2.ID}); err != nil {
		t.Fatal(err)
	}
	got = env.server.MassRecipientsForTest(context.Background(), []int64{u1}, nil, []string{"net-a"})
	if len(got) != 3 || got[0].ID != u1 || got[1].ID != u2 || got[2].ID != u3 {
		t.Fatalf("network+user dedup = %+v", got)
	}
}
