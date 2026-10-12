package web_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

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
	if resp, _ = env.get("/mass/progress?job=x"); resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
		t.Fatalf("member GET /mass/progress = %d %q, want 303 /dashboard", resp.StatusCode, resp.Header.Get("Location"))
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

	// A valid send starts a background job and redirects to its progress
	// page (no transport is attached, so it completes with the zero-count
	// summary: alice plus the admin, who belongs to ops in the fake
	// directory).
	resp, _ = env.postForm("/mass", url.Values{
		"csrf": {csrf}, "message": {"hello"}, "channels": {"aprs", "email"},
		"groups": {idStr(g1.ID)}, "users": {idStr(alice.ID)},
	})
	loc := resp.Header.Get("Location")
	if resp.StatusCode != http.StatusSeeOther || !strings.HasPrefix(loc, "/mass?job=") {
		t.Fatalf("valid send = %d %q, want 303 /mass?job=…", resp.StatusCode, loc)
	}

	// The progress fragment ends with the summary once the job is done.
	job := strings.TrimPrefix(loc, "/mass?job=")
	var progress string
	deadline := time.Now().Add(5 * time.Second)
	for {
		pr, ph := env.get("/mass/progress?job=" + url.QueryEscape(job))
		if pr.StatusCode != http.StatusOK {
			t.Fatalf("GET /mass/progress = %d", pr.StatusCode)
		}
		progress = ph
		if strings.Contains(ph, `data-done="1"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job did not finish; last fragment: %s", ph)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(progress, "Sent to 2 users — APRS 0, SMS 0, e-mail 0, Discord 0, Meshtastic 0.") {
		t.Errorf("progress summary missing: %s", progress)
	}
	// The channel chips are rendered (selected channels only).
	if !strings.Contains(progress, "APRS") || !strings.Contains(progress, "E-mail") {
		t.Errorf("progress missing channel chips: %s", progress)
	}

	// Reopening the progress page keeps the finished panel (the summary is
	// shown without polling).
	_, page := env.get("/mass?job=" + url.QueryEscape(job))
	if !strings.Contains(page, `id="mass-progress"`) || !strings.Contains(page, "Sent to 2 users") {
		t.Errorf("finished job not shown on the page: %.400s", page)
	}

	// An unknown job id is a 404 for the poller.
	if pr, _ := env.get("/mass/progress?job=nope"); pr.StatusCode != http.StatusNotFound {
		t.Errorf("unknown job = %d, want 404", pr.StatusCode)
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

// TestMassLongKind pins the long-message mode of the Mass info composer:
// the kind selector drives the character budget and the channel list, the
// server refuses a channel that cannot carry a long notice (instead of
// silently dropping it) and accepts a full 1000-character markdown
// bulletin.
func TestMassLongKind(t *testing.T) {
	env := newTestEnv(t)
	if _, err := env.users.CreateUser("alice", "", "alice@example.com", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}
	alice, err := env.users.GetUserByUsername("alice")
	if err != nil {
		t.Fatal(err)
	}
	env.login()
	resp, html := env.get("/mass")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /mass = %d", resp.StatusCode)
	}
	if !strings.Contains(html, `<select id="mass-kind" name="kind"`) {
		t.Errorf("mass page missing the message kind selector")
	}
	if !strings.Contains(html, `<textarea id="mass-message" name="message"`) {
		t.Errorf("mass page missing the message textarea")
	}
	// The character budget and the channel rule travel with the markup, so
	// the client-side switch and the server agree.
	for _, want := range []string{
		`data-char-limits="short:50 long:1000"`,
		`data-kinds="short long"`,
		`data-kinds="short"`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("mass page missing %q", want)
		}
	}
	csrf := extractCSRF(t, html)
	target := url.Values{"users": {idStr(alice.ID)}}

	// A short notice still tops out at 50 characters.
	req := url.Values{"csrf": {csrf}, "kind": {"short"}, "message": {strings.Repeat("x", 51)}, "channels": {"aprs"}}
	resp, html = env.postForm("/mass", mergeValues(req, target))
	if resp.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(html, "longer than 50 characters") {
		t.Fatalf("51-char short message = %d, want 422 with the 50-character limit", resp.StatusCode)
	}

	// A long one takes 1000 and refuses 1001.
	long := strings.Repeat("x", 1000)
	req = url.Values{"csrf": {csrf}, "kind": {"long"}, "message": {long}, "channels": {"email"}}
	resp, html = env.postForm("/mass", mergeValues(req, target))
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("1000-char long message = %d, want 303 (error: %s)", resp.StatusCode, html)
	}
	req = url.Values{"csrf": {csrf}, "kind": {"long"}, "message": {long + "x"}, "channels": {"email"}}
	resp, html = env.postForm("/mass", mergeValues(req, target))
	if resp.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(html, "longer than 1000 characters") {
		t.Fatalf("1001-char long message = %d, want 422 with the 1000-character limit", resp.StatusCode)
	}

	// A channel that cannot carry a long notice is an error, not a silent
	// omission: the operator has to see that part of the audience would be
	// skipped.
	req = url.Values{"csrf": {csrf}, "kind": {"long"}, "message": {"Bulletin"}, "channels": {"aprs", "email"}}
	resp, html = env.postForm("/mass", mergeValues(req, target))
	if resp.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(html, "e-mail or Discord only") {
		t.Fatalf("long over APRS = %d, want 422 with the channel-kind error", resp.StatusCode)
	}
	if !strings.Contains(html, `<option value="long" selected>`) {
		t.Error("the re-rendered form lost the long kind")
	}

	// An unknown kind is rejected instead of being guessed.
	req = url.Values{"csrf": {csrf}, "kind": {"huge"}, "message": {"Hallo"}, "channels": {"email"}}
	resp, html = env.postForm("/mass", mergeValues(req, target))
	if resp.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(html, "Unknown message kind") {
		t.Fatalf("unknown kind = %d, want 422", resp.StatusCode)
	}
}

// mergeValues returns a copy of a with b's entries added.
func mergeValues(a, b url.Values) url.Values {
	out := url.Values{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}
