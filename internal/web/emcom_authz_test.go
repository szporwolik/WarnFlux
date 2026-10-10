package web_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// seedEmcomNetwork adds one EMCOM network through the admin flow and
// mirrors the broker loopback into the runtime state (exactly like the
// production receiver would).
func seedEmcomNetwork(t *testing.T, env *testEnv) string {
	t.Helper()
	resp, html := env.get("/config")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /config = %d", resp.StatusCode)
	}
	csrf := extractCSRF(t, html)
	resp, _ = env.postForm("/config/emcom/add", url.Values{"csrf": {csrf}, "name": {"SP9MOA EMCOM"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/emcom/add = %d, want 303", resp.StatusCode)
	}
	raw := env.pub.rawSnapshot()
	if len(raw) != 1 {
		t.Fatalf("raw publish = %+v, want 1 info payload", raw)
	}
	env.state.AddOrUpdateInfo("local", "warnflux/info/emcom/emcom/sp9moa-emcom/emcom", state.InfoEntry{
		Source: "emcom", ProducerID: "emcom", Key: "sp9moa-emcom", Kind: "emcom",
		ReceivedAt: time.Now(), Payload: raw[0].Payload,
	})
	return "sp9moa-emcom"
}

// TestEmcomGroupAssignment pins the admin-only per-network group
// assignment: the Config page renders one checkbox row per group, the
// POST persists the assignment and the redirect flashes confirmation.
func TestEmcomGroupAssignment(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	ops, err := env.users.CreateGroup("ops")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.users.CreateGroup("hams"); err != nil {
		t.Fatal(err)
	}
	slug := seedEmcomNetwork(t, env)

	// The network renders with its group form: both groups as
	// checkboxes, none assigned yet.
	resp, html := env.get("/config")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /config = %d", resp.StatusCode)
	}
	if !strings.Contains(html, `action="/config/emcom/`+slug+`/groups"`) {
		t.Fatalf("config page missing the group-assignment form: %s", html)
	}
	for _, g := range []string{"ops", "hams"} {
		if !strings.Contains(html, `name="groups"`) || !strings.Contains(html, g) {
			t.Fatalf("config page missing group checkbox %s: %s", g, html)
		}
	}
	if strings.Contains(html, `name="groups" value="`+idStr(ops.ID)+`" checked`) {
		t.Fatal("no group must be pre-assigned")
	}

	// Assign ops to the network: persisted through the store.
	csrf := extractCSRF(t, html)
	resp, _ = env.postForm("/config/emcom/"+slug+"/groups", url.Values{
		"csrf": {csrf}, "groups": {idStr(ops.ID)},
	})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/config?msg=emcom-groups" {
		t.Fatalf("POST groups = %d %q, want 303 to the flash", resp.StatusCode, resp.Header.Get("Location"))
	}
	gs, ok := env.users.(storage.GroupStore)
	if !ok {
		t.Fatal("fake users store is not a GroupStore")
	}
	ids, err := gs.EmcomNetworkGroups(context.Background(), slug)
	if err != nil || len(ids) != 1 || ids[0] != ops.ID {
		t.Fatalf("assigned groups = %v, %v; want [%d]", ids, err, ops.ID)
	}

	// The flash renders and the checkbox comes back checked.
	resp, html = env.get("/config?msg=emcom-groups")
	if resp.StatusCode != http.StatusOK || !strings.Contains(html, "saved") {
		t.Fatalf("GET /config flash = %d, %s", resp.StatusCode, html)
	}
	if !strings.Contains(html, `name="groups" value="`+idStr(ops.ID)+`" checked`) {
		t.Fatalf("assigned group must render checked: %s", html)
	}

	// Assignment is replaced, never merged: posting no groups clears it.
	_, html = env.get("/config")
	csrf = extractCSRF(t, html)
	resp, _ = env.postForm("/config/emcom/"+slug+"/groups", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("clear groups = %d, want 303", resp.StatusCode)
	}
	ids, err = gs.EmcomNetworkGroups(context.Background(), slug)
	if err != nil || len(ids) != 0 {
		t.Fatalf("cleared groups = %v, %v; want none", ids, err)
	}
}

// TestEmcomLevelAuthz pins the group authorization on the panel flow:
// an emcom operator outside the assigned groups gets a 403 with the
// explanation, and after the admin assigns them to the network's group
// the same POST succeeds.
func TestEmcomLevelAuthz(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	ops, err := env.users.CreateGroup("ops")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.users.CreateUser("emcom-user", "", "", "", "emcom", "password123"); err != nil {
		t.Fatal(err)
	}
	slug := seedEmcomNetwork(t, env)
	env.logout()

	// Log in as the operator: not a member of any assigned group yet.
	_, html := env.get("/login")
	csrf := extractCSRF(t, html)
	resp, _ := env.postForm("/login", url.Values{
		"csrf": {csrf}, "username": {"emcom-user"}, "password": {"password123"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("operator login = %d, want 303", resp.StatusCode)
	}
	resp, html = env.get("/emcom")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /emcom as operator = %d", resp.StatusCode)
	}
	if !strings.Contains(html, "No networks are authorized for you") {
		t.Fatalf("unauthorized operator must see the hint: %s", html)
	}
	if strings.Contains(html, `<article class="emcom-card">`) {
		t.Fatalf("unauthorized operator must not see network cards: %s", html)
	}
	csrf = extractCSRF(t, html)

	// Level change denied: 403 with the emcom page (not a redirect).
	resp, html = env.postForm("/emcom/"+slug+"/level", url.Values{"csrf": {csrf}, "level": {"1"}})
	if resp.StatusCode != http.StatusForbidden || !strings.Contains(html, "EMCOM networks") {
		t.Fatalf("unauthorized level POST = %d, want 403 with the emcom page", resp.StatusCode)
	}
	env.logout()

	// The admin assigns the operator's group to the network and puts the
	// operator into it.
	env.login()
	u, err := env.users.GetUserByUsername("emcom-user")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.users.SetUserGroups(u.ID, []int64{ops.ID}); err != nil {
		t.Fatal(err)
	}
	gs, ok := env.users.(storage.GroupStore)
	if !ok {
		t.Fatal("fake users store is not a GroupStore")
	}
	if err := gs.SetEmcomNetworkGroups(context.Background(), slug, []int64{ops.ID}); err != nil {
		t.Fatal(err)
	}
	env.logout()

	// The operator now passes the gate.
	_, html = env.get("/login")
	csrf = extractCSRF(t, html)
	resp, _ = env.postForm("/login", url.Values{
		"csrf": {csrf}, "username": {"emcom-user"}, "password": {"password123"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("operator re-login = %d, want 303", resp.StatusCode)
	}
	resp, html = env.get("/emcom")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /emcom after assignment = %d", resp.StatusCode)
	}
	if !strings.Contains(html, `<article class="emcom-card">`) || !strings.Contains(html, `name="level"`) {
		t.Fatalf("authorized operator must see the network card with the slider: %s", html)
	}
	csrf = extractCSRF(t, html)
	resp, _ = env.postForm("/emcom/"+slug+"/level", url.Values{"csrf": {csrf}, "level": {"1"}})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/emcom?msg=level" {
		t.Fatalf("authorized level POST = %d %q, want 303 to the level flash", resp.StatusCode, resp.Header.Get("Location"))
	}
}

// idStr formats an int64 id for the template markup assertions.
func idStr(id int64) string {
	return fmt.Sprint(id)
}

// TestEmcomNavVisibility pins the sidebar rule: the admin always sees
// the EMCOM entry; an emcom operator sees it only while they may change
// the level of at least one network (assigned group membership).
func TestEmcomNavVisibility(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	// Admin: the entry is always there.
	resp, html := env.get("/dashboard")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard as admin = %d", resp.StatusCode)
	}
	if !strings.Contains(html, `href="/emcom"`) {
		t.Fatalf("admin sidebar missing the EMCOM entry: %s", html)
	}

	// An operator with no authorized network must not see it.
	if _, err := env.users.CreateUser("ops-user", "", "", "", "emcom", "password123"); err != nil {
		t.Fatal(err)
	}
	env.logout()
	_, html = env.get("/login")
	csrf := extractCSRF(t, html)
	resp, _ = env.postForm("/login", url.Values{
		"csrf": {csrf}, "username": {"ops-user"}, "password": {"password123"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("operator login = %d, want 303", resp.StatusCode)
	}
	resp, html = env.get("/dashboard")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard as operator = %d", resp.StatusCode)
	}
	if strings.Contains(html, `href="/emcom"`) {
		t.Fatalf("unauthorized operator must not see the EMCOM entry: %s", html)
	}

	// The admin creates a network and authorizes the operator's group:
	// the entry appears.
	env.logout()
	env.login()
	ops, err := env.users.CreateGroup("ops")
	if err != nil {
		t.Fatal(err)
	}
	slug := seedEmcomNetwork(t, env)
	u, err := env.users.GetUserByUsername("ops-user")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.users.SetUserGroups(u.ID, []int64{ops.ID}); err != nil {
		t.Fatal(err)
	}
	gs, ok := env.users.(storage.GroupStore)
	if !ok {
		t.Fatal("fake users store is not a GroupStore")
	}
	if err := gs.SetEmcomNetworkGroups(context.Background(), slug, []int64{ops.ID}); err != nil {
		t.Fatal(err)
	}
	env.logout()

	_, html = env.get("/login")
	csrf = extractCSRF(t, html)
	resp, _ = env.postForm("/login", url.Values{
		"csrf": {csrf}, "username": {"ops-user"}, "password": {"password123"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("operator re-login = %d, want 303", resp.StatusCode)
	}
	resp, html = env.get("/dashboard")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard after assignment = %d", resp.StatusCode)
	}
	if !strings.Contains(html, `href="/emcom"`) {
		t.Fatalf("authorized operator sidebar missing the EMCOM entry: %s", html)
	}
}
