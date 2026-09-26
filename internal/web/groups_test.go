package web_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

func TestGroupsCRUDFlow(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	csrf := env.csrfFromPage("/groups")

	// Create.
	resp, _ := env.postForm("/groups", url.Values{"csrf": {csrf}, "name": {"ops"}})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/groups" {
		t.Fatalf("create = %d %q, want redirect to /groups", resp.StatusCode, resp.Header.Get("Location"))
	}
	_, html := env.get("/groups")
	if !strings.Contains(html, "ops") || !strings.Contains(html, "Members") {
		t.Fatalf("created group not listed: %s", html)
	}

	// Duplicate name rejected.
	resp, html = env.postForm("/groups", url.Values{"csrf": {csrf}, "name": {"ops"}})
	if resp.StatusCode != http.StatusConflict || !strings.Contains(html, "already exists") {
		t.Fatalf("duplicate = %d %s", resp.StatusCode, html)
	}

	// Edit prefills the form.
	_, html = env.get("/groups?edit=1")
	if !strings.Contains(html, `value="ops"`) || !strings.Contains(html, `name="edit_id" value="1"`) {
		t.Fatalf("edit prefill missing: %s", html)
	}

	// Rename.
	resp, _ = env.postForm("/groups", url.Values{"csrf": {csrf}, "edit_id": {"1"}, "name": {"operations"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("update = %d, want redirect", resp.StatusCode)
	}
	_, html = env.get("/groups")
	if !strings.Contains(html, "operations") {
		t.Fatalf("renamed group not listed: %s", html)
	}

	// Delete.
	csrf = env.csrfFromPage("/groups")
	resp, _ = env.postForm("/groups/1/delete", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("delete = %d, want redirect", resp.StatusCode)
	}
	_, html = env.get("/groups")
	if strings.Contains(html, "operations") {
		t.Fatalf("deleted group still listed: %s", html)
	}
}

func TestUserGroupAssignment(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	// Two groups and one regular user.
	if _, err := env.users.CreateGroup("ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := env.users.CreateGroup("news"); err != nil {
		t.Fatal(err)
	}
	if _, err := env.users.CreateUser("alice", "", "alice@example.com", "", "", ""); err != nil {
		t.Fatal(err)
	}

	// Assign alice (ID 2: admin is 1) to both groups.
	csrf := env.csrfFromPage("/users")
	resp, _ := env.postForm("/users/2/prefs?page=1", url.Values{"csrf": {csrf}, "groups": {"1", "2"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("assign = %d, want redirect", resp.StatusCode)
	}

	// The users page shows the chips for alice.
	_, html := env.get("/users")
	if !strings.Contains(html, `class="group-chip">ops</span>`) || !strings.Contains(html, `class="group-chip">news</span>`) {
		t.Fatalf("alice group chips missing: %s", html)
	}

	// The groups page shows member counts.
	_, html = env.get("/groups")
	if !strings.Contains(html, "news") {
		t.Fatalf("groups listing broken: %s", html)
	}

	// Clearing: no groups selected removes alice's chips.
	resp, _ = env.postForm("/users/2/prefs?page=1", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("clear = %d, want redirect", resp.StatusCode)
	}
	_, html = env.get("/users")
	if strings.Contains(html, `class="group-chip">ops</span>`) || strings.Contains(html, `class="group-chip">news</span>`) {
		t.Fatalf("group chips still present after clearing: %s", html)
	}
}

func TestGroupsPageRequiresLogin(t *testing.T) {
	env := newTestEnv(t)
	resp, _ := env.get("/groups")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /groups unauthenticated = %d %q, want redirect to /login", resp.StatusCode, resp.Header.Get("Location"))
	}
}

func TestGroupRoutingMatrix(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	if _, err := env.users.CreateGroup("ops"); err != nil {
		t.Fatal(err)
	}
	csrf := env.csrfFromPage("/groups")

	// Unknown action ID rejected (never silently stored).
	resp, _ := env.postForm("/groups/1/routing", url.Values{"csrf": {csrf}, "cell:|nope": {"severe"}})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("unknown action = %d, want 422", resp.StatusCode)
	}

	// Unknown source rejected.
	resp, _ = env.postForm("/groups/1/routing", url.Values{"csrf": {csrf}, "cell:bogus|logger-action": {"severe"}})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("unknown source = %d, want 422", resp.StatusCode)
	}

	// Disabled actions are not assignable.
	resp, _ = env.postForm("/groups/1/routing", url.Values{"csrf": {csrf}, "cell:|logger-off": {"severe"}})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("disabled action = %d, want 422", resp.StatusCode)
	}

	// Invalid per-cell severity rejected.
	resp, html := env.postForm("/groups/1/routing", url.Values{
		"csrf":                   {csrf},
		"cell:|logger-action":    {"orange"},
		"cell:rso|logger-action": {"severe"},
	})
	if resp.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(html, "invalid severity") {
		t.Fatalf("invalid channel severity = %d %s", resp.StatusCode, html)
	}

	// Valid matrix: any-source fallback at severe, rso-specific at
	// moderate, compose at minor, emcom at severe, an off cell ("")
	// skipped.
	resp, _ = env.postForm("/groups/1/routing", url.Values{
		"csrf":                          {csrf},
		"cell:|logger-action":           {"severe"},
		"cell:rso|logger-action":        {"moderate"},
		"cell:compose|logger-action":    {"minor"},
		"cell:emcom|logger-action":      {"severe"},
		"cell:imgw-meteo|logger-action": {""},
	})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/groups" {
		t.Fatalf("routing save = %d %q, want redirect to /groups", resp.StatusCode, resp.Header.Get("Location"))
	}

	r, err := env.users.GroupRouting(1)
	if err != nil {
		t.Fatalf("GroupRouting: %v", err)
	}
	got := map[storage.ChannelAssignment]bool{}
	for _, a := range r.Actions {
		got[a] = true
	}
	want := []storage.ChannelAssignment{
		{ID: "logger-action", MinSeverity: "severe"},
		{Source: "rso", ID: "logger-action", MinSeverity: "moderate"},
		{Source: "compose", ID: "logger-action", MinSeverity: "minor"},
		{Source: "emcom", ID: "logger-action", MinSeverity: "severe"},
	}
	if len(r.Actions) != len(want) {
		t.Fatalf("actions = %+v, want %+v", r.Actions, want)
	}
	for _, w := range want {
		if !got[w] {
			t.Fatalf("actions = %+v, missing %+v", r.Actions, w)
		}
	}

	// The groups table renders the assignment chips; the full matrix with
	// its severity selects lives on the dedicated routing page.
	_, html = env.get("/groups")
	for _, wantStr := range []string{
		"logger-action",
		"any",
		"rso",
		"compose",
		"emcom",
	} {
		if !strings.Contains(html, wantStr) {
			t.Errorf("groups page missing %q: %s", wantStr, html)
		}
	}
	_, html = env.get("/groups/1/routing")
	for _, wantStr := range []string{
		"Routing · ops",
		`name="cell:|logger-action"`,
		`name="cell:rso|logger-action"`,
		`name="cell:compose|logger-action"`,
		`name="cell:emcom|logger-action"`,
		"severe or higher",
		"moderate or higher",
		"minor or higher",
	} {
		if !strings.Contains(html, wantStr) {
			t.Errorf("routing page missing %q: %s", wantStr, html)
		}
	}

	// Clearing the grid (no cell posted) empties the matrix.
	resp, _ = env.postForm("/groups/1/routing", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("clear routing = %d, want redirect", resp.StatusCode)
	}
	r, err = env.users.GroupRouting(1)
	if err != nil {
		t.Fatalf("GroupRouting after clear: %v", err)
	}
	if len(r.Actions) != 0 {
		t.Fatalf("routing after clear = %+v", r)
	}
}
