package web_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
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
	if _, err := env.users.CreateUser("alice", "", "alice@example.com", ""); err != nil {
		t.Fatal(err)
	}

	// Assign alice (ID 2: admin is 1) to both groups.
	csrf := env.csrfFromPage("/users")
	resp, _ := env.postForm("/users/2/groups?page=1", url.Values{"csrf": {csrf}, "groups": {"1", "2"}})
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

	// Clearing: no groups selected removes all chips.
	resp, _ = env.postForm("/users/2/groups?page=1", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("clear = %d, want redirect", resp.StatusCode)
	}
	_, html = env.get("/users")
	if strings.Contains(html, "group-chip") {
		t.Fatalf("chips still present after clearing: %s", html)
	}
}

func TestGroupsPageRequiresLogin(t *testing.T) {
	env := newTestEnv(t)
	resp, _ := env.get("/groups")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /groups unauthenticated = %d %q, want redirect to /login", resp.StatusCode, resp.Header.Get("Location"))
	}
}

func TestGroupRoutingAssignment(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	if _, err := env.users.CreateGroup("ops"); err != nil {
		t.Fatal(err)
	}
	csrf := env.csrfFromPage("/groups")

	// Invalid severity rejected.
	resp, html := env.postForm("/groups/1/routing", url.Values{"csrf": {csrf}, "min_severity": {"orange"}})
	if resp.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(html, "invalid minimum severity") {
		t.Fatalf("invalid severity = %d %s", resp.StatusCode, html)
	}

	// Unknown channel ID rejected (never silently stored).
	resp, _ = env.postForm("/groups/1/routing", url.Values{"csrf": {csrf}, "min_severity": {"severe"}, "actions": {"nope"}})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("unknown action = %d, want 422", resp.StatusCode)
	}

	// Disabled actions are not assignable.
	resp, _ = env.postForm("/groups/1/routing", url.Values{"csrf": {csrf}, "min_severity": {"severe"}, "actions": {"logger-off"}})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("disabled action = %d, want 422", resp.StatusCode)
	}

	// Valid assignment with one action and one output.
	resp, _ = env.postForm("/groups/1/routing", url.Values{
		"csrf":         {csrf},
		"min_severity": {"severe"},
		"actions":      {"logger-action"},
		"outputs":      {"mqtt-main"},
	})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/groups" {
		t.Fatalf("routing save = %d %q, want redirect to /groups", resp.StatusCode, resp.Header.Get("Location"))
	}

	r, err := env.users.GroupRouting(1)
	if err != nil {
		t.Fatalf("GroupRouting: %v", err)
	}
	if r.MinSeverity != "severe" || len(r.Actions) != 1 || r.Actions[0] != "logger-action" ||
		len(r.Outputs) != 1 || r.Outputs[0] != "mqtt-main" {
		t.Fatalf("routing = %+v", r)
	}

	// The page renders the threshold and channel counts, and the popover
	// prefills the saved selection.
	_, html = env.get("/groups")
	for _, want := range []string{"severe", "1 actions · 1 outputs", `value="logger-action"`, `name="min_severity"`} {
		if !strings.Contains(html, want) {
			t.Errorf("groups page missing %q: %s", want, html)
		}
	}

	// Clearing everything resets the threshold to permissive.
	resp, _ = env.postForm("/groups/1/routing", url.Values{"csrf": {csrf}, "min_severity": {"unknown"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("clear routing = %d, want redirect", resp.StatusCode)
	}
	r, err = env.users.GroupRouting(1)
	if err != nil {
		t.Fatalf("GroupRouting after clear: %v", err)
	}
	if r.MinSeverity != "unknown" || len(r.Actions) != 0 || len(r.Outputs) != 0 {
		t.Fatalf("routing after clear = %+v", r)
	}
}
