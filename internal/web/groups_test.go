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

func TestGroupsStaleCSRF(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	// A token from a previous session (e.g. the admin signed in again in
	// another tab) must not produce a bare 403: the admin is sent back
	// with a friendly flash so the rename can simply be retried.
	resp, _ := env.postForm("/groups", url.Values{"csrf": {"stale-token"}, "edit_id": {"1"}, "name": {"renamed"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("stale csrf = %d, want 303", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/groups?err=csrf" {
		t.Fatalf("stale csrf Location = %q, want /groups?err=csrf", loc)
	}

	_, html := env.get("/groups?err=csrf")
	if !strings.Contains(html, "session changed") {
		t.Fatalf("friendly flash missing after stale csrf: %s", html)
	}

	// The group was NOT renamed.
	_, html = env.get("/groups")
	if strings.Contains(html, "renamed") {
		t.Fatalf("group renamed despite stale csrf: %s", html)
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
	// moderate, compose at minor, emcom at severe, the road/incident
	// sources (gddkia, gios) specific too, an off cell ("") skipped.
	resp, _ = env.postForm("/groups/1/routing", url.Values{
		"csrf":                       {csrf},
		"cell:|logger-action":        {"severe"},
		"cell:rso|logger-action":     {"moderate"},
		"cell:compose|logger-action": {"minor"},
		"cell:emcom|logger-action":   {"severe"},
		"cell:gddkia|logger-action":  {"moderate"},
		"cell:gios|logger-action":    {"severe"},
		"cell:imgw|logger-action":    {""},
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
		{Source: "gddkia", ID: "logger-action", MinSeverity: "moderate"},
		{Source: "gios", ID: "logger-action", MinSeverity: "severe"},
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
		"other sources",
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
		"other sources",
		"WarnFlux messages",
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

// TestGroupRoutingIMGWGroup pins the grouped IMGW row: the matrix shows one
// line for the two IMGW plugins, saving it writes the threshold for both
// event sources, and a legacy divergence is preserved (not silently
// flattened) until the operator picks a value.
func TestGroupRoutingIMGWGroup(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	if _, err := env.users.CreateGroup("ops"); err != nil {
		t.Fatal(err)
	}
	csrf := env.csrfFromPage("/groups")

	// One posted IMGW cell lands as two assignments (meteo + hydro).
	resp, _ := env.postForm("/groups/1/routing", url.Values{
		"csrf":                    {csrf},
		"cell:imgw|logger-action": {"severe"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("grouped routing save = %d, want redirect", resp.StatusCode)
	}
	assertAssignments(t, env, map[storage.ChannelAssignment]bool{
		{Source: "imgw-meteo", ID: "logger-action", MinSeverity: "severe"}: true,
		{Source: "imgw-hydro", ID: "logger-action", MinSeverity: "severe"}: true,
	})

	// The grid renders exactly one IMGW row and no per-plugin rows.
	_, html := env.get("/groups/1/routing")
	if !strings.Contains(html, `name="cell:imgw|logger-action"`) {
		t.Errorf("grouped IMGW row missing: %s", html)
	}
	for _, gone := range []string{`name="cell:imgw-meteo|logger-action"`, `name="cell:imgw-hydro|logger-action"`} {
		if strings.Contains(html, gone) {
			t.Errorf("per-plugin row %q still rendered: %s", gone, html)
		}
	}

	// Legacy divergence (e.g. set before the rows were grouped) is shown as
	// "differs per source" instead of an arbitrary single value.
	if err := env.users.SetGroupRouting(1, []storage.ChannelAssignment{
		{Source: "imgw-meteo", ID: "logger-action", MinSeverity: "severe"},
		{Source: "imgw-hydro", ID: "logger-action", MinSeverity: "minor"},
	}); err != nil {
		t.Fatalf("SetGroupRouting: %v", err)
	}
	_, html = env.get("/groups/1/routing")
	if !strings.Contains(html, `value="mixed" selected`) {
		t.Errorf("divergent IMGW cell not marked as mixed: %s", html)
	}

	// Saving the mixed cell keeps both members untouched.
	resp, _ = env.postForm("/groups/1/routing", url.Values{
		"csrf":                    {csrf},
		"cell:imgw|logger-action": {"mixed"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("mixed routing save = %d, want redirect", resp.StatusCode)
	}
	assertAssignments(t, env, map[storage.ChannelAssignment]bool{
		{Source: "imgw-meteo", ID: "logger-action", MinSeverity: "severe"}: true,
		{Source: "imgw-hydro", ID: "logger-action", MinSeverity: "minor"}:  true,
	})

	// A concrete severity flattens both members again.
	resp, _ = env.postForm("/groups/1/routing", url.Values{
		"csrf":                    {csrf},
		"cell:imgw|logger-action": {"moderate"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("concrete routing save = %d, want redirect", resp.StatusCode)
	}
	assertAssignments(t, env, map[storage.ChannelAssignment]bool{
		{Source: "imgw-meteo", ID: "logger-action", MinSeverity: "moderate"}: true,
		{Source: "imgw-hydro", ID: "logger-action", MinSeverity: "moderate"}: true,
	})

	// "mixed" is only meaningful for a grouped row.
	resp, html = env.postForm("/groups/1/routing", url.Values{
		"csrf":                {csrf},
		"cell:|logger-action": {"mixed"},
	})
	if resp.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(html, "mixed severity") {
		t.Fatalf("mixed on single-source cell = %d %s", resp.StatusCode, html)
	}
}

// assertAssignments compares the stored routing matrix with the expected set.
func assertAssignments(t *testing.T, env *testEnv, want map[storage.ChannelAssignment]bool) {
	t.Helper()
	r, err := env.users.GroupRouting(1)
	if err != nil {
		t.Fatalf("GroupRouting: %v", err)
	}
	if len(r.Actions) != len(want) {
		t.Fatalf("actions = %+v, want %d cells", r.Actions, len(want))
	}
	for _, a := range r.Actions {
		if !want[a] {
			t.Fatalf("actions = %+v, unexpected %+v", r.Actions, a)
		}
	}
}
