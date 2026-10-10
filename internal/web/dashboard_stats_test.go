package web_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
)

// TestDashboardStatsPane pins the small numbers strip under the System
// panel: registered users, groups and EMCOM networks, rendered for every
// signed-in user (the dashboard is shared).
func TestDashboardStatsPane(t *testing.T) {
	env := newTestEnv(t)
	if _, err := env.users.CreateGroup("ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := env.users.CreateGroup("hams"); err != nil {
		t.Fatal(err)
	}
	if _, err := env.users.CreateUser("alice", "", "", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}
	if _, err := env.users.CreateUser("bob", "", "", "", "emcom", "password123"); err != nil {
		t.Fatal(err)
	}
	env.state.AddOrUpdateInfo("local", "warnflux/info/emcom/emcom/sp9moa-emcom/emcom", state.InfoEntry{
		Source: "emcom", ProducerID: "emcom", Key: "sp9moa-emcom", Kind: "emcom",
		ReceivedAt: time.Now(), Payload: []byte(`{"schema_version":1,"network":"SP9MOA EMCOM","slug":"sp9moa-emcom","level":0}`),
	})

	// The fake directory seeds the admin row: 3 users, 2 groups, 1 network.
	env.login()
	resp, html := env.get("/dashboard")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard = %d", resp.StatusCode)
	}
	if !strings.Contains(html, `id="stats-section"`) {
		t.Fatalf("dashboard missing the stats pane: %.400s", html)
	}
	if !strings.Contains(html, `<span class="stat-value">3</span>`) {
		t.Errorf("stats pane missing the user count (3)")
	}
	if !strings.Contains(html, `<span class="stat-value">2</span>`) {
		t.Errorf("stats pane missing the group count (2)")
	}
	if !strings.Contains(html, `<span class="stat-value">1</span>`) {
		t.Errorf("stats pane missing the EMCOM network count (1)")
	}
}
