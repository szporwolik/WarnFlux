package web_test

import (
	"net/http"
	"regexp"
	"strings"
	"testing"
)

// TestAccessPageFormsCarrySessionCSRF pins the merged access page: its
// users and groups panels are rendered with a sub-view ({{template "x"
// .Sub}}), so the CSRF token must live on that sub-view too — inside an
// invoked template {{$.CSRF}} resolves to the sub-view, not the page root.
// An empty token made every user/group save bounce back with the "session
// changed" flash.
func TestAccessPageFormsCarrySessionCSRF(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	resp, html := env.get("/access")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /access = %d", resp.StatusCode)
	}
	re := regexp.MustCompile(`name="csrf" value="([^"]*)"`)
	ms := re.FindAllStringSubmatch(html, -1)
	if len(ms) < 2 {
		t.Fatalf("CSRF fields on the access page = %d, want the sidebar and the panel forms", len(ms))
	}
	want := ms[0][1]
	if want == "" {
		t.Fatal("first CSRF field is empty")
	}
	for i, m := range ms {
		if m[1] != want {
			t.Errorf("CSRF field %d = %q, want the session token %q", i, m[1], want)
		}
	}
}

// TestAccessPanelsFollowPageLanguage pins that the users/groups panels
// render in the page language: the sub-views previously carried no Lang, so
// the panel stayed English on a Polish page.
func TestAccessPanelsFollowPageLanguage(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	if resp, _ := env.get("/lang/pl"); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("GET /lang/pl = %d", resp.StatusCode)
	}
	_, html := env.get("/access")
	if !strings.Contains(html, "Użytkownicy") {
		t.Error("access page users panel is not rendered in Polish")
	}
}
