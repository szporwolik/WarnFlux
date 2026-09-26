package web_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestLanguageSwitch pins the /lang/{code} endpoint: it stores the choice
// in a long-lived cookie, redirects back and rejects unknown codes.
func TestLanguageSwitch(t *testing.T) {
	env := newTestEnv(t)

	// Unsupported code: 404.
	resp, _ := env.get("/lang/de")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /lang/de = %d, want 404", resp.StatusCode)
	}

	// Valid code: 303 back to the requested page and the wf_lang cookie set.
	resp, _ = env.get("/lang/pl?next=%2Fusers")
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("GET /lang/pl = %d, want 303", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/users" {
		t.Fatalf("redirect target = %q, want /users", loc)
	}
	var cookie *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == "wf_lang" {
			cookie = c
		}
	}
	if cookie == nil || cookie.Value != "pl" {
		t.Fatalf("wf_lang cookie = %+v, want pl", cookie)
	}
	if cookie.Path != "/" || !cookie.HttpOnly || cookie.MaxAge <= 0 {
		t.Fatalf("wf_lang cookie flags = %+v", cookie)
	}
}

// TestBrowserLanguageDetection pins the Accept-Language fallback: a Polish
// browser gets the Polish page (html lang="pl") until a cookie overrides
// it.
func TestBrowserLanguageDetection(t *testing.T) {
	env := newTestEnv(t)

	req, err := http.NewRequest(http.MethodGet, env.srv.URL+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept-Language", "pl-PL,pl;q=0.9,en;q=0.8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / = %d", resp.StatusCode)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, `<html lang="pl">`) {
		t.Fatalf("home page not in Polish: %s", body[:imin(len(body), 400)])
	}
	if !strings.Contains(body, "Aktywne komunikaty") {
		t.Errorf("home page missing Polish heading")
	}
	if !strings.Contains(body, "PL</a>") {
		t.Errorf("home page missing language switch")
	}

	// An explicit cookie wins over the browser header.
	req2, _ := http.NewRequest(http.MethodGet, env.srv.URL+"/", nil)
	req2.Header.Set("Accept-Language", "pl-PL")
	req2.AddCookie(&http.Cookie{Name: "wf_lang", Value: "en"})
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	body2 := readBody(t, resp2)
	if !strings.Contains(body2, `<html lang="en">`) {
		t.Fatalf("cookie override failed: %s", body2[:imin(len(body2), 400)])
	}
}

func imin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}
