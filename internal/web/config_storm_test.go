package web_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/config"
)

// stormProvider is a stub of the burze.dzis.net directory: it knows
// Niepołomice and Kraków, answers 0/0 for anything else (which is what the
// real service does, and what would otherwise make the warnings endpoint
// return its sample data), and can be switched into refusing mode.
type stormProvider struct {
	mu      sync.Mutex
	refuse  bool
	queried []string
}

func (p *stormProvider) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		text := string(body)
		p.mu.Lock()
		p.queried = append(p.queried, text)
		refuse := p.refuse
		p.mu.Unlock()
		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		if refuse {
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Body><SOAP-ENV:Fault><faultcode>SOAP-ENV:Server</faultcode><faultstring>Wymagane uwierzytelnienie / Authentication required</faultstring></SOAP-ENV:Fault></SOAP-ENV:Body></SOAP-ENV:Envelope>`)
			return
		}
		lat, lon := "0", "0"
		switch {
		case strings.Contains(text, "<nazwa>Niepołomice</nazwa>"):
			lat, lon = "50.02", "20.13"
		case strings.Contains(text, "<nazwa>Kraków</nazwa>"):
			lat, lon = "50.03", "19.57"
		}
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Body>`+
			`<ns1:miejscowoscResponse xmlns:ns1="https://burze.dzis.net/soap.php"><return><y>`+lat+`</y><x>`+lon+`</x></return>`+
			`</ns1:miejscowoscResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// stormPanelConfig writes a configuration file with one burze.dzis.net
// source instance pointed at the stub directory and registers it on the
// test server.
func stormPanelConfig(t *testing.T, env *testEnv, baseURL string, locations ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := `web:
  enabled: true
  auth:
    username: admin
    password: secret123
sources:
  - id: burze-main
    type: burzedzis
    enabled: true
    config:
      api_key: test-key
      base_url: ` + baseURL + `
      locations: [` + strings.Join(locations, ", ") + `]
      storm_tracking: true
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	env.server.SetConfigFile(path)
	return path
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestConfigStormSection pins the storm-localities section: it lists what
// the FILE holds (not what the running process holds), and saving writes the
// list back as a block sequence.
func TestConfigStormSection(t *testing.T) {
	provider := &stormProvider{}
	srv := provider.start(t)
	env := newTestEnv(t)
	env.login()
	path := stormPanelConfig(t, env, srv.URL, "Niepołomice")
	csrf := env.csrfFromPage("/config")

	_, page := env.get("/config")
	for _, want := range []string{"cfg-storm", "burze-main (burzedzis)", `name="locations_0"`, "Niepołomice"} {
		if !strings.Contains(page, want) {
			t.Errorf("the storm section is missing %q", want)
		}
	}

	resp, _ := env.postForm("/config/storm", url.Values{
		"csrf":        {csrf},
		"path_0":      {"sources[0].config.locations"},
		"locations_0": {"Niepołomice, Kraków"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/storm = %d, want 303", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/config?msg=storm" {
		t.Errorf("redirect = %q, want the saved flash", loc)
	}

	got, ok := config.StringListAt(path, "sources[0].config.locations")
	if !ok || len(got) != 2 || got[1] != "Kraków" {
		t.Fatalf("written list = %v (ok=%v)", got, ok)
	}
	text := readFile(t, path)
	if !strings.Contains(text, "      locations:\n        - Niepołomice\n        - Kraków\n") {
		t.Errorf("list not written as a block sequence:\n%s", text)
	}
	// The provider was asked about both names before the write.
	provider.mu.Lock()
	asked := len(provider.queried)
	provider.mu.Unlock()
	if asked != 2 {
		t.Errorf("the panel checked %d names, want 2", asked)
	}
}

// TestConfigStormRejectsUnknownLocality pins the trap the provider sets: an
// unknown name answers 0/0, and 0/0 is also the "return test data" point, so
// a typo must be refused instead of saved (and definitely instead of
// publishing sample alerts).
func TestConfigStormRejectsUnknownLocality(t *testing.T) {
	provider := &stormProvider{}
	srv := provider.start(t)
	env := newTestEnv(t)
	env.login()
	path := stormPanelConfig(t, env, srv.URL, "Niepołomice")
	csrf := env.csrfFromPage("/config")

	resp, body := env.postForm("/config/storm", url.Values{
		"csrf":        {csrf},
		"path_0":      {"sources[0].config.locations"},
		"locations_0": {"Niepołomice, Niepolomice"},
	})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("POST with an unknown locality = %d, want 422", resp.StatusCode)
	}
	if !strings.Contains(body, "Niepolomice") {
		t.Errorf("the banner does not name the rejected locality: %s", body)
	}
	if got, _ := config.StringListAt(path, "sources[0].config.locations"); len(got) != 1 || got[0] != "Niepołomice" {
		t.Errorf("the file changed despite the rejection: %v", got)
	}
}

// TestConfigStormSavesUnverifiedWhenProviderRefuses: a refusal (bad key,
// quota) is not a typo — the list is saved and reported as unverified,
// because a provider outage must not block the panel.
func TestConfigStormSavesUnverifiedWhenProviderRefuses(t *testing.T) {
	provider := &stormProvider{refuse: true}
	srv := provider.start(t)
	env := newTestEnv(t)
	env.login()
	path := stormPanelConfig(t, env, srv.URL, "Niepołomice")
	csrf := env.csrfFromPage("/config")

	resp, _ := env.postForm("/config/storm", url.Values{
		"csrf":        {csrf},
		"path_0":      {"sources[0].config.locations"},
		"locations_0": {"Kraków"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST = %d, want 303", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/config?msg=storm-unverified" {
		t.Errorf("redirect = %q, want the unverified flash", loc)
	}
	if got, _ := config.StringListAt(path, "sources[0].config.locations"); len(got) != 1 || got[0] != "Kraków" {
		t.Errorf("list not saved: %v", got)
	}
}

func TestConfigStormRejectsStaleFormAndBadInput(t *testing.T) {
	provider := &stormProvider{}
	srv := provider.start(t)
	env := newTestEnv(t)
	env.login()
	path := stormPanelConfig(t, env, srv.URL, "Niepołomice")
	csrf := env.csrfFromPage("/config")

	// A form from another instance (or a tampered page) is refused.
	resp, _ := env.postForm("/config/storm", url.Values{
		"csrf":        {csrf},
		"path_0":      {"sources[9].config.locations"},
		"locations_0": {"Kraków"},
	})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("stale form = %d, want 422", resp.StatusCode)
	}

	// No CSRF token: refused, nothing written.
	resp = env.postFormClose("/config/storm", url.Values{
		"path_0":      {"sources[0].config.locations"},
		"locations_0": {"Kraków"},
	}, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("POST without csrf = %d, want 403", resp.StatusCode)
	}
	if got, _ := config.StringListAt(path, "sources[0].config.locations"); len(got) != 1 || got[0] != "Niepołomice" {
		t.Errorf("the file changed without a CSRF token: %v", got)
	}

	// An over-long name is refused before the provider is asked.
	resp, _ = env.postForm("/config/storm", url.Values{
		"csrf":        {csrf},
		"path_0":      {"sources[0].config.locations"},
		"locations_0": {strings.Repeat("x", 90)},
	})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("over-long locality = %d, want 422", resp.StatusCode)
	}
	if got, _ := config.StringListAt(path, "sources[0].config.locations"); len(got) != 1 || got[0] != "Niepołomice" {
		t.Errorf("the file changed on an invalid name: %v", got)
	}
}

// TestConfigStormWithoutInstance: a station that follows no storms sees the
// hint, not an empty form.
func TestConfigStormWithoutInstance(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	path := panelConfig(t, env)
	_, page := env.get("/config")
	if !strings.Contains(page, "cfg-storm") {
		t.Fatal("the storm section is missing")
	}
	if strings.Contains(page, `name="locations_0"`) {
		t.Error("a form was rendered without a storm source instance")
	}
	if !strings.Contains(page, "burzedzis") {
		t.Errorf("the hint does not name the plugin type to add:\n%s", page)
	}
	_ = path
}
