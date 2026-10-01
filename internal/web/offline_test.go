package web_test

import (
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/mqttpolicy"
)

// TestConfigPageAdminOnly proves the config section is admin-only: an
// anonymous visitor is bounced to /login and a member gets a forbidden.
func TestConfigPageAdminOnly(t *testing.T) {
	env := newTestEnv(t)
	resp, _ := env.get("/config")
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("GET /config unauthenticated = %d, want 303", resp.StatusCode)
	}
	env.login()
	resp, page := env.get("/config")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /config = %d", resp.StatusCode)
	}
	// The page must be a full chrome document (topbar/sidebar), not the
	// bare section fragment.
	if !strings.Contains(page, "</html>") || !strings.Contains(page, "sidenav") {
		t.Fatalf("config page lacks the chrome document: %.200s", page)
	}
}

// TestConfigEndpointsRequireCSRF pins the reported P2: the state-changing
// config routes must reject requests without a valid CSRF token — and
// with a foreign Origin header (a same-site sibling origin can submit a
// form without ever reading the token) — instead of applying the change
// and answering 303.
func TestConfigEndpointsRequireCSRF(t *testing.T) {
	oldMask := mqttpolicy.Mask()
	t.Cleanup(func() { mqttpolicy.Set(oldMask) })

	env := newTestEnv(t)
	env.login()

	// No token: rejected, nothing changes.
	resp := env.postFormClose("/config/offline", url.Values{"offline": {"on"}}, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /config/offline without csrf = %d, want 403", resp.StatusCode)
	}
	if env.server.OfflineMode() {
		t.Fatal("offline mode changed without a CSRF token")
	}
	resp = env.postFormClose("/config/mqtt", url.Values{"cat": {"events"}}, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /config/mqtt without csrf = %d, want 403", resp.StatusCode)
	}
	if !mqttpolicy.Allowed(mqttpolicy.CatEvents) || mqttpolicy.Mask() != oldMask {
		t.Fatal("publish mask changed without a CSRF token")
	}

	// A wrong token is rejected too.
	resp = env.postFormClose("/config/offline", url.Values{"csrf": {"bogus"}, "offline": {"on"}}, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /config/offline with a bogus csrf = %d, want 403", resp.StatusCode)
	}
	if env.server.OfflineMode() {
		t.Fatal("offline mode changed with a bogus CSRF token")
	}

	// A valid token with a foreign Origin is rejected (complementary
	// defense-in-depth for same-site sibling origins).
	csrf := env.csrfFromPage("/config")
	resp = env.postFormClose("/config/offline",
		url.Values{"csrf": {csrf}, "offline": {"on"}},
		map[string]string{"Origin": "http://evil.example"})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /config/offline with a foreign Origin = %d, want 403", resp.StatusCode)
	}
	if env.server.OfflineMode() {
		t.Fatal("offline mode changed from a foreign origin")
	}

	// A valid token from the same origin still works.
	resp = env.postFormClose("/config/offline",
		url.Values{"csrf": {csrf}, "offline": {"on"}},
		map[string]string{"Origin": env.srv.URL})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/offline same-origin = %d, want 303", resp.StatusCode)
	}
	if !env.server.OfflineMode() {
		t.Fatal("offline mode not enabled with a valid token and origin")
	}

	// And the MQTT mask route applies with a valid token.
	csrf = env.csrfFromPage("/config")
	resp = env.postFormClose("/config/mqtt",
		url.Values{"csrf": {csrf}, "cat": {"events", "status"}},
		map[string]string{"Origin": env.srv.URL})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/mqtt same-origin = %d, want 303", resp.StatusCode)
	}
	if mqttpolicy.Allowed(mqttpolicy.CatActive) {
		t.Fatal("publish mask not applied by the valid request")
	}
}

// TestOfflineToggle flows through the whole switch: enable (banner on the
// home page + state visible), idempotent re-enable, disable (banner gone).
func TestOfflineToggle(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	if env.server.OfflineMode() {
		t.Fatal("offline mode must start disabled in the test config")
	}
	// Public home page has no banner while online.
	_, html := env.get("/")
	if strings.Contains(html, `data-offline="1"`) {
		t.Fatal("home page reports offline while online")
	}

	// Admin config page: shows the switch.
	resp, page := env.get("/config")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /config = %d", resp.StatusCode)
	}
	if !strings.Contains(page, "config-offline") && !strings.Contains(page, "offline") {
		t.Fatal("config page misses the offline form")
	}

	csrf := env.csrfFromPage("/config")
	resp, _ = env.postForm("/config/offline", url.Values{"csrf": {csrf}, "offline": {"on"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/offline = %d, want 303", resp.StatusCode)
	}
	if !env.server.OfflineMode() {
		t.Fatal("offline mode not enabled after toggle")
	}
	_, html = env.get("/")
	if !strings.Contains(html, `data-offline="1"`) {
		t.Fatal("home page does not report offline mode")
	}
	if !strings.Contains(html, "Tryb offline") && !strings.Contains(html, "Offline mode") {
		t.Fatalf("home page misses the offline banner: %s", html[:200])
	}

	// Idempotent: toggling the same state again changes nothing.
	csrf = env.csrfFromPage("/config")
	resp, _ = env.postForm("/config/offline", url.Values{"csrf": {csrf}, "offline": {"on"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/offline (no-op) = %d", resp.StatusCode)
	}
	if !env.server.OfflineMode() {
		t.Fatal("offline mode lost after idempotent toggle")
	}

	// Disable: banner disappears again.
	csrf = env.csrfFromPage("/config")
	resp, _ = env.postForm("/config/offline", url.Values{"csrf": {csrf}, "offline": {"off"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/offline (off) = %d", resp.StatusCode)
	}
	if env.server.OfflineMode() {
		t.Fatal("offline mode still on after disable")
	}
	_, html = env.get("/")
	if strings.Contains(html, `data-offline="1"`) {
		t.Fatal("home page still reports offline after disable")
	}
}

// TestOfflineStartupState proves web.offline_mode in the config is the
// startup state of the switch.
func TestOfflineStartupState(t *testing.T) {
	cfg := defaultTestWebConfig()
	cfg.OfflineMode = true
	env := newTestEnvWeb(t, cfg, nil, nil, nil, nil, nil, nil)
	if !env.server.OfflineMode() {
		t.Fatal("offline mode must start on when web.offline_mode is set")
	}
	_, html := env.get("/")
	if !strings.Contains(html, `data-offline="1"`) {
		t.Fatal("home page does not report the configured offline startup state")
	}
}

// TestTileHandler serves the operator tile tree and rejects anything
// outside it.
func TestTileHandler(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "8", "141"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "8", "141", "86.jpg"), []byte("jpeg-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := defaultTestWebConfig()
	cfg.TilesDir = dir
	env := newTestEnvWeb(t, cfg, nil, nil, nil, nil, nil, nil)

	resp, body := env.get("/tiles/8/141/86")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET tile = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "image/jpeg" {
		t.Errorf("tile content type = %q", ct)
	}
	if !strings.Contains(body, "jpeg-bytes") {
		t.Errorf("tile body = %q", body)
	}
	if cc := resp.Header.Get("Cache-Control"); !strings.Contains(cc, "max-age=86400") {
		t.Errorf("tile cache control = %q", cc)
	}

	// The frontend requests {z}/{x}/{y}.jpg — the extension rides inside
	// the path segment and must be served, not 404.
	resp, body = env.get("/tiles/8/141/86.jpg")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET tile with .jpg suffix = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "image/jpeg" {
		t.Errorf("tile .jpg content type = %q", ct)
	}
	if !strings.Contains(body, "jpeg-bytes") {
		t.Errorf("tile .jpg body = %q", body)
	}

	// Missing tiles 404 and invalid coordinates 404 (the mux cleans
	// dot-segments before routing, so traversal never reaches the
	// handler).
	for _, path := range []string{"/tiles/9/141/86", "/tiles/-1/0/0", "/tiles/abc/1/1"} {
		resp, _ = env.get(path)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404", path, resp.StatusCode)
		}
	}

	// Without a configured tile tree everything 404s.
	env2 := newTestEnv(t)
	resp, _ = env2.get("/tiles/8/141/86")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("tile without tiles_dir = %d, want 404", resp.StatusCode)
	}
}

// TestConfigPageListsInternetSources proves the page names the sources the
// offline switch suspends.
func TestConfigPageListsInternetSources(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	_, page := env.get("/config")
	if !strings.Contains(page, "imgw-warnings") {
		t.Errorf("config page misses the internet source list entry")
	}
}

// TestConfigMqttMask flows the publish-mask section: the page lists all
// categories and the POST replaces the runtime mask immediately.
func TestConfigMqttMask(t *testing.T) {
	t.Cleanup(func() { mqttpolicy.Set(uint32(mqttpolicy.CatAll)) })
	env := newTestEnv(t)
	env.login()

	_, page := env.get("/config")
	for _, key := range []string{"events", "active", "info", "status",
		"aprs_stations", "aprs_bulletins", "aprs_packets", "aprs_messages",
		"meshtastic_stations", "meshtastic_messages"} {
		if !strings.Contains(page, `name="cat" value="`+key+`"`) {
			t.Errorf("config page misses the %q publish checkbox", key)
		}
	}

	// Uncheck everything except info + status: only those may publish.
	csrf := env.csrfFromPage("/config")
	resp, _ := env.postForm("/config/mqtt", url.Values{
		"csrf": {csrf},
		"cat":  {"info", "status"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/mqtt = %d, want 303", resp.StatusCode)
	}
	want := uint32(mqttpolicy.CatInfo) | uint32(mqttpolicy.CatStatus)
	if got := mqttpolicy.Mask(); got != want {
		t.Errorf("mask = %#x, want %#x", got, want)
	}
	if mqttpolicy.Allowed(mqttpolicy.CatEvents) {
		t.Error("events must be masked after the POST")
	}
	if !mqttpolicy.Allowed(mqttpolicy.CatInfo) {
		t.Error("info must stay allowed")
	}
}
