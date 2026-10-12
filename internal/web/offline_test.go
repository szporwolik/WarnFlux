package web_test

import (
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/mqttpolicy"
	"github.com/szporwolik/WarnFlux/internal/web"
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
	// The logs surface lives under Config: the page links to /logs.
	if !strings.Contains(page, `href="/logs"`) {
		t.Fatalf("config page missing the Logs link: %.200s", page)
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

	// A "null" origin (sandboxed webviews) is treated like no Origin:
	// the CSRF token remains the gate. This pins the embedded-browser
	// regression where saving users/config forms was rejected.
	csrf = env.csrfFromPage("/config")
	resp = env.postFormClose("/config/mqtt",
		url.Values{"csrf": {csrf}, "cat": {"events", "status", "active"}},
		map[string]string{"Origin": "null"})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/mqtt with a null Origin = %d, want 303", resp.StatusCode)
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
	if !strings.Contains(page, `id="offline-switch"`) {
		t.Fatal("config page misses the offline switch")
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

	// The max-zoom endpoint reports the deepest level with tiles — the
	// offline maps stretch them past that point.
	resp, body = env.get("/api/tiles/maxzoom")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET maxzoom = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(body, `"maxzoom":8`) {
		t.Errorf("maxzoom payload = %q, want 8", body)
	}

	// Without a configured tile tree everything 404s (and maxzoom is 0).
	env2 := newTestEnv(t)
	resp, _ = env2.get("/tiles/8/141/86")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("tile without tiles_dir = %d, want 404", resp.StatusCode)
	}
	resp, body = env2.get("/api/tiles/maxzoom")
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, `"maxzoom":0`) {
		t.Errorf("maxzoom without tiles_dir = %d %q, want 0", resp.StatusCode, body)
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

// TestConfigMeshSplitSwitches pins the two independent mesh switches: the
// group-channel broadcast and the direct messages to users are toggled
// separately from the Config page.
func TestConfigMeshSplitSwitches(t *testing.T) {
	t.Cleanup(func() {
		meshtastic.SetChannelAlerts(true)
		meshtastic.SetDMAlerts(true)
		meshtastic.SetStationAlerts(false)
	})
	env := newTestEnv(t)
	env.login()

	_, page := env.get("/config")
	if !strings.Contains(page, `name="channel"`) || !strings.Contains(page, `name="dm"`) || !strings.Contains(page, `name="stations"`) {
		t.Fatalf("config page misses the three mesh switches: %s", page)
	}

	// Mute only the group channel.
	csrf := env.csrfFromPage("/config")
	resp, _ := env.postForm("/config/mesh", url.Values{"csrf": {csrf}, "channel": {"off"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/mesh channel = %d, want 303", resp.StatusCode)
	}
	if meshtastic.ChannelAlerts() {
		t.Error("channel alerts must be muted after the POST")
	}
	if !meshtastic.DMAlerts() {
		t.Error("DM alerts must be untouched by the channel POST")
	}

	// Mute only the direct messages.
	resp, _ = env.postForm("/config/mesh", url.Values{"csrf": {csrf}, "dm": {"off"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/mesh dm = %d, want 303", resp.StatusCode)
	}
	if meshtastic.ChannelAlerts() {
		t.Error("channel alerts must stay muted")
	}
	if meshtastic.DMAlerts() {
		t.Error("DM alerts must be muted after the POST")
	}

	// Enable only the APRS station announcements (off by default).
	if meshtastic.StationAlerts() {
		t.Fatal("station announcements must default to OFF")
	}
	resp, _ = env.postForm("/config/mesh", url.Values{"csrf": {csrf}, "stations": {"on"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/mesh stations = %d, want 303", resp.StatusCode)
	}
	if !meshtastic.StationAlerts() {
		t.Error("station announcements must be enabled after the POST")
	}
	resp, _ = env.postForm("/config/mesh", url.Values{"csrf": {csrf}, "stations": {"off"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/mesh stations off = %d, want 303", resp.StatusCode)
	}
	if meshtastic.StationAlerts() {
		t.Error("station announcements must be muted again")
	}

	// An empty submit is rejected.
	resp, _ = env.postForm("/config/mesh", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST /config/mesh empty = %d, want 400", resp.StatusCode)
	}
}

// TestConfigSystemLanguage pins the broadcast-language switch: the page
// offers the supported languages and the POST changes the runtime value
// (which the routing engine reads for APRS / channel posts).
func TestConfigSystemLanguage(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	_, page := env.get("/config")
	if !strings.Contains(page, `action="/config/lang"`) {
		t.Fatalf("config page misses the language form: %s", page)
	}
	if !strings.Contains(page, `value="pl"`) {
		t.Errorf("config page misses the pl option: %s", page)
	}

	if env.server.SystemLanguage() != "" {
		t.Fatalf("startup language = %q, want empty (i18n default)", env.server.SystemLanguage())
	}
	csrf := env.csrfFromPage("/config")
	resp, _ := env.postForm("/config/lang", url.Values{
		"csrf": {csrf},
		"lang": {"pl"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/lang = %d, want 303", resp.StatusCode)
	}
	if got := env.server.SystemLanguage(); got != "pl" {
		t.Errorf("system language = %q, want pl", got)
	}

	// Unsupported codes are rejected and leave the value untouched.
	resp, _ = env.postForm("/config/lang", url.Values{
		"csrf": {csrf},
		"lang": {"xx"},
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST /config/lang invalid = %d, want 400", resp.StatusCode)
	}
	if got := env.server.SystemLanguage(); got != "pl" {
		t.Errorf("system language after rejected POST = %q, want pl", got)
	}

	// Clearing returns to the system default.
	resp, _ = env.postForm("/config/lang", url.Values{
		"csrf": {csrf},
		"lang": {""},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/lang clear = %d, want 303", resp.StatusCode)
	}
	if got := env.server.SystemLanguage(); got != "" {
		t.Errorf("system language after clear = %q, want empty", got)
	}
}

// panelConfig writes a minimal valid config.yaml for the persistence
// tests and registers it on the test server.
func panelConfig(t *testing.T, env *testEnv) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	base := "web:\n  enabled: true\n  offline_mode: false\n  auth:\n    username: admin\n    password: secret123\n"
	if err := os.WriteFile(path, []byte(base), 0o600); err != nil {
		t.Fatal(err)
	}
	env.server.SetConfigFile(path)
	return path
}

// TestConfigPanelPersistsYAML pins the durability promise: every switch
// on the Config page is written back to the YAML file, so the choice
// survives restarts.
func TestConfigPanelPersistsYAML(t *testing.T) {
	t.Cleanup(func() {
		meshtastic.SetChannelAlerts(true)
		meshtastic.SetDMAlerts(true)
		meshtastic.SetStationAlerts(false)
		mqttpolicy.Set(uint32(mqttpolicy.CatAll))
	})
	env := newTestEnv(t)
	env.login()
	path := panelConfig(t, env)
	csrf := env.csrfFromPage("/config")

	reload := func() *config.Config {
		cfg, err := config.Load(path)
		if err != nil {
			t.Fatalf("reload config: %v", err)
		}
		return cfg
	}

	// Meshtastic station announcements.
	resp, _ := env.postForm("/config/mesh", url.Values{"csrf": {csrf}, "stations": {"on"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST stations = %d, want 303", resp.StatusCode)
	}
	if !reload().Meshtastic.StationAlerts {
		t.Error("meshtastic.station_alerts not written to the YAML file")
	}

	// Local tile forcing.
	resp, _ = env.postForm("/config/tiles", url.Values{"csrf": {csrf}, "tiles": {"on"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST tiles = %d, want 303", resp.StatusCode)
	}
	if !reload().Web.ForceLocalTiles {
		t.Error("web.force_local_tiles not written to the YAML file")
	}

	// Offline mode.
	resp, _ = env.postForm("/config/offline", url.Values{"csrf": {csrf}, "offline": {"on"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST offline = %d, want 303", resp.StatusCode)
	}
	if !reload().Web.OfflineMode {
		t.Error("web.offline_mode not written to the YAML file")
	}

	// System language.
	resp, _ = env.postForm("/config/lang", url.Values{"csrf": {csrf}, "lang": {"pl"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST lang = %d, want 303", resp.StatusCode)
	}
	if got := reload().Web.SystemLanguage; got != "pl" {
		t.Errorf("web.system_language = %q, want pl", got)
	}

	// MQTT publish mask: only info + status publish.
	resp, _ = env.postForm("/config/mqtt", url.Values{"csrf": {csrf}, "cat": {"info", "status"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST mqtt = %d, want 303", resp.StatusCode)
	}
	cfg := reload()
	if !cfg.MQTTPublish.Info || !cfg.MQTTPublish.Status || cfg.MQTTPublish.Events || cfg.MQTTPublish.Active {
		t.Errorf("mqtt_publish = %+v, want only info+status enabled", cfg.MQTTPublish)
	}
}

// TestConfigContentPanel pins the editable branding section: the fields
// apply immediately (runtime getters) and land in the YAML file; an
// invalid domain is rejected without changing anything.
func TestConfigContentPanel(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	path := panelConfig(t, env)
	csrf := env.csrfFromPage("/config")

	resp, _ := env.postForm("/config/content", url.Values{
		"csrf": {csrf}, "header1": {"Główna"}, "header2": {"Podtytuł"},
		"tagline": {"motto"}, "about": {"# Info\n\ntekst **ważny**"},
		"disclaimer": {"nieoficjalny system"}, "domain": {"sp9moa.pl"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST content = %d, want 303", resp.StatusCode)
	}
	if got := env.server.DisplayHeader1(); got != "Główna" {
		t.Errorf("header1 = %q, want Główna", got)
	}
	if got := env.server.DisplayAbout(); got != "# Info\n\ntekst **ważny**" {
		t.Errorf("about = %q, want the markdown text", got)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if cfg.Web.Header1 != "Główna" || cfg.Web.Header2 != "Podtytuł" || cfg.Web.Tagline != "motto" ||
		cfg.Web.About != "# Info\n\ntekst **ważny**" || cfg.Web.Disclaimer != "nieoficjalny system" ||
		cfg.Web.Domain != "sp9moa.pl" {
		t.Errorf("web content not persisted: %+v", cfg.Web)
	}

	// An invalid domain is rejected.
	csrf = env.csrfFromPage("/config")
	resp, _ = env.postForm("/config/content", url.Values{
		"csrf": {csrf}, "header1": {"Główna"}, "domain": {"https://sp9moa.pl/x"},
	})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("bad domain = %d, want 422", resp.StatusCode)
	}
	if got := env.server.DisplayDomain(); got != "sp9moa.pl" {
		t.Errorf("domain changed despite rejection: %q", got)
	}
}

// TestConfigPersistFailureFlag pins the graceful degradation: when the
// YAML write fails the switch still applies, but the redirect carries
// perr=1 so the page can warn the operator.
func TestConfigPersistFailureFlag(t *testing.T) {
	t.Cleanup(func() {
		meshtastic.SetStationAlerts(false)
	})
	env := newTestEnv(t)
	env.login()
	// A DIRECTORY as the "config file": UpdateFile fails on it.
	env.server.SetConfigFile(t.TempDir())
	csrf := env.csrfFromPage("/config")

	resp, _ := env.postForm("/config/mesh", url.Values{"csrf": {csrf}, "stations": {"on"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST stations = %d, want 303", resp.StatusCode)
	}
	if !meshtastic.StationAlerts() {
		t.Error("switch must apply even when persistence fails")
	}
	if loc := resp.Header.Get("Location"); !strings.Contains(loc, "perr=1") {
		t.Errorf("Location = %q, want the perr=1 persistence warning", loc)
	}

	// The warning renders on the config page.
	_, page := env.get("/config?msg=mesh&perr=1")
	if !strings.Contains(page, "config.yaml") {
		t.Errorf("persistence warning missing from the page: %s", page)
	}
}

// TestConfigCadenceSection pins the live periodic cadence: the panel moves
// the presence anchor and the hazard spacing on the Meshtastic hub (the
// Discord runner gets the same values), and the file keeps them.
func TestConfigCadenceSection(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	path := filepath.Join(t.TempDir(), "config.yaml")
	base := "web:\n  enabled: true\n  auth:\n    username: admin\n    password: secret123\n" +
		"meshtastic:\n  emcom_at: \"06:00\"\n" +
		"actions:\n" +
		"  - id: sosna\n    type: discord\n    config:\n      invite: \"https://discord.gg/x\"\n" +
		"  - id: sosdev\n    type: discord\n    config:\n      hazards_interval: \"0s\"\n      uptime_interval: \"0s\"\n"
	if err := os.WriteFile(path, []byte(base), 0o600); err != nil {
		t.Fatal(err)
	}
	env.server.SetConfigFile(path)

	var mu sync.Mutex
	var got struct {
		hazards, uptime time.Duration
		at              string
		calls           int
	}
	env.server.SetNoticesCadence(func(hazards, uptime time.Duration, uptimeAt string) {
		mu.Lock()
		defer mu.Unlock()
		got.hazards, got.uptime, got.at, got.calls = hazards, uptime, uptimeAt, got.calls+1
	})
	seen := func() (string, time.Duration, time.Duration, int) {
		mu.Lock()
		defer mu.Unlock()
		return got.at, got.uptime, got.hazards, got.calls
	}
	csrf := env.csrfFromPage("/config")

	// The section renders the live cadence and the accepted spelling.
	_, page := env.get("/config")
	if !strings.Contains(page, "Cadence of the periodic announcements") {
		t.Fatalf("cadence section missing from the page")
	}
	if !strings.Contains(page, `name="anchor" maxlength="5" placeholder="06:00" value="06:00"`) {
		t.Errorf("the daily hour is not shown in the field")
	}

	resp, _ := env.postForm("/config/cadence", url.Values{
		"csrf": {csrf}, "anchor": {"07:30"}, "interval": {""}, "hazards": {"2h"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/cadence = %d, want 303", resp.StatusCode)
	}
	if at, up, hz, calls := seen(); calls != 1 || at != "07:30" || up != 0 || hz != 2*time.Hour {
		t.Fatalf("live surfaces got at=%q uptime=%s hazards=%s calls=%d", at, up, hz, calls)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if cfg.Meshtastic.EmcomAt != "07:30" || cfg.Meshtastic.EmcomInterval != 0 || cfg.Meshtastic.EmcomHazardsInterval != 2*time.Hour {
		t.Fatalf("meshtastic config = %+v", cfg.Meshtastic)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The Discord instance carries the same cadence, so the channel
	// announces in step with the mesh.
	for _, want := range []string{"uptime_at: \"07:30\"", "hazards_interval: 2h0m0s", "invite: \"https://discord.gg/x\""} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("config file lacks %q:\n%s", want, raw)
		}
	}
	// The second instance silenced both of its lines (the development
	// arrangement, where production owns the webhook): the panel leaves it
	// alone instead of waking it up.
	if n := strings.Count(string(raw), "hazards_interval: 2h0m0s"); n != 2 {
		t.Fatalf("the new spacing reached %d instances, want 2:\n%s", n, raw)
	}
	silent := string(raw)[strings.Index(string(raw), "id: sosdev"):]
	if !strings.Contains(silent, `hazards_interval: "0s"`) || !strings.Contains(silent, "uptime_interval: \"0s\"") {
		t.Errorf("a silenced instance was modified:\n%s", silent)
	}
	if strings.Contains(silent, "uptime_at") {
		t.Errorf("a silenced instance gained an anchor:\n%s", silent)
	}

	// A refused value changes nothing and names the field.
	resp, bad := env.postForm("/config/cadence", url.Values{
		"csrf": {csrf}, "anchor": {"6"}, "interval": {""}, "hazards": {"2h"},
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad anchor = %d, want 400", resp.StatusCode)
	}
	if !strings.Contains(bad, "Daily hour") || !strings.Contains(bad, "HH:MM") {
		t.Errorf("the banner does not name the refused field: %.300s", bad)
	}
	if _, _, _, calls := seen(); calls != 1 {
		t.Errorf("a refused value reached the live surfaces (calls=%d)", calls)
	}
	resp, _ = env.postForm("/config/cadence", url.Values{
		"csrf": {csrf}, "anchor": {"07:00"}, "interval": {"24h"}, "hazards": {""},
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("anchor + interval = %d, want 400", resp.StatusCode)
	}
}

// TestConfigPollingSection pins the range and polling editor: it writes
// the values, refuses what the consumer would refuse and stays silent
// about rows the operator left alone.
func TestConfigPollingSection(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	path := filepath.Join(t.TempDir(), "config.yaml")
	base := "web:\n  enabled: true\n  auth:\n    username: admin\n    password: secret123\naprs:\n  radius_km: 60\n  station_ttl: 60m\nmeshtastic:\n  node_ttl: 30m\nsources:\n  - id: weather-home\n    type: openmeteo\n    config:\n      poll_interval: 30m\n  - id: planes\n    type: adsb\n    config:\n      poll_interval: 3m\n"
	if err := os.WriteFile(path, []byte(base), 0o600); err != nil {
		t.Fatal(err)
	}
	env.server.SetConfigFile(path)
	env.server.SetPollBounds(func(sourceType string) (time.Duration, time.Duration, bool) {
		if sourceType == "openmeteo" {
			return time.Minute, 24 * time.Hour, true
		}
		if sourceType == "adsb" {
			return 5 * time.Second, 5 * time.Minute, true
		}
		return 0, 0, false
	})
	csrf := env.csrfFromPage("/config")

	_, page := env.get("/config")
	if !strings.Contains(page, "Polling and range") {
		t.Fatalf("the polling section is missing from the page")
	}
	// Every row carries its instance and the accepted range.
	for _, want := range []string{"weather-home (openmeteo)", "planes (adsb)", "accepted: 5s..5m", "Poll interval"} {
		if !strings.Contains(page, want) {
			t.Errorf("the page lacks %q", want)
		}
	}

	// Rows are addressed by path, so an empty one is skipped: the first
	// row is left alone, the second gets 4m.
	resp, _ := env.postForm("/config/polling", url.Values{
		"csrf":   {csrf},
		"path_0": {"aprs.radius_km"}, "value_0": {""},
		"path_1": {"aprs.station_ttl"}, "value_1": {"90m"},
		"path_2": {"meshtastic.node_ttl"}, "value_2": {"1h"},
		"path_3": {"sources[0].config.poll_interval"}, "value_3": {"2h"},
		"path_4": {"sources[1].config.poll_interval"}, "value_4": {"4m"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/polling = %d, want 303", resp.StatusCode)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if cfg.APRS.RadiusKM != 60 || cfg.APRS.StationTTL != 90*time.Minute {
		t.Fatalf("aprs = radius %v ttl %s", cfg.APRS.RadiusKM, cfg.APRS.StationTTL)
	}
	raw, _ := os.ReadFile(path)
	for _, want := range []string{"radius_km: 60", "station_ttl: 1h30m0s", "node_ttl: 1h0m0s", "poll_interval: 2h0m0s", "poll_interval: 4m0s"} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("config file lacks %q:\n%s", want, raw)
		}
	}

	// A value outside the plugin's own range is refused, and the refusal
	// names the plugin.
	resp, bad := env.postForm("/config/polling", url.Values{
		"csrf": {csrf}, "path_0": {"sources[1].config.poll_interval"}, "value_0": {"30m"},
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("out-of-range poll = %d, want 400", resp.StatusCode)
	}
	if !strings.Contains(bad, "adsb accepts 5s..5m") {
		t.Errorf("the banner does not name the plugin range: %.300s", bad)
	}
	resp, _ = env.postForm("/config/polling", url.Values{
		"csrf": {csrf}, "path_0": {"aprs.radius_km"}, "value_0": {"1500"},
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("out-of-range radius = %d, want 400", resp.StatusCode)
	}
	// An all-empty form is a no-op with its own flash.
	resp, _ = env.postForm("/config/polling", url.Values{
		"csrf": {csrf}, "path_0": {"aprs.radius_km"}, "value_0": {""},
	})
	if loc := resp.Header.Get("Location"); !strings.Contains(loc, "msg=polling-none") {
		t.Fatalf("Location = %q, want the nothing-to-save flash", loc)
	}
	after, _ := os.ReadFile(path)
	if string(after) != string(raw) {
		t.Fatalf("a refused edit changed the file:\n%s", after)
	}
}

// TestConfigPollingNoOpSave pins the churn-free rule: saving the form
// unchanged leaves the file bytes and the audit trail alone, even though
// the field spells a duration shorter than the file's own spelling.
func TestConfigPollingNoOpSave(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	path := filepath.Join(t.TempDir(), "config.yaml")
	base := "web:\n  enabled: true\n  auth:\n    username: admin\n    password: secret123\naprs:\n  radius_km: 30\nsources:\n  - id: weather-home\n    type: openmeteo\n    config:\n      poll_interval: 2h0m0s\n"
	if err := os.WriteFile(path, []byte(base), 0o600); err != nil {
		t.Fatal(err)
	}
	env.server.SetConfigFile(path)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	csrf := env.csrfFromPage("/config")
	resp, _ := env.postForm("/config/polling", url.Values{
		"csrf":   {csrf},
		"path_0": {"aprs.radius_km"}, "value_0": {"30"},
		"path_1": {"sources[0].config.poll_interval"}, "value_1": {"2h"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/polling = %d, want 303", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); !strings.Contains(loc, "msg=polling-none") {
		t.Fatalf("Location = %q, want the nothing-to-save flash", loc)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("an unchanged save rewrote the file:\n%s", after)
	}
	if _, err := os.Stat(path + ".bak-panel"); !os.IsNotExist(err) {
		t.Errorf("an unchanged save wrote a backup (err=%v)", err)
	}
}

// TestConfigLimitsSection pins the retention editor: the live windows
// reach the running station, every row lands in the file, "-1" means
// "never prune" and a value outside the loader's own range is refused
// before anything is written.
func TestConfigLimitsSection(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	path := filepath.Join(t.TempDir(), "config.yaml")
	base := "web:\n  enabled: true\n  auth:\n    username: admin\n    password: secret123\napp:\n  change_retention: 24h\n"
	if err := os.WriteFile(path, []byte(base), 0o600); err != nil {
		t.Fatal(err)
	}
	env.server.SetConfigFile(path)

	var mu sync.Mutex
	var got []web.RetentionLimits
	env.server.SetRetentionApplier(func(l web.RetentionLimits) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, l)
	})
	csrf := env.csrfFromPage("/config")

	_, page := env.get("/config")
	if !strings.Contains(page, "Retention and limits") {
		t.Fatalf("the limits section is missing from the page")
	}
	// The placeholders quote the loader's own defaults.
	for _, want := range []string{`placeholder="720h"`, `placeholder="100"`, "never prune"} {
		if !strings.Contains(page, want) {
			t.Errorf("the page lacks %q", want)
		}
	}

	resp, _ := env.postForm("/config/retention", url.Values{
		"csrf":   {csrf},
		"path_0": {"app.change_retention"}, "value_0": {"48h"},
		"path_1": {"app.event_retention"}, "value_1": {""},
		"path_2": {"app.notification_retention"}, "value_2": {"-1"},
		"path_3": {"dispatch.inbox_retention"}, "value_3": {"0"},
		"path_4": {"storage.min_free_mb"}, "value_4": {"250"},
		"path_5": {"dispatch.queue_size"}, "value_5": {"2048"},
		"path_6": {"app.log_max_size_mb"}, "value_6": {"50"},
		"path_7": {"app.log_max_backups"}, "value_7": {"5"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/retention = %d, want 303", resp.StatusCode)
	}
	mu.Lock()
	last := got[len(got)-1]
	calls := len(got)
	mu.Unlock()
	if calls != 1 {
		t.Fatalf("the live applier ran %d times, want 1", calls)
	}
	// The untouched row keeps the default it fell back to.
	if last.Change != 48*time.Hour || last.Event != 30*24*time.Hour {
		t.Fatalf("windows = change %s event %s", last.Change, last.Event)
	}
	if last.Notification >= 0 || last.Inbox != 0 || last.MinFreeMB != 250 {
		t.Fatalf("limits = %+v", last)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if cfg.App.ChangeRetention != 48*time.Hour || cfg.App.NotificationRetention != neverRetentionForTest() {
		t.Fatalf("file = change %s notification %s", cfg.App.ChangeRetention, cfg.App.NotificationRetention)
	}
	if cfg.Storage.MinFreeMB != 250 || cfg.Dispatch.QueueSize != 2048 || cfg.App.LogMaxSizeMB != 50 || cfg.App.LogMaxBackups != 5 {
		t.Fatalf("file limits = %+v %+v %+v", cfg.Storage, cfg.Dispatch, cfg.App)
	}
	// Rows the operator left alone were not written at all.
	if _, ok := config.ScalarAt(path, "app.event_retention"); ok {
		t.Errorf("an empty field was written to the file")
	}

	// The alarm moves at once, and a refused value reaches nothing.
	csrf = env.csrfFromPage("/config")
	resp, bad := env.postForm("/config/retention", url.Values{
		"csrf": {csrf}, "path_0": {"dispatch.queue_size"}, "value_0": {"999999"},
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("out-of-range queue = %d, want 400", resp.StatusCode)
	}
	if !strings.Contains(bad, "Intake queue size") || !strings.Contains(bad, "1..100000") {
		t.Errorf("the banner does not name the field: %.300s", bad)
	}
	mu.Lock()
	calls = len(got)
	mu.Unlock()
	if calls != 1 {
		t.Errorf("a refused value reached the live station (%d calls)", calls)
	}
}

// neverRetentionForTest is the sentinel the page writes for "-1".
func neverRetentionForTest() time.Duration { return -1 * time.Second }
