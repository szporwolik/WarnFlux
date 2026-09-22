package web_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/web"
)

const (
	testUsername = "admin"
	testPassword = "s3cret-pass-9"
)

// fakeRouter satisfies web.RouterStatuses.
type fakeRouter struct{ statuses []plugin.PluginStatus }

func (f *fakeRouter) Statuses() []plugin.PluginStatus { return f.statuses }

type testEnv struct {
	t        *testing.T
	srv      *httptest.Server
	server   *web.Server
	state    *state.State
	ingress  *dispatch.Ingress
	actions  *action.Manager
	client   *http.Client
	receiver *mqttreceiver.Manager
	users    *fakeUsers
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	cfg := config.Web{
		Enabled: true,
		Listen:  ":0",
		Title:   "WarnFlux Test",
		Header2: "Test platform",
		Tagline: "Test tagline",
		Auth:    config.WebAuth{Username: testUsername, Password: testPassword},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := state.New()
	ingress := dispatch.NewIngress(64)

	receivers, err := mqttreceiver.NewManager([]config.Receiver{
		{
			ID: "local", Enabled: true,
			Broker: "tcp://broker:1883", ClientID: "warnflux-dispatch-local",
			ConnectTimeout: time.Second, KeepAlive: 30 * time.Second,
			WF: config.ReceiverWF{Enabled: true, TopicPrefix: "warnflux"},
		},
		{
			ID: "remote-club", Enabled: false,
			Broker: "tcp://user:secret@10.10.10.10:1883",
		},
	}, st, ingress, logger)
	if err != nil {
		t.Fatal(err)
	}

	reg := action.NewRegistry()
	reg.Register("logger", func(*yaml.Node) (action.Plugin, error) {
		return &nopAction{}, nil
	})
	actions, err := action.NewManager([]config.Action{
		{ID: "logger-action", Type: "logger", Enabled: true},
		{ID: "logger-off", Type: "logger", Enabled: false},
	}, reg, logger)
	if err != nil {
		t.Fatal(err)
	}
	actions.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = actions.Shutdown(ctx)
	})

	router := &fakeRouter{statuses: []plugin.PluginStatus{
		{ID: "imgw-warnings", Type: "imgw", Kind: plugin.KindSource, State: plugin.StateRunning},
		{ID: "mqtt-main", Type: "mqtt", Kind: plugin.KindOutput, State: plugin.StateDegraded, LastError: "broker down"},
	}}

	users := newFakeUsers()
	if err := users.EnsureAdminUser(testUsername); err != nil {
		t.Fatal(err)
	}

	srv, err := web.New(cfg, st, receivers, router, actions, ingress, logger, "test-version", "abc1234", users)
	if err != nil {
		t.Fatal(err)
	}
	srv.MarkReady()

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse // observe redirects instead of following
	}}

	return &testEnv{t: t, srv: ts, server: srv, state: st, ingress: ingress, actions: actions, client: client, receiver: receivers, users: users}
}

type nopAction struct{}

func (nopAction) Name() string                                        { return "logger" }
func (nopAction) Execute(context.Context, action.ActionRequest) error { return nil }
func (nopAction) Close(context.Context) error                         { return nil }

func (e *testEnv) get(path string) (*http.Response, string) {
	e.t.Helper()
	resp, err := e.client.Get(e.srv.URL + path)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp, string(body)
}

var csrfRe = regexp.MustCompile(`name="csrf" value="([^"]+)"`)

func extractCSRF(t *testing.T, html string) string {
	t.Helper()
	m := csrfRe.FindStringSubmatch(html)
	if m == nil {
		t.Fatalf("csrf field not found in: %s", html)
	}
	return m[1]
}

func (e *testEnv) login() string {
	e.t.Helper()
	resp, html := e.get("/login")
	if resp.StatusCode != http.StatusOK {
		e.t.Fatalf("GET /login = %d", resp.StatusCode)
	}
	csrf := extractCSRF(e.t, html)

	form := url.Values{"csrf": {csrf}, "username": {testUsername}, "password": {testPassword}}
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp2, err := e.client.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusSeeOther {
		e.t.Fatalf("POST /login = %d, want 303", resp2.StatusCode)
	}
	return strings.Join(resp2.Header.Values("Set-Cookie"), "; ")
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// --- auth tests -----------------------------------------------------------

func TestLoginSuccess(t *testing.T) {
	env := newTestEnv(t)
	raw := env.login()
	if !strings.Contains(raw, "wf_session=") {
		t.Fatalf("session cookie missing: %q", raw)
	}
	if !strings.Contains(raw, "HttpOnly") {
		t.Error("session cookie must be HttpOnly")
	}
	if !strings.Contains(raw, "SameSite=Lax") {
		t.Error("session cookie must have SameSite=Lax or stricter")
	}
}

func TestWrongPasswordRejected(t *testing.T) {
	env := newTestEnv(t)
	_, html := env.get("/login")
	csrf := extractCSRF(t, html)

	form := url.Values{"csrf": {csrf}, "username": {testUsername}, "password": {"wrong-password"}}
	resp, _ := env.postForm("/login", form)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("POST /login = %d, want 401", resp.StatusCode)
	}
	for _, c := range env.client.Jar.Cookies(mustURL(t, env.srv.URL)) {
		if c.Name == "wf_session" {
			t.Fatal("session cookie issued after failed login")
		}
	}
}

func TestCSRFRequiredForLogin(t *testing.T) {
	env := newTestEnv(t)
	form := url.Values{"username": {testUsername}, "password": {testPassword}}
	resp, _ := env.postForm("/login", form)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /login without csrf = %d, want 403", resp.StatusCode)
	}
}

func TestProtectedRouteRedirectsUnauthenticated(t *testing.T) {
	env := newTestEnv(t)
	resp, _ := env.get("/dashboard")
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("GET /dashboard unauthenticated = %d, want 303", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/login" {
		t.Errorf("redirect = %q, want /login", loc)
	}
}

func TestAuthenticatedDashboardAccessible(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	resp, html := env.get("/dashboard")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard = %d", resp.StatusCode)
	}
	if !strings.Contains(html, "WarnFlux Test") {
		t.Error("dashboard does not render app title")
	}
}

func TestLogoutInvalidatesSession(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	_, dash := env.get("/dashboard")
	csrf := extractCSRF(t, dash)
	resp, _ := env.postForm("/logout", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /logout = %d", resp.StatusCode)
	}

	resp2, _ := env.get("/dashboard")
	if resp2.StatusCode != http.StatusSeeOther {
		t.Fatalf("after logout GET /dashboard = %d, want redirect", resp2.StatusCode)
	}
}

func TestLogoutRequiresSessionCSRF(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	resp, _ := env.postForm("/logout", url.Values{"csrf": {"bogus"}})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /logout with bogus csrf = %d, want 403", resp.StatusCode)
	}
}

func TestPasswordNeverRendered(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	_, dash := env.get("/dashboard")
	if strings.Contains(dash, testPassword) {
		t.Error("password leaked into rendered HTML")
	}
}

func TestHealthEndpointsUnauthenticated(t *testing.T) {
	env := newTestEnv(t)
	resp, _ := env.get("/healthz")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /healthz = %d, want 200", resp.StatusCode)
	}
	resp2, _ := env.get("/readyz")
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("GET /readyz = %d, want 200", resp2.StatusCode)
	}
}

// --- dashboard tests ------------------------------------------------------

func TestDashboardReceiversRenderedAndSanitized(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	_, html := env.get("/partials/mqtt")
	if !strings.Contains(html, "local") || !strings.Contains(html, "remote-club") {
		t.Errorf("receivers not rendered: %s", html)
	}
	if !strings.Contains(html, "disconnected") {
		t.Errorf("connection state not rendered: %s", html)
	}
	// Credentials in the broker URL must never reach the page.
	if strings.Contains(html, "secret") || strings.Contains(html, "user:secret") {
		t.Error("broker credentials leaked into HTML")
	}
}

func TestDashboardNoWeather(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	_, html := env.get("/partials/weather")
	if !strings.Contains(html, "No weather data received yet") {
		t.Errorf("expected empty-state message, got: %s", html)
	}
}

func TestDashboardWeatherPresent(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	temp := 21.4
	env.state.AddOrUpdateInfo("local", "warnflux/info/openmeteo/weather-home/home/weather", state.InfoEntry{
		Source:     "openmeteo",
		ProducerID: "weather-home",
		Key:        "home",
		Kind:       "weather",
		ReceivedAt: time.Now(),
		Weather: &state.Weather{
			GeneratedAt:  time.Now(),
			LocationID:   "home",
			LocationName: "Home",
			TemperatureC: &temp,
			Condition:    "partly_cloudy",
		},
	})

	_, html := env.get("/partials/weather")
	if !strings.Contains(html, "21.4") || !strings.Contains(html, "partly_cloudy") {
		t.Errorf("weather not rendered: %s", html)
	}
	if !strings.Contains(html, "local") {
		t.Errorf("receiver origin not rendered with weather: %s", html)
	}
}

func TestDashboardWarningVisibleWithReceiver(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	env.state.AddOrUpdateActive("local", "warnflux/active/imgw-meteo/abc", state.Hazard{
		EventKey: "imgw-meteo:123",
		Source:   "imgw-meteo",
		Severity: "extreme",
		Headline: "Upał – stopień 3",
		Areas:    []string{"powiat slupski"},
		Status:   "active",
	})

	_, html := env.get("/partials/warnings")
	if !strings.Contains(html, "Upał – stopień 3") || !strings.Contains(html, "extreme") {
		t.Errorf("warning not rendered: %s", html)
	}
	if !strings.Contains(html, "local") {
		t.Errorf("receiver origin not rendered with warning: %s", html)
	}
}

func TestDashboardSourcesAndOutputs(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	_, html := env.get("/partials/plugins")
	if !strings.Contains(html, "imgw-warnings") || !strings.Contains(html, "mqtt-main") {
		t.Errorf("router plugin statuses not rendered: %s", html)
	}
	if !strings.Contains(html, "degraded") {
		t.Errorf("output degraded state not rendered: %s", html)
	}
}

func TestDashboardActionsStatus(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	_, html := env.get("/partials/actions")
	if !strings.Contains(html, "logger-action") || !strings.Contains(html, "logger-off") {
		t.Errorf("action statuses not rendered: %s", html)
	}
}

func TestFooterVersionAndRepoLink(t *testing.T) {
	env := newTestEnv(t)

	// Login page bottom bar: app title, version (commit link) and the
	// icon-only GitHub repo link.
	_, loginHTML := env.get("/login")
	if !strings.Contains(loginHTML, "WarnFlux Test") {
		t.Errorf("login footer missing version line: %s", loginHTML)
	}
	if !strings.Contains(loginHTML, `<h1 class="login-name">WarnFlux Test</h1>`) {
		t.Errorf("login page missing system name: %s", loginHTML)
	}
	if !strings.Contains(loginHTML, `class="login-sub">Test platform</p>`) {
		t.Errorf("login page missing header2 subtitle: %s", loginHTML)
	}
	if !strings.Contains(loginHTML, `class="footer-tagline">Test tagline</span>`) {
		t.Errorf("login footer missing tagline: %s", loginHTML)
	}
	if !strings.Contains(loginHTML, `href="https://github.com/szporwolik/WarnFlux/commit/abc1234"`) {
		t.Errorf("login footer missing commit link: %s", loginHTML)
	}
	if !strings.Contains(loginHTML, `class="gh-link"`) {
		t.Errorf("login footer missing GitHub link: %s", loginHTML)
	}

	env.login()
	_, dashHTML := env.get("/dashboard")
	if !strings.Contains(dashHTML, "WarnFlux Test") {
		t.Errorf("dashboard footer missing version line: %s", dashHTML)
	}
	if !strings.Contains(dashHTML, `class="footer-tagline">Test tagline</span>`) {
		t.Errorf("dashboard footer missing tagline: %s", dashHTML)
	}
	if !strings.Contains(dashHTML, `href="https://github.com/szporwolik/WarnFlux/commit/abc1234"`) {
		t.Errorf("dashboard footer missing commit link: %s", dashHTML)
	}
	if !strings.Contains(dashHTML, `class="gh-link"`) {
		t.Errorf("dashboard footer missing GitHub link: %s", dashHTML)
	}
	if !strings.Contains(dashHTML, `<span class="brand-name">WarnFlux Test</span>`) {
		t.Errorf("dashboard sidebar missing system name: %s", dashHTML)
	}
	// header2 is a login-page subtitle only; it never appears in the drawer.
	if strings.Contains(dashHTML, "brand-sub") {
		t.Errorf("dashboard sidebar must not render header2: %s", dashHTML)
	}

	// The users page renders the same shared footer (regression guard:
	// its view must carry the tagline too).
	_, usersHTML := env.get("/users")
	if !strings.Contains(usersHTML, `class="footer-tagline">Test tagline</span>`) {
		t.Errorf("users footer missing tagline: %s", usersHTML)
	}
	_, groupsHTML := env.get("/groups")
	if !strings.Contains(groupsHTML, `class="footer-tagline">Test tagline</span>`) {
		t.Errorf("groups footer missing tagline: %s", groupsHTML)
	}
}

func TestWarningsPagination(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	// 45 warnings → 3 pages of 20. Topics are zero-padded so the
	// lexicographic tiebreaker keeps numeric order.
	for i := 1; i <= 45; i++ {
		env.state.AddOrUpdateActive("local", fmt.Sprintf("warnflux/active/imgw-meteo/%02d", i), state.Hazard{
			EventKey: fmt.Sprintf("imgw-meteo:%d", i),
			Source:   "imgw-meteo",
			Severity: "moderate",
			Headline: fmt.Sprintf("Warning %d", i),
			Status:   "active",
		})
	}

	_, page1 := env.get("/partials/warnings")
	if !strings.Contains(page1, "Warning 1") || strings.Contains(page1, "Warning 21") {
		t.Errorf("page 1 wrong: %s", page1)
	}
	if !strings.Contains(page1, "1–20 of 45") || !strings.Contains(page1, "Next") {
		t.Errorf("pager missing on page 1: %s", page1)
	}

	_, page3 := env.get("/partials/warnings?page=3")
	if !strings.Contains(page3, "Warning 45") || strings.Contains(page3, "Warning 20") {
		t.Errorf("page 3 wrong: %s", page3)
	}
	if !strings.Contains(page3, "41–45 of 45") {
		t.Errorf("page 3 range wrong: %s", page3)
	}

	// Out-of-range and garbage page params clamp safely.
	_, clamped := env.get("/partials/warnings?page=999")
	if !strings.Contains(clamped, "41–45 of 45") {
		t.Errorf("page=999 should clamp to the last page: %s", clamped)
	}
	_, garbage := env.get("/partials/warnings?page=abc")
	if !strings.Contains(garbage, "1–20 of 45") {
		t.Errorf("page=abc should clamp to page 1: %s", garbage)
	}

	// Full-page navigation uses ?wpage= (the pager links).
	_, dash2 := env.get("/dashboard?wpage=2")
	if !strings.Contains(dash2, `data-wpage="2"`) || !strings.Contains(dash2, "21–40 of 45") {
		t.Errorf("dashboard wpage=2 wrong: %s", dash2)
	}
}

func TestDashboardSystemShowsDispatchQueue(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	_, html := env.get("/partials/status")
	if !strings.Contains(html, "Dispatch queue") {
		t.Errorf("dispatch queue stats not rendered: %s", html)
	}
}
