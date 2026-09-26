package web_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/storage"

	"github.com/szporwolik/WarnFlux/internal/aprs"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/ingesthttp"
	"github.com/szporwolik/WarnFlux/internal/metrics"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/trail"
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
	pub      *fakeComposePublisher
	users    *fakeUsers
	logs     *web.LogBuffer
	traffic  *mqttreceiver.TrafficBuffer
	trails   *trail.Recorder
	metrics  *metrics.Registry
}

// fakeComposePublisher records the communications the compose module
// publishes; the test drives the state mirror itself to simulate the
// broker loopback.
type fakeComposePublisher struct {
	published []state.Hazard
	expired   []string
	raw       []fakeRawPublish
}

// fakeRawPublish is one recorded retained raw publication (EMCOM state).
type fakeRawPublish struct {
	Suffix   string
	Retained bool
	Payload  []byte
}

func (f *fakeComposePublisher) PublishActive(source string, h state.Hazard) error {
	f.published = append(f.published, h)
	return nil
}

func (f *fakeComposePublisher) ExpireActive(source, eventKey string) error {
	f.expired = append(f.expired, eventKey)
	return nil
}

func (f *fakeComposePublisher) PublishRaw(suffix string, retained bool, payload []byte) error {
	f.raw = append(f.raw, fakeRawPublish{Suffix: suffix, Retained: retained, Payload: append([]byte(nil), payload...)})
	return nil
}

func newTestEnv(t *testing.T) *testEnv {
	return newTestEnvFull(t, nil, nil, nil)
}

func newTestEnvWithIngest(t *testing.T, ingest map[string]http.Handler) *testEnv {
	return newTestEnvFull(t, ingest, nil, nil)
}

// newTestEnvWithHub builds the test environment with an APRS hub wired
// into the web server (the home-page map tab reads it).
func newTestEnvWithHub(t *testing.T, hub *aprs.Hub) *testEnv {
	return newTestEnvFull(t, nil, hub, nil)
}

// newTestEnvWithStore builds the test environment with an event store
// wired into the web server (the home-page archive tab reads it).
func newTestEnvWithStore(t *testing.T, events storage.EventStore) *testEnv {
	return newTestEnvFull(t, nil, nil, events)
}

func newTestEnvFull(t *testing.T, ingest map[string]http.Handler, hub *aprs.Hub, events storage.EventStore) *testEnv {
	t.Helper()

	cfg := config.Web{
		Enabled:    true,
		Listen:     ":0",
		Title:      "WarnFlux Test",
		Header2:    "Test platform",
		Tagline:    "Test tagline",
		About:      "Test info text. <a href=\"https://sp9moa.pl\">sp9moa.pl</a>",
		Disclaimer: "Test disclaimer text.",
		Auth:       config.WebAuth{Username: testUsername, Password: testPassword},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := state.New()
	ingress := dispatch.NewIngress(64)
	logs := web.NewLogBuffer(web.DefaultLogLines)
	traffic := mqttreceiver.NewTrafficBuffer(mqttreceiver.DefaultTrafficEntries)
	trails := trail.NewRecorder(trail.DefaultMaxTrails)
	met := metrics.New()

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
	}, st, ingress, logger, traffic)
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
	}, reg, logger, trails, met)
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
	if err := users.EnsureAdminUser(testUsername, "secret123"); err != nil {
		t.Fatal(err)
	}

	pub := &fakeComposePublisher{}

	srv, err := web.New(cfg, st, receivers, pub, router, actions, hub, ingress, logger, "test-version", "abc1234", users, events, ingest, logs, traffic, trails, met)
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

	return &testEnv{t: t, srv: ts, server: srv, state: st, ingress: ingress, actions: actions, client: client, receiver: receivers, pub: pub, users: users, logs: logs, traffic: traffic, trails: trails, metrics: met}
}

type nopAction struct{}

func (nopAction) Name() string                                        { return "logger" }
func (nopAction) Execute(context.Context, action.ActionRequest) error { return nil }
func (nopAction) Close(context.Context) error                         { return nil }

// TestLogsViewerFlow pins the /logs page and the incremental feed: the
// page requires login, the first poll returns the whole buffer and a
// cursor-restricted poll returns only the newer lines.
func TestLogsViewerFlow(t *testing.T) {
	env := newTestEnv(t)

	// Unauthenticated: redirect to the login page.
	resp, _ := env.get("/logs")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /logs unauthenticated = %d %q, want redirect", resp.StatusCode, resp.Header.Get("Location"))
	}

	_, _ = env.logs.Write([]byte("time=1 level=ERROR msg=\"boom\"\n"))
	_, _ = env.logs.Write([]byte("time=2 level=INFO msg=hello\n"))

	env.login()
	_, html := env.get("/logs")
	if !strings.Contains(html, `id="log-viewer"`) {
		t.Errorf("logs page missing viewer: %s", html)
	}
	if !strings.Contains(html, `<span class="nav-label">Logs</span>`) {
		t.Errorf("logs page missing sidebar entry: %s", html)
	}

	// Full buffer poll.
	resp, body := env.get("/partials/logs?after=0")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("partial poll = %d", resp.StatusCode)
	}
	if !strings.Contains(body, `"level":"error"`) || !strings.Contains(body, `"level":"info"`) ||
		!strings.Contains(body, "boom") || !strings.Contains(body, "hello") {
		t.Fatalf("log feed = %s", body)
	}
	var feed struct {
		Lines []struct {
			Seq   int64  `json:"seq"`
			Level string `json:"level"`
			Text  string `json:"text"`
		} `json:"lines"`
	}
	if err := json.Unmarshal([]byte(body), &feed); err != nil {
		t.Fatalf("feed is not valid JSON: %v", err)
	}
	if len(feed.Lines) != 2 {
		t.Fatalf("feed lines = %d, want 2", len(feed.Lines))
	}
	cursor := feed.Lines[1].Seq

	// Incremental poll: only the line after the cursor.
	_, _ = env.logs.Write([]byte("time=3 level=WARN msg=careful\n"))
	_, body = env.get("/partials/logs?after=" + strconv.FormatInt(cursor, 10))
	if !strings.Contains(body, "careful") || strings.Contains(body, "hello") {
		t.Errorf("incremental feed = %s, want only the new line", body)
	}
}

// TestTrafficViewerFlow pins the /traffic page and the incremental feed:
// the page requires login, the first poll returns the whole buffer and a
// cursor-restricted poll returns only the newer entries.
func TestTrafficViewerFlow(t *testing.T) {
	env := newTestEnv(t)

	// Unauthenticated: redirect to the login page.
	resp, _ := env.get("/traffic")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /traffic unauthenticated = %d %q, want redirect", resp.StatusCode, resp.Header.Get("Location"))
	}

	env.traffic.Add("local", "events", "warnflux/events/123", 1, false, 412)
	env.traffic.Add("local", "active", "warnflux/active", 0, true, 88)

	env.login()
	_, html := env.get("/traffic")
	if !strings.Contains(html, `id="traffic-viewer"`) {
		t.Errorf("traffic page missing viewer: %s", html)
	}
	if !strings.Contains(html, `<span class="nav-label">MQTT</span>`) {
		t.Errorf("traffic page missing sidebar entry: %s", html)
	}
	if !strings.Contains(html, "Last 100 inbound MQTT frames") {
		t.Errorf("traffic page missing buffer hint: %s", html)
	}
	if !strings.Contains(html, `id="browse-form"`) {
		t.Errorf("traffic page missing MQTT browser: %s", html)
	}

	// Full buffer poll.
	resp, body := env.get("/partials/traffic?after=0")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("partial poll = %d", resp.StatusCode)
	}
	if !strings.Contains(body, `"kind":"events"`) || !strings.Contains(body, `"kind":"active"`) ||
		!strings.Contains(body, "warnflux/events/123") {
		t.Fatalf("traffic feed = %s", body)
	}
	var feed struct {
		Entries []struct {
			Seq      int64  `json:"seq"`
			Kind     string `json:"kind"`
			Topic    string `json:"topic"`
			QoS      byte   `json:"qos"`
			Retained bool   `json:"retained"`
		} `json:"entries"`
	}
	if err := json.Unmarshal([]byte(body), &feed); err != nil {
		t.Fatalf("feed is not valid JSON: %v", err)
	}
	if len(feed.Entries) != 2 {
		t.Fatalf("feed entries = %d, want 2", len(feed.Entries))
	}
	if feed.Entries[0].QoS != 1 || feed.Entries[0].Retained {
		t.Errorf("entry 0 fields = %+v", feed.Entries[0])
	}
	if feed.Entries[1].QoS != 0 || !feed.Entries[1].Retained {
		t.Errorf("entry 1 fields = %+v", feed.Entries[1])
	}
	cursor := feed.Entries[1].Seq

	// Incremental poll: only the entry after the cursor.
	env.traffic.Add("local", "status", "warnflux/status", 0, false, 64)
	_, body = env.get("/partials/traffic?after=" + strconv.FormatInt(cursor, 10))
	if !strings.Contains(body, "warnflux/status") || strings.Contains(body, "warnflux/active") {
		t.Errorf("incremental feed = %s, want only the new entry", body)
	}

	// Bad cursor: rejected.
	resp, _ = env.get("/partials/traffic?after=banana")
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad cursor = %d, want 400", resp.StatusCode)
	}

	// MQTT browser API: missing topic is rejected; a browse against the
	// never-connected test receiver reports the error as JSON.
	resp, _ = env.get("/api/mqtt/browse")
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("browse without topic = %d, want 400", resp.StatusCode)
	}
	resp, body = env.get("/api/mqtt/browse?topic=%23&window=1")
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("browse on disconnected receiver = %d, want 502", resp.StatusCode)
	}
	if !strings.Contains(body, `"error"`) {
		t.Errorf("browse error body = %s, want JSON error", body)
	}
}

// TestAuditFlow pins the user-action audit: the page requires login,
// dashboard actions land in the bounded buffer and the incremental feed
// carries them.
func TestAuditFlow(t *testing.T) {
	env := newTestEnv(t)

	// Unauthenticated: redirect to the login page.
	resp, _ := env.get("/audit")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /audit unauthenticated = %d %q, want redirect", resp.StatusCode, resp.Header.Get("Location"))
	}

	env.login()
	_, html := env.get("/audit")
	if !strings.Contains(html, `id="audit-viewer"`) {
		t.Errorf("audit page missing viewer: %s", html)
	}

	// The login itself is audited.
	_, body := env.get("/partials/audit?after=0")
	if !strings.Contains(body, `"login"`) {
		t.Fatalf("audit feed missing login entry: %s", body)
	}

	// A user creation lands in the feed with the acting username.
	csrf := env.csrfFromPage("/users")
	resp, _ = env.postForm("/users", url.Values{
		"csrf": {csrf}, "username": {"audit-ops"}, "role": {"emcom"},
		"password": {"password123"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("user create = %d", resp.StatusCode)
	}
	_, body = env.get("/partials/audit?after=0")
	if !strings.Contains(body, `"user-create"`) || !strings.Contains(body, `audit-ops`) {
		t.Fatalf("audit feed missing user-create entry: %s", body)
	}

	// Bad cursor: rejected.
	resp, _ = env.get("/partials/audit?after=banana")
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad audit cursor = %d, want 400", resp.StatusCode)
	}
}

// TestNotificationsFlow pins the delivery-history page: login required,
// the trail list renders and the JSON feed carries the audit steps.
func TestNotificationsFlow(t *testing.T) {
	env := newTestEnv(t)

	// Unauthenticated: redirect to the login page.
	resp, _ := env.get("/notifications")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /notifications unauthenticated = %d %q, want redirect", resp.StatusCode, resp.Header.Get("Location"))
	}

	now := time.Now()
	env.trails.Receive("imgw:1", "imgw", "severe", "Storm", "Gale warning", now)
	env.trails.Add("imgw:1", trail.StepMatched, "matched group Niepołomice", now)
	env.trails.Add("imgw:1", trail.StepRoute, "imgw → smtp-alerts ≥ Moderate", now)
	env.trails.Add("imgw:1", trail.StepSubmitted, "smtp-alerts action started", now)
	env.trails.Add("imgw:1", trail.StepDelivered, "delivered", now.Add(time.Second))
	env.trails.SetOutcome("imgw:1", trail.OutcomeDelivered)

	env.login()
	_, html := env.get("/notifications")
	if !strings.Contains(html, `id="notif-list"`) {
		t.Errorf("notifications page missing list: %s", html)
	}
	if !strings.Contains(html, `<span class="nav-label">Notifications</span>`) {
		t.Errorf("notifications page missing sidebar entry: %s", html)
	}
	if !strings.Contains(html, "matched group Niepołomice") ||
		!strings.Contains(html, "imgw → smtp-alerts ≥ Moderate") ||
		!strings.Contains(html, "delivered") {
		t.Errorf("notifications page missing trail steps: %s", html)
	}
	if !strings.Contains(html, "Gale warning") || !strings.Contains(html, `id="notif-imgw:1"`) {
		t.Errorf("notifications page missing trail header: %s", html)
	}

	// JSON feed for the poller.
	resp, body := env.get("/partials/notifications")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("partial poll = %d", resp.StatusCode)
	}
	var feed struct {
		Trails []struct {
			Key     string `json:"key"`
			Outcome string `json:"outcome"`
			Steps   []struct {
				Kind string `json:"kind"`
				Text string `json:"text"`
			} `json:"steps"`
		} `json:"trails"`
	}
	if err := json.Unmarshal([]byte(body), &feed); err != nil {
		t.Fatalf("feed is not valid JSON: %v", err)
	}
	if len(feed.Trails) != 1 || feed.Trails[0].Key != "imgw:1" ||
		feed.Trails[0].Outcome != "delivered" || len(feed.Trails[0].Steps) != 5 {
		t.Fatalf("feed = %s", body)
	}
	kinds := ""
	for _, s := range feed.Trails[0].Steps {
		kinds += s.Kind + ","
	}
	if kinds != "received,matched,route,submitted,delivered," {
		t.Errorf("feed step kinds = %q", kinds)
	}

	// Focus deep link (?key=…) renders the same trail.
	_, html = env.get("/notifications?key=imgw:1")
	if !strings.Contains(html, `id="notif-imgw:1"`) {
		t.Errorf("focused page missing trail: %s", html)
	}
}

// TestHealthFlow pins the system health page: login required, the rows
// render, and the verdict is degraded only when something actually is
// (here: the test receiver never connects).
func TestHealthFlow(t *testing.T) {
	env := newTestEnv(t)

	// Unauthenticated: redirect to the login page.
	resp, _ := env.get("/health")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /health unauthenticated = %d %q, want redirect", resp.StatusCode, resp.Header.Get("Location"))
	}

	env.login()
	_, html := env.get("/health")
	for _, want := range []string{
		`id="health-section"`,
		`<span class="nav-label">Health</span>`,
		"System health",
		"DEGRADED", // receiver "local" is configured but never dialed in tests
		">imgw-warnings</strong>",
		">OK</span>", // the running source
		">DISCONNECTED</span>",
		">Database</strong>",
		"ready",
		">Dispatch queue</strong>",
		">Pending notifications</strong>",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("health page missing %q: %s", want, html)
		}
	}

	// Refresh partial carries the same section markup.
	resp, body := env.get("/partials/health")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("partial poll = %d", resp.StatusCode)
	}
	if !strings.Contains(body, `id="health-section"`) || !strings.Contains(body, "Dispatch queue") {
		t.Errorf("health partial = %s", body)
	}
}

// fakeIngestProbe implements the ingestProbe surface for the health page.
type fakeIngestProbe struct {
	id        string
	connected bool
	started   bool
	counters  ingesthttp.Counters
}

func (f *fakeIngestProbe) ID() string                                       { return f.id }
func (f *fakeIngestProbe) Connected() bool                                  { return f.connected }
func (f *fakeIngestProbe) Started() bool                                    { return f.started }
func (f *fakeIngestProbe) Counters() ingesthttp.Counters                    { return f.counters }
func (f *fakeIngestProbe) ServeHTTP(w http.ResponseWriter, r *http.Request) {}

// TestHealthIngestRow pins the ingest endpoint metrics line.
func TestHealthIngestRow(t *testing.T) {
	probe := &fakeIngestProbe{
		id: "news", connected: true, started: true,
		counters: ingesthttp.Counters{Accepted: 7, Rejected: 2, AuthFailed: 1, RateLimited: 0},
	}
	env := newTestEnvWithIngest(t, map[string]http.Handler{"news": probe})

	env.login()
	_, html := env.get("/health")
	for _, want := range []string{
		">news (ingest)</strong>",
		">OK</span>",
		"accepted=7 rejected=2 auth_failed=1 rate_limited=0 forbidden=0",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("health page missing ingest row %q: %s", want, html)
		}
	}
}

// TestPublicHomePage pins the public landing page: header1/header2 and the
// active-hazard list without any session; the login form lives behind the
// icon button at /login. The partial is public too (auto-refresh).
func TestPublicHomePage(t *testing.T) {
	env := newTestEnv(t)

	now := time.Now()
	if err := env.state.AddOrUpdateActive("local", "warnflux/active/imgw-meteo/aaaa", state.Hazard{
		EventKey:  "imgw-meteo:1",
		Source:    "imgw-meteo",
		Event:     "Burze",
		Severity:  "moderate",
		Headline:  "Umiarkowane burze",
		Areas:     []string{"powiat wielicki"},
		Status:    "active",
		UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := env.state.AddOrUpdateActive("local", "warnflux/active/imgw-meteo/bbbb", state.Hazard{
		EventKey:  "imgw-meteo:2",
		Source:    "imgw-meteo",
		Event:     "Wiatr",
		Severity:  "extreme",
		Headline:  "Ekstremalny wiatr",
		Areas:     []string{"powiat bocheński"},
		Status:    "active",
		UpdatedAt: now.Add(-time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	// Low-priority road noise: must land in the collapsed minor section.
	if err := env.state.AddOrUpdateActive("local", "warnflux/active/gddkia/cccc", state.Hazard{
		EventKey:  "gddkia:3",
		Source:    "gddkia",
		Event:     "Road works",
		Severity:  "minor",
		Headline:  "Drobne prace drogowe",
		Areas:     []string{"droga:79"},
		Status:    "active",
		UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	// Unauthenticated: the home page is public.
	resp, html := env.get("/")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / = %d, want 200 without login", resp.StatusCode)
	}
	for _, want := range []string{
		"WarnFlux Test",            // header1
		"Test platform",            // header2
		"Test info text.",          // configurable about text (one-time popup)
		`href="https://sp9moa.pl"`, // HTML links are allowed in the about text
		`id="about-dialog"`,        // one-time about popup
		`class="home-disclaimer"`,  // unofficial-system notice
		"Test disclaimer text.",
		"Ekstremalny wiatr", // most severe first
		`href="/login"`,     // sign-in behind the icon button
		"Active hazards",
		"Map", // combined stations + weather tab
		`id="home-alerts"`,
		`class="theme-toggle"`, // light/dark switch
	} {
		if !strings.Contains(html, want) {
			t.Errorf("home page missing %q: %s", want, html)
		}
	}
	// The about text lives in the one-time popup dialog at the end of the
	// page — not in the flow above the tabs.
	aboutAt := strings.Index(html, "Test info text.")
	tabsAt := strings.Index(html, `class="home-tabs"`)
	if aboutAt < 0 || tabsAt < 0 || aboutAt < tabsAt {
		t.Errorf("about text must live in the popup dialog after the tabs: about@%d tabs@%d", aboutAt, tabsAt)
	}
	if strings.Contains(html, `name="csrf"`) {
		t.Error("home page must not carry login form state (login is behind the icon button)")
	}
	// No APRS hub in this environment: the tab shows the disabled note,
	// never the map.
	if strings.Contains(html, `id="aprs-map"`) {
		t.Error("home page must not render the APRS map when the hub is disabled")
	}
	if !strings.Contains(html, "Radio stations are not enabled") {
		t.Error("home page should explain that radio stations are disabled: " + html)
	}
	extremeAt := strings.Index(html, "Ekstremalny wiatr")
	moderateAt := strings.Index(html, "Umiarkowane burze")
	if extremeAt < 0 || moderateAt < 0 || extremeAt > moderateAt {
		t.Errorf("hazards not ordered by severity: extreme@%d moderate@%d", extremeAt, moderateAt)
	}

	// The important (moderate+) section is always rendered and open by
	// default; the minor hazard lives in the separate collapsed details.
	importantAt := strings.Index(html, `<details class="home-section home-important" open>`)
	if importantAt < 0 {
		t.Error("important section missing or not open by default")
	}
	if strings.Contains(html, "No active messages.") {
		t.Error("important section must not show the empty note while hazards are active")
	}

	minorAt := strings.Index(html, "Drobne prace drogowe")
	detailsAt := strings.Index(html, `<details class="home-section home-minor">`)
	if minorAt < 0 || detailsAt < 0 || minorAt < detailsAt {
		t.Errorf("minor hazard must sit inside the collapsed details: minor@%d details@%d",
			minorAt, detailsAt)
	}
	if strings.Contains(html, `<details class="home-section home-minor" open`) {
		t.Error("minor section must be collapsed by default")
	}
	if !strings.Contains(html, "Informational") {
		t.Error("minor section summary missing")
	}
	if !strings.Contains(html, `class="count">1</span>`) {
		t.Error("minor section should carry its own count badge of 1")
	}

	// The auto-refresh fragment is public as well.
	resp, body := env.get("/partials/home")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /partials/home = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(body, `id="home-alerts"`) || !strings.Contains(body, "Ekstremalny wiatr") {
		t.Errorf("home partial = %s", body)
	}

	// The admin area still requires login.
	resp, _ = env.get("/dashboard")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Errorf("GET /dashboard unauthenticated = %d %q, want redirect to /login",
			resp.StatusCode, resp.Header.Get("Location"))
	}

	// A logged-in operator sees a Dashboard entry (avatar + label) in the
	// home header instead of the sign-in icon.
	env.login()
	_, html = env.get("/")
	if !strings.Contains(html, `href="/dashboard"`) || !strings.Contains(html, "Dashboard") {
		t.Errorf("home header missing dashboard entry when logged in: %s", html)
	}
	if strings.Contains(html, `href="/login"`) {
		t.Errorf("home header must drop the sign-in icon when logged in: %s", html)
	}
}

// TestPublicHomeMinorOnly pins the collapsed-section rendering when every
// active hazard is low priority: no empty-state box (the page is not
// "empty"), just the collapsed minor details with its own count badge.
func TestPublicHomeMinorOnly(t *testing.T) {
	env := newTestEnv(t)

	if err := env.state.AddOrUpdateActive("local", "warnflux/active/gddkia/only", state.Hazard{
		EventKey:  "gddkia:only",
		Source:    "gddkia",
		Event:     "Road works",
		Severity:  "minor",
		Headline:  "Tylko prace drogowe",
		Areas:     []string{"droga:79"},
		Status:    "active",
		UpdatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	resp, html := env.get("/")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(html, `<details class="home-section home-minor">`) {
		t.Error("collapsed minor details missing")
	}
	if !strings.Contains(html, "Tylko prace drogowe") {
		t.Error("minor hazard headline missing")
	}
	if strings.Contains(html, "No active hazards.") {
		t.Error("the old full-page empty box must be gone")
	}
	// The important section is empty but always visible: it carries the
	// no-active-messages note.
	if !strings.Contains(html, "No active messages.") {
		t.Error("minor-only page must carry the no-active-messages note in the important section")
	}
}

// TestPublicHomeAllEmpty pins the fully empty home page: the important
// section renders with the no-active-messages note and no minor details
// exist.
func TestPublicHomeAllEmpty(t *testing.T) {
	env := newTestEnv(t)

	resp, html := env.get("/")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(html, `<details class="home-section home-important" open>`) {
		t.Error("important section missing")
	}
	if !strings.Contains(html, "No active messages.") {
		t.Error("empty important section must say there are no active messages")
	}
	if strings.Contains(html, "home-section home-minor") {
		t.Error("minor details must not render when there are no minor hazards")
	}
}

// TestHomeAPRSMapTab pins the APRS-enabled second home tab: the map
// container with our locator/radius data attributes and the attribution
// line, plus the public stations endpoint that feeds it.
func TestHomeAPRSMapTab(t *testing.T) {
	hub, err := aprs.NewHub(aprs.HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		Icon:       "/j",
		GridSquare: "JO90WW",
		RadiusKM:   25,
		StationTTL: 30 * time.Minute,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	env := newTestEnvWithHub(t, hub)
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	_, html := env.get("/")
	if !strings.Contains(html, `id="aprs-map"`) {
		t.Fatalf("home page missing the APRS map container: %s", html)
	}
	for _, want := range []string{
		`data-lat="50.9375"`,
		`data-lon="19.875"`,
		`data-own-lat="50.9375"`, // locator dot: gridsquare center until a beacon is heard
		`data-own-lon="19.875"`,
		`data-radius="25"`,
		`data-callsign="SP9MOA-10"`,
		"RainViewer", // radar attribution under the map
		"OpenStreetMap",
		"aprs-symbols",                  // APRS symbol attribution under the map
		`data-tab="tab-radio"`,          // the combined Map tab
		"weather-icons.css",             // weather icon set for the map layer
		"Weather Icons by Erik Flowers", // icon attribution
		`id="hw-reports"`,               // weather report cards below the map
	} {
		if !strings.Contains(html, want) {
			t.Errorf("home APRS tab missing %q: %s", want, html)
		}
	}

	// The bundled APRS symbol sprites are served and embedded.
	resp, _ := env.get("/static/aprs-symbols/aprs-symbols-24-0@2x.png")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET APRS symbol sprite = %d, want 200", resp.StatusCode)
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "image/png") {
		t.Errorf("sprite Content-Type = %q, want image/png", resp.Header.Get("Content-Type"))
	}

	// Embedded assets must revalidate on every request: no validators
	// exist, so stale-cache prevention relies on Cache-Control.
	resp, _ = env.get("/static/app.js")
	if got := resp.Header.Get("Cache-Control"); got != "no-cache" {
		t.Errorf("app.js Cache-Control = %q, want no-cache", got)
	}

	// Regression: a closed about dialog must be hidden even though
	// .about-dialog sets display: flex (which would override the UA
	// dialog:not([open]) rule and keep the popup visible forever).
	_, css := env.get("/static/style.css")
	if !strings.Contains(css, ".about-dialog:not([open]) { display: none; }") {
		t.Error("style.css is missing the .about-dialog:not([open]) hide rule")
	}

	// Stations endpoint: public, JSON list of merged station documents.
	hub.Observe(aprs.ParseFeedLine("SP9XYZ-7>APRS,TCPIP*:!5056.25N/01952.50E-", time.Now()), "aprs-inet")
	var got []aprs.StationDocument
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, body := env.get("/api/aprs/stations")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /api/aprs/stations = %d", resp.StatusCode)
		}
		got = nil
		if err := json.Unmarshal([]byte(body), &got); err != nil {
			t.Fatalf("stations payload = %s: %v", body, err)
		}
		if len(got) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(got) != 1 || got[0].Callsign != "SP9XYZ-7" || got[0].Position == nil {
		t.Fatalf("stations = %+v, want SP9XYZ-7 with a position", got)
	}
}

// TestComposeFlow pins the officer-facing communication module: the form
// publishes onto the broker (fake here), the issued list renders the
// module's communications, edits update the same event key and expire
// removes it.
// TestPublicWeatherAPI pins the public weather tab data: internet reports
// and forecasts from the retained info state plus APRS weather stations
// (with position for the mini-map popup).
func TestPublicWeatherAPI(t *testing.T) {
	hub, err := aprs.NewHub(aprs.HubConfig{
		Enabled:    true,
		Callsign:   "SP9MOA-10",
		Icon:       "/j",
		GridSquare: "JO90WW",
		RadiusKM:   25,
		StationTTL: 30 * time.Minute,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	env := newTestEnvWithHub(t, hub)
	ctx, cancel := context.WithCancel(context.Background())
	hub.Start(ctx)
	defer cancel()

	hub.Observe(aprs.ParseFeedLine("SP9WX>APRS,TCPIP*:!5056.25N/01952.50E_220/004g005t077r000p000P000h50b09900 X-Ray 0.12uSv/h", time.Now()), "aprs-inet")

	temp, tmax, tmin := 21.4, 24.0, 14.0
	env.state.AddOrUpdateInfo("local", "warnflux/info/openmeteo/weather-home/home/weather", state.InfoEntry{
		Source:     "openmeteo",
		ProducerID: "weather-home",
		Key:        "home",
		Kind:       "weather",
		ReceivedAt: time.Now(),
		Weather: &state.Weather{
			GeneratedAt:  time.Now(),
			LocationID:   "home",
			LocationName: "Niepołomice",
			Latitude:     50.03,
			Longitude:    20.22,
			TemperatureC: &temp,
			Condition:    "partly_cloudy",
			Daily: []state.DailyWeather{{
				Date:            "2026-09-25",
				Condition:       "rain",
				TemperatureMaxC: &tmax,
				TemperatureMinC: &tmin,
			}},
		},
	})

	var view struct {
		Reports []struct {
			Name          string   `json:"name"`
			Via           string   `json:"via"`
			TemperatureC  *float64 `json:"temperature_c"`
			RadiationUSvh *float64 `json:"radiation_usv_h"`
		} `json:"reports"`
		Forecasts []struct {
			Name  string `json:"name"`
			Daily []struct {
				Condition string `json:"condition"`
			} `json:"daily"`
		} `json:"forecasts"`
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, body := env.get("/api/weather")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /api/weather = %d", resp.StatusCode)
		}
		view = struct {
			Reports []struct {
				Name          string   `json:"name"`
				Via           string   `json:"via"`
				TemperatureC  *float64 `json:"temperature_c"`
				RadiationUSvh *float64 `json:"radiation_usv_h"`
			} `json:"reports"`
			Forecasts []struct {
				Name  string `json:"name"`
				Daily []struct {
					Condition string `json:"condition"`
				} `json:"daily"`
			} `json:"forecasts"`
		}{}
		if err := json.Unmarshal([]byte(body), &view); err != nil {
			t.Fatalf("weather payload: %s: %v", body, err)
		}
		if len(view.Reports) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	byName := map[string]struct {
		via           string
		radiationUSvh *float64
	}{}
	for _, r := range view.Reports {
		byName[r.Name] = struct {
			via           string
			radiationUSvh *float64
		}{via: r.Via, radiationUSvh: r.RadiationUSvh}
	}
	wx, ok := byName["SP9WX"]
	if !ok || wx.via != "aprs" || wx.radiationUSvh == nil || *wx.radiationUSvh != 0.12 {
		t.Fatalf("APRS weather report missing or wrong: %+v", view.Reports)
	}
	home, ok := byName["Niepołomice"]
	if !ok || home.via != "internet" {
		t.Fatalf("internet weather report missing or wrong: %+v", view.Reports)
	}
	if len(view.Forecasts) != 1 || view.Forecasts[0].Name != "Niepołomice" ||
		len(view.Forecasts[0].Daily) != 1 || view.Forecasts[0].Daily[0].Condition != "rain" {
		t.Fatalf("forecasts = %+v", view.Forecasts)
	}

	// Weather stations are excluded from the neighbourhood map endpoint
	// (they live on the weather tab map instead).
	resp, body := env.get("/api/aprs/stations")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/aprs/stations = %d", resp.StatusCode)
	}
	var stations []aprs.StationDocument
	if err := json.Unmarshal([]byte(body), &stations); err != nil {
		t.Fatalf("stations payload: %v", err)
	}
	for _, st := range stations {
		if st.Callsign == "SP9WX" {
			t.Fatalf("weather station SP9WX still on the APRS map endpoint: %s", body)
		}
	}
}

func TestComposeFlow(t *testing.T) {
	env := newTestEnv(t)

	// The page requires a session like every admin page.
	resp, _ := env.get("/compose")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /compose unauthenticated = %d %q, want redirect to /login",
			resp.StatusCode, resp.Header.Get("Location"))
	}

	env.login()
	_, html := env.get("/compose")
	if !strings.Contains(html, "Compose communication") {
		t.Errorf("compose page missing form heading: %s", html)
	}
	if !strings.Contains(html, `<span class="nav-label">Compose</span>`) {
		t.Errorf("compose page missing sidebar entry: %s", html)
	}
	if !strings.Contains(html, "Issued communications") {
		t.Errorf("compose page missing issued list: %s", html)
	}
	if !strings.Contains(html, `id="compose-debug-fill"`) {
		t.Errorf("compose page missing debug fill button: %s", html)
	}
	if !strings.Contains(html, `/static/app.js`) {
		t.Errorf("compose page missing app.js (debug fill and theme toggle need it): %s", html)
	}
	if !strings.Contains(html, `name="latitude"`) || !strings.Contains(html, `name="longitude"`) || !strings.Contains(html, `id="compose-loc-clear"`) {
		t.Errorf("compose page missing the location fields: %s", html)
	}
	// The picker map renders only with an enabled APRS hub (its center
	// anchors the picker); the test env has no hub.
	if strings.Contains(html, `id="compose-map"`) {
		t.Errorf("compose map must be hidden without an APRS hub: %s", html)
	}
	csrf := extractCSRF(t, html)

	// CSRF is enforced on both mutations.
	resp, _ = env.postForm("/compose", url.Values{"event": {"Flood"}, "headline": {"x"}, "severity": {"severe"}})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /compose without csrf = %d, want 403", resp.StatusCode)
	}

	// Publish a new communication. The timestamps are relative to the
	// test clock: a fixed expiry would rot the test once it drifts into
	// the past (the compose module auto-expires past-dated items).
	effStr := time.Now().Add(-2 * time.Hour).Format("2006-01-02T15:04")
	expStr := time.Now().Add(6 * time.Hour).Format("2006-01-02T15:04")
	resp, _ = env.postForm("/compose", url.Values{
		"csrf":         {csrf},
		"event":        {"Flood"},
		"headline":     {"Flood warning for the Raba river"},
		"severity":     {"severe"},
		"urgency":      {"immediate"},
		"certainty":    {"observed"},
		"status":       {"active"},
		"areas":        {"wieliczka, niepolomice"},
		"latitude":     {"49.985"},
		"longitude":    {"20.065"},
		"effective_at": {effStr},
		"expires_at":   {expStr},
		"description":  {"Heavy rain may cause local flooding."},
		"instruction":  {"Avoid the river bank."},
	})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/compose?msg=published" {
		t.Fatalf("POST /compose = %d %q, want redirect to flash", resp.StatusCode, resp.Header.Get("Location"))
	}
	if len(env.pub.published) != 1 {
		t.Fatalf("publisher saw %d publishes, want 1", len(env.pub.published))
	}
	h := env.pub.published[0]
	if h.Source != "compose" || !strings.HasPrefix(h.EventKey, "compose:") {
		t.Errorf("published hazard identity = %q / %q", h.Source, h.EventKey)
	}
	if h.Severity != "severe" || h.Headline != "Flood warning for the Raba river" || len(h.Areas) != 2 {
		t.Errorf("published hazard = %+v", h)
	}
	if h.EffectiveAt == nil || h.EffectiveAt.Format("2006-01-02T15:04") != effStr {
		t.Errorf("effective_at = %v, want %s", h.EffectiveAt, effStr)
	}
	if h.Latitude == nil || h.Longitude == nil || *h.Latitude != 49.985 || *h.Longitude != 20.065 {
		t.Errorf("published coordinates = %v, %v", h.Latitude, h.Longitude)
	}

	// Lone coordinates are rejected.
	resp, _ = env.postForm("/compose", url.Values{
		"csrf": {csrf}, "event": {"Flood"}, "headline": {"x"},
		"severity": {"severe"}, "latitude": {"49.985"},
	})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("lone latitude = %d, want 422", resp.StatusCode)
	}
	// Out-of-range coordinates are rejected.
	resp, _ = env.postForm("/compose", url.Values{
		"csrf": {csrf}, "event": {"Flood"}, "headline": {"x"},
		"severity": {"severe"}, "latitude": {"99"}, "longitude": {"20"},
	})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("out-of-range coordinates = %d, want 422", resp.StatusCode)
	}

	// The publish also feeds the canonical ingress so group routing fires,
	// with the coordinates riding along for notifications.
	if ev := drainIngress(env); ev == nil || ev.Hazard == nil || ev.Hazard.Type != dispatch.TransitionNew || ev.Hazard.Key != h.EventKey {
		t.Fatalf("compose publish did not enqueue a new transition: %+v", ev)
	} else if ev.Hazard.Hazard.Latitude == nil || *ev.Hazard.Hazard.Latitude != 49.985 {
		t.Errorf("compose transition lost coordinates: %+v", ev.Hazard.Hazard)
	}

	// Simulate the broker loopback: the ingestor mirrors the document.
	if err := env.state.AddOrUpdateActive("local", "warnflux/active/compose/aaaa", h); err != nil {
		t.Fatal(err)
	}

	// The issued list renders it with an edit link (html/template
	// URL-escapes the colon; the query decodes it back server-side).
	_, html = env.get("/compose")
	if !strings.Contains(html, "Flood warning for the Raba river") {
		t.Errorf("issued list missing headline: %s", html)
	}
	if !strings.Contains(html, "/compose?edit=compose") {
		t.Errorf("issued list missing edit link: %s", html)
	}

	// Edit prefills the form.
	_, html = env.get("/compose?edit=" + h.EventKey)
	if !strings.Contains(html, `value="Flood warning for the Raba river"`) {
		t.Errorf("edit page missing prefilled headline: %s", html)
	}
	if !strings.Contains(html, `value="49.98500"`) || !strings.Contains(html, `value="20.06500"`) {
		t.Errorf("edit page missing prefilled coordinates: %s", html)
	}
	if !strings.Contains(html, `name="event_key" value="`+h.EventKey+`"`) {
		t.Errorf("edit page missing hidden event key: %s", html)
	}

	// Update keeps the event key.
	csrf = extractCSRF(t, html)
	resp, _ = env.postForm("/compose", url.Values{
		"csrf":      {csrf},
		"event_key": {h.EventKey},
		"event":     {"Flood"},
		"headline":  {"Flood warning updated: level rising"},
		"severity":  {"extreme"},
		"status":    {"active"},
	})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/compose?msg=updated" {
		t.Fatalf("POST /compose update = %d %q, want updated flash", resp.StatusCode, resp.Header.Get("Location"))
	}
	if len(env.pub.published) != 2 || env.pub.published[1].EventKey != h.EventKey {
		t.Errorf("update did not reuse the event key: %+v", env.pub.published)
	}
	if ev := drainIngress(env); ev == nil || ev.Hazard == nil || ev.Hazard.Type != dispatch.TransitionUpdated {
		t.Fatalf("compose update did not enqueue an updated transition: %+v", ev)
	}

	// Expire removes it.
	csrf = extractCSRF(t, html)
	resp, _ = env.postForm("/compose/expire", url.Values{"csrf": {csrf}, "event_key": {h.EventKey}})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/compose?msg=expired" {
		t.Fatalf("POST /compose/expire = %d %q, want expired flash", resp.StatusCode, resp.Header.Get("Location"))
	}
	if len(env.pub.expired) != 1 || env.pub.expired[0] != h.EventKey {
		t.Errorf("expire did not target the event key: %v", env.pub.expired)
	}
	if ev := drainIngress(env); ev == nil || ev.Hazard == nil || ev.Hazard.Type != dispatch.TransitionExpired {
		t.Fatalf("compose expire did not enqueue an expired transition: %+v", ev)
	}

	// Unknown keys cannot be expired.
	resp, _ = env.postForm("/compose/expire", url.Values{"csrf": {csrf}, "event_key": {"compose:nope"}})
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("POST /compose/expire unknown key = %d, want 404", resp.StatusCode)
	}
}

// drainIngress non-blockingly reads one event from the test env's
// dispatch ingress (nil when empty).
func drainIngress(env *testEnv) *dispatch.Event {
	select {
	case e := <-env.ingress.Events():
		return &e
	default:
		return nil
	}
}

// TestMetricsEndpoint pins the Prometheus exposition: unauthenticated,
// text format, live gauges and the registry-held counters.
func TestMetricsEndpoint(t *testing.T) {
	probe := &fakeIngestProbe{
		id: "news", connected: true, started: true,
		counters: ingesthttp.Counters{Accepted: 5, AuthFailed: 1, RateLimited: 2},
	}
	env := newTestEnvWithIngest(t, map[string]http.Handler{"news": probe})

	// Registry counters: simulate an ingested event and a duplicate.
	env.metrics.Counter("warnflux_events_ingested_total", "Hazard events accepted into the journal (new, updated or cancelled).")(1)
	env.metrics.Counter("warnflux_events_duplicates_total", "Hazard events rejected as identical duplicates.")(2)

	// Unauthenticated: metrics must NOT require a session.
	resp, body := env.get("/metrics")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200 without login", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content type = %q", ct)
	}
	for _, want := range []string{
		`warnflux_source_polls_total{source="imgw-warnings"} 0`,
		`warnflux_source_errors_total{source="imgw-warnings"} 0`,
		`warnflux_events_filtered_total{source="imgw-warnings"} 0`,
		`warnflux_mqtt_connected{receiver="local"} 0`,
		`warnflux_dispatch_queue_depth 0`,
		`warnflux_pending_changes 3`,
		`warnflux_events_active 4`,
		`warnflux_ingest_http_requests_total{instance="news",result="accepted"} 5`,
		`warnflux_ingest_http_requests_total{instance="news",result="auth_failed"} 1`,
		`warnflux_ingest_http_requests_total{instance="news",result="rate_limited"} 2`,
		`warnflux_events_ingested_total 1`,
		`warnflux_events_duplicates_total 2`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q:\n%s", want, body)
		}
	}
}

// TestIngestEndpointRouting pins the public ingest route: requests are
// delegated to the matching instance handler without session auth, unknown
// ids 404, and the ingest id appears as a routing-matrix source row.
func TestIngestEndpointRouting(t *testing.T) {
	var hits int
	stub := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":true}`))
	})
	env := newTestEnvWithIngest(t, map[string]http.Handler{"news": stub})

	// The stub is called without any session cookie.
	req, _ := http.NewRequest(http.MethodPost, env.srv.URL+"/api/v1/ingest/news", strings.NewReader("{}"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || hits != 1 || !strings.Contains(string(body), "accepted") {
		t.Fatalf("ingest post = %d %q hits=%d, want 202 accepted and one delegation", resp.StatusCode, body, hits)
	}

	// Unknown endpoint id is a 404.
	req, _ = http.NewRequest(http.MethodPost, env.srv.URL+"/api/v1/ingest/bogus", strings.NewReader("{}"))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown ingest id = %d, want 404", resp.StatusCode)
	}

	// The groups routing popover offers the ingest id as a matrix source.
	env.login()
	if _, err := env.users.CreateGroup("ops"); err != nil {
		t.Fatal(err)
	}
	_, html := env.get("/groups")
	for _, want := range []string{
		`name="cell:news|logger-action"`,
		"news (ingest)",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("groups page missing ingest source %q: %s", want, html)
		}
	}
}

// TestEmcomRoleFlow pins the restricted emcom role: an emcom directory
// account signs in with its own password, lands on the shared dashboard,
// sees the Dashboard and Compose nav entries, and is redirected away from
// every admin page.
func TestEmcomRoleFlow(t *testing.T) {
	env := newTestEnv(t)
	if _, err := env.users.CreateUser("ops-user", "", "", "", "emcom", "password123"); err != nil {
		t.Fatal(err)
	}

	_, html := env.get("/login")
	csrf := extractCSRF(t, html)
	form := url.Values{"csrf": {csrf}, "username": {"ops-user"}, "password": {"password123"}}
	resp, _ := env.postForm("/login", form)
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
		t.Fatalf("emcom login = %d %q, want 303 to /dashboard", resp.StatusCode, resp.Header.Get("Location"))
	}

	resp, html = env.get("/compose")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /compose as emcom = %d", resp.StatusCode)
	}
	if !strings.Contains(html, `<span class="nav-label">Dashboard</span>`) {
		t.Error("emcom must see the Dashboard nav entry")
	}
	if !strings.Contains(html, `<span class="nav-label">Compose</span>`) {
		t.Error("compose page missing Compose nav entry")
	}
	if !strings.Contains(html, `href="/account"`) {
		t.Error("emcom must see the Account entry in the user menu")
	}
	for _, forbidden := range []string{"Users", "Groups", "Notifications"} {
		if strings.Contains(html, `<span class="nav-label">`+forbidden+`</span>`) {
			t.Errorf("emcom must not see %s nav entry", forbidden)
		}
	}

	// The dashboard itself is open to emcom too.
	resp, _ = env.get("/dashboard")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /dashboard as emcom = %d, want 200", resp.StatusCode)
	}

	for _, path := range []string{"/users", "/groups", "/health", "/logs", "/traffic", "/notifications"} {
		resp, _ := env.get(path)
		if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
			t.Errorf("GET %s as emcom = %d %q, want 303 to /dashboard", path, resp.StatusCode, resp.Header.Get("Location"))
		}
	}
}

// TestForgotPasswordFlow pins the standard self-service reset: request →
// one-time email token → new password → sign-in works. Unknown accounts
// get the same generic answer (no enumeration); the configured admin gets
// the explicit configuration message.
func TestForgotPasswordFlow(t *testing.T) {
	env := newTestEnv(t)
	if _, err := env.users.CreateUser("member1", "", "member1@example.com", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}
	var sentTo, sentSubject, sentText string
	env.server.SetPasswordResetMailer(func(to, subject, text string) error {
		sentTo, sentSubject, sentText = to, subject, text
		return nil
	})

	// The login page links the recovery flow.
	_, loginHTML := env.get("/login")
	if !strings.Contains(loginHTML, `href="/forgot"`) {
		t.Fatalf("login page missing the forgot-password link: %s", loginHTML)
	}

	// Unknown account: generic answer, no email.
	_, html := env.get("/forgot")
	resp, html := env.postForm("/forgot", url.Values{
		"csrf": {extractCSRF(t, html)}, "username": {"ghost"}, "email": {"ghost@example.com"},
	})
	if resp.StatusCode != http.StatusOK || !strings.Contains(html, "If the account exists") {
		t.Fatalf("unknown account = %d %s", resp.StatusCode, html)
	}
	if sentTo != "" {
		t.Fatalf("unknown account sent an email to %q", sentTo)
	}

	// The configured admin is explicitly out of scope.
	_, html = env.get("/forgot")
	resp, html = env.postForm("/forgot", url.Values{
		"csrf": {extractCSRF(t, html)}, "username": {testUsername}, "email": {"admin@example.com"},
	})
	if resp.StatusCode != http.StatusOK || !strings.Contains(html, "admin pass is defined in the configuration file") {
		t.Fatalf("admin forgot = %d %s", resp.StatusCode, html)
	}

	// Wrong email: generic answer, no email.
	_, html = env.get("/forgot")
	resp, _ = env.postForm("/forgot", url.Values{
		"csrf": {extractCSRF(t, html)}, "username": {"member1"}, "email": {"wrong@example.com"},
	})
	if sentTo != "" {
		t.Fatalf("wrong email sent an email to %q", sentTo)
	}

	// Correct request: one email with a token link.
	_, html = env.get("/forgot")
	resp, html = env.postForm("/forgot", url.Values{
		"csrf": {extractCSRF(t, html)}, "username": {"member1"}, "email": {"member1@example.com"},
	})
	if resp.StatusCode != http.StatusOK || !strings.Contains(html, "If the account exists") {
		t.Fatalf("forgot submit = %d %s", resp.StatusCode, html)
	}
	if sentTo != "member1@example.com" || sentSubject == "" || sentText == "" {
		t.Fatalf("mailer = %q / %q / %q", sentTo, sentSubject, sentText)
	}
	m := regexp.MustCompile(`token=([0-9a-f]+)`).FindStringSubmatch(sentText)
	if m == nil {
		t.Fatalf("email body has no token link: %s", sentText)
	}
	token := m[1]

	// The link renders the new-password form; the token is single-use.
	_, resetHTML := env.get("/reset?token=" + token)
	if !strings.Contains(resetHTML, `name="password"`) {
		t.Fatalf("reset page missing the password form: %s", resetHTML)
	}
	resp, _ = env.postForm("/reset", url.Values{
		"csrf": {extractCSRF(t, resetHTML)}, "token": {token}, "password": {"brand-new-password"},
	})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login?reset=1" {
		t.Fatalf("reset submit = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	if _, err := env.users.Authenticate("member1", "brand-new-password"); err != nil {
		t.Fatalf("new password does not authenticate: %v", err)
	}
	// Replay is rejected.
	resp, _ = env.postForm("/reset", url.Values{
		"csrf": {extractCSRF(t, resetHTML)}, "token": {token}, "password": {"another-password-1"},
	})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("token replay = %d, want 422", resp.StatusCode)
	}
}

// TestAdminResetPassword pins the admin-side "Reset password" action: a
// fresh random password is shown once; the configured admin row is
// blocked with the configuration message.
func TestAdminResetPassword(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	if _, err := env.users.CreateUser("member1", "", "", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}
	csrf := env.csrfFromPage("/users")

	// The configured admin row is read-only.
	resp, html := env.postForm("/users/1/reset", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusForbidden || !strings.Contains(html, "admin pass is defined in the configuration file") {
		t.Fatalf("admin reset = %d %s, want 403 with the configuration message", resp.StatusCode, html)
	}

	// A regular user gets a fresh password, shown once.
	resp, html = env.postForm("/users/2/reset", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusOK || !strings.Contains(html, "New password for member1:") {
		t.Fatalf("user reset = %d %s", resp.StatusCode, html)
	}
	m := regexp.MustCompile(`New password for member1: ([A-Za-z0-9]+) —`).FindStringSubmatch(html)
	if m == nil {
		t.Fatalf("reset notice missing the generated password: %s", html)
	}
	if _, err := env.users.Authenticate("member1", m[1]); err != nil {
		t.Fatalf("generated password does not authenticate: %v", err)
	}
}

// TestAdminAccountReadOnly pins the configuration-managed admin account:
// the self-service page renders the form disabled (no Save button) and the
// server rejects any direct POST to /account for the admin session.
func TestAdminAccountReadOnly(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	resp, html := env.get("/account")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /account as admin = %d", resp.StatusCode)
	}
	if !strings.Contains(html, "account-note") || !strings.Contains(html, "managed in configuration") {
		t.Errorf("account page missing the managed-account note: %s", html)
	}
	if !strings.Contains(html, `name="email" value="" maxlength="128" placeholder="you@example.com" disabled`) {
		t.Errorf("email field must be disabled for the admin: %s", html)
	}
	if !strings.Contains(html, `name="password" value="" autocomplete="new-password" placeholder="leave empty to keep the current one" disabled`) {
		t.Errorf("password field must be disabled for the admin: %s", html)
	}
	if strings.Contains(html, "Save changes") {
		t.Errorf("admin account page must not offer Save changes: %s", html)
	}

	// A direct POST is blocked server-side too.
	csrf := extractCSRF(t, html)
	resp, _ = env.postForm("/account", url.Values{
		"csrf": {csrf}, "email": {"hacker@example.com"}, "password": {"newpassword1"},
	})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /account as admin = %d, want 403", resp.StatusCode)
	}
}

// TestAdminAccountPageKeepsAdminNav pins the sidebar sync: the admin
// session must see the full admin navigation on the self-service account
// page too (the view uses the session role, not the directory row role —
// the admin row's role is always empty).
func TestAdminAccountPageKeepsAdminNav(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	resp, html := env.get("/account")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /account as admin = %d", resp.StatusCode)
	}
	for _, want := range []string{"Users", "Groups", "Health", "Logs", "Notifications"} {
		if !strings.Contains(html, `<span class="nav-label">`+want+`</span>`) {
			t.Errorf("admin /account sidebar missing %s nav entry", want)
		}
	}
}

// TestRouteAuthorizationMatrix pins server-side route protection: hiding
// links in the UI is cosmetic — every protected route must reject
// unauthorized sessions at the middleware level, for GET and POST alike.
func TestRouteAuthorizationMatrix(t *testing.T) {
	env := newTestEnv(t)
	if _, err := env.users.CreateUser("member1", "", "", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}
	if _, err := env.users.CreateUser("ops1", "", "", "", "emcom", "password123"); err != nil {
		t.Fatal(err)
	}

	adminPages := []string{"/users", "/groups", "/health", "/logs", "/audit", "/traffic", "/notifications"}
	adminPartials := []string{"/partials/logs", "/partials/audit", "/partials/traffic", "/partials/notifications", "/partials/health"}
	sharedPartials := []string{"/partials/status", "/partials/mqtt", "/partials/weather", "/partials/warnings", "/partials/plugins", "/partials/actions"}
	adminPosts := []string{"/users", "/users/2/delete", "/users/2/prefs", "/groups", "/groups/1/delete", "/groups/1/routing"}

	loginAs := func(user, pass string) {
		t.Helper()
		_, html := env.get("/login")
		form := url.Values{"csrf": {extractCSRF(t, html)}, "username": {user}, "password": {pass}}
		resp, _ := env.postForm("/login", form)
		if resp.StatusCode != http.StatusSeeOther {
			t.Fatalf("login %s = %d, want 303", user, resp.StatusCode)
		}
	}

	// Unauthenticated: pages redirect to /login, partials answer 401 and
	// POSTs are blocked before any handler runs.
	for _, p := range adminPages {
		resp, _ := env.get(p)
		if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
			t.Errorf("unauthenticated GET %s = %d %q, want 303 /login", p, resp.StatusCode, resp.Header.Get("Location"))
		}
	}
	for _, p := range append(append([]string{}, adminPartials...), sharedPartials...) {
		resp, _ := env.get(p)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("unauthenticated GET %s = %d, want 401", p, resp.StatusCode)
		}
	}
	for _, p := range adminPosts {
		resp, _ := env.postForm(p, nil)
		if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
			t.Errorf("unauthenticated POST %s = %d %q, want 303 /login", p, resp.StatusCode, resp.Header.Get("Location"))
		}
	}
	if resp, _ := env.get("/compose"); resp.StatusCode != http.StatusSeeOther {
		t.Errorf("unauthenticated GET /compose = %d, want 303", resp.StatusCode)
	}
	if resp, _ := env.get("/dashboard"); resp.StatusCode != http.StatusSeeOther {
		t.Errorf("unauthenticated GET /dashboard = %d, want 303", resp.StatusCode)
	}
	if resp, _ := env.get("/account"); resp.StatusCode != http.StatusSeeOther {
		t.Errorf("unauthenticated GET /account = %d, want 303", resp.StatusCode)
	}

	// Member: admin pages bounce to the dashboard, admin partials answer
	// 401, admin POSTs bounce, compose bounces; shared surfaces work.
	loginAs("member1", "password123")
	for _, p := range adminPages {
		resp, _ := env.get(p)
		if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
			t.Errorf("member GET %s = %d %q, want 303 /dashboard", p, resp.StatusCode, resp.Header.Get("Location"))
		}
	}
	for _, p := range adminPartials {
		resp, _ := env.get(p)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("member GET %s = %d, want 401", p, resp.StatusCode)
		}
	}
	for _, p := range adminPosts {
		resp, _ := env.postForm(p, nil)
		if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
			t.Errorf("member POST %s = %d %q, want 303 /dashboard", p, resp.StatusCode, resp.Header.Get("Location"))
		}
	}
	for _, p := range []string{"/compose"} {
		resp, _ := env.get(p)
		if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
			t.Errorf("member GET %s = %d %q, want 303 /dashboard", p, resp.StatusCode, resp.Header.Get("Location"))
		}
	}
	for _, p := range append(sharedPartials, "/dashboard", "/account") {
		resp, _ := env.get(p)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("member GET %s = %d, want 200", p, resp.StatusCode)
		}
	}

	// Emcom: same admin walls; compose opens.
	env.logout()
	loginAs("ops1", "password123")
	for _, p := range adminPages {
		resp, _ := env.get(p)
		if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
			t.Errorf("emcom GET %s = %d %q, want 303 /dashboard", p, resp.StatusCode, resp.Header.Get("Location"))
		}
	}
	for _, p := range adminPartials {
		resp, _ := env.get(p)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("emcom GET %s = %d, want 401", p, resp.StatusCode)
		}
	}
	for _, p := range adminPosts {
		resp, _ := env.postForm(p, nil)
		if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
			t.Errorf("emcom POST %s = %d %q, want 303 /dashboard", p, resp.StatusCode, resp.Header.Get("Location"))
		}
	}
	if resp, _ := env.get("/compose"); resp.StatusCode != http.StatusOK {
		t.Errorf("emcom GET /compose = %d, want 200", resp.StatusCode)
	}

	// Admin: everything listed above opens.
	env.logout()
	env.login()
	for _, p := range append(append(append([]string{}, adminPages...), adminPartials...), sharedPartials...) {
		resp, _ := env.get(p)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("admin GET %s = %d, want 200", p, resp.StatusCode)
		}
	}
}

// TestMemberRoleFlow pins the self-service member role: a member signs in
// with their own password, lands on the shared dashboard, sees only the
// Dashboard nav entry, and is redirected away from compose and every admin
// page.
func TestMemberRoleFlow(t *testing.T) {
	env := newTestEnv(t)
	ops, err := env.users.CreateGroup("ops")
	if err != nil {
		t.Fatal(err)
	}
	hams, err := env.users.CreateGroup("hams")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.users.CreateUser("plain-user", "", "", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}

	_, html := env.get("/login")
	csrf := extractCSRF(t, html)
	form := url.Values{"csrf": {csrf}, "username": {"plain-user"}, "password": {"password123"}}
	resp, _ := env.postForm("/login", form)
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
		t.Fatalf("member login = %d %q, want 303 to /dashboard", resp.StatusCode, resp.Header.Get("Location"))
	}

	// The dashboard is open to members too.
	resp, html = env.get("/dashboard")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard as member = %d", resp.StatusCode)
	}
	if !strings.Contains(html, `<span class="nav-label">Dashboard</span>`) {
		t.Error("dashboard page missing the Dashboard nav entry")
	}

	resp, html = env.get("/account")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /account as member = %d", resp.StatusCode)
	}
	if !strings.Contains(html, `href="/account"`) {
		t.Error("account page missing the Account entry in the user menu")
	}
	if !strings.Contains(html, `id="user-menu-panel"`) {
		t.Error("account page missing the user menu panel")
	}
	if !strings.Contains(html, `name="phone"`) || !strings.Contains(html, `name="email"`) {
		t.Error("account page missing the self-service contact form")
	}
	// Default subscription: every channel checkbox is checked.
	for _, g := range []storage.Group{ops, hams} {
		want := fmt.Sprintf(`name="groups" value="%d" checked`, g.ID)
		if !strings.Contains(html, want) {
			t.Errorf("account page missing checked channel %s (%d)", g.Name, g.ID)
		}
	}
	// Default delivery channels: every known medium is enabled.
	for _, kind := range []string{"aprs", "smtp"} {
		want := fmt.Sprintf(`name="channels" value="%s" checked`, kind)
		if !strings.Contains(html, want) {
			t.Errorf("account page missing checked delivery channel %s", kind)
		}
	}
	for _, forbidden := range []string{"Compose", "Users", "Groups"} {
		if strings.Contains(html, `<span class="nav-label">`+forbidden+`</span>`) {
			t.Errorf("member must not see %s nav entry", forbidden)
		}
	}

	// Compose and every admin page redirect the member to the dashboard.
	for _, path := range []string{"/compose", "/users", "/groups", "/health", "/logs", "/traffic", "/notifications"} {
		resp, _ := env.get(path)
		if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
			t.Errorf("GET %s as member = %d %q, want 303 to /dashboard", path, resp.StatusCode, resp.Header.Get("Location"))
		}
	}

	// Self-service save: contact fields update, role stays member, and
	// only the channels still checked stay subscribed.
	csrf2 := extractCSRF(t, html)
	form = url.Values{
		"csrf": {csrf2}, "phone": {"600700800"}, "email": {"member@example.com"},
		"password": {""}, "groups": {strconv.FormatInt(hams.ID, 10)},
		// Only aprs stays checked: smtp becomes a per-user opt-out.
		"channels": {"aprs"},
	}
	resp, _ = env.postForm("/account", form)
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/account?msg=saved" {
		t.Fatalf("account save = %d %q, want 303 with saved flash", resp.StatusCode, resp.Header.Get("Location"))
	}
	u, err := env.users.GetUserByUsername("plain-user")
	if err != nil {
		t.Fatal(err)
	}
	if u.Phone != "600700800" || u.Email != "member@example.com" || u.Role != "member" {
		t.Errorf("after save = phone %q email %q role %q", u.Phone, u.Email, u.Role)
	}
	ids, err := env.users.GroupIDsForUser(u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != hams.ID {
		t.Errorf("after save memberships = %v, want only %d (ops unsubscribed)", ids, hams.ID)
	}
	opts, err := env.users.UserChannelOptOuts(u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !opts["smtp"] || opts["aprs"] {
		t.Errorf("after save channel opt-outs = %v, want smtp disabled and aprs enabled", opts)
	}
}

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

// logout ends the current session (the CSRF token comes from any rendered
// page that embeds it, e.g. the dashboard).
func (e *testEnv) logout() {
	e.t.Helper()
	_, html := e.get("/dashboard")
	resp, _ := e.postForm("/logout", url.Values{"csrf": {extractCSRF(e.t, html)}})
	if resp.StatusCode != http.StatusSeeOther {
		e.t.Fatalf("logout = %d", resp.StatusCode)
	}
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
	usvh := 0.12
	cpm := 15.0
	env.state.AddOrUpdateInfo("local", "warnflux/info/openmeteo/weather-home/home/weather", state.InfoEntry{
		Source:     "openmeteo",
		ProducerID: "weather-home",
		Key:        "home",
		Kind:       "weather",
		ReceivedAt: time.Now(),
		Weather: &state.Weather{
			GeneratedAt:   time.Now(),
			LocationID:    "home",
			LocationName:  "Home",
			TemperatureC:  &temp,
			Condition:     "partly_cloudy",
			RadiationUSvh: &usvh,
			RadiationCPM:  &cpm,
		},
	})

	_, html := env.get("/partials/weather")
	if !strings.Contains(html, "21.4") || !strings.Contains(html, "partly_cloudy") {
		t.Errorf("weather not rendered: %s", html)
	}
	if !strings.Contains(html, "local") {
		t.Errorf("receiver origin not rendered with weather: %s", html)
	}
	if !strings.Contains(html, "Radiation 0.12 µSv/h") {
		t.Errorf("radiation (µSv/h) not rendered: %s", html)
	}
	if !strings.Contains(html, "Radiation 15 cpm") {
		t.Errorf("radiation (cpm) not rendered: %s", html)
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
	if !strings.Contains(loginHTML, `class="footer-h2">Test platform</h2>`) {
		t.Errorf("login footer missing header2 lead: %s", loginHTML)
	}
	if !strings.Contains(loginHTML, "Account creation is disabled.") {
		t.Errorf("login page missing account creation note: %s", loginHTML)
	}
	if !strings.Contains(loginHTML, "Back to the public page") || !strings.Contains(loginHTML, `href="/"`) {
		t.Errorf("login page missing back link to the public page: %s", loginHTML)
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
	if !strings.Contains(dashHTML, `class="footer-h2">Test platform</h2>`) {
		t.Errorf("dashboard footer missing header2 lead: %s", dashHTML)
	}
	if !strings.Contains(dashHTML, `<span class="footer-warnflux">WarnFlux</span>`) {
		t.Errorf("dashboard footer missing WarnFlux item: %s", dashHTML)
	}
	if !strings.Contains(dashHTML, `href="https://github.com/szporwolik/WarnFlux/commit/abc1234"`) {
		t.Errorf("dashboard footer missing commit link: %s", dashHTML)
	}
	if !strings.Contains(dashHTML, `class="gh-link"`) {
		t.Errorf("dashboard footer missing GitHub link: %s", dashHTML)
	}
	if !strings.Contains(dashHTML, `class="theme-toggle"`) {
		t.Errorf("dashboard topbar missing theme toggle: %s", dashHTML)
	}
	if !strings.Contains(dashHTML, `class="topbar-home"`) || !strings.Contains(dashHTML, `href="/"`) {
		t.Errorf("dashboard topbar missing public page link: %s", dashHTML)
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
	if !strings.Contains(usersHTML, `class="footer-h2">Test platform</h2>`) {
		t.Errorf("users footer missing header2 lead: %s", usersHTML)
	}
	_, groupsHTML := env.get("/groups")
	if !strings.Contains(groupsHTML, `class="footer-h2">Test platform</h2>`) {
		t.Errorf("groups footer missing header2 lead: %s", groupsHTML)
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

func TestLoginThrottled(t *testing.T) {
	env := newTestEnv(t)
	_, html := env.get("/login")
	csrf := extractCSRF(t, html)
	form := url.Values{"csrf": {csrf}, "username": {testUsername}, "password": {"wrong-password"}}
	for i := 0; i < 5; i++ {
		resp, _ := env.postForm("/login", form)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401", i+1, resp.StatusCode)
		}
	}
	resp, _ := env.postForm("/login", form)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("throttled login = %d, want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("429 must carry Retry-After")
	}
}

func TestSecurityHeaders(t *testing.T) {
	env := newTestEnv(t)
	resp, _ := env.get("/")
	for h, want := range map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "no-referrer",
	} {
		if got := resp.Header.Get(h); got != want {
			t.Errorf("%s = %q, want %q", h, got, want)
		}
	}
}

// TestAirQualityAPI pins the public air-quality station layer: the map
// endpoint serves the newest air_quality snapshot per station code and
// ignores other informational kinds.
func TestAirQualityAPI(t *testing.T) {
	env := newTestEnv(t)
	now := time.Now()
	lvl := 2
	pm10 := 3
	aq := func(code, name string, lat, lon float64, at time.Time) map[string]any {
		return map[string]any{
			"station_code":     code,
			"station_name":     name,
			"latitude":         lat,
			"longitude":        lon,
			"index_level_id":   lvl,
			"index_level_name": "Umiarkowany",
			"generated_at":     at.Add(-10 * time.Minute).Format(time.RFC3339),
			"pollutants": []map[string]any{{
				"code": "PM10", "level_id": pm10, "level_name": "Dostateczny",
			}},
		}
	}
	mk := func(code string, body map[string]any, at time.Time) state.InfoEntry {
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		return state.InfoEntry{
			Source: "giosaq", ProducerID: "giosaq-main", Key: code,
			Kind: "air_quality", ReceivedAt: at, Payload: payload,
		}
	}
	// Two snapshots of the same station: the newer one wins. One
	// unrelated informational kind is ignored.
	env.state.AddOrUpdateInfo("local", "warnflux/info/giosaq/giosaq-main/mpkrakbulwar/air_quality",
		mk("mpkrakbulwar", aq("mpkrakbulwar", "Kraków, Bulwarowa", 50.05, 19.95, now.Add(-1*time.Hour)), now.Add(-1*time.Hour)))
	env.state.AddOrUpdateInfo("local", "warnflux/info/giosaq/giosaq-main/mpkrakbulwar/air_quality",
		mk("mpkrakbulwar", aq("mpkrakbulwar", "Kraków, Bulwarowa", 50.05, 19.95, now), now))
	env.state.AddOrUpdateInfo("local", "warnflux/info/giosaq/giosaq-main/mpniepo3maja/air_quality",
		mk("mpniepo3maja", aq("mpniepo3maja", "Niepołomice, 3 Maja", 50.04, 20.22, now.Add(-30*time.Minute)), now.Add(-30*time.Minute)))
	env.state.AddOrUpdateInfo("local", "warnflux/info/openmeteo/weather-home/home/weather", state.InfoEntry{
		Source: "openmeteo", ProducerID: "weather-home", Key: "home",
		Kind: "weather", ReceivedAt: now,
	})

	resp, body := env.get("/api/airquality")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/airquality = %d", resp.StatusCode)
	}
	var view struct {
		Stations []struct {
			StationCode    string  `json:"station_code"`
			StationName    string  `json:"station_name"`
			Latitude       float64 `json:"latitude"`
			Longitude      float64 `json:"longitude"`
			IndexLevelID   int     `json:"index_level_id"`
			IndexLevelName string  `json:"index_level_name"`
			Pollutants     []struct {
				Code      string `json:"code"`
				LevelID   int    `json:"level_id"`
				LevelName string `json:"level_name"`
			} `json:"pollutants"`
		} `json:"stations"`
	}
	if err := json.Unmarshal([]byte(body), &view); err != nil {
		t.Fatalf("airquality payload: %s: %v", body, err)
	}
	if len(view.Stations) != 2 {
		t.Fatalf("stations = %d, want 2 (dedup by code, weather ignored): %s", len(view.Stations), body)
	}
	got := view.Stations[0]
	if got.StationCode != "mpkrakbulwar" || got.StationName != "Kraków, Bulwarowa" ||
		got.Latitude != 50.05 || got.Longitude != 19.95 {
		t.Errorf("station = %+v", got)
	}
	if got.IndexLevelID != 2 || got.IndexLevelName != "Umiarkowany" {
		t.Errorf("index = %+v", got)
	}
	if len(got.Pollutants) != 1 || got.Pollutants[0].Code != "PM10" || got.Pollutants[0].LevelName != "Dostateczny" {
		t.Errorf("pollutants = %+v", got.Pollutants)
	}
	if view.Stations[1].StationCode != "mpniepo3maja" {
		t.Errorf("second station = %+v", view.Stations[1])
	}
}

// TestEmcomPanelFlow pins the EMCOM networks module end to end: an
// operator adds a network (retained MQTT state at level 0), raises it to
// level 2 (severe hazard + dispatch transition so the routing matrix
// fires), sees the public header chip, lowers it back to monitoring
// (hazard retired) and deletes it.
func TestEmcomPanelFlow(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	// The page is operator-only and carries the readiness legend.
	resp, html := env.get("/emcom")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /emcom = %d", resp.StatusCode)
	}
	if !strings.Contains(html, "EMCOM networks") || !strings.Contains(html, "Pełna aktywacja") {
		t.Fatalf("emcom page missing sections: %s", html)
	}
	csrf := extractCSRF(t, html)

	// CSRF is enforced on mutations.
	resp, _ = env.postForm("/emcom", url.Values{"name": {"SP9MOA EMCOM"}})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /emcom without csrf = %d, want 403", resp.StatusCode)
	}

	// Add a network: one retained info document at level 0.
	resp, _ = env.postForm("/emcom", url.Values{"csrf": {csrf}, "name": {"SP9MOA EMCOM"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /emcom = %d, want 303", resp.StatusCode)
	}
	if len(env.pub.raw) != 1 || env.pub.raw[0].Suffix != "info/emcom/emcom/sp9moa-emcom/emcom" || !env.pub.raw[0].Retained {
		t.Fatalf("raw publish = %+v", env.pub.raw)
	}
	var wire struct {
		Network string `json:"network"`
		Slug    string `json:"slug"`
		Level   int    `json:"level"`
	}
	if err := json.Unmarshal(env.pub.raw[0].Payload, &wire); err != nil || wire.Slug != "sp9moa-emcom" || wire.Level != 0 {
		t.Fatalf("state payload = %s (%v)", env.pub.raw[0].Payload, err)
	}

	// The fake publisher does not echo: mirror the broker loopback into
	// the state like the receiver would.
	mirrorInfo := func(i int) {
		env.state.AddOrUpdateInfo("local", "warnflux/info/emcom/emcom/sp9moa-emcom/emcom", state.InfoEntry{
			Source: "emcom", ProducerID: "emcom", Key: "sp9moa-emcom", Kind: "emcom",
			ReceivedAt: time.Now(), Payload: env.pub.raw[i].Payload,
		})
	}
	mirrorInfo(0)

	// Duplicate names are rejected.
	_, html = env.get("/emcom")
	csrf = extractCSRF(t, html)
	resp, _ = env.postForm("/emcom", url.Values{"csrf": {csrf}, "name": {"sp9moa EMCOM"}})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate add = %d, want 409", resp.StatusCode)
	}

	// Raise to level 2: a severe hazard document plus a canonical
	// transition through the dispatch ingress.
	resp, _ = env.postForm("/emcom/sp9moa-emcom/level", url.Values{"csrf": {csrf}, "level": {"2"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST level 2 = %d, want 303", resp.StatusCode)
	}
	if len(env.pub.published) != 1 {
		t.Fatalf("published hazards = %d, want 1", len(env.pub.published))
	}
	h := env.pub.published[0]
	if h.EventKey != "emcom:sp9moa-emcom" || h.Severity != "severe" || h.Urgency != "immediate" ||
		!strings.Contains(h.Headline, "poziom 2 – Aktywacja lokalna") || !strings.Contains(h.Headline, "SP9MOA EMCOM") {
		t.Errorf("hazard = %+v", h)
	}
	var ev dispatch.Event
	select {
	case ev = <-env.ingress.Events():
	case <-time.After(time.Second):
		t.Fatal("no transition enqueued")
	}
	if ev.Hazard == nil || ev.Hazard.Type != dispatch.TransitionNew || ev.Hazard.Hazard.Severity != "severe" {
		t.Errorf("transition = %+v", ev)
	}

	// The broker would mirror the hazard into the active state.
	env.state.AddOrUpdateActive("local", "warnflux/active/emcom/abc", h)
	mirrorInfo(1) // level 2 payload

	// Raising again updates the same document (no duplicate firing).
	resp, _ = env.postForm("/emcom/sp9moa-emcom/level", url.Values{"csrf": {csrf}, "level": {"2"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("second POST level 2 = %d, want 303", resp.StatusCode)
	}
	select {
	case ev = <-env.ingress.Events():
	case <-time.After(time.Second):
		t.Fatal("no update transition enqueued")
	}
	if ev.Hazard == nil || ev.Hazard.Type != dispatch.TransitionUpdated {
		t.Errorf("second transition = %+v, want updated", ev)
	}

	// The public home page shows the colored chip and the severe
	// communication in the Important section.
	_, homeHTML := env.get("/")
	if !strings.Contains(homeHTML, "SP9MOA EMCOM") || !strings.Contains(homeHTML, "poziom 2") ||
		!strings.Contains(homeHTML, "emcom-chip-l2") {
		t.Fatalf("home header chip missing: %s", homeHTML)
	}
	if !strings.Contains(homeHTML, "poziom 2 – Aktywacja lokalna") {
		t.Errorf("active communication missing on home page: %s", homeHTML)
	}

	// Back to monitoring: the hazard is retired and the chip drops to l0.
	resp, _ = env.postForm("/emcom/sp9moa-emcom/level", url.Values{"csrf": {csrf}, "level": {"0"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST level 0 = %d, want 303", resp.StatusCode)
	}
	if len(env.pub.expired) != 1 || env.pub.expired[0] != "emcom:sp9moa-emcom" {
		t.Fatalf("expired = %v", env.pub.expired)
	}
	select {
	case ev = <-env.ingress.Events():
	case <-time.After(time.Second):
		t.Fatal("no expiry transition enqueued")
	}
	if ev.Hazard == nil || ev.Hazard.Type != dispatch.TransitionExpired {
		t.Errorf("expiry transition = %+v", ev)
	}
	mirrorInfo(3) // level 0 payload
	_, homeHTML = env.get("/")
	if !strings.Contains(homeHTML, "emcom-chip-l0") || !strings.Contains(homeHTML, "Monitoring") {
		t.Errorf("chip did not fall back to monitoring: %s", homeHTML)
	}

	// Deleting the network clears the retained document.
	_, html = env.get("/emcom")
	csrf = extractCSRF(t, html)
	resp, _ = env.postForm("/emcom/sp9moa-emcom/delete", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST delete = %d, want 303", resp.StatusCode)
	}
	last := env.pub.raw[len(env.pub.raw)-1]
	if !last.Retained || len(last.Payload) != 0 {
		t.Errorf("delete publish = %+v, want retained empty payload", last)
	}

	// Unauthenticated access redirects to the login page.
	env.logout()
	resp, _ = env.get("/emcom")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /emcom unauthenticated = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
}
