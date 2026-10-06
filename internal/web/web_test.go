package web_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/storage"

	"github.com/szporwolik/WarnFlux/internal/aprs"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/ingesthttp"
	"github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/metrics"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
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
	users    storage.DirectoryStore
	logs     *web.LogBuffer
	traffic  *mqttreceiver.TrafficBuffer
	trails   *trail.Recorder
	metrics  *metrics.Registry
}

// fakeComposePublisher records the communications the compose module
// publishes; the test drives the state mirror itself to simulate the
// broker loopback. fail simulates a down broker (every publish errors).
type fakeComposePublisher struct {
	mu        sync.Mutex
	published []state.Hazard
	expired   []string
	raw       []fakeRawPublish
	fail      bool
}

// fakeRawPublish is one recorded retained raw publication (EMCOM state).
type fakeRawPublish struct {
	Suffix   string
	Retained bool
	Payload  []byte
}

func (f *fakeComposePublisher) PublishActive(source string, h state.Hazard) error {
	if f.fail {
		return errors.New("broker down")
	}
	f.mu.Lock()
	f.published = append(f.published, h)
	f.mu.Unlock()
	return nil
}

func (f *fakeComposePublisher) ExpireActive(source, eventKey string) error {
	if f.fail {
		return errors.New("broker down")
	}
	f.mu.Lock()
	f.expired = append(f.expired, eventKey)
	f.mu.Unlock()
	return nil
}

func (f *fakeComposePublisher) PublishRaw(suffix string, retained bool, payload []byte) error {
	if f.fail {
		return errors.New("broker down")
	}
	f.mu.Lock()
	f.raw = append(f.raw, fakeRawPublish{Suffix: suffix, Retained: retained, Payload: append([]byte(nil), payload...)})
	f.mu.Unlock()
	return nil
}

// publishedSnapshot returns a copy of the recorded hazard publications
// (safe against the asynchronous publish goroutines).
func (f *fakeComposePublisher) publishedSnapshot() []state.Hazard {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]state.Hazard(nil), f.published...)
}

// expiredSnapshot returns a copy of the recorded active-retire calls.
func (f *fakeComposePublisher) expiredSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.expired...)
}

// rawSnapshot returns a copy of the recorded raw publications.
func (f *fakeComposePublisher) rawSnapshot() []fakeRawPublish {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeRawPublish(nil), f.raw...)
}

func newTestEnv(t *testing.T) *testEnv {
	return newTestEnvFull(t, nil, nil, nil, nil)
}

func newTestEnvWithIngest(t *testing.T, ingest map[string]http.Handler) *testEnv {
	return newTestEnvFull(t, ingest, nil, nil, nil)
}

// newTestEnvWithHub builds the test environment with an APRS hub wired
// into the web server (the home-page map tab reads it).
func newTestEnvWithHub(t *testing.T, hub *aprs.Hub) *testEnv {
	return newTestEnvFull(t, nil, hub, nil, nil)
}

// newTestEnvWithMesh builds the test environment with a Meshtastic hub
// wired into the web server (the home-page map reads its nodes).
func newTestEnvWithMesh(t *testing.T, meshtasticHub *meshtastic.Hub) *testEnv {
	return newTestEnvFull(t, nil, nil, nil, meshtasticHub)
}

// newTestEnvWithStore builds the test environment with an event store
// wired into the web server (the home-page archive tab reads it).
func newTestEnvWithStore(t *testing.T, events storage.EventStore) *testEnv {
	return newTestEnvFull(t, nil, nil, events, nil)
}

func newTestEnvFull(t *testing.T, ingest map[string]http.Handler, hub *aprs.Hub, events storage.EventStore, meshtasticHub *meshtastic.Hub) *testEnv {
	return newTestEnvAll(t, ingest, hub, events, meshtasticHub, nil, nil)
}

// defaultTestWebConfig is the shared web configuration for test
// environments.
func defaultTestWebConfig() config.Web {
	return config.Web{
		Enabled:    true,
		Listen:     ":0",
		Title:      "WarnFlux Test",
		Header2:    "Test platform",
		Tagline:    "Test tagline",
		About:      "Test info text. [sp9moa.pl](https://sp9moa.pl)",
		Disclaimer: "Test disclaimer text.",
		Domain:     "spok.example.com",
		Auth:       config.WebAuth{Username: testUsername, Password: testPassword},
	}
}

// newTestEnvWithAuth builds the environment with a custom web auth block
// (e.g. trusted proxies for the login rate-limit tests).
func newTestEnvWithAuth(t *testing.T, auth config.WebAuth) *testEnv {
	cfg := defaultTestWebConfig()
	cfg.Auth = auth
	return newTestEnvWeb(t, cfg, nil, nil, nil, nil, nil, nil)
}

// newTestEnvAll is newTestEnvFull plus explicit APRS/mesh message stores
// (nil leaves the corresponding admin history empty).
func newTestEnvAll(t *testing.T, ingest map[string]http.Handler, hub *aprs.Hub, events storage.EventStore, meshtasticHub *meshtastic.Hub, aprsMsgs storage.APRSMessageStore, meshtasticMsgs storage.MeshtasticMessageStore) *testEnv {
	return newTestEnvWeb(t, defaultTestWebConfig(), ingest, hub, events, meshtasticHub, aprsMsgs, meshtasticMsgs)
}

// newTestEnvWeb builds the environment with an explicit web config.
func newTestEnvWeb(t *testing.T, cfg config.Web, ingest map[string]http.Handler, hub *aprs.Hub, events storage.EventStore, meshtasticHub *meshtastic.Hub, aprsMsgs storage.APRSMessageStore, meshtasticMsgs storage.MeshtasticMessageStore) *testEnv {
	return newTestEnvWebUsers(t, cfg, nil, ingest, hub, events, meshtasticHub, aprsMsgs, meshtasticMsgs)
}

// newTestEnvWithUsers builds the environment with an explicit directory
// store (nil = the in-memory fake): tests exercising the local-first
// panel paths inject the real SQLite store.
func newTestEnvWithUsers(t *testing.T, users storage.DirectoryStore) *testEnv {
	return newTestEnvWebUsers(t, defaultTestWebConfig(), users, nil, nil, nil, nil, nil, nil)
}

// newTestEnvWebUsers builds the environment with an explicit web config
// and directory store.
func newTestEnvWebUsers(t *testing.T, cfg config.Web, users storage.DirectoryStore, ingest map[string]http.Handler, hub *aprs.Hub, events storage.EventStore, meshtasticHub *meshtastic.Hub, aprsMsgs storage.APRSMessageStore, meshtasticMsgs storage.MeshtasticMessageStore) *testEnv {
	t.Helper()

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
		{ID: "imgw-warnings", Type: "imgw", Kind: plugin.KindSource, State: plugin.StateRunning, Internet: true},
		{ID: "mqtt-main", Type: "mqtt", Kind: plugin.KindOutput, State: plugin.StateDegraded, LastError: "broker down"},
	}}

	users2 := users
	if users2 == nil {
		users2 = newFakeUsers()
	}
	if err := users2.EnsureAdminUser(testUsername, "secret123"); err != nil {
		t.Fatal(err)
	}

	pub := &fakeComposePublisher{}

	srv, err := web.New(cfg, st, receivers, pub, router, actions, hub, meshtasticHub, ingress, logger, "test-version", "abc1234", users2, events, aprsMsgs, meshtasticMsgs, ingest, logs, traffic, trails, met)
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

	return &testEnv{t: t, srv: ts, server: srv, state: st, ingress: ingress, actions: actions, client: client, receiver: receivers, pub: pub, users: users2, logs: logs, traffic: traffic, trails: trails, metrics: met}
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

// TestTrafficViewerFlow pins the MQTT traffic tab of the merged /logs
// page (the old /traffic URL still serves it) and the incremental feed:
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
		t.Errorf("traffic tab missing viewer: %s", html)
	}
	if !strings.Contains(html, `<span class="nav-label">Logs</span>`) {
		t.Errorf("traffic tab missing the Logs sidebar entry: %s", html)
	}
	if !strings.Contains(html, `data-tab="panel-traffic"`) {
		t.Errorf("logs page missing the MQTT traffic tab: %s", html)
	}
	if !strings.Contains(html, "Last 100 inbound MQTT frames") {
		t.Errorf("traffic tab missing buffer hint: %s", html)
	}
	if !strings.Contains(html, `id="browse-form"`) {
		t.Errorf("traffic tab missing MQTT browser: %s", html)
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
	// The old page URL redirects to the audit tab of the merged logs page.
	resp, _ = env.get("/audit")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/logs?tab=audit" {
		t.Fatalf("GET /audit as admin = %d %q, want 303 /logs?tab=audit", resp.StatusCode, resp.Header.Get("Location"))
	}
	_, html := env.get("/logs?tab=audit")
	if !strings.Contains(html, `id="audit-viewer"`) {
		t.Errorf("audit tab missing viewer: %s", html)
	}
	if !strings.Contains(html, `data-tab="panel-applog"`) || !strings.Contains(html, `data-tab="panel-notif"`) {
		t.Errorf("merged logs page missing the other tabs: %s", html)
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
	// The old page URL redirects to the notification tab of the merged
	// logs page.
	resp, _ = env.get("/notifications")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/logs?tab=notif" {
		t.Fatalf("GET /notifications as admin = %d %q, want 303 /logs?tab=notif", resp.StatusCode, resp.Header.Get("Location"))
	}
	_, html := env.get("/logs?tab=notif")
	if !strings.Contains(html, `id="notif-list"`) {
		t.Errorf("notifications tab missing list: %s", html)
	}
	if !strings.Contains(html, `<span class="nav-label">Logs</span>`) {
		t.Errorf("merged logs page missing the Logs sidebar entry: %s", html)
	}
	if !strings.Contains(html, "matched group Niepołomice") ||
		!strings.Contains(html, "imgw → smtp-alerts ≥ Moderate") ||
		!strings.Contains(html, "delivered") {
		t.Errorf("notifications page missing trail steps: %s", html)
	}
	if !strings.Contains(html, "Gale warning") || !strings.Contains(html, `id="notif-imgw:1"`) {
		t.Errorf("notifications page missing trail header: %s", html)
	}
	// The details block collapses behind a click: the steps render but
	// live inside the details element, and the retention hint is shown.
	if !strings.Contains(html, `class="nt-details"`) || !strings.Contains(html, "Details") {
		t.Errorf("notifications page missing the details toggle: %s", html)
	}
	if !strings.Contains(html, "retention period") {
		t.Errorf("notifications page missing the retention hint: %s", html)
	}
	if !strings.Contains(html, "No delivery attempts recorded") {
		t.Errorf("notifications page missing the empty deliveries state: %s", html)
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
		Deliveries map[string][]struct {
			ActionID string `json:"action_id"`
			Status   string `json:"status"`
			Attempts int    `json:"attempts"`
		} `json:"deliveries"`
	}
	if err := json.Unmarshal([]byte(body), &feed); err != nil {
		t.Fatalf("feed is not valid JSON: %v", err)
	}
	if len(feed.Trails) != 1 || feed.Trails[0].Key != "imgw:1" ||
		feed.Trails[0].Outcome != "delivered" || len(feed.Trails[0].Steps) != 5 {
		t.Fatalf("feed = %s", body)
	}
	// The poller renders the per-action delivery table from this payload:
	// the key must always be present (empty map when the store has no
	// ledger) or the client-side render drops every delivery path.
	if feed.Deliveries == nil {
		t.Errorf("feed missing the deliveries payload: %s", body)
	}
	kinds := ""
	for _, s := range feed.Trails[0].Steps {
		kinds += s.Kind + ","
	}
	if kinds != "received,matched,route,submitted,delivered," {
		t.Errorf("feed step kinds = %q", kinds)
	}

	// Focus deep link (?key=…) keeps working through the legacy URL and
	// renders the same trail with the details block OPEN.
	resp, _ = env.get("/notifications?key=imgw:1")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/logs?tab=notif&key=imgw%3A1" {
		t.Fatalf("focused legacy URL = %d %q, want 303 /logs?tab=notif&key=imgw%%3A1", resp.StatusCode, resp.Header.Get("Location"))
	}
	_, html = env.get("/logs?tab=notif&key=imgw:1")
	if !strings.Contains(html, `id="notif-imgw:1"`) {
		t.Errorf("focused page missing trail: %s", html)
	}
	if !strings.Contains(html, `<details class="nt-details" open>`) {
		t.Errorf("focused page must render the details block open: %s", html)
	}
}

// TestNotificationsDeliveriesRender pins the durable delivery-ledger
// table: every action path renders (the page previously crashed mid-table
// because timeHMS received a time.Time) and the partial feed carries the
// same rows for the client-side re-render.
func TestNotificationsDeliveriesRender(t *testing.T) {
	u := newFakeUsers()
	now := time.Now()
	u.deliveries = []storage.DeliveryRecord{
		{EventKey: "imgw:1", ActionID: "smtp-alerts", Status: "succeeded", Attempts: 1, FiredAt: now,
			Payload: `{"bcc":["a@x.pl","b@x.pl"],"aprs_callsigns":[],"mesh_node_ids":[],"discord_handles":null}`},
		{EventKey: "imgw:1", ActionID: "meshtastic-alerts", Status: "accepted", Attempts: 2, FiredAt: now.Add(time.Second),
			Payload: `{"bcc":[],"aprs_callsigns":["SP9SPM","SP9SPM-7","SP9SPM-9","SP9WSS-2"],"mesh_node_ids":["a0a85934"],"discord_handles":null}`},
	}
	env := newTestEnvWithUsers(t, u)

	env.trails.Receive("imgw:1", "imgw", "severe", "Storm", "Gale warning", now)
	env.trails.Add("imgw:1", trail.StepMatched, "matched group SP9MOA", now)
	env.trails.Add("imgw:1", trail.StepDelivered, "delivered", now.Add(time.Second))
	env.trails.SetOutcome("imgw:1", trail.OutcomeDelivered)

	env.login()
	_, html := env.get("/logs?tab=notif")
	for _, want := range []string{
		"Details (2)",
		`class="badge nt-status-succeeded"`,
		`class="badge nt-status-accepted"`,
		`class="mono">smtp-alerts</td>`,
		`class="mono">meshtastic-alerts</td>`,
		// The recipients column answers "to whom" — mesh node IDs in
		// full, emails as a count, APRS callsigns up to three.
		"Recipients",
		"mesh: a0a85934",
		"email: 2",
		// html/template escapes the "+" of the overflow marker.
		"APRS: SP9SPM, SP9SPM-7, SP9SPM-9 &#43;1",
		// The audit steps must render AFTER the table: the old timeHMS
		// type error truncated the page mid-table, dropping every step,
		// every later delivery row and the rest of the document.
		`<ol class="nt-steps">`,
		"matched group SP9MOA",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("notifications page missing %q: %s", want, html)
		}
	}
	if strings.Contains(html, "No delivery attempts recorded") {
		t.Errorf("page shows the empty deliveries state despite ledger rows: %s", html)
	}

	_, body := env.get("/partials/notifications")
	var feed struct {
		Deliveries map[string][]struct {
			ActionID   string `json:"action_id"`
			Status     string `json:"status"`
			Attempts   int    `json:"attempts"`
			Recipients string `json:"recipients"`
		} `json:"deliveries"`
	}
	if err := json.Unmarshal([]byte(body), &feed); err != nil {
		t.Fatalf("feed is not valid JSON: %v", err)
	}
	rows := feed.Deliveries["imgw:1"]
	if len(rows) != 2 || rows[0].ActionID != "smtp-alerts" || rows[0].Status != "succeeded" ||
		rows[1].ActionID != "meshtastic-alerts" || rows[1].Status != "accepted" || rows[1].Attempts != 2 {
		t.Fatalf("feed deliveries = %+v", rows)
	}
	if rows[0].Recipients != "email: 2" || rows[1].Recipients != "mesh: a0a85934 · APRS: SP9SPM, SP9SPM-7, SP9SPM-9 +1" {
		t.Fatalf("feed recipients = %q / %q", rows[0].Recipients, rows[1].Recipients)
	}
}

// TestDashboardHealthCard pins the merged system-health card: the former
// /health page lives on the dashboard now, the verdict degrades only when
// something actually is (here: the test receiver never connects), and the
// old URL redirects there.
func TestDashboardHealthCard(t *testing.T) {
	env := newTestEnv(t)

	// Unauthenticated: redirect to the login page.
	resp, _ := env.get("/health")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /health unauthenticated = %d %q, want redirect", resp.StatusCode, resp.Header.Get("Location"))
	}

	env.login()
	// The old page URL redirects to the dashboard.
	resp, _ = env.get("/health")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
		t.Fatalf("GET /health as admin = %d %q, want 303 /dashboard", resp.StatusCode, resp.Header.Get("Location"))
	}

	_, html := env.get("/dashboard")
	for _, want := range []string{
		`id="health-section"`,
		"System health",
		"DEGRADED", // receiver "local" is configured but never dialed in tests
		">Database</strong>",
		"ready",
		">Dispatch queue</strong>",
		">Pending notifications</strong>",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("dashboard health card missing %q: %s", want, html)
		}
	}
	// The standalone health page and its sidebar entry are gone.
	if strings.Contains(html, `<span class="nav-label">Health</span>`) {
		t.Error("sidebar still offers the removed Health entry")
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

// TestHealthEmergencyAcceptance pins the visible degradation: an event
// accepted without durable storage (no inbox in the test env) turns the
// dispatch queue row amber and names the emergency mode — the operator
// can never mistake it for durable acceptance.
func TestHealthEmergencyAcceptance(t *testing.T) {
	env := newTestEnv(t)
	if res := env.ingress.Enqueue(dispatch.Event{Kind: dispatch.EventMQTTMessage}); res != dispatch.AcceptedEmergency {
		t.Fatalf("enqueue = %v, want emergency (no inbox in test env)", res)
	}

	env.login()
	_, html := env.get("/dashboard")
	for _, want := range []string{
		"EMERGENCY",
		"accepted without durable storage",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("dashboard health card missing %q: %s", want, html)
		}
	}
}

// TestHealthLowDiskAlarm pins the low-disk alarm: below the configured
// threshold the Database row turns red and reports the free space.
func TestHealthLowDiskAlarm(t *testing.T) {
	env := newTestEnv(t)
	// fakeUsers reports 12 GiB free; a 1 TiB threshold trips the alarm.
	env.server.SetStorageAlarm(1 << 40)

	env.login()
	_, html := env.get("/dashboard")
	for _, want := range []string{
		"LOW DISK",
		"free space 12288 MB (alarm below 1048576 MB)",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("dashboard health card missing %q: %s", want, html)
		}
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
	_, html := env.get("/dashboard")
	for _, want := range []string{
		">news (ingest)</strong>",
		">OK</span>",
		"accepted=7 rejected=2 auth_failed=1 rate_limited=0 forbidden=0",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("dashboard health card missing ingest row %q: %s", want, html)
		}
	}
}

// TestPublicHomePage pins the public landing page: header1/header2 and the
// active-hazard list without any session; the login form lives behind the
// icon button at /login. The partial is public too (auto-refresh).
// TestPublicHomeServedFromStore pins the offline serving surface: the
// public home page and the map endpoint read active hazards from the
// LOCAL database — the MQTT mirror is empty (broker down) and the
// communication is still served.
func TestPublicHomeServedFromStore(t *testing.T) {
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "home.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	now := time.Now()
	expires := now.Add(time.Hour)
	lat, lon := 49.98, 20.06
	ev := core.HazardEvent{
		Source: "imgw-meteo", SourceID: "warn-1", Event: "Storm",
		Severity: "severe", Headline: "Offline storm",
		Status: core.StatusActive, ExpiresAt: &expires,
		Latitude: &lat, Longitude: &lon,
		ReceivedAt: now, UpdatedAt: now,
	}
	if _, _, err := store.Ingest(context.Background(), ev, core.Fingerprint(ev)); err != nil {
		t.Fatal(err)
	}

	// events=store (active hazards served from SQLite); the mirror is
	// deliberately EMPTY — no broker ever delivered anything.
	env := newTestEnvFull(t, nil, nil, store, nil)

	_, html := env.get("/")
	if !strings.Contains(html, "Offline storm") || !strings.Contains(html, "imgw-meteo") {
		t.Errorf("home page missing the offline communication: %s", html)
	}

	resp, body := env.get("/api/events")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/events = %d", resp.StatusCode)
	}
	if !strings.Contains(body, `"event_key":"imgw-meteo:warn-1"`) ||
		!strings.Contains(body, `"latitude":49.98`) {
		t.Errorf("map endpoint missing the offline communication: %s", body)
	}
}

// TestHazardByKeyServesEnded pins the mail-deep-link contract: a hazard
// that already expired still renders (with its ended status) through
// /api/hazard/<key>, so the notification link never dead-ends.
func TestHazardByKeyServesEnded(t *testing.T) {
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "ended.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	now := time.Now()
	expires := now.Add(-time.Hour) // already in the past
	ev := core.HazardEvent{
		Source: "imgw-meteo", SourceID: "warn-9", Event: "Storm",
		Severity: "severe", Headline: "Ended storm", Description: "It already passed",
		Status: core.StatusActive, ExpiresAt: &expires,
		ReceivedAt: now.Add(-3 * time.Hour), UpdatedAt: now.Add(-3 * time.Hour),
	}
	if _, _, err := store.Ingest(context.Background(), ev, core.Fingerprint(ev)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Expire(context.Background(), now); err != nil {
		t.Fatal(err)
	}

	env := newTestEnvFull(t, nil, nil, store, nil)

	resp, body := env.get("/api/hazard/imgw-meteo:warn-9")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/hazard = %d: %s", resp.StatusCode, body)
	}
	for _, want := range []string{
		`"event_key":"imgw-meteo:warn-9"`,
		`"headline":"Ended storm"`,
		`"status":"expired"`,
		`"description":"It already passed"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("hazard payload missing %q: %s", want, body)
		}
	}

	// Unknown keys answer 404 (the popup then shows the not-found note).
	resp, _ = env.get("/api/hazard/nope:1")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /api/hazard/nope:1 = %d, want 404", resp.StatusCode)
	}
}

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
	// A TERYT-heavy message: the raw code wall must never reach the
	// public card (the named tokens already carry the geography).
	if err := env.state.AddOrUpdateActive("local", "warnflux/active/imgw-meteo/dddd", state.Hazard{
		EventKey:  "imgw-meteo:4",
		Source:    "imgw-meteo",
		Event:     "Mgła",
		Severity:  "moderate",
		Headline:  "Gęsta mgła",
		Areas:     []string{"powiat:krakowski", "teryt:0201", "teryt:0202", "gmina:skawina"},
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
		"WarnFlux Test",                  // header1
		"Test platform",                  // header2
		"Test info text.",                // configurable about text (one-time popup)
		`[sp9moa.pl](https://sp9moa.pl)`, // markdown link in the about text
		`id="about-dialog"`,              // one-time about popup
		`id="about-help"`,                // the top-bar question-mark reopens it
		`class="home-disclaimer"`,        // unofficial-system notice
		"Test disclaimer text.",
		"Ekstremalny wiatr", // most severe first
		`href="/login"`,     // sign-in behind the icon button
		"Active messages",
		"Situation map", // combined stations + weather tab
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
	// Clean public shell: archive and sources are NOT tabs anymore — the
	// archive lives behind the alerts-section link, sources behind the
	// footer link.
	for _, absent := range []string{`data-tab="tab-archive"`, `data-tab="tab-channels"`} {
		if strings.Contains(html, absent) {
			t.Errorf("home page still exposes %q as a tab: %s", absent, html)
		}
	}
	if !strings.Contains(html, `class="home-sub-link" href="/archive"`) {
		t.Errorf("home alerts section missing the archive entry link: %s", html)
	}
	if !strings.Contains(html, `class="home-foot-link" href="/sources"`) {
		t.Errorf("home footer missing the sources link: %s", html)
	}
	extremeAt := strings.Index(html, "Ekstremalny wiatr")
	moderateAt := strings.Index(html, "Umiarkowane burze")
	if extremeAt < 0 || moderateAt < 0 || extremeAt > moderateAt {
		t.Errorf("hazards not ordered by severity: extreme@%d moderate@%d", extremeAt, moderateAt)
	}
	// With an extreme hazard active the calm-state banner stays away.
	if strings.Contains(html, "home-no-severe") {
		t.Error("home page must not show the no-severe banner while severe+ hazards are active")
	}

	// Groups are 1:1 with the severity scale; moderate and above start
	// expanded, only minor collapsed.
	if !strings.Contains(html, `<details class="home-section home-sev-extreme" open>`) {
		t.Error("extreme group missing or not open by default")
	}
	if !strings.Contains(html, `<details class="home-section home-sev-moderate" open>`) {
		t.Error("moderate group missing or not open by default")
	}
	if strings.Contains(html, `<details class="home-section home-sev-minor" open`) {
		t.Error("minor group must be collapsed by default")
	}
	if !strings.Contains(html, `class="sev sev-extreme">Extreme</span>`) {
		t.Error("extreme badge must carry the localized severity label")
	}
	if !strings.Contains(html, `class="sev sev-minor">Minor</span>`) {
		t.Error("minor badge must carry the localized severity label")
	}
	if strings.Contains(html, "No active messages.") {
		t.Error("groups must not show the empty note while hazards are active")
	}

	minorAt := strings.Index(html, "Drobne prace drogowe")
	detailsAt := strings.Index(html, `<details class="home-section home-sev-minor">`)
	if minorAt < 0 || detailsAt < 0 || minorAt < detailsAt {
		t.Errorf("minor hazard must sit inside the collapsed minor group: minor@%d details@%d",
			minorAt, detailsAt)
	}
	if !strings.Contains(html, "Minor") {
		t.Error("minor group summary missing")
	}
	if !strings.Contains(html, `<summary>Minor <span class="count count-live">1</span></summary>`) {
		t.Error("minor group should carry its own count badge of 1")
	}

	// The TERYT code wall is filtered from the public card: the named
	// tokens stay, the raw teryt: codes never reach the page (neither
	// the card nor the embedded JSON for the popup).
	if !strings.Contains(html, "Gęsta mgła") ||
		!strings.Contains(html, "powiat:krakowski") ||
		!strings.Contains(html, "gmina:skawina") {
		t.Error("home page missing the TERYT-heavy card or its named areas")
	}
	if strings.Contains(html, "teryt:0201") || strings.Contains(html, "teryt:0202") {
		t.Error("home page renders raw teryt: codes")
	}
	if !strings.Contains(html, `"areas":"powiat:krakowski, gmina:skawina"`) {
		t.Error("embedded hazard JSON must carry the filtered areas")
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

// TestPublicSourcesPage pins the standalone sources page: the feed list
// and the notification channels live behind the footer link, not on the
// home tabs.
func TestPublicSourcesPage(t *testing.T) {
	env := newTestEnv(t)
	resp, html := env.get("/sources")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /sources = %d", resp.StatusCode)
	}
	for _, want := range []string{
		"Where the data comes from",
		`class="home-top"`,
		"Back to home",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("sources page missing %q:\n%s", want, html)
		}
	}
	// The channels section renders either its list or the explicit empty
	// state (the test env has no channel-mapped action types).
	if !strings.Contains(html, "channels-list") && !strings.Contains(html, "Notification channels are not configured") {
		t.Errorf("sources page missing the channels section:\n%s", html)
	}
}

// TestPublicHomeMinorOnly pins the collapsed-group rendering when every
// active hazard is low priority: no empty-state note (the page is not
// "empty"), just the collapsed minor group with its own count badge.
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
	if !strings.Contains(html, `<details class="home-section home-sev-minor">`) {
		t.Error("collapsed minor group missing")
	}
	if !strings.Contains(html, "Tylko prace drogowe") {
		t.Error("minor hazard headline missing")
	}
	if strings.Contains(html, "No active hazards.") {
		t.Error("the old full-page empty box must be gone")
	}
	// Only groups that have hazards render: with a single minor hazard
	// there is no severe/extreme group and no empty-state note.
	if strings.Contains(html, "home-sev-severe") || strings.Contains(html, "home-sev-extreme") {
		t.Error("empty severity groups must not render")
	}
	if strings.Contains(html, "No active messages.") {
		t.Error("minor-only page must not carry the empty-state note")
	}
	// Calm state: no severe-or-higher hazard, so the gentle banner sits
	// above the collapsed minor group.
	if !strings.Contains(html, "home-no-severe") || !strings.Contains(html, "No severe or higher messages right now.") {
		t.Error("minor-only page missing the no-severe banner")
	}
}

// TestPublicHomeAllEmpty pins the fully empty home page: the empty-state
// note renders instead of any severity group.
func TestPublicHomeAllEmpty(t *testing.T) {
	env := newTestEnv(t)

	resp, html := env.get("/")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / = %d, want 200", resp.StatusCode)
	}
	if strings.Contains(html, "home-section home-sev-") {
		t.Error("no severity groups must render when nothing is active")
	}
	if strings.Contains(html, "home-no-severe") {
		t.Error("empty home must not carry the no-severe banner")
	}
	if !strings.Contains(html, "No active messages.") {
		t.Error("empty home must say there are no active messages")
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

	// A weather station (symbol '_') is excluded from the public list —
	// the weather layer draws it — but the admin map asks for all=1.
	hub.Observe(aprs.ParseFeedLine("SP9WX>APRS,TCPIP*:!5056.25N/01952.50E_220/004g005t077", time.Now()), "aprs-inet")
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, body := env.get("/api/aprs/stations?all=1")
		var all []aprs.StationDocument
		if err := json.Unmarshal([]byte(body), &all); err != nil {
			t.Fatalf("stations all payload = %s: %v", body, err)
		}
		if len(all) >= 2 {
			got = all
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(got) != 2 {
		t.Fatalf("stations?all=1 = %d entries, want 2 (weather station included)", len(got))
	}
	foundWX := false
	for _, doc := range got {
		if doc.Callsign == "SP9WX" {
			foundWX = true
		}
	}
	if !foundWX {
		t.Fatalf("stations?all=1 missing the weather station: %+v", got)
	}
}

// TestAPRSMapPage pins the combined admin APRS section: the page
// renders the station map with the ALL / APRS-IS / APRS-RF filter AND
// the messages tab (send form) behind one navigation entry.
func TestAPRSMapPage(t *testing.T) {
	hub, err := aprs.NewHub(aprs.HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: 25, StationTTL: 30 * time.Minute,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	env := newTestEnvWithHub(t, hub)
	env.login()

	resp, body := env.get("/aprs")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /aprs = %d", resp.StatusCode)
	}
	for _, want := range []string{`id="aprs-admin-map"`, `data-filter="all"`, `data-filter="inet"`, `data-filter="radio"`, `data-tab="panel-msgs"`, `action="/messages/send"`, `id="aprs-dir-table"`} {
		if !strings.Contains(body, want) {
			t.Errorf("/aprs missing %s", want)
		}
	}
}

// TestMeshMapPage pins the admin Meshtastic node-map tab (merged into
// /meshtastic; the old /meshmap route stays as a redirect): the page
// renders the map element, and the stations endpoint in all=1 mode
// returns every heard node — outside the operational ring and past the
// node TTL included — plus the positionless ones.
func TestMeshMapPage(t *testing.T) {
	lat, lon := 50.05, 20.10
	aprsHub, err := aprs.NewHub(aprs.HubConfig{
		Enabled: true, Callsign: "SP9MOA-10", GridSquare: "JO90WW",
		RadiusKM: 25, StationTTL: 30 * time.Minute,
		Latitude: &lat, Longitude: &lon,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	mesh, err := meshtastic.NewHub(meshtastic.Config{
		Enabled: true, Device: "/dev/fake", NodeTTL: time.Minute,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	env := newTestEnvAll(t, nil, aprsHub, nil, mesh, nil, nil)
	env.login()

	now := time.Now()
	mesh.SeedNode("11111111", "Near", "N1", 50.06, 20.11, now, []string{"telemetry"})             // inside the ring
	mesh.SeedNode("22222222", "Far", "F2", 52.00, 18.00, now, []string{"position"})               // far outside
	mesh.SeedNode("33333333", "Old", "O3", 50.00, 20.00, now.Add(-2*time.Hour), []string{"text"}) // stale (TTL)
	mesh.SeedNode("44444444", "NoPos", "N4", 0, 0, now, []string{"text"})                         // no position

	resp, _ := env.get("/meshmap")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/meshtastic?tab=map" {
		t.Fatalf("GET /meshmap = %d %q, want 303 /meshtastic?tab=map", resp.StatusCode, resp.Header.Get("Location"))
	}
	resp, body := env.get("/meshtastic?tab=map")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /meshtastic?tab=map = %d", resp.StatusCode)
	}
	if !strings.Contains(body, `id="mesh-admin-map"`) {
		t.Errorf("/meshtastic map tab missing the map element")
	}
	// The map tab carries the heard-window filter (all / 15 min / 1 h /
	// 4 h / today).
	for _, want := range []string{`id="mesh-map-filter"`, `data-filter="m15"`, `data-filter="h1"`, `data-filter="h4"`, `data-filter="today"`} {
		if !strings.Contains(body, want) {
			t.Errorf("/meshtastic map tab missing %s", want)
		}
	}

	var view struct {
		Nodes []struct {
			ID string `json:"id"`
		} `json:"nodes"`
		NoPos []struct {
			ID string `json:"id"`
		} `json:"nopos"`
	}
	resp, body = env.get("/api/meshtastic/stations")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/meshtastic/stations = %d", resp.StatusCode)
	}
	if err := json.Unmarshal([]byte(body), &view); err != nil {
		t.Fatalf("stations payload: %v", err)
	}
	if len(view.Nodes) != 1 || view.Nodes[0].ID != "11111111" {
		t.Fatalf("default nodes = %+v, want only the in-ring fresh node", view.Nodes)
	}
	if len(view.NoPos) != 1 || view.NoPos[0].ID != "44444444" {
		t.Fatalf("default nopos = %+v, want the positionless node", view.NoPos)
	}

	resp, body = env.get("/api/meshtastic/stations?all=1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/meshtastic/stations?all=1 = %d", resp.StatusCode)
	}
	if err := json.Unmarshal([]byte(body), &view); err != nil {
		t.Fatalf("all stations payload: %v", err)
	}
	got := map[string]bool{}
	for _, n := range view.Nodes {
		got[n.ID] = true
	}
	for _, id := range []string{"11111111", "22222222", "33333333"} {
		if !got[id] {
			t.Errorf("all=1 nodes missing %s: %+v", id, view.Nodes)
		}
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
	// An RF-heard weather station: KISS frames have no q-construct, so
	// the radio backend marks the report rf by definition.
	hub.Observe(aprs.ParseFeedLine("SP9WY>APRS,WIDE1-1*:!5057.00N/01951.00E_.../...g...t078", time.Now()), "aprs-radio")

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
			Origin        string   `json:"origin"`
			ReceivedVia   []string `json:"received_via"`
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
				Origin        string   `json:"origin"`
				ReceivedVia   []string `json:"received_via"`
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
		if len(view.Reports) >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	byName := map[string]struct {
		via           string
		origin        string
		receivedVia   []string
		radiationUSvh *float64
	}{}
	for _, r := range view.Reports {
		byName[r.Name] = struct {
			via           string
			origin        string
			receivedVia   []string
			radiationUSvh *float64
		}{via: r.Via, origin: r.Origin, receivedVia: r.ReceivedVia, radiationUSvh: r.RadiationUSvh}
	}
	wx, ok := byName["SP9WX"]
	if !ok || wx.via != "aprs" || wx.origin != "internet" || wx.radiationUSvh == nil || *wx.radiationUSvh != 0.12 {
		t.Fatalf("APRS weather report missing or wrong: %+v", view.Reports)
	}
	if len(wx.receivedVia) != 1 || wx.receivedVia[0] != "aprs-inet" {
		t.Fatalf("APRS-IS weather report received_via = %v, want [aprs-inet]", wx.receivedVia)
	}
	wy, ok := byName["SP9WY"]
	if !ok || wy.via != "aprs" || wy.origin != "rf" {
		t.Fatalf("RF weather report missing or wrong origin: %+v", view.Reports)
	}
	if len(wy.receivedVia) != 1 || wy.receivedVia[0] != "aprs-radio" {
		t.Fatalf("RF weather report received_via = %v, want [aprs-radio]", wy.receivedVia)
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
	if !strings.Contains(html, "Messages") {
		t.Errorf("compose page missing the heading: %s", html)
	}
	if !strings.Contains(html, `<span class="nav-label">Messages</span>`) {
		t.Errorf("compose page missing sidebar entry: %s", html)
	}
	if !strings.Contains(html, "Issued messages") {
		t.Errorf("compose page missing issued list: %s", html)
	}
	if !strings.Contains(html, `id="compose-new"`) {
		t.Errorf("compose page missing the compose button: %s", html)
	}
	// The form panel starts HIDDEN on a fresh page (the issued list is
	// the initial view) — only the New message button reveals it.
	if !strings.Contains(html, `<div class="compose-panel" id="compose-panel" hidden>`) {
		t.Errorf("compose panel must be hidden by default: %s", html)
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
	pub := env.pub.publishedSnapshot()
	if len(pub) != 1 {
		t.Fatalf("publisher saw %d publishes, want 1", len(pub))
	}
	h := pub[0]
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
	} else if ev.Hazard.Hazard.Description != "Heavy rain may cause local flooding." ||
		ev.Hazard.Hazard.Instruction != "Avoid the river bank." {
		// The SMTP action prints these free-text fields; without them
		// every mail falls back to the default instruction.
		t.Errorf("compose transition lost description/instruction: %+v", ev.Hazard.Hazard)
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
	// Editing opens the compose panel on the initial render (no hidden).
	if !strings.Contains(html, `<div class="compose-panel" id="compose-panel">`) {
		t.Errorf("edit page must render the compose panel visible: %s", html)
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
	pub = env.pub.publishedSnapshot()
	if len(pub) != 2 || pub[1].EventKey != h.EventKey {
		t.Errorf("update did not reuse the event key: %+v", pub)
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
	exp := env.pub.expiredSnapshot()
	if len(exp) != 1 || exp[0] != h.EventKey {
		t.Errorf("expire did not target the event key: %v", exp)
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
		`warnflux_dispatch_accepts_total{durability="durable"} 0`,
		`warnflux_dispatch_accepts_total{durability="emergency"} 0`,
		`warnflux_dispatch_rejects_total 0`,
		`warnflux_dispatch_inbox_failures_total 0`,
		`warnflux_pending_changes 3`,
		`warnflux_events_active 4`,
		`warnflux_inbox_backlog 7`,
		`warnflux_ingest_outbox_backlog 2`,
		`warnflux_storage_free_bytes 12884901888`,
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

	// The routing editor offers the ingest id as a matrix source.
	env.login()
	if _, err := env.users.CreateGroup("ops"); err != nil {
		t.Fatal(err)
	}
	_, html := env.get("/groups/1/routing")
	for _, want := range []string{
		`name="cell:news|logger-action"`,
		"news (ingest)",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("routing page missing ingest source %q: %s", want, html)
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
	if !strings.Contains(html, `<span class="nav-label">Messages</span>`) {
		t.Error("compose page missing Messages nav entry")
	}
	if !strings.Contains(html, `href="/account"`) {
		t.Error("emcom must see the Account entry in the user menu")
	}
	for _, forbidden := range []string{"Access", "Notifications"} {
		if strings.Contains(html, `<span class="nav-label">`+forbidden+`</span>`) {
			t.Errorf("emcom must not see %s nav entry", forbidden)
		}
	}

	// The dashboard itself is open to emcom too.
	resp, _ = env.get("/dashboard")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /dashboard as emcom = %d, want 200", resp.StatusCode)
	}

	// The EMCOM panel is the emcom operator's surface: the level slider
	// is present, but adding and deleting networks is admin-only — the
	// controls are hidden and the endpoints redirect to the dashboard.
	resp, html = env.get("/emcom")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /emcom as emcom = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(html, "data-emcom-slider") || !strings.Contains(html, "data-emcom-save") {
		t.Errorf("emcom operator must see the level controls: %s", html)
	}
	// Network management moved to the Config page: the operator panel
	// carries neither the add form nor delete buttons.
	if strings.Contains(html, "emcom-add") {
		t.Errorf("emcom operator must not see the add-network form: %s", html)
	}
	// The exact button markup (the page script keeps the bare JS
	// selector, which must not count).
	if strings.Contains(html, `btn-danger btn-small" data-emcom-delete`) {
		t.Errorf("emcom operator must not see the delete buttons: %s", html)
	}
	csrf = extractCSRF(t, html)
	resp, _ = env.postForm("/config/emcom/add", url.Values{"csrf": {csrf}, "name": {"ROGUE"}})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
		t.Errorf("POST /config/emcom/add as emcom = %d %q, want 303 to /dashboard", resp.StatusCode, resp.Header.Get("Location"))
	}
	resp, _ = env.postForm("/config/emcom/nope/delete", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
		t.Errorf("POST delete as emcom = %d %q, want 303 to /dashboard", resp.StatusCode, resp.Header.Get("Location"))
	}
	// Changing the level stays open to the emcom operator: the handler is
	// reached (unknown slug answers 404, not a permission redirect).
	resp, _ = env.postForm("/emcom/nope/level", url.Values{"csrf": {csrf}, "level": {"2"}})
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("POST level as emcom = %d, want 404 from the handler (not a redirect)", resp.StatusCode)
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

// TestForgotCanonicalHostOnly pins the host-header hardening: a reset
// request carrying a spoofed Host header still mails a link to the
// configured canonical domain, never the attacker's.
func TestForgotCanonicalHostOnly(t *testing.T) {
	env := newTestEnv(t)
	if _, err := env.users.CreateUser("member1", "", "member1@example.com", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}
	var sentText string
	env.server.SetPasswordResetMailer(func(to, subject, text string) error {
		sentText = text
		return nil
	})

	_, html := env.get("/forgot")
	// Go's http.Client looks the cookie jar up by req.Host, so a spoofed
	// Host would also drop the CSRF cookie; attach it explicitly (an
	// attacker controls their own session anyway — the mail link is the
	// surface under test).
	u, _ := url.Parse(env.srv.URL)
	makeReq := func(host string) *http.Request {
		req, err := http.NewRequest(http.MethodPost, env.srv.URL+"/forgot",
			strings.NewReader(url.Values{
				"csrf":     {extractCSRF(t, html)},
				"username": {"member1"},
				"email":    {"member1@example.com"},
			}.Encode()))
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range env.client.Jar.Cookies(u) {
			req.AddCookie(c)
		}
		req.Host = host
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return req
	}

	resp, err := env.client.Do(makeReq("evil.example.net")) // spoofed Host header
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("forgot with spoofed host = %d, want 200", resp.StatusCode)
	}
	if sentText == "" || !strings.Contains(sentText, "http://spok.example.com/reset?token=") {
		t.Fatalf("reset link must use the canonical domain: %q", sentText)
	}
	if strings.Contains(sentText, "evil.example.net") {
		t.Fatalf("spoofed host leaked into the reset link: %q", sentText)
	}

	// A malformed Host (userinfo injection) is rejected outright.
	resp2, err := env.client.Do(makeReq("spok.example.com@evil.example.net"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("forgot with userinfo host = %d, want 400", resp2.StatusCode)
	}
}

// TestForgotOfflineWithoutDomain pins the canonical-origin requirement:
// without web.domain the flow never mails a link built from the request
// Host — it answers with the offline (contact an administrator) message.
func TestForgotOfflineWithoutDomain(t *testing.T) {
	cfg := defaultTestWebConfig()
	cfg.Domain = ""
	env := newTestEnvWeb(t, cfg, nil, nil, nil, nil, nil, nil)
	if _, err := env.users.CreateUser("member1", "", "member1@example.com", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}
	sent := false
	env.server.SetPasswordResetMailer(func(to, subject, text string) error {
		sent = true
		return nil
	})

	_, html := env.get("/forgot")
	resp, html := env.postForm("/forgot", url.Values{
		"csrf":     {extractCSRF(t, html)},
		"username": {"member1"},
		"email":    {"member1@example.com"},
	})
	if resp.StatusCode != http.StatusOK || !strings.Contains(html, "contact an administrator") {
		t.Fatalf("offline forgot = %d %s", resp.StatusCode, html)
	}
	if sent {
		t.Fatal("email sent without a canonical domain")
	}
}

// TestForgotSuccessCooldown pins the post-success cooldown: a successful
// send does NOT clear the limiter — repeating the correct request within
// the window is throttled with 429 and sends no second email.
func TestForgotSuccessCooldown(t *testing.T) {
	env := newTestEnv(t)
	if _, err := env.users.CreateUser("member1", "", "member1@example.com", "", "member", "password123"); err != nil {
		t.Fatal(err)
	}
	sends := 0
	env.server.SetPasswordResetMailer(func(to, subject, text string) error {
		sends++
		return nil
	})

	_, html := env.get("/forgot")
	resp, _ := env.postForm("/forgot", url.Values{
		"csrf":     {extractCSRF(t, html)},
		"username": {"member1"},
		"email":    {"member1@example.com"},
	})
	if resp.StatusCode != http.StatusOK || sends != 1 {
		t.Fatalf("first correct request = %d (%d sends), want 200 with 1 send", resp.StatusCode, sends)
	}

	// The identical correct request inside the cooldown window: 429, no
	// second email.
	_, html = env.get("/forgot")
	resp, _ = env.postForm("/forgot", url.Values{
		"csrf":     {extractCSRF(t, html)},
		"username": {"member1"},
		"email":    {"member1@example.com"},
	})
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("repeat after success = %d, want 429", resp.StatusCode)
	}
	if sends != 1 {
		t.Fatalf("repeat after success sent %d emails, want 1", sends)
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

// TestAdminAccountSelfService pins the self-service surface for the
// configuration-managed admin: username, role and password stay
// config-owned (the password field is disabled and any posted password
// is ignored), while contact data and notification subscriptions are
// edited and persisted here.
func TestAdminAccountSelfService(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	g, err := env.users.CreateGroup("SP9MOA")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.users.SetUserGroups(1, []int64{g.ID}); err != nil {
		t.Fatal(err)
	}

	resp, html := env.get("/account")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /account as admin = %d", resp.StatusCode)
	}
	if !strings.Contains(html, "account-note") || !strings.Contains(html, "stay in the server configuration") {
		t.Errorf("account page missing the managed-account note: %s", html)
	}
	if strings.Contains(html, `name="email" value="" maxlength="128" placeholder="you@example.com" disabled`) {
		t.Errorf("email field must be editable for the admin: %s", html)
	}
	if !strings.Contains(html, `name="password" value="" autocomplete="new-password" placeholder="leave empty to keep the current one" disabled`) {
		t.Errorf("password field must stay disabled for the admin: %s", html)
	}
	if !strings.Contains(html, "Save changes") {
		t.Errorf("admin account page must offer Save changes: %s", html)
	}

	// Contact fields update, the group unsubscribe works, and a posted
	// password is ignored (the admin password is config-owned).
	csrf := extractCSRF(t, html)
	resp, _ = env.postForm("/account", url.Values{
		"csrf": {csrf}, "email": {"admin@sp9moa.pl"}, "phone": {"+48 600 000 000"},
		"groups": {}, "channels": {"smtp", "aprs", "meshtastic", "discord"},
		"password": {"newpassword1"},
	})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/account?msg=saved" {
		t.Fatalf("POST /account as admin = %d %q, want redirect to the saved flash", resp.StatusCode, resp.Header.Get("Location"))
	}

	u, err := env.users.GetUserByUsername(testUsername)
	if err != nil {
		t.Fatal(err)
	}
	if u.Phone != "+48 600 000 000" || u.Email != "admin@sp9moa.pl" {
		t.Errorf("admin contact data not persisted: %+v", u)
	}
	if !u.IsAdmin || u.Username != testUsername {
		t.Errorf("admin identity must stay config-owned: %+v", u)
	}
	// The fake store never receives a password write for the admin row:
	// the posted password must have been dropped server-side.
	fu := env.users.(*fakeUsers)
	fu.mu.Lock()
	storedPassword := fu.passwords[testUsername]
	fu.mu.Unlock()
	if storedPassword != "" {
		t.Fatalf("posted password leaked into the admin row: %q", storedPassword)
	}
	if ids, err := env.users.GroupIDsForUser(u.ID); err != nil {
		t.Fatal(err)
	} else if len(ids) != 0 {
		t.Fatalf("admin groups = %v, want none after the unsubscribe", ids)
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
	for _, want := range []string{"Access", "Logs"} {
		if !strings.Contains(html, `<span class="nav-label">`+want+`</span>`) {
			t.Errorf("admin /account sidebar missing %s nav entry", want)
		}
	}
	if strings.Contains(html, `<span class="nav-label">Health</span>`) {
		t.Error("admin /account sidebar still shows the removed Health entry")
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

	adminPages := []string{"/users", "/groups", "/access", "/logs", "/traffic", "/aprs?tab=msgs", "/meshtastic"}
	// /audit and /notifications merged into /logs tabs: every role gets a
	// redirect (to the dashboard unless admin).
	legacyLogPages := []string{"/audit", "/notifications"}
	adminPartials := []string{"/partials/logs", "/partials/audit", "/partials/traffic", "/partials/notifications"}
	sharedPartials := []string{"/partials/status", "/partials/mqtt", "/partials/weather", "/partials/warnings", "/partials/plugins", "/partials/actions", "/partials/health"}
	adminPosts := []string{"/users", "/users/2/delete", "/users/2/prefs", "/groups", "/groups/1/delete", "/groups/1/routing", "/api/meshtastic/traceroute"}

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
	for _, p := range append(append([]string{}, adminPages...), legacyLogPages...) {
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
	for _, p := range append(append([]string{}, adminPages...), legacyLogPages...) {
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
	for _, p := range append(append([]string{}, adminPages...), legacyLogPages...) {
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

	// Admin: everything listed above opens; the merged /logs tabs answer
	// through the legacy URLs with a redirect.
	env.logout()
	env.login()
	for _, p := range append(append(append([]string{}, adminPages...), adminPartials...), sharedPartials...) {
		resp, _ := env.get(p)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("admin GET %s = %d, want 200", p, resp.StatusCode)
		}
	}
	if resp, _ := env.get("/audit"); resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/logs?tab=audit" {
		t.Errorf("admin GET /audit = %d %q, want 303 /logs?tab=audit", resp.StatusCode, resp.Header.Get("Location"))
	}
	if resp, _ := env.get("/notifications"); resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/logs?tab=notif" {
		t.Errorf("admin GET /notifications = %d %q, want 303 /logs?tab=notif", resp.StatusCode, resp.Header.Get("Location"))
	}
	resp, _ := env.get("/health")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
		t.Errorf("admin GET /health = %d %q, want 303 /dashboard", resp.StatusCode, resp.Header.Get("Location"))
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
	if strings.Contains(html, `class="user-menu-item" role="menuitem" href="/"`) {
		t.Error("user menu must not offer the public page (the topbar button covers it)")
	}
	if !strings.Contains(html, `class="topbar-home" href="/"`) {
		t.Error("topbar missing the public page button")
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

	// No cookie at all (the 15-minute token expired while the page was
	// open): the server must hand back a fresh form instead of a bare 403.
	form := url.Values{"username": {testUsername}, "password": {testPassword}}
	resp, html := env.postForm("/login", form)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /login without csrf = %d, want 200 (self-healed form)", resp.StatusCode)
	}
	if !strings.Contains(html, "session expired") {
		t.Errorf("self-healed login form should explain the expiry: %s", html)
	}
	csrf := extractCSRF(t, html)
	if csrf == "" {
		t.Fatal("self-healed login form missing its csrf token")
	}
	// The fresh token must actually log in (the jar keeps the new cookie).
	resp2, _ := env.postForm("/login", url.Values{"csrf": {csrf}, "username": {testUsername}, "password": {testPassword}})
	if resp2.StatusCode != http.StatusSeeOther {
		t.Fatalf("login with the refreshed token = %d, want 303", resp2.StatusCode)
	}

	// A present but wrong token self-heals the same way.
	env.logout()
	_, html2 := env.get("/login")
	csrf2 := extractCSRF(t, html2)
	resp3, _ := env.postForm("/login", url.Values{"csrf": {csrf2 + "x"}, "username": {testUsername}, "password": {testPassword}})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("POST /login with a wrong csrf = %d, want 200 with a fresh form", resp3.StatusCode)
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

	// The dashboard is purely technical: warnings live on the public home
	// page and their own partial, not in the dashboard grid.
	_, dash := env.get("/dashboard?wpage=2")
	if strings.Contains(dash, `data-wpage="2"`) || strings.Contains(dash, "21–40 of 45") {
		t.Errorf("dashboard still renders the warnings section: %s", dash)
	}
}

func TestDashboardSystemCard(t *testing.T) {
	env := newTestEnv(t)
	env.login()
	_, html := env.get("/partials/status")
	for _, want := range []string{"Version", "Uptime", "Memory"} {
		if !strings.Contains(html, want) {
			t.Errorf("system card missing %q: %s", want, html)
		}
	}
	// The DB and dispatch rows moved to the health card (same page).
	if strings.Contains(html, "Dispatch queue") {
		t.Errorf("system card still renders the dispatch queue row: %s", html)
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

// postFormClose performs a POST on a FRESH TCP connection (Connection:
// close) with optional extra headers — a new source port per call, like
// a client that reconnects between attempts.
func (e *testEnv) postFormClose(path string, values url.Values, headers map[string]string) *http.Response {
	e.t.Helper()
	req, err := http.NewRequest(http.MethodPost, e.srv.URL+path, strings.NewReader(values.Encode()))
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Close = true
	resp, err := e.client.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

// TestLoginThrottledAcrossConnections pins the port normalization: a
// fresh TCP connection (new source port) must NOT reset the limiter —
// the sixth failure from the same IP is still 429.
func TestLoginThrottledAcrossConnections(t *testing.T) {
	env := newTestEnv(t)
	_, html := env.get("/login")
	form := url.Values{"csrf": {extractCSRF(t, html)}, "username": {testUsername}, "password": {"wrong-password"}}
	for i := 0; i < 5; i++ {
		resp := env.postFormClose("/login", form, nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401", i+1, resp.StatusCode)
		}
	}
	resp := env.postFormClose("/login", form, nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("sixth attempt on a fresh connection = %d, want 429", resp.StatusCode)
	}
}

// TestLoginThrottledByForwardedIP pins the trusted-proxy path: behind a
// configured proxy the per-IP budget keys on the forwarded client
// address, so attacks spread across accounts are still caught.
func TestLoginThrottledByForwardedIP(t *testing.T) {
	env := newTestEnvWithAuth(t, config.WebAuth{
		Username: testUsername, Password: testPassword,
		TrustedProxies: []string{"127.0.0.1"},
	})
	_, html := env.get("/login")
	csrf := extractCSRF(t, html)
	post := func(user, xff string) *http.Response {
		return env.postFormClose("/login",
			url.Values{"csrf": {csrf}, "username": {user}, "password": {"wrong"}},
			map[string]string{"X-Forwarded-For": xff})
	}
	// Five failures from 10.0.0.1 spread over five accounts; four more
	// from 10.0.0.2 over four more accounts.
	for i := 0; i < 5; i++ {
		if resp := post(fmt.Sprintf("u%d", i), "10.0.0.1"); resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("account %d attempt = %d, want 401", i, resp.StatusCode)
		}
	}
	for i := 5; i < 9; i++ {
		if resp := post(fmt.Sprintf("u%d", i), "10.0.0.2"); resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("account %d attempt = %d, want 401", i, resp.StatusCode)
		}
	}
	// The per-IP budget of 10.0.0.1 is spent: the next attempt from it
	// is throttled even under a fresh account.
	if resp := post("u9", "10.0.0.1"); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("sixth attempt from 10.0.0.1 = %d, want 429", resp.StatusCode)
	}
}

// TestLoginProxyHeadersOnlyFromTrustedProxies pins the untrusted case:
// without configured proxies X-Forwarded-For is ignored and the per-IP
// budget keys on the direct peer, so header rotation buys nothing.
func TestLoginProxyHeadersOnlyFromTrustedProxies(t *testing.T) {
	env := newTestEnv(t) // no trusted proxies configured
	_, html := env.get("/login")
	csrf := extractCSRF(t, html)
	post := func(user, xff string) *http.Response {
		return env.postFormClose("/login",
			url.Values{"csrf": {csrf}, "username": {user}, "password": {"wrong"}},
			map[string]string{"X-Forwarded-For": xff})
	}
	// Four failures under one forged XFF identity.
	for i := 0; i < 4; i++ {
		if resp := post(fmt.Sprintf("u%d", i), "10.0.0.1"); resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401", i, resp.StatusCode)
		}
	}
	// The fifth failure rotates to a fresh identity AND a fresh account:
	// still within the shared budget (headers are ignored), so 401.
	if resp := post("u4", "10.0.0.2"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("fifth attempt = %d, want 401 (within the shared budget)", resp.StatusCode)
	}
	// The sixth failure rotates again. Honoring the header would keep
	// every budget below the threshold (401); the direct peer is what
	// counts, so it is throttled (429).
	if resp := post("u5", "10.0.0.3"); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("sixth attempt with rotated headers = %d, want 429 (headers ignored)", resp.StatusCode)
	}
}

// TestLoginGlobalBudget pins the fleet-wide budget: distributed guessing
// across accounts and IPs trips the global counter and locks the login
// form for everyone.
func TestLoginGlobalBudget(t *testing.T) {
	env := newTestEnvWithAuth(t, config.WebAuth{
		Username: testUsername, Password: testPassword,
		TrustedProxies: []string{"127.0.0.1"},
	})
	_, html := env.get("/login")
	csrf := extractCSRF(t, html)
	// loginGlobalBudget lives in the web package; this mirror keeps the
	// external test readable.
	const globalBudget = 30
	for i := 0; i < globalBudget; i++ {
		resp := env.postFormClose("/login",
			url.Values{"csrf": {csrf}, "username": {fmt.Sprintf("u%d", i)}, "password": {"wrong"}},
			map[string]string{"X-Forwarded-For": fmt.Sprintf("10.1.%d.%d", i/250, i%250+1)})
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401", i+1, resp.StatusCode)
		}
	}
	resp := env.postFormClose("/login",
		url.Values{"csrf": {csrf}, "username": {"another"}, "password": {"wrong"}},
		map[string]string{"X-Forwarded-For": "10.2.2.2"})
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("attempt past the global budget = %d, want 429", resp.StatusCode)
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
	if !strings.Contains(html, "EMCOM networks") || !strings.Contains(html, "Full activation") {
		t.Fatalf("emcom page missing sections: %s", html)
	}
	csrf := extractCSRF(t, html)

	// CSRF is enforced on mutations (network management lives on the
	// admin Config page).
	resp, _ = env.postForm("/config/emcom/add", url.Values{"name": {"SP9MOA EMCOM"}})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /config/emcom/add without csrf = %d, want 403", resp.StatusCode)
	}

	// Add a network: one retained info document at level 0.
	resp, _ = env.postForm("/config/emcom/add", url.Values{"csrf": {csrf}, "name": {"SP9MOA EMCOM"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /config/emcom/add = %d, want 303", resp.StatusCode)
	}
	raw := env.pub.rawSnapshot()
	if len(raw) != 1 || raw[0].Suffix != "info/emcom/emcom/sp9moa-emcom/emcom" || !raw[0].Retained {
		t.Fatalf("raw publish = %+v", raw)
	}
	var wire struct {
		Network string `json:"network"`
		Slug    string `json:"slug"`
		Level   int    `json:"level"`
	}
	if err := json.Unmarshal(raw[0].Payload, &wire); err != nil || wire.Slug != "sp9moa-emcom" || wire.Level != 0 {
		t.Fatalf("state payload = %s (%v)", raw[0].Payload, err)
	}

	// The fake publisher does not echo: mirror the broker loopback into
	// the state like the receiver would.
	mirrorInfo := func(i int) {
		raw := env.pub.rawSnapshot()
		env.state.AddOrUpdateInfo("local", "warnflux/info/emcom/emcom/sp9moa-emcom/emcom", state.InfoEntry{
			Source: "emcom", ProducerID: "emcom", Key: "sp9moa-emcom", Kind: "emcom",
			ReceivedAt: time.Now(), Payload: raw[i].Payload,
		})
	}
	mirrorInfo(0)

	// Duplicate names are rejected.
	_, html = env.get("/emcom")
	csrf = extractCSRF(t, html)
	resp, _ = env.postForm("/config/emcom/add", url.Values{"csrf": {csrf}, "name": {"sp9moa EMCOM"}})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate add = %d, want 409", resp.StatusCode)
	}

	// Raise to level 2: a severe hazard document plus a canonical
	// transition through the dispatch ingress.
	resp, _ = env.postForm("/emcom/sp9moa-emcom/level", url.Values{"csrf": {csrf}, "level": {"2"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST level 2 = %d, want 303", resp.StatusCode)
	}
	pub := env.pub.publishedSnapshot()
	if len(pub) != 1 {
		t.Fatalf("published hazards = %d, want 1", len(pub))
	}
	h := pub[0]
	if h.EventKey != "emcom:sp9moa-emcom" || h.Severity != "severe" || h.Urgency != "immediate" ||
		!strings.Contains(h.Headline, "level 2 – Local activation") || !strings.Contains(h.Headline, "SP9MOA EMCOM") {
		t.Errorf("hazard = %+v", h)
	}
	// Activation notifications must be self-explanatory: the activated
	// level's definition, the FULL readiness scale and the operator
	// instruction ride along (the SMTP action prints them).
	if !strings.Contains(h.Description, "level 2 – Local activation") ||
		!strings.Contains(h.Description, "directed net") {
		t.Errorf("hazard description missing the activated level definition: %+v", h.Description)
	}
	if !strings.Contains(h.Description, "Level 0 – Monitoring") ||
		!strings.Contains(h.Description, "Level 3 – Full activation") {
		t.Errorf("hazard description missing the full readiness scale: %+v", h.Description)
	}
	if !strings.Contains(h.Instruction, "directed net") {
		t.Errorf("hazard instruction missing the operator directive: %+v", h.Instruction)
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
	if !strings.Contains(ev.Hazard.Hazard.Description, "Operational readiness levels") ||
		ev.Hazard.Hazard.Instruction == "" {
		t.Errorf("transition lost the EMCOM description/instruction: %+v", ev.Hazard.Hazard)
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
	if !strings.Contains(homeHTML, "SP9MOA EMCOM") || !strings.Contains(homeHTML, "level 2") ||
		!strings.Contains(homeHTML, "emcom-chip-l2") {
		t.Fatalf("home header chip missing: %s", homeHTML)
	}
	if !strings.Contains(homeHTML, "level 2 – Local activation") {
		t.Errorf("active communication missing on home page: %s", homeHTML)
	}
	// While raised, the communications heading carries the readiness
	// info icon and the shared levels legend.
	if !strings.Contains(homeHTML, `data-emcom-info`) || !strings.Contains(homeHTML, `id="emcom-info-tpl"`) {
		t.Errorf("raised home page missing the readiness info popup: %s", homeHTML)
	}
	if !strings.Contains(homeHTML, "Operational readiness levels") {
		t.Errorf("raised home page missing the shared levels legend: %s", homeHTML)
	}

	// Back to monitoring: the hazard is retired and the chip drops to l0.
	resp, _ = env.postForm("/emcom/sp9moa-emcom/level", url.Values{"csrf": {csrf}, "level": {"0"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST level 0 = %d, want 303", resp.StatusCode)
	}
	exp := env.pub.expiredSnapshot()
	if len(exp) != 1 || exp[0] != "emcom:sp9moa-emcom" {
		t.Fatalf("expired = %v", exp)
	}
	// The drop dispatches ONE expiry transition flagged Notify: it
	// retires the document AND starts the notification machine so
	// people learn the network stood down.
	select {
	case ev = <-env.ingress.Events():
	case <-time.After(time.Second):
		t.Fatal("no deactivation transition enqueued")
	}
	if ev.Hazard == nil || ev.Hazard.Type != dispatch.TransitionExpired || !ev.Hazard.Notify ||
		ev.Hazard.Hazard.Severity != "severe" ||
		!strings.Contains(ev.Hazard.Hazard.Description, "was lowered") {
		t.Errorf("deactivation transition = %+v", ev)
	}
	mirrorInfo(3) // level 0 payload
	// The fake publisher does not echo the retire: simulate the broker
	// deleting the active document like the receiver would.
	env.state.DeleteActive("local", "warnflux/active/emcom/abc")
	_, homeHTML = env.get("/")
	// Monitoring is the default state: level-0 networks disappear from
	// the public header entirely.
	if strings.Contains(homeHTML, "emcom-chip") || strings.Contains(homeHTML, "level 2 – Local activation") {
		t.Errorf("monitoring network still shown on the home header: %s", homeHTML)
	}
	if strings.Contains(homeHTML, "data-emcom-info") {
		t.Errorf("readiness info icon must disappear at level 0: %s", homeHTML)
	}

	// Deleting the network clears the retained document.
	_, html = env.get("/emcom")
	csrf = extractCSRF(t, html)
	resp, _ = env.postForm("/config/emcom/sp9moa-emcom/delete", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST delete = %d, want 303", resp.StatusCode)
	}
	last := env.pub.rawSnapshot()
	lastRaw := last[len(last)-1]
	if !lastRaw.Retained || len(lastRaw.Payload) != 0 {
		t.Errorf("delete publish = %+v, want retained empty payload", lastRaw)
	}

	// Unauthenticated access redirects to the login page.
	env.logout()
	resp, _ = env.get("/emcom")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /emcom unauthenticated = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
}

// newLocalPanelStore opens a real SQLite store for the local-first panel
// tests (the directory store doubles as the compose/emcom local record).
func newLocalPanelStore(t *testing.T) *sqlite.Store {
	t.Helper()
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "panel.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// TestComposeAutoExpireReconcile pins the restart-survival fix: the
// AfterFunc expiry timers die with the process, so the maintenance loop
// reconciles every active communication whose expires_at already passed
// (the prod incident: a message expired between restarts and stayed
// active on the public page for hours).
func TestComposeAutoExpireReconcile(t *testing.T) {
	store := newLocalPanelStore(t)
	env := newTestEnvWithUsers(t, store)
	ctx := context.Background()
	now := time.Now()
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)

	seed := func(key string, status string, exp *time.Time) {
		b, err := json.Marshal(state.Hazard{
			EventKey: key, Source: "compose", Headline: key,
			Severity: "severe", ExpiresAt: exp, UpdatedAt: now,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.SaveComposeHazard(ctx, storage.ComposeHazard{
			EventKey: key, State: b, Status: status, UpdatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	seed("compose:gone", "active", &past)
	seed("compose:live", "active", &future)
	seed("compose:dead", "expired", &past)

	n, err := env.server.AutoExpireCompose(ctx)
	if err != nil {
		t.Fatalf("AutoExpireCompose = %v", err)
	}
	if n != 1 {
		t.Fatalf("AutoExpireCompose expired %d, want 1", n)
	}

	rows, err := store.ComposeHazards(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status := map[string]string{}
	for _, row := range rows {
		status[row.EventKey] = row.Status
	}
	if status["compose:gone"] != "expired" {
		t.Errorf("compose:gone status = %q, want expired", status["compose:gone"])
	}
	if status["compose:live"] != "active" {
		t.Errorf("compose:live status = %q, want active (still valid)", status["compose:live"])
	}

	// The expiry transition reaches the canonical ingress.
	ev := drainIngress(env)
	if ev == nil || ev.Hazard == nil || ev.Hazard.Type != dispatch.TransitionExpired || ev.Hazard.Key != "compose:gone" {
		t.Fatalf("reconcile did not enqueue the expiry transition: %+v", ev)
	}

	// The broker retirement is asynchronous — wait for it.
	deadline := time.Now().Add(2 * time.Second)
	for len(env.pub.expiredSnapshot()) < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := env.pub.expiredSnapshot(); len(got) != 1 || got[0] != "compose:gone" {
		t.Fatalf("broker retirement = %v, want [compose:gone]", got)
	}

	// A second run is a no-op: the expiry is idempotent.
	if n, err := env.server.AutoExpireCompose(ctx); err != nil || n != 0 {
		t.Fatalf("second AutoExpireCompose = %d, %v; want 0, nil", n, err)
	}
}

// TestComposeLocalFirstNoBroker pins the P1 fix: a communication is
// persisted and routed locally even when the broker is unreachable — the
// save succeeds (303), the transition hits the ingress and the issued
// list renders from the local record.
func TestComposeLocalFirstNoBroker(t *testing.T) {
	env := newTestEnvWithUsers(t, newLocalPanelStore(t))
	env.pub.fail = true // broker down
	env.login()

	_, html := env.get("/compose")
	csrf := extractCSRF(t, html)
	resp, _ := env.postForm("/compose", url.Values{
		"csrf":     {csrf},
		"event":    {"Storm"},
		"headline": {"Test storm"},
		"severity": {"severe"},
		"status":   {"active"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("compose save with a down broker = %d, want 303 (local-first)", resp.StatusCode)
	}
	var ev dispatch.Event
	select {
	case ev = <-env.ingress.Events():
	case <-time.After(time.Second):
		t.Fatal("no transition enqueued")
	}
	if ev.Hazard == nil || ev.Hazard.Type != dispatch.TransitionNew || ev.Hazard.Key == "" {
		t.Fatalf("transition = %+v, want a new transition", ev)
	}
	key := ev.Hazard.Key

	// The local record renders the issued list even though the mirror
	// never saw the document.
	_, html = env.get("/compose")
	if !strings.Contains(html, "Test storm") {
		t.Errorf("issued list missing the communication: %s", html)
	}

	// Expiry works without the broker too.
	resp, _ = env.postForm("/compose/expire", url.Values{"csrf": {csrf}, "event_key": {key}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("compose expire with a down broker = %d, want 303 (local-first)", resp.StatusCode)
	}
	select {
	case ev = <-env.ingress.Events():
	case <-time.After(time.Second):
		t.Fatal("no expiry transition enqueued")
	}
	if ev.Hazard == nil || ev.Hazard.Type != dispatch.TransitionExpired {
		t.Errorf("expiry transition = %+v", ev)
	}
	_, html = env.get("/compose")
	if strings.Contains(html, "Test storm") {
		t.Errorf("expired communication still listed: %s", html)
	}
}

// brokenExpireStore is a store whose compose EXPIRY saves always fail:
// the reported fault injection for the panel expiry path.
type brokenExpireStore struct{ *sqlite.Store }

func (b *brokenExpireStore) SaveComposeHazard(ctx context.Context, h storage.ComposeHazard) error {
	if h.Status == "expired" {
		return errors.New("disk broken")
	}
	return b.Store.SaveComposeHazard(ctx, h)
}

// TestComposeExpireSaveFailureRejects pins the reported P1 at the panel
// boundary: expiring a message whose local record cannot be persisted
// must fail the OPERATION — the form sees the failure and no expiry
// transition is dispatched (previously the handler only logged the error
// and answered success while the delivery queue stayed open for the
// retired message).
func TestComposeExpireSaveFailureRejects(t *testing.T) {
	inner := newLocalPanelStore(t)
	store := &brokenExpireStore{Store: inner}
	env := newTestEnvWithUsers(t, store)
	env.login()

	// Publish through the healthy path so the record exists.
	_, html := env.get("/compose")
	csrf := extractCSRF(t, html)
	resp, _ := env.postForm("/compose", url.Values{
		"csrf":     {csrf},
		"event":    {"Storm"},
		"headline": {"Test storm"},
		"severity": {"severe"},
		"status":   {"active"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("compose save = %d, want 303", resp.StatusCode)
	}
	var ev dispatch.Event
	select {
	case ev = <-env.ingress.Events():
	case <-time.After(time.Second):
		t.Fatal("no transition enqueued")
	}
	key := ev.Hazard.Key

	// The expiry save fails: the operation must be REJECTED and no
	// expiry transition may be dispatched.
	resp, _ = env.postForm("/compose/expire", url.Values{"csrf": {csrf}, "event_key": {key}})
	if resp.StatusCode == http.StatusSeeOther {
		t.Fatalf("compose expire reported success while the local record could not be saved (status %d)", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("compose expire with a broken record = %d, want 503", resp.StatusCode)
	}
	select {
	case ev := <-env.ingress.Events():
		t.Fatalf("expiry transition dispatched despite the failed save: %+v", ev.Hazard)
	case <-time.After(150 * time.Millisecond):
	}
}

// TestEmcomLocalFirstNoBroker pins the P1 fix for the readiness panel:
// networks are saved in the local database and level changes route
// locally even when the broker is unreachable.
func TestEmcomLocalFirstNoBroker(t *testing.T) {
	env := newTestEnvWithUsers(t, newLocalPanelStore(t))
	env.pub.fail = true // broker down
	env.login()

	_, html := env.get("/emcom")
	csrf := extractCSRF(t, html)
	resp, _ := env.postForm("/config/emcom/add", url.Values{"csrf": {csrf}, "name": {"SP9MOA EMCOM"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("emcom add with a down broker = %d, want 303 (local-first)", resp.StatusCode)
	}
	_, html = env.get("/emcom")
	if !strings.Contains(html, "SP9MOA EMCOM") {
		t.Fatalf("network missing from the panel despite the dead broker: %s", html)
	}

	// Raising the level dispatches locally even though the broker is down.
	resp, _ = env.postForm("/emcom/sp9moa-emcom/level", url.Values{"csrf": {csrf}, "level": {"2"}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("emcom level with a down broker = %d, want 303 (local-first)", resp.StatusCode)
	}
	var ev dispatch.Event
	select {
	case ev = <-env.ingress.Events():
	case <-time.After(time.Second):
		t.Fatal("no transition enqueued")
	}
	if ev.Hazard == nil || ev.Hazard.Type != dispatch.TransitionNew || ev.Hazard.Hazard.Severity != "severe" {
		t.Errorf("transition = %+v, want a new severe transition", ev)
	}
	_, html = env.get("/emcom")
	if !strings.Contains(html, "emcom-badge-l2") || !strings.Contains(html, "Local activation") {
		t.Errorf("panel missing the raised level: %s", html)
	}

	// Deleting works locally too.
	resp, _ = env.postForm("/config/emcom/sp9moa-emcom/delete", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("emcom delete with a down broker = %d, want 303 (local-first)", resp.StatusCode)
	}
	_, html = env.get("/emcom")
	if strings.Contains(html, "SP9MOA EMCOM") {
		t.Errorf("deleted network still listed: %s", html)
	}
}

// TestEmcomLevelZeroBlocksQueuedActivation pins the P1 at the panel
// boundary: EMCOM transitions carry the instance publisher and the
// network state version, and dropping a network back to monitoring
// records the cancellation atomically with the network state — the
// delivery gate then blocks the previously queued activation, so a
// returning radio cannot transmit the stale raise.
func TestEmcomLevelZeroBlocksQueuedActivation(t *testing.T) {
	store := newLocalPanelStore(t)
	env := newTestEnvWithUsers(t, store)
	env.login()

	_, html := env.get("/emcom")
	csrf := extractCSRF(t, html)
	if resp, _ := env.postForm("/config/emcom/add", url.Values{"csrf": {csrf}, "name": {"SP9MOA EMCOM"}}); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("add = %d, want 303", resp.StatusCode)
	}

	publisher, err := store.InstanceID(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Raise: the activation carries the instance publisher and the
	// network state version as its ChangeID.
	if resp, _ := env.postForm("/emcom/sp9moa-emcom/level", url.Values{"csrf": {csrf}, "level": {"2"}}); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("raise = %d, want 303", resp.StatusCode)
	}
	var activation dispatch.Event
	select {
	case activation = <-env.ingress.Events():
	case <-time.After(time.Second):
		t.Fatal("no activation transition enqueued")
	}
	if activation.Hazard == nil {
		t.Fatal("activation without hazard")
	}
	if activation.Hazard.Publisher != publisher {
		t.Errorf("activation publisher = %q, want the instance id %q", activation.Hazard.Publisher, publisher)
	}
	if activation.Hazard.ChangeID == 0 {
		t.Error("activation ChangeID = 0, want the network state version")
	}
	key := activation.Hazard.Key

	// The fresh activation passes the delivery gate right now.
	blocked, err := store.LifecycleBlocks(context.Background(), publisher, key, activation.Hazard.ChangeID)
	if err != nil || blocked {
		t.Fatalf("gate for the fresh activation = (%v, %v), want allowed", blocked, err)
	}

	// Drop back to monitoring: one expiry transition flagged Notify — it
	// records the cancellation atomically with the network state (the
	// delivery gate then blocks the queued activation) AND still starts
	// the notification machine for the stand-down.
	if resp, _ := env.postForm("/emcom/sp9moa-emcom/level", url.Values{"csrf": {csrf}, "level": {"0"}}); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("drop = %d, want 303", resp.StatusCode)
	}
	var expiry dispatch.Event
	select {
	case expiry = <-env.ingress.Events():
	case <-time.After(time.Second):
		t.Fatal("no expiry transition enqueued")
	}
	if expiry.Hazard == nil || expiry.Hazard.Type != dispatch.TransitionExpired || !expiry.Hazard.Notify {
		t.Fatalf("expiry transition = %+v", expiry)
	}
	blocked, err = store.LifecycleBlocks(context.Background(), publisher, key, activation.Hazard.ChangeID)
	if err != nil || !blocked {
		t.Fatalf("gate for the queued activation after level 0 = (%v, %v), want blocked", blocked, err)
	}
}

// TestEmcomDeleteBlocksQueuedActivation pins the reported P1 at the
// panel boundary: deleting a network commits the tombstone, the
// lifecycle record and the expiry transition's durable inbox row in ONE
// transaction — the dispatched expiry carries the allocated version AND
// its inbox id, so a crash between the deletion and the dispatch (or a
// refused live handoff) can never lose the cancellation of a previously
// queued activation.
func TestEmcomDeleteBlocksQueuedActivation(t *testing.T) {
	store := newLocalPanelStore(t)
	env := newTestEnvWithUsers(t, store)
	env.login()

	_, html := env.get("/emcom")
	csrf := extractCSRF(t, html)
	if resp, _ := env.postForm("/config/emcom/add", url.Values{"csrf": {csrf}, "name": {"SP9MOA EMCOM"}}); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("add = %d, want 303", resp.StatusCode)
	}
	if resp, _ := env.postForm("/emcom/sp9moa-emcom/level", url.Values{"csrf": {csrf}, "level": {"2"}}); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("raise = %d, want 303", resp.StatusCode)
	}
	publisher, err := store.InstanceID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var activation dispatch.Event
	select {
	case activation = <-env.ingress.Events():
	case <-time.After(time.Second):
		t.Fatal("no activation transition enqueued")
	}
	key := activation.Hazard.Key

	// Delete: the expiry transition is dispatched with its committed
	// inbox id and the allocated version — the cancellation is durable
	// before the panel even answers.
	if resp, _ := env.postForm("/config/emcom/sp9moa-emcom/delete", url.Values{"csrf": {csrf}}); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("delete = %d, want 303", resp.StatusCode)
	}
	var expiry dispatch.Event
	select {
	case expiry = <-env.ingress.Events():
	case <-time.After(time.Second):
		t.Fatal("no expiry transition enqueued")
	}
	if expiry.Hazard == nil || expiry.Hazard.Type != dispatch.TransitionExpired {
		t.Fatalf("expiry transition = %+v", expiry.Hazard)
	}
	if expiry.InboxID == 0 {
		t.Error("expiry transition has no durable inbox row — a crash after the deletion would lose the cancellation")
	}
	if expiry.Hazard.ChangeID <= activation.Hazard.ChangeID {
		t.Errorf("expiry ChangeID = %d, want > the activation version %d", expiry.Hazard.ChangeID, activation.Hazard.ChangeID)
	}

	// The previously queued activation is blocked.
	blocked, err := store.LifecycleBlocks(context.Background(), publisher, key, activation.Hazard.ChangeID)
	if err != nil || !blocked {
		t.Fatalf("gate for the queued activation after delete = (%v, %v), want blocked", blocked, err)
	}
}

// TestAPRSMessagesPage pins the admin APRS message history: admin-only,
// reachable through the messages tab of the combined APRS section (the
// old /messages route stays as a redirect), and renders the (empty or
// populated) durable history. Row rendering is covered by the sqlite
// store tests plus live verification.
func TestAPRSMessagesPage(t *testing.T) {
	env := newTestEnv(t)

	resp, _ := env.get("/messages")
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("GET /messages unauthenticated = %d %q, want 303 /login", resp.StatusCode, resp.Header.Get("Location"))
	}

	env.login()
	resp2, _ := env.get("/messages")
	if resp2.StatusCode != http.StatusSeeOther || resp2.Header.Get("Location") != "/aprs?tab=msgs" {
		t.Fatalf("GET /messages = %d %q, want 303 /aprs?tab=msgs", resp2.StatusCode, resp2.Header.Get("Location"))
	}
	resp3, html := env.get("/aprs?tab=msgs")
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("GET /aprs?tab=msgs = %d, want 200", resp3.StatusCode)
	}
	if !strings.Contains(html, "messages.none") && !strings.Contains(html, "No APRS messages recorded yet") {
		t.Errorf("messages tab missing empty state: %s", html)
	}
	if !strings.Contains(html, "/aprs?tab=msgs&dir=rx") || !strings.Contains(html, "/aprs?tab=msgs&dir=tx") {
		t.Errorf("messages tab missing direction filters: %s", html)
	}
	if !strings.Contains(html, `action="/messages/send"`) {
		t.Errorf("messages tab missing send form: %s", html)
	}
	if !strings.Contains(html, `<span class="nav-label">APRS</span>`) {
		t.Errorf("combined APRS page nav should read APRS: %s", html)
	}
}

// TestContactPickers pins the recipient pickers: the APRS send form
// offers registered user callsigns and the Meshtastic send form offers the
// registered node ids.
func TestContactPickers(t *testing.T) {
	env := newTestEnv(t)
	u, err := env.users.CreateUser("sp9kow", "600111222", "", "", "member", "pw1")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.users.SetUserAPRS(u.ID, []string{"sp9kow-7"}); err != nil {
		t.Fatal(err)
	}
	if err := env.users.SetUserMeshtasticIDs(u.ID, []string{"abcd1234"}); err != nil {
		t.Fatal(err)
	}
	env.login()

	_, html := env.get("/aprs?tab=msgs")
	if !strings.Contains(html, `list="aprs-calls"`) || !strings.Contains(html, `<datalist id="aprs-calls"><option value="SP9KOW-7">sp9kow</option>`) {
		t.Errorf("APRS page missing callsign picker: %s", html)
	}

	_, html = env.get("/meshtastic")
	if !strings.Contains(html, `list="mesh-contacts"`) || !strings.Contains(html, `<datalist id="mesh-contacts"><option value="abcd1234">sp9kow</option>`) {
		t.Errorf("meshtastic page missing contact picker: %s", html)
	}
}

// TestAPRSSendValidation pins the send form: bad CSRF is rejected and an
// invalid addressee flashes the error instead of transmitting.
func TestAPRSSendValidation(t *testing.T) {
	hub, err := aprs.NewHub(aprs.HubConfig{Enabled: false}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	env := newTestEnvWithHub(t, hub)
	env.login()
	_, html := env.get("/aprs?tab=msgs")
	csrf := extractCSRF(t, html)

	// Wrong CSRF: hard 403.
	resp, _ := env.postForm("/messages/send", url.Values{"csrf": {"bogus"}, "to": {"SP9XXX"}, "text": {"hello"}})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST send bad csrf = %d, want 403", resp.StatusCode)
	}

	// Invalid addressee: redirect with an error flash, nothing sent.
	resp, _ = env.postForm("/messages/send", url.Values{"csrf": {csrf}, "to": {"!!!"}, "text": {"hello"}})
	if resp.StatusCode != http.StatusSeeOther || !strings.Contains(resp.Header.Get("Location"), "err=") {
		t.Fatalf("POST send invalid callsign = %d %q, want redirect with err", resp.StatusCode, resp.Header.Get("Location"))
	}

	// Empty text: same treatment.
	resp, _ = env.postForm("/messages/send", url.Values{"csrf": {csrf}, "to": {"SP9XXX"}, "text": {"  "}})
	if resp.StatusCode != http.StatusSeeOther || !strings.Contains(resp.Header.Get("Location"), "err=") {
		t.Fatalf("POST send empty text = %d %q, want redirect with err", resp.StatusCode, resp.Header.Get("Location"))
	}

	// No transmitter available: error surfaced as a flash too.
	resp, _ = env.postForm("/messages/send", url.Values{"csrf": {csrf}, "to": {"SP9XXX"}, "text": {"hello"}})
	if resp.StatusCode != http.StatusSeeOther || !strings.Contains(resp.Header.Get("Location"), "err=") {
		t.Fatalf("POST send no transmitter = %d %q, want redirect with err", resp.StatusCode, resp.Header.Get("Location"))
	}
}

// TestAPRSBeaconValidation pins the manual beacon button: bad CSRF is
// rejected and, with no beacon-capable transmitter connected, the error
// flashes back on the page instead of pretending success.
func TestAPRSBeaconValidation(t *testing.T) {
	hub, err := aprs.NewHub(aprs.HubConfig{Enabled: false}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	env := newTestEnvWithHub(t, hub)
	env.login()
	_, html := env.get("/aprs?tab=msgs")
	csrf := extractCSRF(t, html)

	resp, _ := env.postForm("/messages/beacon", url.Values{"csrf": {"bogus"}})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST beacon bad csrf = %d, want 403", resp.StatusCode)
	}
	resp, _ = env.postForm("/messages/beacon", url.Values{"csrf": {csrf}})
	if resp.StatusCode != http.StatusSeeOther || !strings.Contains(resp.Header.Get("Location"), "err=") {
		t.Fatalf("POST beacon no transmitter = %d %q, want redirect with err", resp.StatusCode, resp.Header.Get("Location"))
	}
}

// TestMeshtasticTracerouteGuard pins the admin route-probe endpoint:
// it is CSRF-guarded and answers 404 while the mesh hub is disabled.
func TestMeshtasticTracerouteGuard(t *testing.T) {
	env := newTestEnv(t)
	env.login()

	resp, _ := env.postForm("/api/meshtastic/traceroute", url.Values{"csrf": {"bogus"}, "to": {"abcd1234"}})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST traceroute bad csrf = %d, want 403", resp.StatusCode)
	}
	_, html := env.get("/meshtastic")
	csrf := extractCSRF(t, html)
	resp, _ = env.postForm("/api/meshtastic/traceroute", url.Values{"csrf": {csrf}, "to": {"abcd1234"}})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("POST traceroute without hub = %d, want 404", resp.StatusCode)
	}
}

func TestMeshtasticStationsAPI(t *testing.T) {
	// Without a mesh hub the public endpoint is absent.
	env := newTestEnv(t)
	resp, _ := env.get("/api/meshtastic/stations")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /api/meshtastic/stations without mesh = %d, want 404", resp.StatusCode)
	}

	// With a mesh hub configured the endpoint serves the two lists
	// (located nodes and the position-less badge list).
	meshtasticHub, err := meshtastic.NewHub(meshtastic.Config{
		Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	env = newTestEnvWithMesh(t, meshtasticHub)
	resp, body := env.get("/api/meshtastic/stations")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/meshtastic/stations = %d", resp.StatusCode)
	}
	var view struct {
		Nodes []struct {
			ID   string  `json:"id"`
			Name string  `json:"name"`
			Lat  float64 `json:"latitude"`
			Lon  float64 `json:"longitude"`
		} `json:"nodes"`
		NoPos []struct {
			ID string `json:"id"`
		} `json:"nopos"`
	}
	if err := json.Unmarshal([]byte(body), &view); err != nil {
		t.Fatalf("payload = %s: %v", body, err)
	}
	if view.Nodes == nil || view.NoPos == nil {
		t.Fatalf("payload = %s, want nodes and nopos arrays", body)
	}
}

// TestMessageListPartials pins the live-refresh fragments of the APRS and
// Meshtastic message lists: admin-only, filtered by dir, and always
// rendering either the table or the empty-state marker.
func TestMessageListPartials(t *testing.T) {
	env := newTestEnv(t)

	// Unauthenticated: hard 401 so the poller redirects.
	resp, _ := env.get("/partials/messages")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /partials/messages anon = %d, want 401", resp.StatusCode)
	}

	env.login()
	for _, p := range []string{
		"/partials/messages",
		"/partials/messages?dir=rx",
		"/partials/meshtastic?tab=messages",
		"/partials/meshtastic?tab=messages&dir=tx",
		"/partials/meshtastic?tab=messages&dir=ch0",
	} {
		resp, body := env.get(p)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s = %d", p, resp.StatusCode)
		}
		if !strings.Contains(body, "home-none") && !strings.Contains(body, "msgs-table") {
			t.Fatalf("GET %s missing list or empty-state: %.120s", p, body)
		}
	}
}

// fakeMeshMsgs is an in-memory MeshtasticMessageStore for the admin history.
type fakeMeshMsgs struct {
	rows []storage.MeshMessage
}

func (f *fakeMeshMsgs) RecordMeshtasticMessage(_ context.Context, direction, sender, recipient, channel, text, operator string, hops int, at time.Time) error {
	f.rows = append(f.rows, storage.MeshMessage{Direction: direction, Sender: sender, Recipient: recipient, Channel: channel, Hops: hops, Operator: operator, Text: text, At: at})
	return nil
}

func (f *fakeMeshMsgs) UpdateMeshtasticMessageStatus(_ context.Context, status string, at time.Time, text string) error {
	for i := range f.rows {
		if f.rows[i].Direction == "tx" && f.rows[i].At.Equal(at) && f.rows[i].Text == text {
			f.rows[i].Status = status
		}
	}
	return nil
}

func (f *fakeMeshMsgs) ListMeshtasticMessages(_ context.Context, flt storage.MeshtasticMessageFilter, limit, offset int) ([]storage.MeshMessage, error) {
	var out []storage.MeshMessage
	for _, m := range f.rows {
		if flt.Direction != "" && m.Direction != flt.Direction {
			continue
		}
		if flt.Channel != "" {
			if flt.Exclude && m.Channel == flt.Channel {
				continue
			}
			if !flt.Exclude && m.Channel != flt.Channel {
				continue
			}
		}
		if flt.Peer != "" {
			ok := (m.Direction == "rx" && m.Sender == flt.Peer) ||
				(m.Direction == "tx" && m.Recipient == flt.Peer)
			if !ok {
				continue
			}
		}
		out = append(out, m)
	}
	if offset > len(out) {
		offset = len(out)
	}
	out = out[offset:]
	if limit < len(out) {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeMeshMsgs) CountMeshtasticMessages(_ context.Context, flt storage.MeshtasticMessageFilter) (int, error) {
	n := 0
	for _, m := range f.rows {
		if flt.Direction != "" && m.Direction != flt.Direction {
			continue
		}
		if flt.Channel != "" {
			if flt.Exclude && m.Channel == flt.Channel {
				continue
			}
			if !flt.Exclude && m.Channel != flt.Channel {
				continue
			}
		}
		if flt.Peer != "" {
			ok := (m.Direction == "rx" && m.Sender == flt.Peer) ||
				(m.Direction == "tx" && m.Recipient == flt.Peer)
			if !ok {
				continue
			}
		}
		n++
	}
	return n, nil
}

func (f *fakeMeshMsgs) MeshtasticPeers(_ context.Context) ([]string, error) {
	seen := make(map[string]bool)
	for _, m := range f.rows {
		if m.Channel != "dm" {
			continue
		}
		if m.Direction == "rx" && m.Sender != "" {
			seen[m.Sender] = true
		}
		if m.Direction == "tx" && m.Recipient != "" {
			seen[m.Recipient] = true
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// TestMeshMessageChannelNames pins the channel display: legacy "ch0" rows
// keep the raw label (the device provides friendly names when
// connected), and direct-message senders registered in the directory
// show their username with the hop count.
func TestMeshMessageChannelNames(t *testing.T) {
	hub, err := meshtastic.NewHub(meshtastic.Config{
		Enabled: true,
		Device:  "/dev/fake",
		NodeTTL: time.Hour,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	// deadbeef has a heard node name but no directory user.
	hub.SeedNode("deadbeef", "Bunkier", "", 0, 0, time.Now(), []string{"text"})
	store := &fakeMeshMsgs{rows: []storage.MeshMessage{
		{Direction: "rx", Channel: "ch0", Text: "hello", At: time.Now()},
		{Direction: "tx", Channel: "LongFast", Operator: "admin", Text: "73", At: time.Now()},
		{Direction: "rx", Sender: "abcd1234", Channel: "dm", Hops: 3, Text: "ggg", At: time.Now()},
		{Direction: "rx", Sender: "deadbeef", Channel: "dm", Text: "from-bunkier", At: time.Now()},
	}}
	env := newTestEnvAll(t, nil, nil, nil, hub, nil, store)
	// Register the sender's node id in the directory so the history can
	// show the username next to the raw id.
	u, err := env.users.CreateUser("sp9kow", "600111222", "", "", "member", "pw1")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.users.SetUserMeshtasticIDs(u.ID, []string{"abcd1234"}); err != nil {
		t.Fatal(err)
	}
	env.login()

	// The default DM view shows direct messages only.
	resp, body := env.get("/partials/meshtastic?tab=messages")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("partial = %d", resp.StatusCode)
	}
	if strings.Contains(body, "hello") || strings.Contains(body, "LongFast") {
		t.Errorf("DM partial must hide channel rows: %.300s", body)
	}
	// Directory users show their username; unknown senders show the
	// heard node name (the bare id stays visible as a suffix).
	for _, want := range []string{"ggg", "sp9kow", "!abcd1234", "via 3 hops", "Bunkier", "!deadbeef"} {
		if !strings.Contains(body, want) {
			t.Errorf("messages partial missing %q: %.300s", want, body)
		}
	}

	// The ch0 tab shows the primary-channel rows (both directions).
	resp, body = env.get("/partials/meshtastic?tab=messages&dir=ch0")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ch0 partial = %d", resp.StatusCode)
	}
	if !strings.Contains(body, "hello") || strings.Contains(body, "LongFast") || strings.Contains(body, "ggg") {
		t.Errorf("ch0 partial wrong rows: %.300s", body)
	}
}

// TestMeshtasticCh0Tab pins the primary-channel tab: it sits right after
// the tx tab, lists rx+tx ch0 rows with pagination, and every other dir
// view excludes ch0.
func TestMeshtasticCh0Tab(t *testing.T) {
	hub, err := meshtastic.NewHub(meshtastic.Config{
		Enabled: true,
		Device:  "/dev/fake",
		NodeTTL: time.Hour,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeMeshMsgs{}
	// The fake lists rows in append order (oldest first): append the tx
	// row before the filler so it lands on page 1 like a newest row.
	store.rows = append(store.rows,
		storage.MeshMessage{Direction: "tx", Channel: "ch0", Operator: "admin", Text: "tx-ch0", At: time.Now()},
	)
	for i := 0; i < 101; i++ {
		store.rows = append(store.rows, storage.MeshMessage{Direction: "rx", Channel: "ch0", Text: "ch0-fill", At: time.Now()})
	}
	store.rows = append(store.rows,
		storage.MeshMessage{Direction: "rx", Channel: "SP9MOA", Text: "sp9-row", At: time.Now()},
		storage.MeshMessage{Direction: "rx", Channel: "dm", Text: "dm-row", At: time.Now()},
	)
	env := newTestEnvAll(t, nil, nil, nil, hub, nil, store)
	env.login()

	// The tab strip: DM first, then ch0 (the rx/tx tabs are gone).
	_, html := env.get("/meshtastic")
	dmIdx := strings.Index(html, `href="/meshtastic?tab=msgs"`)
	ch0Idx := strings.Index(html, "/meshtastic?tab=msgs&dir=ch0")
	if dmIdx < 0 || ch0Idx < 0 || !(dmIdx < ch0Idx) {
		t.Fatalf("tab order wrong (dm=%d ch0=%d): %.200s", dmIdx, ch0Idx, html)
	}
	if strings.Contains(html, "/meshtastic?dir=rx") || strings.Contains(html, "/meshtastic?dir=tx") {
		t.Errorf("rx/tx tabs must be gone: %.300s", html)
	}

	// The DM view shows direct messages only (ch0 and channel rows live
	// on their own tabs).
	_, html = env.get("/meshtastic")
	if strings.Contains(html, "ch0-fill") || strings.Contains(html, "tx-ch0") || strings.Contains(html, "sp9-row") {
		t.Errorf("DM view leaked channel rows: %.300s", html)
	}
	if !strings.Contains(html, "dm-row") {
		t.Errorf("DM view missing dm rows: %.300s", html)
	}

	// The ch0 view shows both directions of the primary channel and
	// paginates (101 rx + 1 tx = 102 rows, 100 per page).
	_, html = env.get("/meshtastic?tab=msgs&dir=ch0")
	if !strings.Contains(html, "tx-ch0") {
		t.Errorf("ch0 view missing the tx row: %.300s", html)
	}
	if !strings.Contains(html, "/meshtastic?tab=msgs&dir=ch0&amp;page=2") {
		t.Errorf("ch0 view missing page-2 link: %.300s", html)
	}
	if strings.Contains(html, "sp9-row") || strings.Contains(html, "dm-row") {
		t.Errorf("ch0 view leaked other channels: %.300s", html)
	}
}

// TestMeshtasticDMTab pins the DM view: the dropdown lists the
// conversation partners (directory username, heard node name or bare id),
// the peer filter shows only that conversation with pagination, and the
// partial poller honors the peer parameter.
func TestMeshtasticDMTab(t *testing.T) {
	hub, err := meshtastic.NewHub(meshtastic.Config{
		Enabled: true,
		Device:  "/dev/fake",
		NodeTTL: time.Hour,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	// deadbeef has a heard node name; c0ffee11 is unknown entirely.
	hub.SeedNode("deadbeef", "Bunkier", "", 0, 0, time.Now(), []string{"text"})

	store := &fakeMeshMsgs{}
	// The fake lists rows in append order (oldest first): prepend the tx
	// row so it lands on page 1 like a newest row.
	store.rows = append(store.rows,
		storage.MeshMessage{Direction: "tx", Recipient: "a0a85934", Channel: "dm", Text: "to-spm", At: time.Now()},
	)
	for i := 0; i < 101; i++ {
		store.rows = append(store.rows, storage.MeshMessage{Direction: "rx", Sender: "a0a85934", Channel: "dm", Text: "from-spm", At: time.Now()})
	}
	store.rows = append(store.rows,
		storage.MeshMessage{Direction: "rx", Sender: "deadbeef", Channel: "dm", Text: "from-bunkier", At: time.Now()},
		storage.MeshMessage{Direction: "rx", Sender: "c0ffee11", Channel: "dm", Text: "from-unknown", At: time.Now()},
	)
	env := newTestEnvAll(t, nil, nil, nil, hub, nil, store)
	// Register a0a85934 in the directory as sp9spm.
	u, err := env.users.CreateUser("sp9spm", "", "", "", "member", "pw1")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.users.SetUserMeshtasticIDs(u.ID, []string{"a0a85934"}); err != nil {
		t.Fatal(err)
	}
	env.login()

	// The DM dropdown lists every dm participant with the best label:
	// directory username, heard node name, then the bare id.
	_, html := env.get("/meshtastic")
	for _, want := range []string{
		`name="peer"`,
		`<option value="a0a85934">sp9spm</option>`,
		`<option value="deadbeef">Bunkier</option>`,
		`<option value="c0ffee11">!c0ffee11</option>`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("DM filter missing %q: %.400s", want, html)
		}
	}

	// Default: no filter, every dm row, paginated.
	_, html = env.get("/meshtastic")
	if !strings.Contains(html, "to-spm") {
		t.Errorf("DM default view missing rows: %.300s", html)
	}
	if !strings.Contains(html, "/meshtastic?tab=msgs&dir=all&amp;page=2") {
		t.Errorf("DM default view missing page-2 link: %.300s", html)
	}
	_, html = env.get("/meshtastic?tab=msgs&page=2")
	if !strings.Contains(html, "from-unknown") {
		t.Errorf("DM page 2 missing rows: %.300s", html)
	}

	// Selecting a peer shows only that conversation (rx from them + tx
	// to them) and keeps the peer in the pagination links.
	_, html = env.get("/meshtastic?tab=msgs&peer=a0a85934")
	if strings.Contains(html, "from-bunkier") || strings.Contains(html, "from-unknown") {
		t.Errorf("peer view leaked other peers: %.300s", html)
	}
	if !strings.Contains(html, "to-spm") {
		t.Errorf("peer view missing tx row: %.300s", html)
	}
	if !strings.Contains(html, "/meshtastic?tab=msgs&dir=all&amp;peer=a0a85934&amp;page=2") {
		t.Errorf("peer view missing page-2 link with peer: %.300s", html)
	}
	if !strings.Contains(html, `<option value="a0a85934" selected>sp9spm</option>`) {
		t.Errorf("selected peer not marked: %.300s", html)
	}

	// The partial poller carries the peer filter too.
	_, body := env.get("/partials/meshtastic?tab=messages&peer=deadbeef")
	if !strings.Contains(body, "from-bunkier") || strings.Contains(body, "from-spm") || strings.Contains(body, "from-unknown") {
		t.Errorf("peer partial wrong rows: %.300s", body)
	}
}

// TestMeshtasticEmcomTab pins the configured-emcom-channel tab: it
// appears only when meshtastic.emcom_channel is set, sits right after
// ch0, lists that channel's rx+tx rows with pagination, and the DM view
// leaves the emcom rows to their own tab.
func TestMeshtasticEmcomTab(t *testing.T) {
	hub, err := meshtastic.NewHub(meshtastic.Config{
		Enabled:      true,
		Device:       "/dev/fake",
		NodeTTL:      time.Hour,
		EmcomChannel: 1,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeMeshMsgs{}
	// The fake lists rows in append order (oldest first): prepend the tx
	// row so it lands on page 1 like a newest row.
	store.rows = append(store.rows,
		storage.MeshMessage{Direction: "tx", Channel: "ch1", Operator: "admin", Text: "emcom-tx", At: time.Now()},
	)
	for i := 0; i < 101; i++ {
		store.rows = append(store.rows, storage.MeshMessage{Direction: "rx", Channel: "ch1", Text: "emcom-fill", At: time.Now()})
	}
	store.rows = append(store.rows,
		storage.MeshMessage{Direction: "rx", Channel: "ch0", Text: "c0-row", At: time.Now()},
		storage.MeshMessage{Direction: "rx", Channel: "dm", Text: "dm-row", At: time.Now()},
	)
	env := newTestEnvAll(t, nil, nil, nil, hub, nil, store)
	env.login()

	// The emcom tab sits right after ch0 (the device has not named the
	// channel yet, so the label is plain "ch1").
	_, html := env.get("/meshtastic")
	ch0Idx := strings.Index(html, "/meshtastic?tab=msgs&dir=ch0")
	emcomIdx := strings.Index(html, "/meshtastic?tab=msgs&dir=ch1")
	if ch0Idx < 0 || emcomIdx < 0 || !(ch0Idx < emcomIdx) {
		t.Fatalf("emcom tab missing or misplaced (ch0=%d ch1=%d): %.250s", ch0Idx, emcomIdx, html)
	}

	// The DM view shows direct messages only; the emcom channel rows
	// live on their own tab.
	if !strings.Contains(html, "dm-row") || strings.Contains(html, "emcom-tx") || strings.Contains(html, "c0-row") {
		t.Errorf("DM view wrong rows: %.300s", html)
	}

	// The emcom tab lists only the emcom channel, rx+tx, with pagination.
	_, html = env.get("/meshtastic?tab=msgs&dir=ch1")
	if !strings.Contains(html, `class="page-tab active" href="/meshtastic?tab=msgs&dir=ch1"`) {
		t.Errorf("emcom tab not active: %.300s", html)
	}
	if !strings.Contains(html, "emcom-tx") {
		t.Errorf("emcom view missing the tx row: %.300s", html)
	}
	if !strings.Contains(html, "/meshtastic?tab=msgs&dir=ch1&amp;page=2") {
		t.Errorf("emcom view missing page-2 link: %.300s", html)
	}
	if strings.Contains(html, "c0-row") || strings.Contains(html, "dm-row") {
		t.Errorf("emcom view leaked other channels: %.300s", html)
	}

	// The partial endpoint honors the emcom dir too.
	_, body := env.get("/partials/meshtastic?tab=messages&dir=ch1")
	if !strings.Contains(body, "emcom-tx") || strings.Contains(body, "dm-row") {
		t.Errorf("emcom partial wrong rows: %.300s", body)
	}
}

// TestMeshtasticEmcomTabHidden pins the config gate: without
// meshtastic.emcom_channel the page offers no emcom tab.
func TestMeshtasticEmcomTabHidden(t *testing.T) {
	hub, err := meshtastic.NewHub(meshtastic.Config{
		Enabled: true,
		Device:  "/dev/fake",
		NodeTTL: time.Hour,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	env := newTestEnvAll(t, nil, nil, nil, hub, nil, &fakeMeshMsgs{})
	env.login()

	_, html := env.get("/meshtastic")
	if strings.Contains(html, "/meshtastic?dir=ch1") {
		t.Errorf("emcom tab rendered without a configured emcom channel: %.300s", html)
	}
}

// fakeAPRSMsgs is an in-memory APRSMessageStore for the admin history.
type fakeAPRSMsgs struct {
	rows []storage.APRSMessage
}

func (f *fakeAPRSMsgs) RecordAPRSMessage(_ context.Context, direction, from, to, text, msgID, via string, at time.Time) error {
	f.rows = append(f.rows, storage.APRSMessage{Direction: direction, From: from, To: to, Text: text, MsgID: msgID, Via: via, At: at})
	return nil
}

func (f *fakeAPRSMsgs) UpdateAPRSMessageStatus(_ context.Context, msgID, status string, _ time.Time) error {
	for i := range f.rows {
		if f.rows[i].Direction == "tx" && f.rows[i].MsgID == msgID {
			f.rows[i].Status = status
		}
	}
	return nil
}

func (f *fakeAPRSMsgs) ListAPRSMessages(_ context.Context, direction string, limit, offset int) ([]storage.APRSMessage, error) {
	var out []storage.APRSMessage
	for _, m := range f.rows {
		if direction == "" || m.Direction == direction {
			out = append(out, m)
		}
	}
	if offset > len(out) {
		offset = len(out)
	}
	out = out[offset:]
	if limit < len(out) {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeAPRSMsgs) CountAPRSMessages(_ context.Context, direction string) (int, error) {
	n := 0
	for _, m := range f.rows {
		if direction == "" || m.Direction == direction {
			n++
		}
	}
	return n, nil
}

// TestAPRSBulletinBadge pins the bulletin marker in the APRS history:
// rows addressed to BLNn carry the bulletin badge and stay click-to-send.
func TestAPRSBulletinBadge(t *testing.T) {
	store := &fakeAPRSMsgs{rows: []storage.APRSMessage{
		{Direction: "rx", From: "SP9XYZ-7", To: "BLN0", Text: "ops bulletin", Via: "aprs-inet", At: time.Now()},
		{Direction: "rx", From: "SP9XYZ-7", To: "SP9MOA-10", Text: "personal", Via: "aprs-inet", At: time.Now()},
	}}
	env := newTestEnvAll(t, nil, nil, nil, nil, store, nil)
	env.login()

	resp, body := env.get("/partials/messages")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("partial = %d", resp.StatusCode)
	}
	if !strings.Contains(body, "BLN0") || !strings.Contains(body, "bulletin") {
		t.Errorf("partial missing bulletin row/badge: %.300s", body)
	}
}

// TestAPRSAckStatusBadge pins the delivery-status badge: a tx row shows
// delivered (with its msg id) once the addressee acked it, failed on a
// rej, and the neutral sent badge while unanswered.
func TestAPRSAckStatusBadge(t *testing.T) {
	now := time.Now()
	store := &fakeAPRSMsgs{rows: []storage.APRSMessage{
		{Direction: "tx", From: "SP9MOA-10", To: "SP9XYZ-7", Text: "pogoda", MsgID: "00123", Status: "delivered", Via: "aprs-radio", At: now},
		{Direction: "tx", From: "SP9MOA-10", To: "SP9XYZ-7", Text: "alert", MsgID: "00124", Status: "failed", Via: "aprs-radio", At: now},
		{Direction: "tx", From: "SP9MOA-10", To: "SP9XYZ-7", Text: "info", MsgID: "00125", Via: "aprs-radio", At: now},
	}}
	env := newTestEnvAll(t, nil, nil, nil, nil, store, nil)
	env.login()

	resp, body := env.get("/partials/messages?dir=tx")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("partial = %d", resp.StatusCode)
	}
	if !strings.Contains(body, "#00123") || !strings.Contains(body, "delivered") {
		t.Errorf("partial missing delivered badge: %.400s", body)
	}
	if !strings.Contains(body, "failed") {
		t.Errorf("partial missing failed badge: %.400s", body)
	}
	if !strings.Contains(body, "sent") {
		t.Errorf("partial missing sent badge: %.400s", body)
	}
}
func TestMeshNodeOwnerLabel(t *testing.T) {
	hub, err := meshtastic.NewHub(meshtastic.Config{
		Enabled: true, Device: "/dev/fake", NodeTTL: time.Hour,
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	id := "abcd1234"
	hub.SeedNode(id, "RKSR-TN-R3", "R3", 50.02, 20.0, time.Now(), []string{"telemetry"})
	env := newTestEnvWithMesh(t, hub)
	u, err := env.users.CreateUser("sp9kow", "600111222", "", "", "member", "pw1")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.users.SetUserMeshtasticIDs(u.ID, []string{id}); err != nil {
		t.Fatal(err)
	}
	env.login()

	resp, body := env.get("/partials/meshtastic?tab=nodes")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("nodes partial = %d", resp.StatusCode)
	}
	if !strings.Contains(body, "RKSR-TN-R3") || !strings.Contains(body, "(sp9kow)") {
		t.Errorf("nodes partial missing owner label: %.300s", body)
	}
}

// TestAPRSOwnerNames pins the directory match in the APRS history: rx
// senders and tx addressees whose callsign belongs to a registered user
// show the username (exact SSID or base-callsign match).
func TestAPRSOwnerNames(t *testing.T) {
	store := &fakeAPRSMsgs{rows: []storage.APRSMessage{
		{Direction: "rx", From: "SP9XYZ-7", To: "SP9MOA-10", Text: "hello", Via: "aprs-inet", At: time.Now()},
		{Direction: "tx", From: "SP9MOA-10", To: "SP9XYZ-2", Text: "reply", Via: "aprs-inet", At: time.Now()},
	}}
	env := newTestEnvAll(t, nil, nil, nil, nil, store, nil)
	u, err := env.users.CreateUser("sp9kow", "600111222", "", "", "member", "pw1")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.users.SetUserAPRS(u.ID, []string{"SP9XYZ-7"}); err != nil {
		t.Fatal(err)
	}
	env.login()

	resp, body := env.get("/partials/messages")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("partial = %d", resp.StatusCode)
	}
	// rx exact match + tx base-callsign match both label sp9kow.
	if got := strings.Count(body, "(sp9kow)"); got != 2 {
		t.Errorf("owner labels = %d, want 2: %.300s", got, body)
	}
}

// TestSessionRevocation pins the account-change session policy: deleting,
// demoting, renaming or re-passwording a user revokes every live session
// of theirs — an old cookie must no longer authorize anything.
func TestSessionRevocation(t *testing.T) {
	env := newTestEnv(t)
	u1, err := env.users.CreateUser("emcom1", "", "", "", "emcom", "pw12345678")
	if err != nil {
		t.Fatal(err)
	}
	u2, err := env.users.CreateUser("emcom2", "", "", "", "emcom", "pw87654321")
	if err != nil {
		t.Fatal(err)
	}

	// A second browser for the directory users, with its own cookie jar.
	jar2, _ := cookiejar.New(nil)
	client2 := &http.Client{Jar: jar2, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	client2Get := func(path string) (*http.Response, []byte) {
		t.Helper()
		resp, err := client2.Get(env.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, body
	}
	loginAs := func(username, password string) {
		t.Helper()
		_, html := client2Get("/login")
		form := url.Values{"csrf": {extractCSRF(t, string(html))}, "username": {username}, "password": {password}}
		req, _ := http.NewRequest(http.MethodPost, env.srv.URL+"/login", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r2, err := client2.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, r2.Body)
		r2.Body.Close()
		if r2.StatusCode != http.StatusSeeOther {
			t.Fatalf("login %s = %d, want 303", username, r2.StatusCode)
		}
	}
	// accountResult reports whether the cookie still authorizes a
	// session (any role may open /account) — 303 to /login means the
	// session is gone.
	accountResult := func() (int, string) {
		t.Helper()
		resp, _ := client2Get("/account")
		return resp.StatusCode, resp.Header.Get("Location")
	}
	wantAuthorized := func(step string) {
		t.Helper()
		if st, _ := accountResult(); st != http.StatusOK {
			t.Fatalf("%s: account = %d, want 200", step, st)
		}
	}
	wantRevoked := func(step string) {
		t.Helper()
		st, loc := accountResult()
		if st != http.StatusSeeOther || loc != "/login" {
			t.Fatalf("%s: account = %d %q, want 303 redirect to /login", step, st, loc)
		}
	}
	adminCSRF := func() string {
		t.Helper()
		_, html := env.get("/users")
		return extractCSRF(t, html)
	}

	loginAs("emcom1", "pw12345678")
	wantAuthorized("fresh emcom1 session")

	env.login() // the admin client

	// 1) Admin edit: new password + demotion to member. The old cookie
	// must stop authorizing immediately (redirect to /login, not to the
	// member landing page — the session is gone, not merely demoted).
	form := url.Values{
		"csrf":     {adminCSRF()},
		"edit_id":  {strconv.FormatInt(u1.ID, 10)},
		"username": {"emcom1"},
		"role":     {"member"},
		"password": {"newpw1234"},
	}
	if resp, _ := env.postForm("/users", form); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("user save = %d, want 303", resp.StatusCode)
	}
	wantRevoked("after password+role edit")

	// 2) The new credentials work, and the demotion is real: a member
	// session may open /account but is bounced from compose.
	loginAs("emcom1", "newpw1234")
	wantAuthorized("member session after re-login")
	if resp, _ := client2Get("/compose"); resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
		t.Fatalf("member compose = %d %q, want 303 to the member landing", resp.StatusCode, resp.Header.Get("Location"))
	}

	// 3) Username change revokes the session as well.
	form = url.Values{
		"csrf":     {adminCSRF()},
		"edit_id":  {strconv.FormatInt(u1.ID, 10)},
		"username": {"emcom9"},
		"role":     {"member"},
	}
	if resp, _ := env.postForm("/users", form); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("user rename = %d, want 303", resp.StatusCode)
	}
	wantRevoked("after username change")
	loginAs("emcom9", "newpw1234")
	wantAuthorized("renamed account session")

	// 4) Self-service password change kills the session and bounces the
	// browser to /login.
	_, acctHTML := client2Get("/account")
	form = url.Values{"csrf": {extractCSRF(t, string(acctHTML))}, "password": {"selfpw1234"}}
	req, _ := http.NewRequest(http.MethodPost, env.srv.URL+"/account", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r2, err := client2.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, r2.Body)
	r2.Body.Close()
	if r2.StatusCode != http.StatusSeeOther || r2.Header.Get("Location") != "/login" {
		t.Fatalf("account password change = %d %q, want 303 to /login", r2.StatusCode, r2.Header.Get("Location"))
	}
	wantRevoked("after self-service password change")

	// 5) Admin password reset revokes the (re-established) session.
	loginAs("emcom9", "selfpw1234")
	wantAuthorized("session before admin reset")
	if resp, _ := env.postForm("/users/"+strconv.FormatInt(u1.ID, 10)+"/reset", url.Values{"csrf": {adminCSRF()}}); resp.StatusCode != http.StatusOK {
		t.Fatalf("user reset = %d, want 200", resp.StatusCode)
	}
	wantRevoked("after admin password reset")

	// 6) Deletion revokes the last session too.
	loginAs("emcom2", "pw87654321")
	wantAuthorized("emcom2 session before delete")
	if resp, _ := env.postForm("/users/"+strconv.FormatInt(u2.ID, 10)+"/delete", url.Values{"csrf": {adminCSRF()}}); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("user delete = %d, want 303", resp.StatusCode)
	}
	wantRevoked("after deletion")
}
