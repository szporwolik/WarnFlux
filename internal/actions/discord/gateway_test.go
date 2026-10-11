package discord

// The inbound path is exercised end to end against a fake gateway: one
// httptest server serves both the WebSocket gateway and the REST API, so
// a test drives a real session (HELLO, IDENTIFY, heartbeats, MESSAGE_CREATE)
// and asserts the replies the station sends back.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/radiocli"
)

// sentMessage is one recorded POST to /channels/<id>/messages.
type sentMessage struct {
	channel string
	content string
}

type fakeGateway struct {
	t   *testing.T
	srv *httptest.Server

	flags int64 // application flags answered by /applications/@me

	mu        sync.Mutex
	upgrades  int
	urls      []string
	identify  []map[string]any
	beats     []int64
	opened    int // POST /users/@me/channels calls
	sent      []sentMessage
	webhooks  []sentMessage
	frames    chan map[string]any // frames the test asks the gateway to send
	ready     chan struct{}       // closed when the first session identified
	closeCode int                 // close code used right after IDENTIFY (0 = stay open)
	closeOnce sync.Once
	conn      *websocket.Conn // the live session, for the drop test
}

// drop closes the live session the way a network outage would.
func (g *fakeGateway) drop() {
	g.mu.Lock()
	conn := g.conn
	g.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func newFakeGateway(t *testing.T) *fakeGateway {
	t.Helper()
	g := &fakeGateway{
		t:      t,
		flags:  flagMessageContentLimited,
		frames: make(chan map[string]any, 16),
		ready:  make(chan struct{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/gateway", g.handleGateway)
	mux.HandleFunc("/webhook", g.handleWebhook)
	mux.HandleFunc("/applications/@me", g.handleApplication)
	mux.HandleFunc("/users/@me/channels", g.handleOpenDM)
	mux.HandleFunc("/channels/", g.handleChannelMessage)
	g.srv = httptest.NewServer(mux)

	gatewayURL = "ws://" + strings.TrimPrefix(g.srv.URL, "http://") + "/gateway"
	apiBase = g.srv.URL
	t.Cleanup(func() {
		apiBase = "https://discord.com/api/v10"
		gatewayURL = "wss://gateway.discord.gg/?v=10&encoding=json"
		g.srv.Close()
	})
	return g
}

func (g *fakeGateway) handleApplication(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	flags := g.flags
	g.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{"id": "app-1", "flags": flags})
}

func (g *fakeGateway) handleOpenDM(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	g.opened++
	g.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{"id": "dm-opened"})
}

func (g *fakeGateway) handleChannelMessage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Content string `json:"content"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	channel := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/channels/"), "/messages")
	g.mu.Lock()
	g.sent = append(g.sent, sentMessage{channel: channel, content: body.Content})
	g.mu.Unlock()
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"id":"sent"}`))
}

// handleWebhook records one webhook post (the channel surface).
func (g *fakeGateway) handleWebhook(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Content string `json:"content"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	g.mu.Lock()
	g.webhooks = append(g.webhooks, sentMessage{channel: "webhook", content: body.Content})
	g.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// webhookPosts returns the recorded channel posts.
func (g *fakeGateway) webhookPosts() []sentMessage {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]sentMessage(nil), g.webhooks...)
}

// waitWebhook blocks until n posts reached the webhook.
func (g *fakeGateway) waitWebhook(n int) []sentMessage {
	g.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if posts := g.webhookPosts(); len(posts) >= n {
			return posts
		}
		time.Sleep(5 * time.Millisecond)
	}
	g.t.Fatalf("webhook posts = %d, want %d", len(g.webhookPosts()), n)
	return nil
}

func (g *fakeGateway) handleGateway(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		g.t.Logf("gateway upgrade failed: %v", err)
		return
	}
	defer conn.Close()
	g.mu.Lock()
	g.upgrades++
	g.urls = append(g.urls, r.URL.RequestURI())
	g.conn = conn
	g.mu.Unlock()

	var writeMu sync.Mutex
	write := func(v any) {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.WriteJSON(v)
	}
	write(map[string]any{"op": opHello, "d": map[string]any{"heartbeat_interval": 45000}})

	go func() {
		for {
			select {
			case <-r.Context().Done():
				return
			case frame, ok := <-g.frames:
				if !ok {
					return
				}
				write(frame)
			}
		}
	}()

	for {
		_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
		var frame gatewayFrame
		if err := conn.ReadJSON(&frame); err != nil {
			return
		}
		switch frame.Op {
		case opIdentify, opResume:
			var payload map[string]any
			_ = json.Unmarshal(frame.Data, &payload)
			g.mu.Lock()
			g.identify = append(g.identify, map[string]any{"op": frame.Op, "d": payload})
			g.mu.Unlock()
			if frame.Op == opIdentify {
				write(map[string]any{"op": opDispatch, "s": 1, "t": "READY", "d": map[string]any{
					"session_id":         "sess-1",
					"resume_gateway_url": gatewayURL,
				}})
				g.closeOnce.Do(func() {
					close(g.ready)
					g.mu.Lock()
					code := g.closeCode
					g.mu.Unlock()
					if code != 0 {
						// Refuse the session the way Discord does: the
						// close code decides whether it is fatal.
						go func() {
							time.Sleep(20 * time.Millisecond)
							_ = conn.WriteControl(websocket.CloseMessage,
								websocket.FormatCloseMessage(code, "refused"), time.Now().Add(time.Second))
							_ = conn.Close()
						}()
					}
				})
			} else {
				write(map[string]any{"op": opDispatch, "s": 2, "t": "RESUMED", "d": map[string]any{}})
			}
		case opHeartbeat:
			var seq int64
			_ = json.Unmarshal(frame.Data, &seq)
			g.mu.Lock()
			g.beats = append(g.beats, seq)
			g.mu.Unlock()
			write(map[string]any{"op": opHeartbeatAck, "d": nil})
		}
	}
}

// push asks the gateway to dispatch one frame to the client.
func (g *fakeGateway) push(frame map[string]any) {
	g.frames <- frame
}

// pushDM dispatches one direct message.
func (g *fakeGateway) pushDM(id, userID, content string) {
	g.push(map[string]any{"op": opDispatch, "s": 7, "t": "MESSAGE_CREATE", "d": map[string]any{
		"id":         id,
		"channel_id": "dm-1",
		"content":    content,
		"author":     map[string]any{"id": userID, "username": "operator", "bot": false},
	}})
}

// waitReady blocks until the client identified.
func (g *fakeGateway) waitReady() {
	g.t.Helper()
	select {
	case <-g.ready:
	case <-time.After(5 * time.Second):
		g.t.Fatal("the listener never identified on the gateway")
	}
}

// waitSent returns the first message sent after n already-recorded ones.
func (g *fakeGateway) waitSent(after int) sentMessage {
	g.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		g.mu.Lock()
		if len(g.sent) > after {
			m := g.sent[after]
			g.mu.Unlock()
			return m
		}
		g.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	g.t.Fatalf("no direct message sent within the deadline (sent=%d)", g.sentCount())
	return sentMessage{}
}

// expectNoSend asserts that no message is sent within settle.
func (g *fakeGateway) expectNoSend(after int, settle time.Duration) {
	g.t.Helper()
	time.Sleep(settle)
	if n := g.sentCount(); n > after {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.t.Fatalf("unexpected direct message: %+v", g.sent[after])
	}
}

func (g *fakeGateway) sentCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.sent)
}

// upgradeURLs returns the request URIs the client dialled with.
func (g *fakeGateway) upgradeURLs() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.urls...)
}

func (g *fakeGateway) upgradeCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.upgrades
}

func (g *fakeGateway) openCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.opened
}

// waitHeartbeats blocks until the client answered at least n heartbeats.
func (g *fakeGateway) waitHeartbeats(n int) []int64 {
	g.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if beats := g.heartbeats(); len(beats) >= n {
			return beats
		}
		time.Sleep(5 * time.Millisecond)
	}
	g.t.Fatalf("the client answered %d heartbeats, want %d", len(g.heartbeats()), n)
	return nil
}

func (g *fakeGateway) heartbeats() []int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]int64(nil), g.beats...)
}

func (g *fakeGateway) sessions() []map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]map[string]any(nil), g.identify...)
}

// listener builds an action with a bot token, wired to a command bot with
// the given commands, and starts the inbound path.
type listenerFixture struct {
	action   *discordAction
	dm       *dmListener
	gateway  *fakeGateway
	cli      *radiocli.Bot
	accepted chan MessageEventWire
	owners   map[string]string
	// recipients are the Discord ids the notices message directly.
	recipients []string
	// summary is what the hazard status line reports.
	summary HazardSummary
	// uptime is what the presence line reports.
	uptime string
	// clock is the fake now the listener reads: the tests move it forward
	// while the listener runs, so it is shared through an atomic.
	clock atomic.Int64
}

// setNow moves the fake clock.
func (f *listenerFixture) setNow(at time.Time) { f.clock.Store(at.UnixNano()) }

// at reads the fake clock.
func (f *listenerFixture) at() time.Time { return time.Unix(0, f.clock.Load()).UTC() }

func newListenerFixture(t *testing.T, g *fakeGateway) *listenerFixture {
	t.Helper()
	cfg := map[string]any{"url": g.srv.URL + "/webhook", "token": "bot-token"}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		t.Fatal(err)
	}
	p, err := New(&node)
	if err != nil {
		t.Fatal(err)
	}
	a, ok := p.(*discordAction)
	if !ok {
		t.Fatalf("New returned %T", p)
	}

	f := &listenerFixture{
		action:   a,
		dm:       a.dm,
		gateway:  g,
		cli:      radiocli.New("WarnFlux 0.7.4 - SOSNA - sosna.sp9moa.pl"),
		accepted: make(chan MessageEventWire, 4),
		owners:   map[string]string{"42": "sp9spm"},
	}
	f.setNow(time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC))
	if f.dm == nil {
		t.Fatal("a token-carrying instance must carry the inbound half")
	}
	f.dm.now = f.at
	f.action.notices.now = f.at
	f.cli.Register("ping", "station alive", func(string, radiocli.Sender) radiocli.Result {
		return radiocli.Result{Handled: true, Reply: "Pong from WarnFlux 0.7.4 | uptime 1h"}
	})
	f.cli.RegisterRestricted("alert", "alert", func(args string, _ radiocli.Sender) radiocli.Result {
		return radiocli.Result{
			Handled: true,
			Reply:   "OK: alert raised",
			Alert:   &radiocli.AlertSpec{Headline: strings.TrimSpace(args), TTL: time.Hour},
		}
	})
	a.ConfigureDMs(f.cli, func(userID string) string { return f.owners[userID] },
		func(payload []byte) dispatch.Acceptance {
			var doc MessageEventWire
			if err := json.Unmarshal(payload, &doc); err != nil {
				t.Errorf("alert payload is not a wire document: %v", err)
				return dispatch.Rejected
			}
			f.accepted <- doc
			return dispatch.AcceptedDurable
		},
		func(context.Context, string) (time.Time, time.Time, string, bool) {
			return time.Time{}, time.Time{}, "", false
		},
	)
	return f
}

// run starts the inbound path and waits for the session to be up.
func (f *listenerFixture) run(t *testing.T) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.action.RunDMs(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("RunDMs did not return after the context was cancelled")
		}
	})
	f.gateway.waitReady()
	return cancel
}

func TestDMCommandRepliesOnIncomingChannel(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.run(t)

	sessions := g.sessions()
	if len(sessions) == 0 {
		t.Fatal("no IDENTIFY frame reached the gateway")
	}
	if op, _ := sessions[0]["op"].(int); op != opIdentify {
		t.Errorf("first session frame op = %v, want IDENTIFY", sessions[0]["op"])
	}
	d, _ := sessions[0]["d"].(map[string]any)
	if d["token"] != "bot-token" {
		t.Errorf("identify token = %v", d["token"])
	}
	sentIntents, _ := d["intents"].(float64)
	if int(sentIntents) != intents {
		t.Errorf("identify intents = %v, want %d", d["intents"], intents)
	}

	// The gateway asks for an immediate beat: the client answers with the
	// last sequence it saw (the READY dispatch carried 1).
	g.push(map[string]any{"op": opHeartbeat})
	beats := g.waitHeartbeats(1)
	if beats[0] != 1 {
		t.Errorf("heartbeat seq = %d, want 1", beats[0])
	}

	f.gateway.pushDM("m1", "42", "/ping")
	got := g.waitSent(0)
	if got.content != "Pong from WarnFlux 0.7.4 | uptime 1h" {
		t.Errorf("reply = %q", got.content)
	}
	// The reply rides the channel the message arrived on: no extra
	// request to open it, and the cache still learns it.
	if got.channel != "dm-1" {
		t.Errorf("reply channel = %q, want dm-1", got.channel)
	}
	if n := g.openCount(); n != 0 {
		t.Errorf("opened %d direct-message channels, want 0", n)
	}
}

func TestDMUnknownSenderIsIgnored(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.run(t)

	// A sender the directory does not resolve gets NOTHING: no command
	// answer, no banner, no alert — the station answers only its own
	// operators on Discord.
	f.gateway.pushDM("m1", "999", "/ping")
	f.gateway.pushDM("m2", "999", "/alert flood")
	f.gateway.pushDM("m3", "999", "czesc")
	g.expectNoSend(0, 400*time.Millisecond)
	select {
	case doc := <-f.accepted:
		t.Fatalf("an unknown sender raised an alert: %+v", doc)
	case <-time.After(100 * time.Millisecond):
	}

	// The same station still answers a known operator.
	f.gateway.pushDM("m4", "42", "/ping")
	got := g.waitSent(0)
	if !strings.HasPrefix(got.content, "Pong") {
		t.Errorf("known operator reply = %q", got.content)
	}
}

func TestDMAlertFromAuthorizedSender(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.run(t)

	f.gateway.pushDM("m1", "42", "/alert silny wiatr")
	got := g.waitSent(0)
	if got.content != "OK: alert raised" {
		t.Errorf("reply = %q", got.content)
	}
	select {
	case doc := <-f.accepted:
		if doc.SchemaVersion != messageEventSchemaVersion {
			t.Errorf("schema version = %d", doc.SchemaVersion)
		}
		if doc.Event.Source != "discord" || doc.Event.Severity != "severe" {
			t.Errorf("event = %+v", doc.Event)
		}
		if doc.Event.Headline != "silny wiatr" {
			t.Errorf("headline = %q", doc.Event.Headline)
		}
		if doc.EventKey != "discord:42:alert:"+radiocli.ContentID("/alert silny wiatr") {
			t.Errorf("event key = %q", doc.EventKey)
		}
		if !strings.Contains(doc.Event.Description, "sp9spm") {
			t.Errorf("description = %q, want the directory owner", doc.Event.Description)
		}
		if doc.CommandResult != "OK: alert raised" {
			t.Errorf("command result = %q", doc.CommandResult)
		}
	case <-time.After(time.Second):
		t.Fatal("no alert event was published")
	}
}

func TestDMIgnoresChannelPostsAndBots(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.run(t)

	// A channel post: not addressed to the station.
	g.push(map[string]any{"op": opDispatch, "s": 3, "t": "MESSAGE_CREATE", "d": map[string]any{
		"id": "c1", "channel_id": "chan-1", "guild_id": "guild-1", "content": "/ping",
		"author": map[string]any{"id": "42", "username": "operator", "bot": false},
	}})
	// A bot: never answered, not even ourselves.
	g.push(map[string]any{"op": opDispatch, "s": 4, "t": "MESSAGE_CREATE", "d": map[string]any{
		"id": "b1", "channel_id": "dm-1", "content": "/ping",
		"author": map[string]any{"id": "77", "username": "bot", "bot": true},
	}})
	g.expectNoSend(0, 300*time.Millisecond)

	// The same channel id, from a person, is still answered.
	g.pushDM("m2", "42", "/ping")
	if got := g.waitSent(0); !strings.HasPrefix(got.content, "Pong") {
		t.Errorf("reply = %q", got.content)
	}
}

func TestDMDuplicateMessageAnsweredOnce(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.run(t)

	f.gateway.pushDM("m1", "42", "/ping")
	g.waitSent(0)
	f.gateway.pushDM("m1", "42", "/ping")
	g.expectNoSend(1, 300*time.Millisecond)

	// A new message id is answered again.
	f.gateway.pushDM("m2", "42", "/ping")
	g.waitSent(1)
}

func TestDMBannerIsPaced(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.run(t)

	f.gateway.pushDM("m1", "42", "czesc")
	got := g.waitSent(0)
	if !strings.Contains(got.content, "type /help for help") {
		t.Errorf("banner = %q", got.content)
	}
	f.gateway.pushDM("m2", "42", "czesc jeszcze raz")
	g.expectNoSend(1, 300*time.Millisecond)

	// Past the interval the banner comes back.
	f.setNow(f.at().Add(dmBannerInterval + time.Minute))
	f.gateway.pushDM("m3", "42", "czesc po przerwie")
	g.waitSent(1)
}

func TestDMBurstLimitsReplies(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.run(t)

	for i := 0; i < dmBurst+2; i++ {
		f.gateway.pushDM("m"+string(rune('a'+i)), "42", "/ping")
	}
	// The burst window admits dmBurst replies; the rest is dropped.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if g.sentCount() >= dmBurst {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)
	if n := g.sentCount(); n != dmBurst {
		t.Fatalf("sent %d replies, want %d (burst limit)", n, dmBurst)
	}
}

func TestDMIntentMissingSkipsGateway(t *testing.T) {
	g := newFakeGateway(t)
	g.flags = 0 // the privileged Message Content intent is off
	f := newListenerFixture(t, g)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.action.RunDMs(ctx)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunDMs did not return after the context was cancelled")
	}
	if n := g.upgradeCount(); n != 0 {
		t.Errorf("opened %d gateway sessions without the intent, want 0", n)
	}
}

func TestDMFatalCloseStopsListener(t *testing.T) {
	g := newFakeGateway(t)
	g.closeCode = closeDisallowedIntents
	f := newListenerFixture(t, g)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.action.RunDMs(ctx)
	}()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("a refused session must stop the listener instead of retrying")
	}
	if n := g.upgradeCount(); n != 1 {
		t.Errorf("gateway sessions = %d, want exactly 1 (no retry)", n)
	}
}

func TestDMResumesDroppedSession(t *testing.T) {
	g := newFakeGateway(t)
	f := newListenerFixture(t, g)
	f.run(t)
	f.gateway.pushDM("m1", "42", "/ping")
	g.waitSent(0)

	// Drop the connection: the next attempt resumes the session.
	f.gateway.drop()

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range g.sessions() {
			if op, _ := s["op"].(int); op == opResume {
				d, _ := s["d"].(map[string]any)
				if d["session_id"] != "sess-1" {
					t.Fatalf("resume session = %v", d["session_id"])
				}
				if seq, _ := d["seq"].(float64); int(seq) != 7 {
					t.Fatalf("resume seq = %v, want 7", d["seq"])
				}
				// The resume endpoint must carry the gateway parameters:
				// dialled bare it answers INVALID_SESSION.
				urls := g.upgradeURLs()
				last := urls[len(urls)-1]
				if !strings.Contains(last, "v=10") || !strings.Contains(last, "encoding=json") {
					t.Fatalf("resume dial %q misses the gateway parameters", last)
				}
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the listener never resumed the dropped session")
}
