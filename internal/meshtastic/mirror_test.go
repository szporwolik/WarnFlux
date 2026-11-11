package meshtastic

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/kabili207/meshtastic-go/core/proto"
)

// webhookSink is a Discord webhook stub: it records every content it is given.
type webhookSink struct {
	mu        sync.Mutex
	contents  []string
	usernames []string
	status    int
	bodies    int
}

func (w *webhookSink) handler() http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload struct {
			Content  string `json:"content"`
			Username string `json:"username"`
		}
		_ = json.Unmarshal(body, &payload)
		code := w.status
		if code == 0 {
			code = http.StatusNoContent
		}
		w.mu.Lock()
		w.bodies++
		if code < 300 {
			w.contents = append(w.contents, payload.Content)
			w.usernames = append(w.usernames, payload.Username)
		}
		w.mu.Unlock()
		rw.WriteHeader(code)
	})
}

func (w *webhookSink) posted() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.contents...)
}

// waitPosted waits for n posts (the mirror is asynchronous by design).
func (w *webhookSink) waitPosted(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := w.posted(); len(got) >= n {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	return w.posted()
}

// The header has to say which way the message went and who is talking, because
// that is the whole point of mirroring a two-way channel into a chat.
func TestMirrorContentShapesBothDirections(t *testing.T) {
	rx := mirrorContent(mirrorMsg{
		direction: "rx", sender: "a1b2c3d4", channel: "SOSNA", text: "  test łączności ", hops: 3,
	})
	if !strings.HasPrefix(rx, "📥 **SOSNA** · a1b2c3d4\n📶 3 hop\n") {
		t.Fatalf("rx content = %q", rx)
	}
	if !strings.HasSuffix(rx, "\ntest łączności") {
		t.Fatalf("rx content = %q, want the trimmed text", rx)
	}

	tx := mirrorContent(mirrorMsg{
		direction: "tx", recipient: "", operator: "szymon", channel: "SOSNA", text: "od stacji",
	})
	if !strings.HasPrefix(tx, "📤 **SOSNA** · all · szymon") {
		t.Fatalf("tx broadcast content = %q", tx)
	}

	// A known node shows the name the station knows, with the id in brackets.
	named := mirrorContent(mirrorMsg{
		direction: "rx", sender: "6d2c8f6c", senderName: "szymon", channel: "SP9MOA", text: "Test", hops: 6,
	})
	if !strings.HasPrefix(named, "📥 **SP9MOA** · szymon (6d2c8f6c)\n📶 6 hop\n") {
		t.Fatalf("named content = %q", named)
	}

	dm := mirrorContent(mirrorMsg{
		direction: "tx", recipient: "deadbeef", channel: "SOSNA", text: "priv",
	})
	if !strings.HasPrefix(dm, "📤 **SOSNA** · deadbeef") {
		t.Fatalf("tx direct content = %q", dm)
	}

	// The station's own sends are labelled: the reader must know the text came
	// from the machine and not from a person.
	scheduled := mirrorContent(mirrorMsg{direction: "tx", operator: "system", channel: "SOSNA", text: "beacon"})
	if !strings.HasPrefix(scheduled, "📤 **SOSNA** · all · system") {
		t.Fatalf("scheduled content = %q, want the system label", scheduled)
	}
}

// Discord rejects anything over 2000 characters: the mirror cuts long traffic
// instead of losing the whole post.
func TestMirrorContentTruncates(t *testing.T) {
	long := strings.Repeat("ą", 4000)
	got := mirrorContent(mirrorMsg{direction: "rx", sender: "a1b2c3d4", channel: "SOSNA", text: long})
	if n := len([]rune(got)); n > mirrorMaxContent {
		t.Fatalf("content has %d runes, want at most %d", n, mirrorMaxContent)
	}
	if !strings.HasSuffix(got, "…") {
		t.Fatalf("content = %q, want a truncation mark", got[max(0, len(got)-10):])
	}
}

// The webhook gets exactly what the formatter built, with the optional
// username override.
func TestTrafficMirrorPostsToWebhook(t *testing.T) {
	sink := &webhookSink{}
	srv := httptest.NewServer(sink.handler())
	defer srv.Close()

	m := newTrafficMirror(MirrorConfig{WebhookURL: srv.URL, Username: "SOSNA Mesh"}, nil)
	if m == nil {
		t.Fatal("mirror = nil, want a configured mirror")
	}
	m.offer(mirrorMsg{direction: "rx", sender: "a1b2c3d4", channel: "SOSNA", text: "hej", hops: 1})

	got := sink.waitPosted(t, 1)
	if len(got) != 1 {
		t.Fatalf("posted %d messages, want 1", len(got))
	}
	if !strings.Contains(got[0], "hej") || !strings.Contains(got[0], "a1b2c3d4") {
		t.Fatalf("content = %q, want the message and its sender", got[0])
	}
	sink.mu.Lock()
	user := sink.usernames[0]
	sink.mu.Unlock()
	if user != "SOSNA Mesh" {
		t.Fatalf("username = %q, want the configured override", user)
	}
}

// No configured webhook means no work at all.
func TestTrafficMirrorDisabledWithoutURL(t *testing.T) {
	if m := newTrafficMirror(MirrorConfig{}, nil); m != nil {
		t.Fatalf("mirror = %v, want nil without a webhook", m)
	}
}

// A rejecting webhook must not panic or block the caller: the mirror is an
// observer, not a delivery guarantee.
func TestTrafficMirrorSurvivesRejection(t *testing.T) {
	sink := &webhookSink{status: http.StatusInternalServerError}
	srv := httptest.NewServer(sink.handler())
	defer srv.Close()

	m := newTrafficMirror(MirrorConfig{WebhookURL: srv.URL}, nil)
	m.offer(mirrorMsg{direction: "rx", sender: "a1b2c3d4", channel: "SOSNA", text: "hej"})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sink.mu.Lock()
		n := sink.bodies
		sink.mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the webhook was never asked")
}

// hubMirror builds a hub whose mirror points at the given webhook, without a
// radio session: mirrorTraffic is the seam under test.
func hubMirror(t *testing.T, mirrorURL string, channel int, includeSystem bool) *Hub {
	t.Helper()
	h, err := NewHub(Config{
		Enabled:       true,
		Device:        "/dev/fake",
		EmcomChannel:  1,
		TrafficMirror: MirrorConfig{WebhookURL: mirrorURL, Channel: channel, IncludeSystem: includeSystem},
	}, nil)
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	return h
}

// Only the club channel is mirrored — the primary channel, direct messages and
// the watch channel stay off the chat.
func TestMirrorTrafficOnlyForClubChannel(t *testing.T) {
	sink := &webhookSink{}
	srv := httptest.NewServer(sink.handler())
	defer srv.Close()
	h := hubMirror(t, srv.URL, 0, false) // 0 = the announcement channel (1)

	h.mirrorTraffic("rx", "a1b2c3d4", "", 2, "INNE", "nie ten kanał", 0, time.Now(), "", 0, 0, false)
	h.mirrorTraffic("rx", "a1b2c3d4", "", 0, "PRIMARY", "kanał 0", 0, time.Now(), "", 0, 0, false)
	// A direct message carries channel index 0 and the "dm" label.
	h.mirrorTraffic("tx", "", "deadbeef", 0, "dm", "pw", 0, time.Now(), "", 0, 0, false)

	time.Sleep(150 * time.Millisecond)
	if got := sink.posted(); len(got) != 0 {
		t.Fatalf("posted %v, want nothing outside the club channel", got)
	}

	h.mirrorTraffic("rx", "a1b2c3d4", "", 1, "SOSNA", "na kanale klubowym", 2, time.Now(), "", 0, 0, false)
	got := sink.waitPosted(t, 1)
	if len(got) != 1 || !strings.Contains(got[0], "na kanale klubowym") {
		t.Fatalf("posted %v, want the club-channel message", got)
	}
}

// The station's own sends are mirrored by default (that is how alarms and the
// EMCOM handling reach the chat) and can be turned off.
func TestMirrorTrafficIncludesStationSends(t *testing.T) {
	sink := &webhookSink{}
	srv := httptest.NewServer(sink.handler())
	defer srv.Close()

	withSystem := hubMirror(t, srv.URL, 1, true)
	withSystem.mirrorTraffic("tx", "", "", 1, "SOSNA", "alarm IMGW", 0, time.Now(), "system", 0, 0, false)
	if got := sink.waitPosted(t, 1); len(got) != 1 {
		t.Fatalf("posted %v, want the station send mirrored", got)
	}

	withoutSystem := hubMirror(t, srv.URL, 1, false)
	withoutSystem.mirrorTraffic("tx", "", "", 1, "SOSNA", "beacon", 0, time.Now(), "system", 0, 0, false)
	time.Sleep(150 * time.Millisecond)
	if got := sink.posted(); len(got) != 1 {
		t.Fatalf("posted %v, want the opt-out to skip the station send", got)
	}
	withoutSystem.mirrorTraffic("tx", "", "", 1, "SOSNA", "od człowieka", 0, time.Now(), "szymon", 0, 0, false)
	if got := sink.waitPosted(t, 2); len(got) != 2 {
		t.Fatalf("posted %v, want the human message either way", got)
	}
}

// Everything the radio reported travels with the message: hops, SNR and RSSI
// are what tells the chat how well the frame actually arrived.
func TestMirrorContentCarriesSignal(t *testing.T) {
	got := mirrorContent(mirrorMsg{
		direction: "rx", sender: "6d2c8f6c", senderName: "szymon", channel: "SP9MOA",
		text: "sygnał", hops: 2, snr: 9.25, rssi: -78,
	})
	if !strings.HasPrefix(got, "📥 **SP9MOA** · szymon (6d2c8f6c)\n📶 2 hop · SNR 9.2 dB · RSSI -78 dBm\n") {
		t.Fatalf("content = %q, want the signal metadata line", got)
	}

	// No signal reported (our own transmission, or a device that omitted it):
	// the metadata line disappears instead of showing zeroes.
	plain := mirrorContent(mirrorMsg{direction: "rx", sender: "6d2c8f6c", channel: "SP9MOA", text: "x"})
	if strings.Contains(plain, "SNR") || strings.Contains(plain, "RSSI") {
		t.Fatalf("content = %q, want no empty signal fields", plain)
	}
	// A direct frame still says so: 0 hops is information, not noise.
	if !strings.HasPrefix(plain, "📥 **SP9MOA** · 6d2c8f6c\n📶 0 hop\n") {
		t.Fatalf("content = %q, want the direct-reception marker", plain)
	}

	// A frame the node pulled from the channel's MQTT uplink was never on the
	// air: say so instead of printing hop 0 and no signal.
	viaMQTT := mirrorContent(mirrorMsg{
		direction: "rx", sender: "6d2c8f6c", channel: "SP9MOA", text: "z internetu", hops: 1, viaMQTT: true,
	})
	if !strings.HasPrefix(viaMQTT, "📥 **SP9MOA** · 6d2c8f6c\n📶 via MQTT\n") {
		t.Fatalf("content = %q, want the MQTT marker", viaMQTT)
	}

	// Our own transmissions were not measured by anything: no signal line.
	txPlain := mirrorContent(mirrorMsg{direction: "tx", channel: "SP9MOA", text: "x", operator: "admin"})
	if strings.Contains(txPlain, "📶") {
		t.Fatalf("content = %q, want no signal line for a transmission", txPlain)
	}
}

// senderDisplayName prefers the directory user, then the node's long name, then
// its short name: the chat should show the friendliest label the station has.
func TestSenderDisplayNamePreference(t *testing.T) {
	h, err := NewHub(Config{Enabled: true, Device: "/dev/fake", EmcomChannel: 1}, nil)
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	h.mu.Lock()
	h.nodes["6d2c8f6c"] = &Node{ID: "6d2c8f6c", Name: "MOA SOSNA", Short: "SOS"}
	h.nodes["deadbeef"] = &Node{ID: "deadbeef", Short: "MK"}
	h.mu.Unlock()

	if got := h.senderDisplayName("6d2c8f6c"); got != "MOA SOSNA" {
		t.Fatalf("name = %q, want the node long name", got)
	}
	if got := h.senderDisplayName("deadbeef"); got != "MK" {
		t.Fatalf("name = %q, want the short name as the fallback", got)
	}
	if got := h.senderDisplayName("00000000"); got != "" {
		t.Fatalf("name = %q, want empty for an unknown node", got)
	}

	// A registered directory user wins over the radio's own long name.
	h.SetSenderGate(func(id string) string {
		if id == "6d2c8f6c" {
			return "szymon"
		}
		return ""
	})
	if got := h.senderDisplayName("6d2c8f6c"); got != "szymon" {
		t.Fatalf("name = %q, want the directory user", got)
	}
}

// The radio path test above also has to show the name once the node is known.
func TestMirrorRadioPathShowsKnownName(t *testing.T) {
	sink := &webhookSink{}
	srv := httptest.NewServer(sink.handler())
	defer srv.Close()

	radio := newTestRadio(t, Config{
		Enabled:       true,
		Device:        "/dev/fake",
		EmcomChannel:  1,
		TrafficMirror: MirrorConfig{WebhookURL: srv.URL},
	}, testRadioOpts{setup: func(h *Hub) {
		h.SetSenderGate(func(id string) string {
			if id == "ef010203" {
				return "szymon"
			}
			return ""
		})
	}})
	radio.waitConnected(t)

	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xef010203,
		To:   0xffffffff,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_TEXT_MESSAGE_APP, Payload: []byte("z nazwa")},
		},
		Channel: 1,
	})

	got := sink.waitPosted(t, 1)
	if len(got) != 1 {
		t.Fatalf("posted %v, want one message", got)
	}
	if !strings.Contains(got[0], "szymon (ef010203)") {
		t.Fatalf("content = %q, want the directory user with the node id", got[0])
	}
}

// A frame that arrives without its own measurement still shows the SNR the
// radio last measured for that peer: the chat should not lose the number just
// because one packet omitted it.
func TestMirrorFallsBackToThePeersKnownSNR(t *testing.T) {
	sink := &webhookSink{}
	srv := httptest.NewServer(sink.handler())
	defer srv.Close()

	h := hubMirror(t, srv.URL, 1, true)
	h.mu.Lock()
	h.nodes["ef010203"] = &Node{ID: "ef010203", SNR: 7.25}
	h.mu.Unlock()

	h.mirrorTraffic("rx", "ef010203", "", 1, "SP9MOA", "bez pomiaru w ramce", 0, time.Now(), "", 0, 0, false)

	got := sink.waitPosted(t, 1)
	if len(got) != 1 {
		t.Fatalf("posted %v, want one message", got)
	}
	if !strings.Contains(got[0], "SNR 7.2 dB") {
		t.Fatalf("content = %q, want the peer's last SNR", got[0])
	}
}

// A station configured without the mesh must stay quiet: no session retries, no
// "no serial device path configured" warnings on every backoff.
func TestDisabledHubRunIsIdle(t *testing.T) {
	h, err := NewHub(Config{Enabled: false}, nil)
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()

	select {
	case err := <-done:
		t.Fatalf("Run returned %v while the context was still live, want it to idle", err)
	case <-time.After(200 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after cancel, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
	h.mu.Lock()
	lastErr := h.lastErr
	h.mu.Unlock()
	if lastErr != nil {
		t.Fatalf("last error = %v, want none for a disabled hub", lastErr)
	}
}

// Two mirrors, two channels: the club channel keeps its webhook and the public
// primary channel gets its own, so the neighbourhood chatter can be followed
// in Discord without mixing it into the club feed.
func TestMirrorTrafficFansOutToEveryChannelMirror(t *testing.T) {
	clubSink := &webhookSink{}
	clubSrv := httptest.NewServer(clubSink.handler())
	defer clubSrv.Close()
	primarySink := &webhookSink{}
	primarySrv := httptest.NewServer(primarySink.handler())
	defer primarySrv.Close()

	h, err := NewHub(Config{
		Enabled:       true,
		Device:        "/dev/fake",
		EmcomChannel:  1,
		TrafficMirror: MirrorConfig{WebhookURL: clubSrv.URL, Channel: 1, IncludeSystem: true},
		TrafficMirrors: []MirrorConfig{
			{WebhookURL: primarySrv.URL, Channel: 0},
		},
	}, nil)
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}

	h.mirrorTraffic("rx", "a1b2c3d4", "", 0, "ch0", "z publicznego", 3, time.Now(), "", 0, 0, false)
	h.mirrorTraffic("rx", "a1b2c3d4", "", 1, "SP9MOA", "z klubowego", 1, time.Now(), "", 0, 0, false)

	primary := primarySink.waitPosted(t, 1)
	if len(primary) != 1 || !strings.Contains(primary[0], "z publicznego") {
		t.Fatalf("primary webhook got %v, want only the primary-channel message", primary)
	}
	club := clubSink.waitPosted(t, 1)
	if len(club) != 1 || !strings.Contains(club[0], "z klubowego") {
		t.Fatalf("club webhook got %v, want only the club-channel message", club)
	}
}

// TestTrafficMirrorRedactsTheWebhookToken is WF-REV-02: a transport failure
// arrives as a *url.Error that quotes the request URL, and a Discord webhook
// URL is a credential. The log must keep the kind of failure, never the token.
func TestTrafficMirrorRedactsTheWebhookToken(t *testing.T) {
	const token = "REVIEW_SECRET_TOKEN_VALUE"
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// A closed listener: the connection is refused, so http.Client.Do returns a
	// *url.Error carrying the full webhook URL.
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL + "/api/webhooks/123456789012345678/" + token
	srv.Close()

	m := newTrafficMirror(MirrorConfig{WebhookURL: url}, nil)
	if m == nil {
		t.Fatal("mirror = nil")
	}
	m.log = logger
	m.post(mirrorMsg{direction: "rx", sender: "a1b2c3d4", channel: "SOSNA", text: "hej"})

	if strings.Contains(logs.String(), token) {
		t.Fatalf("webhook token leaked into the log: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "discord mirror post failed") {
		t.Errorf("the failure was not reported at all: %s", logs.String())
	}
}

// TestTrafficMirrorBlocksMentions is WF-REV-08: radio text is written by
// whoever is on the channel, so a mirrored message must never be able to ping a
// Discord user, role or @everyone — not through the text and not through a
// sender name.
func TestTrafficMirrorBlocksMentions(t *testing.T) {
	sink := &mentionSink{}
	srv := httptest.NewServer(sink.handler())
	defer srv.Close()

	m := newTrafficMirror(MirrorConfig{WebhookURL: srv.URL}, nil)
	m.post(mirrorMsg{
		direction: "rx", sender: "<@123456789012345678>",
		senderName: "@everyone", channel: "SOSNA",
		text: "hej <@123456789012345678> <@&987654321098765432> @here",
	})
	payload := sink.wait(t)
	mentions, ok := payload["allowed_mentions"].(map[string]any)
	if !ok {
		t.Fatalf("payload has no allowed_mentions: %v", payload)
	}
	parse, _ := mentions["parse"].([]any)
	if len(parse) != 0 {
		t.Fatalf("allowed_mentions.parse = %v, want an empty list", parse)
	}
	// The text itself stays readable: blocking the ping must not censor the
	// message a human wants to read.
	if content, _ := payload["content"].(string); !strings.Contains(content, "<@123456789012345678>") {
		t.Errorf("content was mangled: %q", content)
	}
}

// mentionSink captures the decoded JSON body of one webhook call.
type mentionSink struct {
	mu      sync.Mutex
	payload map[string]any
	got     chan struct{}
}

func (s *mentionSink) handler() http.Handler {
	if s.got == nil {
		s.got = make(chan struct{}, 1)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		s.mu.Lock()
		s.payload = body
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		select {
		case s.got <- struct{}{}:
		default:
		}
	})
}

func (s *mentionSink) wait(t *testing.T) map[string]any {
	t.Helper()
	select {
	case <-s.got:
	case <-time.After(2 * time.Second):
		t.Fatal("the webhook was never called")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.payload
}
