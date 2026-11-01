package meshtastic

import (
	"encoding/json"
	"io"
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
	if !strings.HasPrefix(rx, "📥 **SOSNA** · a1b2c3d4 · 3 hop") {
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

	dm := mirrorContent(mirrorMsg{
		direction: "tx", recipient: "deadbeef", channel: "SOSNA", text: "priv",
	})
	if !strings.HasPrefix(dm, "📤 **SOSNA** · deadbeef") {
		t.Fatalf("tx direct content = %q", dm)
	}

	// A scheduled send carries the "system" operator; it must not be shown as
	// if a person wrote it.
	scheduled := mirrorContent(mirrorMsg{direction: "tx", operator: "system", channel: "SOSNA", text: "beacon"})
	if strings.Contains(scheduled, "system") {
		t.Fatalf("scheduled content = %q, want no operator label", scheduled)
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

	h.mirrorTraffic("rx", "a1b2c3d4", "", 2, "INNE", "nie ten kanał", 0, time.Now(), "")
	h.mirrorTraffic("rx", "a1b2c3d4", "", 0, "PRIMARY", "kanał 0", 0, time.Now(), "")
	// A direct message carries channel index 0 and the "dm" label.
	h.mirrorTraffic("tx", "", "deadbeef", 0, "dm", "pw", 0, time.Now(), "")

	time.Sleep(150 * time.Millisecond)
	if got := sink.posted(); len(got) != 0 {
		t.Fatalf("posted %v, want nothing outside the club channel", got)
	}

	h.mirrorTraffic("rx", "a1b2c3d4", "", 1, "SOSNA", "na kanale klubowym", 2, time.Now(), "")
	got := sink.waitPosted(t, 1)
	if len(got) != 1 || !strings.Contains(got[0], "na kanale klubowym") {
		t.Fatalf("posted %v, want the club-channel message", got)
	}
}

// The station's own scheduled beacons would otherwise land in the chat every
// hour; they are skipped unless the operator asks for them.
func TestMirrorTrafficSkipsScheduledSends(t *testing.T) {
	sink := &webhookSink{}
	srv := httptest.NewServer(sink.handler())
	defer srv.Close()
	h := hubMirror(t, srv.URL, 1, false)

	h.mirrorTraffic("tx", "", "", 1, "SOSNA", "beacon", 0, time.Now(), "system")
	time.Sleep(150 * time.Millisecond)
	if got := sink.posted(); len(got) != 0 {
		t.Fatalf("posted %v, want the scheduled send skipped", got)
	}

	h.mirrorTraffic("tx", "", "", 1, "SOSNA", "wiadomość od człowieka", 0, time.Now(), "szymon")
	if got := sink.waitPosted(t, 1); len(got) != 1 {
		t.Fatalf("posted %v, want the human message", got)
	}

	// include_system flips that.
	all := hubMirror(t, srv.URL, 1, true)
	all.mirrorTraffic("tx", "", "", 1, "SOSNA", "beacon", 0, time.Now(), "system")
	if got := sink.waitPosted(t, 2); len(got) != 2 {
		t.Fatalf("posted %v, want include_system to mirror the beacon too", got)
	}
}

// TestMirrorSeesTrafficOnTheRadioPath is the wiring proof: a broadcast text
// arriving on the club channel reaches the webhook without anyone calling the
// mirror directly.
func TestMirrorSeesTrafficOnTheRadioPath(t *testing.T) {
	sink := &webhookSink{}
	srv := httptest.NewServer(sink.handler())
	defer srv.Close()

	radio := newTestRadio(t, Config{
		Enabled:       true,
		Device:        "/dev/fake",
		EmcomChannel:  1,
		TrafficMirror: MirrorConfig{WebhookURL: srv.URL},
	})
	radio.waitConnected(t)

	dispatchPkt(t, radio, &pb.MeshPacket{
		From: 0xef010203,
		To:   0xffffffff,
		PayloadVariant: &pb.MeshPacket_Decoded{
			Decoded: &pb.Data{Portnum: pb.PortNum_TEXT_MESSAGE_APP, Payload: []byte("hej klub")},
		},
		Channel: 1,
	})

	got := sink.waitPosted(t, 1)
	if len(got) != 1 {
		t.Fatalf("posted %v, want one club-channel message", got)
	}
	if !strings.Contains(got[0], "hej klub") || !strings.Contains(got[0], "ef010203") {
		t.Fatalf("content = %q, want the text and the sender", got[0])
	}
}
