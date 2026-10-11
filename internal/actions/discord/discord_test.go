package discord

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

func node(t *testing.T, v any) *yaml.Node {
	t.Helper()
	data, err := yaml.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var n yaml.Node
	if err := yaml.Unmarshal(data, &n); err != nil {
		t.Fatal(err)
	}
	return &n
}

func hazardRequest() action.ActionRequest {
	exp := time.Date(2026, 9, 25, 17, 0, 0, 0, time.UTC)
	return action.ActionRequest{
		ID: "imgw-meteo:1/discord",
		Event: dispatch.Event{
			Kind: dispatch.EventHazardTransition,
			Hazard: &dispatch.HazardTransition{
				Type: dispatch.TransitionNew,
				Key:  "imgw-meteo:1",
				Hazard: dispatch.Hazard{
					EventKey:  "imgw-meteo:1",
					Source:    "imgw-meteo",
					Event:     "Storm",
					Severity:  "severe",
					Headline:  "Strong wind warning",
					Areas:     []string{"powiat wielicki"},
					ExpiresAt: &exp,
				},
			},
		},
		App: action.AppInfo{Version: "0.2.41", Header1: "SOSNA"},
	}
}

func TestNewValidation(t *testing.T) {
	cases := []struct {
		name string
		cfg  map[string]any
		want string
	}{
		{"missing url", map[string]any{}, "config.url is required"},
		{"non-http url", map[string]any{"url": "ftp://x/y"}, "absolute http(s) URL"},
	}
	for _, c := range cases {
		if _, err := New(node(t, c.cfg)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want mention of %q", c.name, err, c.want)
		}
	}
	if _, err := New(node(t, map[string]any{"url": "http://x/y"})); err != nil {
		t.Errorf("valid config rejected: %v", err)
	}
}

func TestExecuteDeliversDiscordPayload(t *testing.T) {
	var gotCT, gotUA string
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		gotUA = r.Header.Get("User-Agent")
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p, err := New(node(t, map[string]any{"url": srv.URL, "username": "SOSNA Bot"}))
	if err != nil {
		t.Fatal(err)
	}
	req := hazardRequest()
	req.DiscordHandles = []string{"ada#1234", "@bob"}
	if err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if gotCT != "application/json" || gotUA != "WarnFlux/0.2.41" {
		t.Errorf("headers = ct %q ua %q", gotCT, gotUA)
	}
	if body["username"] != "SOSNA Bot" {
		t.Errorf("username = %v", body["username"])
	}
	content, _ := body["content"].(string)
	for _, want := range []string{"[SOSNA]", "SEVERE", "Storm", "Strong wind warning", "Areas: powiat wielicki", "Valid until: 2026-09-25T17:00:00Z", "For: ada#1234, @bob"} {
		if !strings.Contains(content, want) {
			t.Errorf("content = %q, want mention of %q", content, want)
		}
	}
}

func TestExecuteNon2xxFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	p, err := New(node(t, map[string]any{"url": srv.URL}))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Execute(context.Background(), hazardRequest()); err == nil || !strings.Contains(err.Error(), "500") {
		t.Errorf("Execute = %v, want a 500 error", err)
	}
}

func TestMessageTextSpeaksSystemLanguage(t *testing.T) {
	p, err := New(node(t, map[string]any{"url": "http://x/y"}))
	if err != nil {
		t.Fatal(err)
	}
	req := hazardRequest()
	req.Lang = "pl"
	req.App.Domain = "sosna.sp9moa.pl"
	req.DiscordHandles = []string{"ada#1234"}
	text := p.(*discordAction).messageText(req)
	for _, want := range []string{
		"POWAŻNE", "Obszary: powiat wielicki", "Ważny do: 2026-09-25T17:00:00Z",
		"Dla: ada#1234", "Szczegóły: https://sosna.sp9moa.pl/message/imgw-meteo:1",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("pl content = %q, want mention of %q", text, want)
		}
	}
	if strings.Contains(text, "SEVERE") || strings.Contains(text, "Areas:") {
		t.Errorf("pl content must not carry English labels: %q", text)
	}
}

func TestMessageTextTruncation(t *testing.T) {
	p, err := New(node(t, map[string]any{"url": "http://x/y"}))
	if err != nil {
		t.Fatal(err)
	}
	req := hazardRequest()
	req.Event.Hazard.Hazard.Headline = strings.Repeat("a", 3000)
	text := p.(*discordAction).messageText(req)
	if got := len([]rune(text)); got > 2000 {
		t.Errorf("content has %d runes, maximum 2000", got)
	}
	if !strings.HasSuffix(text, "…") {
		t.Errorf("truncated content must end with an ellipsis: %q", text[len(text)-20:])
	}
}

func TestMessageTextFallback(t *testing.T) {
	p, err := New(node(t, map[string]any{"url": "http://x/y"}))
	if err != nil {
		t.Fatal(err)
	}
	req := action.ActionRequest{
		Event: dispatch.Event{Kind: dispatch.EventMQTTMessage, MQTT: &dispatch.MQTTMessage{Topic: "warnflux/events"}},
	}
	text := p.(*discordAction).messageText(req)
	if text != "[WarnFlux] WarnFlux notification" {
		t.Errorf("fallback = %q", text)
	}
}

// TestDirectMessage pins the mass-info path: the instance opens ONE
// direct-message channel per user id (mentions accepted, non-ids
// rejected), posts the notice there and reuses the channel afterwards.
func TestDirectMessage(t *testing.T) {
	var created, posted []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bot test-token" {
			t.Errorf("Authorization = %q, want the bot token", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/users/@me/channels":
			body, _ := io.ReadAll(r.Body)
			created = append(created, strings.TrimSpace(string(body)))
			_, _ = w.Write([]byte(`{"id":"dm-42"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/channels/dm-42/messages":
			body, _ := io.ReadAll(r.Body)
			posted = append(posted, string(body))
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected call %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	restore := apiBase
	apiBase = server.URL
	t.Cleanup(func() { apiBase = restore })

	p, err := New(node(t, map[string]any{
		"url":   "https://discord.com/api/webhooks/1/x",
		"token": "test-token",
	}))
	if err != nil {
		t.Fatal(err)
	}
	dm, ok := DirectMessengerOf(p)
	if !ok {
		t.Fatal("a token-configured instance must offer DirectMessenger")
	}

	// A mention works as well as a bare id.
	for _, id := range []string{"284112326204850177", "<@!284112326204850177>"} {
		if err := dm.SendDM(context.Background(), id, "Awaria sieci"); err != nil {
			t.Fatalf("SendDM(%q): %v", id, err)
		}
	}
	if len(created) != 1 {
		t.Fatalf("opened %d direct channels, want 1 (cached): %v", len(created), created)
	}
	if !strings.Contains(created[0], `"recipient_id":"284112326204850177"`) {
		t.Errorf("recipient_id payload = %s", created[0])
	}
	if len(posted) != 2 {
		t.Fatalf("posted %d messages, want 2: %v", len(posted), posted)
	}
	for _, body := range posted {
		if !strings.Contains(body, `"content":"Awaria sieci"`) {
			t.Errorf("message payload = %s", body)
		}
	}

	// Profile values that are not user ids are refused, and nothing is
	// opened for them.
	if err := dm.SendDM(context.Background(), "sp9spm#1234", "x"); err == nil {
		t.Fatal("a username must not be accepted as a user id")
	}
	if len(created) != 1 {
		t.Fatalf("a rejected id opened a channel: %v", created)
	}

	// The webhook-only instance cannot DM.
	plain, err := New(node(t, map[string]any{"url": "https://discord.com/api/webhooks/1/x"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := DirectMessengerOf(plain); ok {
		t.Error("an instance without a token must not offer DirectMessenger")
	}
	if err := plain.(*discordAction).SendDM(context.Background(), "1", "x"); err == nil {
		t.Error("SendDM without a token must fail")
	}
}

// TestDirectMessageRebuildsStaleChannel pins the 404 recovery: a cached
// direct-message channel Discord no longer knows is opened again and the
// message retried once.
func TestDirectMessageRebuildsStaleChannel(t *testing.T) {
	var opens, posts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/users/@me/channels":
			opens++
			_, _ = w.Write([]byte(`{"id":"dm-` + strconv.Itoa(opens) + `"}`))
		case strings.HasPrefix(r.URL.Path, "/channels/dm-1/messages"):
			posts++
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"Unknown Channel"}`))
		case strings.HasPrefix(r.URL.Path, "/channels/dm-2/messages"):
			posts++
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	restore := apiBase
	apiBase = server.URL
	t.Cleanup(func() { apiBase = restore })

	p, err := New(node(t, map[string]any{
		"url": "https://discord.com/api/webhooks/1/x", "token": "t",
	}))
	if err != nil {
		t.Fatal(err)
	}
	dm, ok := DirectMessengerOf(p)
	if !ok {
		t.Fatal("DirectMessengerOf must accept a token-configured instance")
	}
	if err := dm.SendDM(context.Background(), "42", "x"); err != nil {
		t.Fatalf("SendDM: %v", err)
	}
	if opens != 2 || posts != 2 {
		t.Fatalf("opens = %d, posts = %d, want 2 and 2", opens, posts)
	}

	// A real failure (not 404) is reported and does not rebuild.
	apiBase = server.URL + "/gone"
	if err := dm.SendDM(context.Background(), "42", "x"); err == nil {
		t.Fatal("a failing post must return an error")
	}
}
