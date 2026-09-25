package discord

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
