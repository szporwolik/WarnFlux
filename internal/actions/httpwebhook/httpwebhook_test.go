package httpwebhook

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	eff := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	return action.ActionRequest{
		ID: "imgw-meteo:1/wh",
		Event: dispatch.Event{
			Kind: dispatch.EventHazardTransition,
			Hazard: &dispatch.HazardTransition{
				Type: dispatch.TransitionNew,
				Key:  "imgw-meteo:1",
				Hazard: dispatch.Hazard{
					EventKey:         "imgw-meteo:1",
					Source:           "imgw-meteo",
					SourceID:         "1",
					Event:            "Storm",
					Severity:         "severe",
					ProviderSeverity: "2",
					Urgency:          "immediate",
					Certainty:        "observed",
					Headline:         "Strong wind warning",
					Areas:            []string{"powiat wielicki"},
					EffectiveAt:      &eff,
					ReceivedAt:       eff.Add(-time.Hour),
					UpdatedAt:        eff.Add(-30 * time.Minute),
				},
			},
		},
		App: action.AppInfo{Version: "0.2.1", Header1: "SOSNA", Domain: "sosna.example.org", RepoURL: "https://github.com/example/w"},
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
		{"token and file", map[string]any{"url": "http://x/y", "token": "a", "token_file": "/tmp/t"}, "mutually exclusive"},
		{"bad header name", map[string]any{"url": "http://x/y", "headers": map[string]string{"Bad\nName": "v"}}, "invalid header name"},
		{"header newline value", map[string]any{"url": "http://x/y", "headers": map[string]string{"X-Test": "a\nb"}}, "newline"},
	}
	for _, c := range cases {
		if _, err := New(node(t, c.cfg)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want mention of %q", c.name, err, c.want)
		}
	}

	if _, err := New(node(t, map[string]any{"url": "http://x/y", "token_file": "/no/such/file"})); err == nil {
		t.Error("missing token file accepted")
	}
}

func TestExecuteDeliversCanonicalBody(t *testing.T) {
	var gotMethod, gotAuth, gotCT, gotCustom string
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		gotCustom = r.Header.Get("X-Custom")
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p, err := New(node(t, map[string]any{
		"url":     srv.URL,
		"token":   "sekret",
		"headers": map[string]string{"X-Custom": "yes"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Execute(context.Background(), hazardRequest()); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if gotMethod != http.MethodPost || gotAuth != "Bearer sekret" || gotCT != "application/json" || gotCustom != "yes" {
		t.Errorf("headers = method %q auth %q ct %q custom %q", gotMethod, gotAuth, gotCT, gotCustom)
	}
	if body["schema_version"].(float64) != 1 || body["type"] != "hazard_notification" {
		t.Errorf("envelope = %v", body)
	}
	if body["change_type"] != "hazard_new" || body["event_key"] != "imgw-meteo:1" {
		t.Errorf("envelope = %v", body)
	}
	ev, ok := body["event"].(map[string]any)
	if !ok {
		t.Fatalf("event block = %v", body["event"])
	}
	if ev["severity"] != "severe" || ev["provider_severity"] != "2" || ev["headline"] != "Strong wind warning" {
		t.Errorf("event = %v", ev)
	}
	if ev["effective_at"] == nil || ev["received_at"] == "" {
		t.Errorf("timestamps = %v", ev)
	}
	app, ok := body["app"].(map[string]any)
	if !ok || app["version"] != "0.2.1" || app["header1"] != "SOSNA" {
		t.Errorf("app = %v", body["app"])
	}
}

func TestExecuteNon2xxIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	p, err := New(node(t, map[string]any{"url": srv.URL}))
	if err != nil {
		t.Fatal(err)
	}
	err = p.Execute(context.Background(), hazardRequest())
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("Execute = %v, want 500 error", err)
	}
}

func TestExecuteTransportError(t *testing.T) {
	p, err := New(node(t, map[string]any{"url": "http://127.0.0.1:1/hook", "timeout": "500ms"}))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Execute(context.Background(), hazardRequest()); err == nil {
		t.Fatal("delivery to a dead endpoint must fail")
	}
}

func TestExecuteContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
	}))
	defer srv.Close()

	p, err := New(node(t, map[string]any{"url": srv.URL, "timeout": "2s"}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = p.Execute(ctx, hazardRequest())
	if err == nil {
		t.Fatal("cancelled delivery must fail")
	}
}

func TestTokenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("file-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p, err := New(node(t, map[string]any{"url": srv.URL, "token_file": path}))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Execute(context.Background(), hazardRequest()); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer file-secret" {
		t.Errorf("Authorization = %q, want bearer from file", gotAuth)
	}
}

// TestCloseIsIdempotent pins the no-op close contract.
func TestCloseIsIdempotent(t *testing.T) {
	p, err := New(node(t, map[string]any{"url": "http://x/y"}))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Errorf("Close = %v", err)
	}
	if err := p.Close(context.Background()); err != nil && !errors.Is(err, nil) {
		t.Errorf("second Close = %v", err)
	}
}
