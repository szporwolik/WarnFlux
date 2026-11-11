package web_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	mesh "github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// outboxRecorder is the queue store seen from the HTTP test: it only has to
// answer "what was queued?".
type outboxRecorder struct {
	items []storage.MeshOutboxItem
}

func (o *outboxRecorder) EnqueueMeshOutbox(_ context.Context, item storage.MeshOutboxItem) (int64, error) {
	o.items = append(o.items, item)
	return int64(len(o.items)), nil
}

func (o *outboxRecorder) PendingMeshOutbox(_ context.Context, limit int) ([]storage.MeshOutboxItem, error) {
	return o.items, nil
}

func (o *outboxRecorder) RecentMeshOutbox(_ context.Context, limit int) ([]storage.MeshOutboxItem, error) {
	return o.items, nil
}

func (o *outboxRecorder) MarkMeshOutboxSent(_ context.Context, _ int64, _ time.Time) error {
	return nil
}
func (o *outboxRecorder) MarkMeshOutboxAttempt(_ context.Context, _ int64, _ string) error {
	return nil
}
func (o *outboxRecorder) DeleteMeshOutbox(_ context.Context, _ int64) error    { return nil }
func (o *outboxRecorder) PruneMeshOutbox(_ context.Context, _ time.Time) error { return nil }

// TestMeshtasticSendQueuesWhenTheRadioIsAway is the reported defect end to end:
// the operator sends a DM while the node is between sessions and the message
// must not vanish — the panel says it is queued and the text waits in the
// outbox for the reconnect.
//
// The hub below has no transport attached, so it is disconnected: exactly the
// state the production radio was in when the send failed.
func TestMeshtasticSendQueuesWhenTheRadioIsAway(t *testing.T) {
	hub, err := mesh.NewHub(mesh.Config{Enabled: true, Device: "/dev/null/none"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	env := newTestEnvWithMesh(t, hub)
	queue := &outboxRecorder{}
	env.server.SetMeshOutbox(queue)

	b := env.newBrowser()
	b.signIn(testUsername, testPassword)
	_, html := b.get("/meshtastic?tab=msgs&dir=dm")
	resp, body := b.postForm("/meshtastic/send", url.Values{
		"csrf":    {extractCSRF(t, html)},
		"target":  {"contact"},
		"contact": {"1ba292c8"},
		"text":    {"Wiadomosc testowa"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("send = %d, want 303", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); !strings.Contains(loc, "queued=1") {
		t.Errorf("redirect = %q, want queued=1 (the message must not be reported as sent)", loc)
	}
	_ = body
	if len(queue.items) != 1 {
		t.Fatalf("queued %d messages, want 1", len(queue.items))
	}
	item := queue.items[0]
	if item.Kind != "dm" || item.TargetID != "1ba292c8" {
		t.Errorf("queued item = %+v", item)
	}
	if !strings.Contains(item.Text, "Wiadomosc testowa") {
		t.Errorf("queued text = %q", item.Text)
	}
	if item.Operator != testUsername {
		t.Errorf("operator = %q, want %q", item.Operator, testUsername)
	}
}

// TestMeshtasticSendWithoutAQueueStillReportsTheError keeps the old behaviour
// where no queue is configured: the operator is told, nothing is invented.
func TestMeshtasticSendWithoutAQueueStillReportsTheError(t *testing.T) {
	hub, err := mesh.NewHub(mesh.Config{Enabled: true, Device: "/dev/null/none"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	env := newTestEnvWithMesh(t, hub)

	b := env.newBrowser()
	b.signIn(testUsername, testPassword)
	_, html := b.get("/meshtastic?tab=msgs&dir=dm")
	resp, _ := b.postForm("/meshtastic/send", url.Values{
		"csrf":    {extractCSRF(t, html)},
		"target":  {"contact"},
		"contact": {"1ba292c8"},
		"text":    {"Bez kolejki"},
	})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("send = %d, want 303", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); !strings.Contains(loc, "err=") {
		t.Errorf("redirect = %q, want an error flash", loc)
	}
}
