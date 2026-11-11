package web

import (
	"bytes"
	"context"
	"errors"
	"html/template"
	"log/slog"
	"strings"
	"testing"
	"time"

	mesh "github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// quietServer is a Server with only what the dispatcher touches: it logs to
// nowhere so a test can watch the decisions, not the noise.
func quietServer() *Server {
	return &Server{logger: slog.New(slog.DiscardHandler)}
}

// outboxStub is an in-memory MeshOutboxStore: enough of it to watch the
// dispatcher decide, without a database.
type outboxStub struct {
	items []storage.MeshOutboxItem
	next  int64
}

func (o *outboxStub) EnqueueMeshOutbox(_ context.Context, item storage.MeshOutboxItem) (int64, error) {
	o.next++
	item.ID = o.next
	o.items = append(o.items, item)
	return item.ID, nil
}

func (o *outboxStub) PendingMeshOutbox(_ context.Context, limit int) ([]storage.MeshOutboxItem, error) {
	var out []storage.MeshOutboxItem
	for _, item := range o.items {
		if !item.SentAt.IsZero() || !item.FinishedAt.IsZero() || item.Attempts >= storage.MeshOutboxMaxAttempts {
			continue
		}
		out = append(out, item)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (o *outboxStub) RecentMeshOutbox(_ context.Context, limit int) ([]storage.MeshOutboxItem, error) {
	var out []storage.MeshOutboxItem
	for i := len(o.items) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, o.items[i])
	}
	return out, nil
}

func (o *outboxStub) item(id int64) storage.MeshOutboxItem {
	for _, item := range o.items {
		if item.ID == id {
			return item
		}
	}
	return storage.MeshOutboxItem{}
}

func (o *outboxStub) ClaimMeshOutbox(_ context.Context, id int64, at, staleBefore time.Time) (bool, error) {
	for i := range o.items {
		item := &o.items[i]
		if item.ID != id || !item.SentAt.IsZero() || !item.FinishedAt.IsZero() {
			continue
		}
		if !item.ClaimedAt.IsZero() && !item.ClaimedAt.Before(staleBefore) {
			return false, nil
		}
		item.ClaimedAt = at
		return true, nil
	}
	return false, nil
}

func (o *outboxStub) MarkMeshOutboxSent(_ context.Context, id int64, at time.Time) error {
	for i := range o.items {
		if o.items[i].ID == id {
			o.items[i].SentAt = at
			o.items[i].FinishedAt = at
			o.items[i].ClaimedAt = time.Time{}
		}
	}
	return nil
}

func (o *outboxStub) MarkMeshOutboxAttempt(_ context.Context, id int64, errText string, at time.Time) error {
	for i := range o.items {
		if o.items[i].ID != id {
			continue
		}
		o.items[i].Attempts++
		o.items[i].LastError = errText
		o.items[i].ClaimedAt = time.Time{}
		if o.items[i].Attempts >= storage.MeshOutboxMaxAttempts {
			o.items[i].FinishedAt = at
		}
	}
	return nil
}

func (o *outboxStub) DeletePendingMeshOutbox(_ context.Context, id int64) (bool, error) {
	for i := range o.items {
		item := o.items[i]
		if item.ID != id {
			continue
		}
		if !item.ClaimedAt.IsZero() || !item.FinishedAt.IsZero() {
			return false, nil
		}
		o.items = append(o.items[:i], o.items[i+1:]...)
		return true, nil
	}
	return false, nil
}

func (o *outboxStub) PruneMeshOutbox(_ context.Context, before time.Time) error {
	kept := o.items[:0]
	for _, item := range o.items {
		if item.FinishedAt.IsZero() || item.FinishedAt.After(before) {
			kept = append(kept, item)
		}
	}
	o.items = kept
	return nil
}

// senderStub answers the dispatcher's two send calls and records what it was
// asked to send.
type senderStub struct {
	connected bool
	dm        []string
	channel   []string
	dmErr     error
	chErr     error
}

func (s *senderStub) Connected() bool { return s.connected }

func (s *senderStub) SendContactMessage(_ context.Context, addr, text, _ string) error {
	if s.dmErr != nil {
		return s.dmErr
	}
	s.dm = append(s.dm, addr+": "+text)
	return nil
}

func (s *senderStub) SendChannelText(_ context.Context, idx int, text, _ string) error {
	if s.chErr != nil {
		return s.chErr
	}
	s.channel = append(s.channel, text)
	return nil
}

// TestMeshOutboxFlushDeliversAndMarks pins the happy path: what an operator
// queued during a reconnect leaves as soon as the device is back, oldest first,
// and the row stops being pending.
func TestMeshOutboxFlushDeliversAndMarks(t *testing.T) {
	store := &outboxStub{}
	ctx := context.Background()
	if _, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{
		Kind: "dm", TargetID: "1ba292c8", Label: "SP9MOA", Text: "pierwsza", Operator: "admin",
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{
		Kind: "channel", ChannelIdx: 1, Label: "ch1", Text: "druga", Operator: "admin",
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	sender := &senderStub{connected: true}
	srv := quietServer()
	srv.flushMeshOutboxWith(ctx, sender, store)

	if len(sender.dm) != 1 || sender.dm[0] != "1ba292c8: pierwsza" {
		t.Errorf("direct messages sent = %v", sender.dm)
	}
	if len(sender.channel) != 1 || sender.channel[0] != "druga" {
		t.Errorf("channel messages sent = %v", sender.channel)
	}
	for _, item := range store.items {
		if item.SentAt.IsZero() {
			t.Errorf("row %d is still waiting after a successful flush: %+v", item.ID, item)
		}
	}
	if pending, _ := store.PendingMeshOutbox(ctx, 10); len(pending) != 0 {
		t.Errorf("queue still holds %d rows", len(pending))
	}
}

// TestMeshOutboxKeepsTheMessageWhenTheRadioIsGone is the defect this queue
// exists for: the device is between sessions, so the message must stay — with
// the reason recorded — instead of being lost.
func TestMeshOutboxKeepsTheMessageWhenTheRadioIsGone(t *testing.T) {
	store := &outboxStub{}
	ctx := context.Background()
	if _, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{
		Kind: "dm", TargetID: "1ba292c8", Text: "nie zgub mnie", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	// A hub that reports itself connected but loses the link during the send:
	// the one case the panel's own send cannot handle.
	sender := &senderStub{connected: true, dmErr: mesh.ErrDeviceNotConnected}
	srv := quietServer()
	srv.flushMeshOutboxWith(ctx, sender, store)

	pending, err := store.PendingMeshOutbox(ctx, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending = %v, %v; the message was lost", pending, err)
	}
	if pending[0].Attempts != 1 || !strings.Contains(pending[0].LastError, "device not connected") {
		t.Errorf("attempt not recorded: %+v", pending[0])
	}

	// A disconnected hub does not even try.
	idle := &senderStub{}
	store2 := &outboxStub{}
	if _, err := store2.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{Kind: "dm", TargetID: "1ba292c8", Text: "x", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	srv.flushMeshOutboxWith(ctx, idle, store2)
	if len(idle.dm) != 0 {
		t.Errorf("a disconnected hub attempted a send: %v", idle.dm)
	}
	if pending, _ := store2.PendingMeshOutbox(ctx, 10); len(pending) != 1 {
		t.Errorf("an offline pass dropped the message")
	}

	// And when the link returns, the very same row goes out.
	sender.dmErr = nil
	srv.flushMeshOutboxWith(ctx, sender, store)
	if len(sender.dm) != 1 || sender.dm[0] != "1ba292c8: nie zgub mnie" {
		t.Errorf("after the reconnect the queue sent %v", sender.dm)
	}
	if pending, _ := store.PendingMeshOutbox(ctx, 10); len(pending) != 0 {
		t.Errorf("queue still holds rows after the reconnect")
	}
}

// TestMeshOutboxRejectsAreNotRetriedForever separates the operator's mistake
// from a link problem: a refused send is recorded and the queue moves on, so one
// bad row cannot block the messages behind it.
func TestMeshOutboxRejectsAreNotRetriedForever(t *testing.T) {
	store := &outboxStub{}
	ctx := context.Background()
	bad, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{Kind: "dm", TargetID: "zz", Text: "zly adres", CreatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueMeshOutbox(ctx, storage.MeshOutboxItem{Kind: "dm", TargetID: "1ba292c8", Text: "dobra", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}

	sender := &senderStub{connected: true, dmErr: errors.New("meshtastic: node id must be 8 hex characters")}
	srv := quietServer()
	srv.flushMeshOutboxWith(ctx, sender, store)

	if got := store.item(bad).Attempts; got != 1 {
		t.Errorf("the refused row was attempted %d times", got)
	}
	if !isTransientMeshSend(mesh.ErrDeviceNotConnected) {
		t.Errorf("the not-connected error must count as transient")
	}
	if isTransientMeshSend(errors.New("meshtastic: node id must be 8 hex characters")) {
		t.Errorf("a refused send must not be queued as transient")
	}
}

// TestMeshOutboxRendersInThePanel keeps the queue visible: the operator has to
// see what is still waiting, what went out and what was abandoned.
func TestMeshOutboxRendersInThePanel(t *testing.T) {
	srv := &Server{}
	tmpl, err := template.New("root").Funcs(srv.templateFuncs()).ParseFS(templatesFS, "templates/*.html")
	if err != nil {
		t.Fatalf("parse templates: %v", err)
	}
	var buf bytes.Buffer
	v := meshtasticView{
		Lang: "pl", Role: "admin", CSRF: "tok", Dir: "dm", Tab: "msgs",
		OutboxWaiting: 1,
		Outbox: []meshOutboxView{
			{ID: 7, Kind: "dm", Label: "SP9MOA", Text: "czeka na radio", Preview: "czeka na radio",
				Created: "11.11 20:15", State: "waiting", Attempts: 2, Error: "device not connected"},
			{ID: 6, Kind: "channel", Label: "ch1", Text: "poszla", Preview: "poszla",
				Created: "11.11 20:10", State: "sent", Sent: true, SentAt: "20:12"},
		},
	}
	if err := tmpl.ExecuteTemplate(&buf, "meshtastic", v); err != nil {
		t.Fatalf("render meshtastic: %v", err)
	}
	html := buf.String()
	for _, want := range []string{"czeka na radio", "Kolejka radia", "/meshtastic/outbox/cancel", "poszla", "wysłana", "czeka"} {
		if !strings.Contains(html, want) {
			t.Errorf("panel is missing %q", want)
		}
	}
	// A sent row offers no cancel button: only the id of the waiting one is in
	// a cancel form.
	if strings.Contains(html, `value="6"`) {
		t.Errorf("a delivered row is still cancelable")
	}
}
