package ingesthttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/mqttpolicy"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
)

type fakeToken struct {
	err     error
	timeout bool
}

func (t *fakeToken) Wait() bool                     { return true }
func (t *fakeToken) WaitTimeout(time.Duration) bool { return !t.timeout }
func (t *fakeToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (t *fakeToken) Error() error { return t.err }

type fakePublish struct {
	topic    string
	qos      byte
	retained bool
	payload  string
}

type fakePublisher struct {
	mu        sync.Mutex
	connected bool
	pubErr    error
	timeout   bool
	published []fakePublish
}

func (f *fakePublisher) Connect() mqtt.Token {
	f.mu.Lock()
	f.connected = true
	f.mu.Unlock()
	return &fakeToken{}
}
func (f *fakePublisher) Disconnect(uint) {}
func (f *fakePublisher) Publish(topic string, qos byte, retained bool, payload any) mqtt.Token {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, _ := payload.([]byte)
	f.published = append(f.published, fakePublish{topic: topic, qos: qos, retained: retained, payload: string(data)})
	return &fakeToken{err: f.pubErr, timeout: f.timeout}
}
func (f *fakePublisher) IsConnected() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connected
}

func testInstance(t *testing.T, pub *fakePublisher) *Instance {
	t.Helper()
	return &Instance{
		cfg: config.IngestHTTP{
			ID:          "news",
			APIKey:      "test-key-1234567890abcdef",
			TopicPrefix: "warnflux",
		},
		client: pub,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func post(t *testing.T, inst *Instance, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-key-1234567890abcdef")
	rec := httptest.NewRecorder()
	inst.ServeHTTP(rec, req)
	return rec
}

func lastPublish(t *testing.T, pub *fakePublisher) fakePublish {
	t.Helper()
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.published) == 0 {
		t.Fatal("no publish recorded")
	}
	return pub.published[len(pub.published)-1]
}

// fakeOutbox is an in-memory durable outbox; fail simulates a broken
// database.
type fakeOutbox struct {
	mu     sync.Mutex
	rows   []storage.OutboxItem
	nextID int64
	fail   bool
}

func (f *fakeOutbox) AppendOutbox(_ context.Context, _, topic string, payload []byte) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return 0, errors.New("db down")
	}
	f.nextID++
	f.rows = append(f.rows, storage.OutboxItem{ID: f.nextID, Topic: topic, Payload: payload})
	return f.nextID, nil
}

func (f *fakeOutbox) PendingOutbox(_ context.Context, _ string, limit int) ([]storage.OutboxItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit <= 0 || limit > len(f.rows) {
		limit = len(f.rows)
	}
	return append([]storage.OutboxItem(nil), f.rows[:limit]...), nil
}

func (f *fakeOutbox) AckOutbox(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, it := range f.rows {
		if it.ID == id {
			f.rows = append(f.rows[:i], f.rows[i+1:]...)
			return nil
		}
	}
	return nil
}

func (f *fakeOutbox) OutboxCount(_ context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows), nil
}

func (f *fakeOutbox) pending() []storage.OutboxItem {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]storage.OutboxItem(nil), f.rows...)
}

// postAndSync posts and then runs one outbox pass, so publish-asserting
// tests observe the durable sync worker's behavior deterministically.
func postAndSync(t *testing.T, inst *Instance, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := post(t, inst, body)
	inst.drainOutbox(context.Background())
	return rec
}

func TestAuthRejectsMissingOrWrongKey(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe"}`))
	rec := httptest.NewRecorder()
	inst.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing key = %d, want 401", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe"}`))
	req.Header.Set("Authorization", "Bearer wrong-key")
	rec = httptest.NewRecorder()
	inst.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong key = %d, want 401", rec.Code)
	}
	if pub.published != nil {
		t.Fatalf("unauthorized request published: %+v", pub.published)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	inst := testInstance(t, &fakePublisher{connected: true})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ingest/news", nil)
	req.Header.Set("Authorization", "Bearer test-key-1234567890abcdef")
	rec := httptest.NewRecorder()
	inst.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET = %d, want 405", rec.Code)
	}
}

func TestBuilderModePublishesCanonicalWireEvent(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	inst.SetOutbox(&fakeOutbox{})

	rec := postAndSync(t, inst, `{"severity":"severe","headline":"Pożar w lesie","event":"Pożar","areas":["gmina Niepołomice"]}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("builder post = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}
	if len(pub.published) != 2 {
		t.Fatalf("published %d messages, want 2 (events journal + active view)", len(pub.published))
	}

	// First publish: the non-retained /events journal entry.
	p := pub.published[0]
	if p.topic != "warnflux/events" || p.qos != 1 || p.retained {
		t.Errorf("publish = %+v, want warnflux/events qos 1 non-retained", p)
	}
	var we mqttreceiver.EventPayload
	if err := json.Unmarshal([]byte(p.payload), &we); err != nil {
		t.Fatalf("published payload is not valid wire JSON: %v", err)
	}
	if we.SchemaVersion != mqttreceiver.WireSchemaVersion || we.ChangeType != mqttreceiver.ChangeNew {
		t.Errorf("envelope = schema %d type %q, want 1/new", we.SchemaVersion, we.ChangeType)
	}
	if we.Event.Source != "news" || !strings.HasPrefix(we.EventKey, "news:") {
		t.Errorf("identity = source %q key %q, want source news, key news:...", we.Event.Source, we.EventKey)
	}
	if we.Event.Severity != "severe" || we.Event.Headline != "Pożar w lesie" || we.Event.Event != "Pożar" {
		t.Errorf("event fields = %+v", we.Event)
	}
	if we.Event.Status != "active" || we.Event.ReceivedAt == "" || we.Event.UpdatedAt == "" {
		t.Errorf("status/timestamps = %q/%q/%q", we.Event.Status, we.Event.ReceivedAt, we.Event.UpdatedAt)
	}

	// Second publish: the retained active-view document mirroring the
	// transition onto <prefix>/active/<source>/<hash>.
	a := pub.published[1]
	if a.topic != "warnflux/active/news/"+mqttreceiver.TopicHash(we.EventKey) || a.qos != 1 || !a.retained {
		t.Errorf("active publish = %+v", a)
	}
	var aw mqttreceiver.ActivePayload
	if err := json.Unmarshal([]byte(a.payload), &aw); err != nil {
		t.Fatalf("active payload invalid: %v", err)
	}
	if aw.Type != mqttreceiver.TypeActiveHazard || aw.EventKey != we.EventKey || aw.Event.Severity != "severe" {
		t.Errorf("active payload = %+v", aw)
	}
}

func TestBuilderDeduplicatesIdenticalPosts(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	inst.SetOutbox(&fakeOutbox{})
	body := `{"severity":"moderate","headline":"Zalana droga","source_id":"scraper-7"}`

	if rec := postAndSync(t, inst, body); rec.Code != http.StatusAccepted {
		t.Fatalf("first post = %d", rec.Code)
	}
	first := lastPublish(t, pub)
	if rec := postAndSync(t, inst, body); rec.Code != http.StatusAccepted {
		t.Fatalf("second post = %d", rec.Code)
	}
	second := lastPublish(t, pub)

	var w1, w2 mqttreceiver.EventPayload
	_ = json.Unmarshal([]byte(first.payload), &w1)
	_ = json.Unmarshal([]byte(second.payload), &w2)
	if w1.EventKey != "news:scraper-7" || w2.EventKey != w1.EventKey {
		t.Errorf("event keys = %q / %q, want stable news:scraper-7", w1.EventKey, w2.EventKey)
	}

	// A changed body without a source_id gets a fresh (hashed) key.
	if rec := postAndSync(t, inst, `{"severity":"moderate","headline":"Zalana droga 2"}`); rec.Code != http.StatusAccepted {
		t.Fatalf("third post = %d", rec.Code)
	}
	var w3 mqttreceiver.EventPayload
	_ = json.Unmarshal([]byte(lastPublish(t, pub).payload), &w3)
	if !strings.HasPrefix(w3.EventKey, "news:") || w3.EventKey == w1.EventKey {
		t.Errorf("hashed event key = %q, want news:<hash> different from %q", w3.EventKey, w1.EventKey)
	}
}

func TestBuilderValidationErrors(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	cases := []struct {
		name string
		body string
		want string
	}{
		{"missing severity", `{"headline":"x"}`, "severity"},
		{"invalid severity", `{"severity":"orange","headline":"x"}`, "severity"},
		{"missing event and headline", `{"severity":"severe"}`, "event or headline"},
		{"bad transition", `{"severity":"severe","headline":"x","transition":"bogus"}`, "transition"},
		{"bad json", `{not json`, "JSON"},
	}
	for _, c := range cases {
		rec := post(t, inst, c.body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", c.name, rec.Code)
			continue
		}
		if !strings.Contains(rec.Body.String(), c.want) {
			t.Errorf("%s: body = %s, want mention of %q", c.name, rec.Body.String(), c.want)
		}
	}
	if pub.published != nil {
		t.Errorf("invalid posts published: %+v", pub.published)
	}
}

func TestWireModePassThroughNormalized(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	inst.SetOutbox(&fakeOutbox{})

	wire := `{"schema_version":1,"change_id":42,"change_type":"updated","event_key":"imgw-meteo:123","extra_junk":"dropped","event":{"source":"imgw-meteo","source_id":"123","event":"Wiatr","severity":"severe","status":"active","received_at":"2026-09-22T10:00:00Z","updated_at":"2026-09-22T10:05:00Z"}}`
	rec := postAndSync(t, inst, wire)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("wire post = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}

	p := pub.published[0]
	if p.topic != "warnflux/events" {
		t.Errorf("topic = %q, want warnflux/events", p.topic)
	}
	if strings.Contains(p.payload, "extra_junk") {
		t.Errorf("unknown fields must be dropped: %s", p.payload)
	}
	var we mqttreceiver.EventPayload
	if err := json.Unmarshal([]byte(p.payload), &we); err != nil {
		t.Fatalf("published payload invalid: %v", err)
	}
	if we.EventKey != "imgw-meteo:123" || we.ChangeID != 42 || we.ChangeType != "updated" || we.Event.Source != "imgw-meteo" {
		t.Errorf("wire event = %+v", we)
	}

	// The updated transition is also mirrored into the retained active
	// view under the source-keyed topic.
	a := pub.published[1]
	if a.topic != "warnflux/active/imgw-meteo/"+mqttreceiver.TopicHash(we.EventKey) || !a.retained {
		t.Errorf("active publish = %+v", a)
	}
}

func TestWireModeCancelledDeletesActiveView(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	inst.SetOutbox(&fakeOutbox{})

	wire := `{"schema_version":1,"change_id":7,"change_type":"cancelled","event_key":"rso:99","event":{"source":"rso","source_id":"99","event":"Burze","severity":"moderate","status":"cancelled","received_at":"2026-09-22T10:00:00Z","updated_at":"2026-09-22T10:05:00Z"}}`
	rec := postAndSync(t, inst, wire)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("cancelled wire post = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}
	if len(pub.published) != 2 {
		t.Fatalf("published %d messages, want 2", len(pub.published))
	}

	a := pub.published[1]
	if a.topic != "warnflux/active/rso/"+mqttreceiver.TopicHash("rso:99") || !a.retained || a.payload != "" {
		t.Errorf("cancelled active publish = %+v, want retained empty payload (delete)", a)
	}
}

// fakeInbox is a minimal durable inbox for the local dispatch ingress.
type fakeInbox struct {
	mu     sync.Mutex
	events []dispatch.Event
}

func (f *fakeInbox) AppendEvent(ctx context.Context, e dispatch.Event) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
	return int64(len(f.events)), nil
}

// TestMaskedEventsStillIngestLocally pins the reported P1: the MQTT
// publish mask must never lose the local pipeline. The mask now applies
// at PUBLISH time in the outbox worker: a fully masked request answers
// 202 with a durable local inbox row AND a durable (deferred) outbox
// row; unmasking a category syncs the deferred rows.
func TestMaskedEventsStillIngestLocally(t *testing.T) {
	oldMask := mqttpolicy.Mask()
	t.Cleanup(func() { mqttpolicy.Set(oldMask) })

	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	inbox := &fakeInbox{}
	g := dispatch.NewIngress(4)
	g.SetInbox(inbox)
	inst.SetIngress(g)
	box := &fakeOutbox{}
	inst.SetOutbox(box)

	wire := func(changeID int) string {
		return `{"schema_version":1,"change_id":` + fmt.Sprintf("%d", changeID) +
			`,"change_type":"new","event_key":"imgw-meteo:` + fmt.Sprintf("%d", changeID) +
			`","event":{"source":"imgw-meteo","source_id":"` + fmt.Sprintf("%d", changeID) +
			`","event":"Wiatr","severity":"severe","status":"active","received_at":"2026-09-22T10:00:00Z","updated_at":"2026-09-22T10:05:00Z"}}`
	}

	// Events + active masked: nothing reaches the broker; the local
	// inbox holds the transition durably and the outbox row is DEFERRED
	// (not dropped) — the sync resumes when the mask allows.
	mqttpolicy.Set(oldMask &^ uint32(mqttpolicy.CatEvents) &^ uint32(mqttpolicy.CatActive))
	rec := post(t, inst, wire(42))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("masked wire post = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"accepted":true`) {
		t.Fatalf("masked wire post body = %s, want accepted:true", rec.Body.String())
	}
	if len(pub.published) != 0 {
		t.Fatalf("broker publishes = %d, want 0 (everything masked)", len(pub.published))
	}
	inbox.mu.Lock()
	n := len(inbox.events)
	var gotKey string
	if n > 0 && inbox.events[0].Hazard != nil {
		gotKey = inbox.events[0].Hazard.Key
	}
	inbox.mu.Unlock()
	if n != 1 || gotKey != "imgw-meteo:42" {
		t.Fatalf("local inbox = (%d, %q), want (1, imgw-meteo:42)", n, gotKey)
	}
	if pending := len(box.pending()); pending != 1 {
		t.Fatalf("deferred outbox rows = %d, want 1", pending)
	}

	// Only events masked: the next outbox pass publishes the deferred
	// active-view mirrors (the first row included — the mask is checked
	// at publish time) and keeps /events silent. The allowed part of
	// each row is published and acknowledged.
	mqttpolicy.Set(oldMask &^ uint32(mqttpolicy.CatEvents))
	rec = post(t, inst, wire(43))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("events-masked wire post = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}
	inst.drainOutbox(context.Background())
	pub.mu.Lock()
	var topics []string
	for _, p := range pub.published {
		topics = append(topics, p.topic)
	}
	pub.mu.Unlock()
	if len(topics) != 2 {
		t.Fatalf("broker topics = %v, want the two deferred active-view mirrors", topics)
	}
	for _, tp := range topics {
		if !strings.HasPrefix(tp, "warnflux/active/") {
			t.Errorf("broker topic %q, want the active-view mirror", tp)
		}
	}
	inbox.mu.Lock()
	n = len(inbox.events)
	inbox.mu.Unlock()
	if n != 2 {
		t.Fatalf("local inbox rows = %d, want 2", n)
	}
	if pending := len(box.pending()); pending != 0 {
		t.Fatalf("outbox rows after the allowed parts synced = %d, want 0", pending)
	}

	// Fully masked again: the row defers, and a later unmask drains it.
	mqttpolicy.Set(oldMask &^ uint32(mqttpolicy.CatEvents) &^ uint32(mqttpolicy.CatActive))
	if rec := post(t, inst, wire(44)); rec.Code != http.StatusAccepted {
		t.Fatalf("second fully-masked post = %d", rec.Code)
	}
	inst.drainOutbox(context.Background())
	if pending := len(box.pending()); pending != 1 {
		t.Fatalf("deferred outbox rows = %d, want 1", pending)
	}
	mqttpolicy.Set(oldMask)
	inst.drainOutbox(context.Background())
	pub.mu.Lock()
	topics = topics[:0]
	for _, p := range pub.published {
		topics = append(topics, p.topic)
	}
	pub.mu.Unlock()
	if len(topics) != 4 || topics[2] != "warnflux/events" || !strings.HasPrefix(topics[3], "warnflux/active/") {
		t.Fatalf("after the unmask topics = %v, want events + active for the deferred row", topics)
	}
	if pending := len(box.pending()); pending != 0 {
		t.Fatalf("outbox rows after the unmask = %d, want 0", pending)
	}
}

// TestDisabledInputExplicitError pins the deliberate-disable contract:
// a configured-but-disabled ingest input answers with an explicit error
// instead of a misleading 404.
func TestDisabledInputExplicitError(t *testing.T) {
	h := Disabled(slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled input status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "disabled") {
		t.Fatalf("disabled input body = %s, want an explicit disable message", rec.Body.String())
	}
}

func TestWireModeRejectsInvalid(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	cases := []string{
		`{"schema_version":9,"change_type":"new","event_key":"a:1","event":{}}`,
		`{"schema_version":1,"change_type":"bogus","event_key":"a:1","event":{}}`,
		`{"schema_version":1,"change_type":"new","event":{}}`,
	}
	for _, body := range cases {
		rec := post(t, inst, body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("wire post %s = %d, want 400", body, rec.Code)
		}
	}
	if pub.published != nil {
		t.Errorf("invalid wire posts published: %+v", pub.published)
	}
}

// TestBrokerDownAcceptsLocallyAndSyncsWhenBack pins the reported P1: the
// broker state must never gate local acceptance. With working SQLite and
// a disconnected broker the endpoint answers 202 with a durable local
// inbox row AND a durable outbox row (previously 503 with inbox=0); once
// the broker reconnects, the outbox worker publishes the payload and the
// backlog drains — even across a restart.
func TestBrokerDownAcceptsLocallyAndSyncsWhenBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ingest.db")
	store, _, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	pub := &fakePublisher{connected: false}
	inst := testInstance(t, pub)
	inst.SetOutbox(store)

	g := dispatch.NewIngress(4)
	g.SetInbox(store)
	inst.SetIngress(g)

	wire := `{"schema_version":1,"change_id":42,"change_type":"new","event_key":"imgw-meteo:123","event":{"source":"imgw-meteo","source_id":"123","event":"Wiatr","severity":"severe","status":"active","received_at":"2026-09-22T10:00:00Z","updated_at":"2026-09-22T10:05:00Z"}}`
	rec := post(t, inst, wire)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("disconnected broker = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}
	if len(pub.published) != 0 {
		t.Fatalf("publishes with a disconnected broker = %d, want 0", len(pub.published))
	}

	// Durable local acceptance AND durable broker sync, both persisted.
	ctx := context.Background()
	inbox, err := store.PendingInboxEvents(ctx, 10)
	if err != nil || len(inbox) != 1 {
		t.Fatalf("local inbox = (%v, %v), want 1 durable row", inbox, err)
	}
	outbox, err := store.PendingOutbox(ctx, "news", 10)
	if err != nil || len(outbox) != 1 {
		t.Fatalf("outbox = (%v, %v), want 1 durable row", outbox, err)
	}

	// Restart simulation: a fresh handle over the same file still sees
	// the pending outbox row, so the cross-instance sync survives.
	store.Close()
	restarted, _, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if outbox, err = restarted.PendingOutbox(ctx, "news", 10); err != nil || len(outbox) != 1 {
		t.Fatalf("outbox after restart = (%v, %v), want 1 pending row", outbox, err)
	}
	inst.SetOutbox(restarted)

	// The broker returns: one outbox pass publishes /events + the
	// active-view mirror and drains the backlog.
	pub.mu.Lock()
	pub.connected = true
	pub.mu.Unlock()
	inst.drainOutbox(ctx)
	pub.mu.Lock()
	publishes := len(pub.published)
	pub.mu.Unlock()
	if publishes != 2 {
		t.Fatalf("publishes after the broker returned = %d, want 2 (events + active)", publishes)
	}
	if outbox, err = restarted.PendingOutbox(ctx, "news", 10); err != nil || len(outbox) != 0 {
		t.Fatalf("outbox after sync = (%v, %v), want empty", outbox, err)
	}
}

func TestOutboxUnavailableRejects(t *testing.T) {
	inst := testInstance(t, &fakePublisher{connected: true})
	box := &fakeOutbox{fail: true}
	inst.SetOutbox(box)
	rec := post(t, inst, `{"severity":"severe","headline":"x"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("post with a broken outbox = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "outbox") {
		t.Fatalf("body = %s, want an outbox mention", rec.Body.String())
	}
}

// TestOutboxRetriesFailedPublishes pins the at-least-once sync: a broker
// that rejects (or times out) a publish leaves the row pending — the
// endpoint has already answered 202 — and the next pass retries it. The
// old contract (503 on a publish failure) is gone: acceptance is local.
func TestOutboxRetriesFailedPublishes(t *testing.T) {
	pub := &fakePublisher{connected: true, pubErr: io.ErrUnexpectedEOF}
	inst := testInstance(t, pub)
	box := &fakeOutbox{}
	inst.SetOutbox(box)

	if rec := post(t, inst, `{"severity":"severe","headline":"x"}`); rec.Code != http.StatusAccepted {
		t.Fatalf("post with a failing broker = %d, want 202 (local acceptance)", rec.Code)
	}
	inst.drainOutbox(context.Background())
	if got := len(box.pending()); got != 1 {
		t.Fatalf("outbox rows after a failed publish = %d, want 1 (retry pending)", got)
	}

	// Broker healed: the next pass publishes and acknowledges.
	pub.mu.Lock()
	pub.pubErr = nil
	pub.mu.Unlock()
	inst.drainOutbox(context.Background())
	if got := len(box.pending()); got != 0 {
		t.Fatalf("outbox rows after a successful retry = %d, want 0", got)
	}

	// Timeout flavor: the same at-least-once behavior.
	inst2 := testInstance(t, &fakePublisher{connected: true, timeout: true})
	box2 := &fakeOutbox{}
	inst2.SetOutbox(box2)
	if rec := post(t, inst2, `{"severity":"severe","headline":"x"}`); rec.Code != http.StatusAccepted {
		t.Fatalf("post with a timing-out broker = %d, want 202", rec.Code)
	}
	inst2.drainOutbox(context.Background())
	if got := len(box2.pending()); got != 1 {
		t.Fatalf("outbox rows after a publish timeout = %d, want 1 (retry pending)", got)
	}
}

func TestOversizedBody(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	big := `{"severity":"severe","headline":"` + strings.Repeat("x", mqttreceiver.MaxPayload) + `"}`
	rec := post(t, inst, big)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized = %d, want 413", rec.Code)
	}
}

// TestResolve pins the broker inheritance: empty instance settings come
// from the primary mqtt output; explicit values always win.
func TestResolve(t *testing.T) {
	fallback := config.IngestHTTP{
		Broker:       "tcp://main:1883",
		ClientID:     "warnflux-main-out",
		Username:     "main-user",
		Password:     "main-pass",
		TopicPrefix:  "warnflux",
		PasswordFile: "",
	}

	got := Resolve(config.IngestHTTP{ID: "news"}, fallback)
	if got.Broker != fallback.Broker || got.Username != fallback.Username ||
		got.Password != fallback.Password || got.TopicPrefix != fallback.TopicPrefix {
		t.Errorf("inherited = %+v, want fallback values", got)
	}
	if got.ClientID != "warnflux-ingest-news" {
		t.Errorf("client_id = %q, want default warnflux-ingest-news", got.ClientID)
	}

	override := config.IngestHTTP{
		ID:          "news",
		Broker:      "tcp://other:1883",
		TopicPrefix: "e2etest",
	}
	got = Resolve(override, fallback)
	if got.Broker != "tcp://other:1883" || got.TopicPrefix != "e2etest" {
		t.Errorf("overrides lost: %+v", got)
	}
	if got.Username != fallback.Username || got.Password != fallback.Password {
		t.Errorf("credentials should still inherit: %+v", got)
	}
}

// TestKeyRotation pins the two-key window: the current and the previous
// key both authenticate; anything else fails and counts as auth_failed.
func TestKeyRotation(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	inst.previousKey = "previous-key-1234567890abc"

	for _, key := range []string{"test-key-1234567890abcdef", "previous-key-1234567890abc"} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe","headline":"x"}`))
		req.Header.Set("Authorization", "Bearer "+key)
		rec := httptest.NewRecorder()
		inst.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Errorf("key %q = %d, want 202", key, rec.Code)
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe","headline":"x"}`))
	req.Header.Set("Authorization", "Bearer neither-of-them")
	rec := httptest.NewRecorder()
	inst.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("unknown key = %d, want 401", rec.Code)
	}
	if c := inst.Counters(); c.AuthFailed != 1 || c.Accepted != 2 {
		t.Errorf("counters = %+v, want accepted=2 auth_failed=1", c)
	}
}

// TestAllowedCIDRs pins the optional source allowlist: matching sources
// pass, others get 403 with the forbidden counter.
func TestAllowedCIDRs(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	inst.cfg.AllowedCIDRs = []string{"192.0.2.0/24", "2001:db8::1"}
	inst.ensureInit()

	allowed := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe","headline":"x"}`))
	allowed.RemoteAddr = "192.0.2.7:5555"
	allowed.Header.Set("Authorization", "Bearer test-key-1234567890abcdef")
	rec := httptest.NewRecorder()
	inst.ServeHTTP(rec, allowed)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("allowed source = %d, want 202", rec.Code)
	}

	blocked := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe","headline":"x"}`))
	blocked.RemoteAddr = "198.51.100.9:5555"
	blocked.Header.Set("Authorization", "Bearer test-key-1234567890abcdef")
	rec = httptest.NewRecorder()
	inst.ServeHTTP(rec, blocked)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("blocked source = %d, want 403", rec.Code)
	}
	if c := inst.Counters(); c.Forbidden != 1 {
		t.Errorf("counters = %+v, want forbidden=1", c)
	}
}

// TestRateLimit pins the token bucket: the third request inside the
// window is rejected with 429 + Retry-After and counted.
func TestRateLimit(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	inst.cfg.RateLimitPerMinute = 2
	inst.ensureInit()

	for i := 0; i < 2; i++ {
		if rec := post(t, inst, `{"severity":"severe","headline":"x"}`); rec.Code != http.StatusAccepted {
			t.Fatalf("request %d = %d, want 202", i+1, rec.Code)
		}
	}
	rec := post(t, inst, `{"severity":"severe","headline":"x"}`)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("third request = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("429 response missing Retry-After header")
	}
	if c := inst.Counters(); c.Accepted != 2 || c.RateLimited != 1 {
		t.Errorf("counters = %+v, want accepted=2 rate_limited=1", c)
	}
}

// TestRateLimitUnlimited pins the negative-rate escape hatch.
func TestRateLimitUnlimited(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)
	inst.cfg.RateLimitPerMinute = -1
	inst.ensureInit()
	for i := 0; i < 200; i++ {
		if rec := post(t, inst, `{"severity":"severe","headline":"x"}`); rec.Code != http.StatusAccepted {
			t.Fatalf("request %d = %d, want 202 with limiting disabled", i+1, rec.Code)
		}
	}
}

// TestRequestID pins the audit/response request id: generated when absent,
// echoed when the client supplies one.
func TestRequestID(t *testing.T) {
	pub := &fakePublisher{connected: true}
	inst := testInstance(t, pub)

	rec := post(t, inst, `{"severity":"severe","headline":"x"}`)
	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("response missing generated X-Request-ID")
	}
	if !strings.Contains(rec.Body.String(), "request_id") {
		t.Errorf("accepted body missing request id: %s", rec.Body.String())
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe","headline":"x"}`))
	req.Header.Set("Authorization", "Bearer test-key-1234567890abcdef")
	req.Header.Set("X-Request-ID", "scraper-run-42")
	rec = httptest.NewRecorder()
	inst.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); got != "scraper-run-42" {
		t.Errorf("X-Request-ID = %q, want echoed client value", got)
	}
}

// TestInvalidCIDRRejectedAtNew pins the fail-at-startup contract.
func TestInvalidCIDRRejectedAtNew(t *testing.T) {
	_, err := New(config.IngestHTTP{
		ID: "news", APIKey: "supersecret-key-123",
		AllowedCIDRs: []string{"not-a-cidr"},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "not-a-cidr") {
		t.Fatalf("New with bad CIDR = %v, want rejection", err)
	}
}

// TestPreviousKeyFile pins the file-based rotation key.
func TestPreviousKeyFile(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/prev.key"
	if err := os.WriteFile(path, []byte("previous-key-1234567890abc\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	inst, err := New(config.IngestHTTP{
		ID: "news", APIKey: "current-key-1234567890ab",
		PreviousKeyFile: path,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	inst.client = &fakePublisher{connected: true}
	inst.started.Store(true)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/news", strings.NewReader(`{"severity":"severe","headline":"x"}`))
	req.Header.Set("Authorization", "Bearer previous-key-1234567890abc")
	rec := httptest.NewRecorder()
	inst.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("previous key from file = %d, want 202", rec.Code)
	}
}
