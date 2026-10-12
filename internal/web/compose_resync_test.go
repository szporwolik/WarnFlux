package web_test

import (
	"context"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
)

// publishesFor counts the hazard publications of one event key.
func publishesFor(env *testEnv, eventKey string) int {
	n := 0
	for _, h := range env.pub.publishedSnapshot() {
		if h.EventKey == eventKey {
			n++
		}
	}
	return n
}

// composeStatusOf reads one communication's status back from the local
// record (the panel's source of truth).
func composeStatusOf(t *testing.T, store *sqlite.Store, eventKey string) string {
	t.Helper()
	rows, err := store.ComposeHazards(context.Background())
	if err != nil {
		t.Fatalf("read compose hazards: %v", err)
	}
	for _, row := range rows {
		if row.EventKey == eventKey {
			return row.Status
		}
	}
	return ""
}

// TestBrokerResyncCannotResurrectARetiredCommunication pins the reported
// P1: the resync and the panel published independently, so a resync that
// had read an ACTIVE communication could publish it back AFTER the
// operator expired it and the tombstone had gone out. The broker then
// served an alarm the local record had already retired. Every publication
// now goes through one version-ordered path, so the stale copy loses.
func TestBrokerResyncCannotResurrectARetiredCommunication(t *testing.T) {
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "compose.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	env := newTestEnvWithUsers(t, store)
	env.login()
	csrf := env.csrfFromPage("/compose")

	const key = "compose:resync-race"
	eff := time.Now().Add(-time.Hour).Format("2006-01-02T15:04")
	exp := time.Now().Add(6 * time.Hour).Format("2006-01-02T15:04")
	resp, _ := env.postForm("/compose", url.Values{
		"csrf": {csrf}, "event_key": {key}, "event": {"Flood"},
		"headline": {"Ewakuacja na północ"}, "severity": {"severe"},
		"urgency": {"immediate"}, "certainty": {"observed"},
		"status": {"active"}, "effective_at": {eff}, "expires_at": {exp},
		"description": {"Heavy rain"}, "instruction": {"Avoid the river bank."},
	})
	if resp.StatusCode != 303 {
		t.Fatalf("POST /compose = %d, want 303", resp.StatusCode)
	}
	if _, ok := env.pub.activeDoc("compose", key); !ok {
		t.Fatal("the panel published no retained active document")
	}

	// The resync reads the ACTIVE state and gets held inside its publish:
	// exactly the window the report describes.
	release, started := env.pub.holdActivePublications()
	resynced := make(chan struct{})
	go func() {
		defer close(resynced)
		env.server.SyncBrokerState()
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("the resync never published the active communication")
	}

	// The operator retires the communication while that publication is
	// still in flight.
	resp, _ = env.postForm("/compose/expire", url.Values{"csrf": {csrf}, "event_key": {key}})
	if resp.StatusCode != 303 {
		t.Fatalf("POST /compose/expire = %d, want 303", resp.StatusCode)
	}
	deadline := time.Now().Add(2 * time.Second)
	for composeStatusOf(t, store, key) != "expired" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := composeStatusOf(t, store, key); got != "expired" {
		t.Fatalf("local record status = %q, want expired", got)
	}
	// Give the retire a moment to reach the broker BEFORE the delayed
	// publication lands (without the fix that is the reported order: the
	// tombstone followed by the earlier active state).
	time.Sleep(150 * time.Millisecond)

	release()
	<-resynced

	// Nothing may put the retired alarm back: the resync's copy is older
	// than the version the expiry wrote.
	deadline = time.Now().Add(2 * time.Second)
	for {
		if _, ok := env.pub.activeDoc("compose", key); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the delayed resync publication put the retired communication back on the broker")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := composeStatusOf(t, store, key); got != "expired" {
		t.Fatalf("local record status after the resync = %q, want expired", got)
	}
}

// TestBrokerResyncKeepsTheRetiredStateRetired is the same promise without
// the race: after the communication is retired, a later resync must not
// republish it either (the resync reads the tombstone and retires again).
func TestBrokerResyncKeepsTheRetiredStateRetired(t *testing.T) {
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "compose2.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	env := newTestEnvWithUsers(t, store)
	env.login()
	csrf := env.csrfFromPage("/compose")

	const key = "compose:resync-retired"
	exp := time.Now().Add(6 * time.Hour).Format("2006-01-02T15:04")
	resp, _ := env.postForm("/compose", url.Values{
		"csrf": {csrf}, "event_key": {key}, "event": {"Flood"},
		"headline": {"Flood warning"}, "severity": {"severe"}, "status": {"active"},
		"expires_at": {exp}, "instruction": {"Avoid the river bank."},
	})
	if resp.StatusCode != 303 {
		t.Fatalf("POST /compose = %d, want 303", resp.StatusCode)
	}
	resp, _ = env.postForm("/compose/expire", url.Values{"csrf": {csrf}, "event_key": {key}})
	if resp.StatusCode != 303 {
		t.Fatalf("POST /compose/expire = %d, want 303", resp.StatusCode)
	}
	// The panel's own activation publish happened before the expiry; what
	// must not happen is another one after it.
	publishesBefore := publishesFor(env, key)
	env.server.SyncBrokerState()
	if doc, ok := env.pub.activeDoc("compose", key); ok {
		t.Fatalf("the resync republished a retired communication: %q", doc)
	}
	if got := publishesFor(env, key); got != publishesBefore {
		t.Fatalf("the resync published the retired communication %d times (was %d)", got, publishesBefore)
	}
	rows, err := store.ComposeHazards(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Status != "expired" {
		t.Fatalf("local records = %+v, want one expired tombstone", rows)
	}
}
