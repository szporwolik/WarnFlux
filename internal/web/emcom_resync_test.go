package web_test

import (
	"encoding/json"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
)

// emcomKey is the event key of the network the resync tests add.
const emcomKey = "emcom:sp9moa-emcom"

// emcomResyncEnv builds a test station with the REAL store, a network at
// the given level and the alarm document already published.
func emcomResyncEnv(t *testing.T, level int) (*testEnv, string) {
	t.Helper()
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "emcom.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	env := newTestEnvWithUsers(t, store)
	env.login()
	csrf := env.csrfFromPage("/emcom")

	resp, _ := env.postForm("/config/emcom/add", url.Values{"csrf": {csrf}, "name": {"SP9MOA EMCOM"}})
	if resp.StatusCode != 303 {
		t.Fatalf("POST /config/emcom/add = %d, want 303", resp.StatusCode)
	}
	if level >= 1 {
		resp, _ = env.postForm("/emcom/sp9moa-emcom/level", url.Values{
			"csrf": {csrf}, "level": {string(rune('0' + level))},
		})
		if resp.StatusCode != 303 {
			t.Fatalf("POST level %d = %d, want 303", level, resp.StatusCode)
		}
		// The level change publishes asynchronously (the alarm document
		// and the retained state document): wait for BOTH before handing
		// the station to the test, so nothing from the setup is still in
		// flight while the test drives the resync (reported P2: six flaky
		// tests that asserted the outcome right after the redirect).
		env.pub.awaitActiveDoc(t, "emcom", emcomKey)
		env.pub.awaitRawFor(t, "sp9moa-emcom")
	}
	return env, csrf
}

// waitForEmcomRetireAttempt waits until one removal of eventKey was
// attempted (and, with a failed broker, refused). The removal runs in a
// goroutine, so the wait is on the publisher's own state change.
func waitForEmcomRetireAttempt(t *testing.T, env *testEnv, eventKey string) {
	t.Helper()
	env.pub.awaitExpireAttempt(t, eventKey)
}

// TestEmcomStandDownWhileBrokerIsDownIsRepairedOnResync pins the reported
// P1: a network stood down while the broker was unavailable kept its
// alarm document published, so subscribers went on seeing an active
// alarm although the local level was 0. The resync (a receiver
// reconnect) must remove the document it can no longer justify.
func TestEmcomStandDownWhileBrokerIsDownIsRepairedOnResync(t *testing.T) {
	env, csrf := emcomResyncEnv(t, 2)
	const eventKey = emcomKey

	// The broker goes down and the operator stands the network down: the
	// local record says monitoring, the broker still holds the alarm.
	env.pub.setFail(true)
	resp, _ := env.postForm("/emcom/sp9moa-emcom/level", url.Values{"csrf": {csrf}, "level": {"0"}})
	if resp.StatusCode != 303 {
		t.Fatalf("POST level 0 = %d, want 303 (the local-first path records it)", resp.StatusCode)
	}
	waitForEmcomRetireAttempt(t, env, eventKey)
	if ok, _ := env.pub.expireAttempts(eventKey); ok != 0 {
		t.Fatalf("the broker was down yet the alarm was retired %d times", ok)
	}

	// The broker comes back and the receiver reconnects.
	env.pub.setFail(false)
	env.server.SyncBrokerState()

	if ok, _ := env.pub.expireAttempts(eventKey); ok == 0 {
		t.Fatal("the resync left the alarm document published after the stand-down")
	}
	// The state document carries the monitoring level: the broker's copy
	// agrees with the station again. The publish is asynchronous, so wait
	// for the document that carries the level.
	if doc := env.pub.awaitInfoDoc(t, "info/emcom/emcom/sp9moa-emcom/emcom", `"level":0`); doc == "" {
		t.Fatal("the resync published no state document with level 0")
	}
	env.pub.awaitRawFor(t, "sp9moa-emcom")
	raw := env.pub.rawSnapshotFor("sp9moa-emcom")
	if len(raw) == 0 {
		t.Fatal("the resync published no state document")
	}
	var wire struct {
		Level int `json:"level"`
	}
	if err := json.Unmarshal(raw[len(raw)-1].Payload, &wire); err != nil || wire.Level != 0 {
		t.Fatalf("resynced state = %s (%v), want level 0", raw[len(raw)-1].Payload, err)
	}
}

// TestEmcomDeletedNetworkAlarmIsRemovedOnResync pins the second half of
// the report: the same gap applies to networks that were DELETED — the
// tombstone removed the state document but left the alarm behind.
func TestEmcomDeletedNetworkAlarmIsRemovedOnResync(t *testing.T) {
	env, csrf := emcomResyncEnv(t, 2)
	const eventKey = emcomKey

	env.pub.setFail(true)
	resp, _ := env.postForm("/config/emcom/sp9moa-emcom/delete", url.Values{"csrf": {csrf}})
	if resp.StatusCode != 303 {
		t.Fatalf("POST delete = %d, want 303", resp.StatusCode)
	}
	waitForEmcomRetireAttempt(t, env, eventKey)

	env.pub.setFail(false)
	env.server.SyncBrokerState()

	if ok, _ := env.pub.expireAttempts(eventKey); ok == 0 {
		t.Fatal("the resync left the alarm document of a deleted network published")
	}
	raw := env.pub.rawSnapshotFor("sp9moa-emcom")
	if len(raw) == 0 || raw[len(raw)-1].Payload != nil {
		t.Fatalf("the resync did not tombstone the state document: %+v", raw)
	}
	if doc, ok := env.pub.infoDoc("info/emcom/emcom/sp9moa-emcom/emcom"); ok {
		t.Fatalf("the resync left the state document of a deleted network on the broker: %q", doc)
	}
}

// TestEmcomFailedRetireIsRetriedOnTheNextResync pins the retry promise: a
// removal the broker refuses stays pending, is attempted again by the
// following resync, and is dropped only once it lands.
func TestEmcomFailedRetireIsRetriedOnTheNextResync(t *testing.T) {
	env, csrf := emcomResyncEnv(t, 1)
	const eventKey = emcomKey

	env.pub.setFail(true)
	resp, _ := env.postForm("/emcom/sp9moa-emcom/level", url.Values{"csrf": {csrf}, "level": {"0"}})
	if resp.StatusCode != 303 {
		t.Fatalf("POST level 0 = %d, want 303", resp.StatusCode)
	}
	waitForEmcomRetireAttempt(t, env, eventKey)
	_, failedBefore := env.pub.expireAttempts(eventKey)

	// First resync: the broker is STILL down, so the removal is refused
	// again — and must be kept for another attempt.
	env.server.SyncBrokerState()
	_, failedAfter := env.pub.expireAttempts(eventKey)
	if failedAfter <= failedBefore {
		t.Fatalf("the resync did not retry the refused removal (%d -> %d)", failedBefore, failedAfter)
	}

	// Second resync with the broker back: the removal finally lands.
	env.pub.setFail(false)
	env.server.SyncBrokerState()
	if ok, _ := env.pub.expireAttempts(eventKey); ok == 0 {
		t.Fatal("the pending removal was never applied")
	}

	// A network raised again supersedes the removal: no alarm document is
	// taken down for a live network.
	env.pub.setFail(false)
	resp, _ = env.postForm("/emcom/sp9moa-emcom/level", url.Values{"csrf": {csrf}, "level": {"3"}})
	if resp.StatusCode != 303 {
		t.Fatalf("POST level 3 = %d, want 303", resp.StatusCode)
	}
	okBefore, _ := env.pub.expireAttempts(eventKey)
	env.server.SyncBrokerState()
	if okAfter, _ := env.pub.expireAttempts(eventKey); okAfter != okBefore {
		t.Fatalf("the resync retired the alarm of a network at level 3 (%d -> %d)", okBefore, okAfter)
	}
	pub := env.pub.publishedSnapshot()
	if last := pub[len(pub)-1]; last.EventKey != eventKey || last.Status != "active" {
		t.Fatalf("the resync did not republish the live alarm: %+v", last)
	}
}

// TestEmcomResyncKeepsRaisedNetworksActive is the guard rail around the
// removals: everything the station still holds above monitoring must stay
// published (and be republished after an outage) — a resync must never
// take down a live alarm.
func TestEmcomResyncKeepsRaisedNetworksActive(t *testing.T) {
	env, _ := emcomResyncEnv(t, 2)
	const eventKey = emcomKey

	env.pub.setFail(true)
	env.server.SyncBrokerState() // outage: nothing lands
	env.pub.setFail(false)

	before := len(env.pub.publishedSnapshot())
	env.server.SyncBrokerState()

	pub := env.pub.publishedSnapshot()
	if len(pub) != before+1 || pub[len(pub)-1].EventKey != eventKey ||
		pub[len(pub)-1].Status != "active" {
		t.Fatalf("the resync did not republish the raised network: %+v", pub)
	}
	if ok, failed := env.pub.expireAttempts(eventKey); ok != 0 || failed != 0 {
		t.Fatalf("the resync retired the alarm of a raised network (ok=%d failed=%d)", ok, failed)
	}
}

// TestEmcomResyncKeepsARaisedAlarmWhenItsStatePublishFails pins the
// reported P1: with a removal still owed (an earlier stand-down the
// broker refused), the resync used to classify a network only WHILE
// publishing its state document — a failing info publish therefore left
// it looking retired, and the replay of the stale removal carried version
// 0, which the version guard never rejects. The alarm the operator had
// just raised again was taken down. The local record decides, before any
// MQTT operation, and a removal that a newer record superseded is
// forgotten instead of replayed.
func TestEmcomResyncKeepsARaisedAlarmWhenItsStatePublishFails(t *testing.T) {
	env, csrf := emcomResyncEnv(t, 2)
	const eventKey = emcomKey

	// The broker is down: the stand-down cannot remove the alarm, so the
	// removal stays pending.
	env.pub.setFail(true)
	resp, _ := env.postForm("/emcom/sp9moa-emcom/level", url.Values{"csrf": {csrf}, "level": {"0"}})
	if resp.StatusCode != 303 {
		t.Fatalf("POST level 0 = %d, want 303", resp.StatusCode)
	}
	waitForEmcomRetireAttempt(t, env, eventKey)
	attempts0, failed0 := env.pub.expireAttempts(eventKey)

	// The operator raises the network again (the alarm went out before).
	env.pub.setFail(false)
	resp, _ = env.postForm("/emcom/sp9moa-emcom/level", url.Values{"csrf": {csrf}, "level": {"2"}})
	if resp.StatusCode != 303 {
		t.Fatalf("POST level 2 = %d, want 303", resp.StatusCode)
	}

	// The resync cannot publish the state document (only that fails), so
	// the removal must not be replayed over the live alarm.
	env.pub.setFailRaw(true)
	env.server.SyncBrokerState()

	if ok, failed := env.pub.expireAttempts(eventKey); ok != attempts0 || failed != failed0 {
		t.Fatalf("the resync retired the alarm of a raised network (ok=%d failed=%d, want %d/%d)",
			ok, failed, attempts0, failed0)
	}
	if _, ok := env.pub.activeDoc("emcom", eventKey); !ok {
		t.Fatal("the resync dropped the alarm document of an active network")
	}

	// The obsolete removal is forgotten: the recovery resync publishes
	// the state again and still keeps the alarm.
	env.pub.setFailRaw(false)
	env.server.SyncBrokerState()

	if ok, failed := env.pub.expireAttempts(eventKey); ok != attempts0 || failed != failed0 {
		t.Fatalf("a superseded removal was replayed after the recovery (ok=%d failed=%d, want %d/%d)",
			ok, failed, attempts0, failed0)
	}
	if _, ok := env.pub.activeDoc("emcom", eventKey); !ok {
		t.Fatal("the recovered resync dropped the alarm document of an active network")
	}
	if doc, ok := env.pub.infoDoc("info/emcom/emcom/sp9moa-emcom/emcom"); !ok || !strings.Contains(doc, `"level":2`) {
		t.Fatalf("state document on the broker = %q, %v; want level 2", doc, ok)
	}
}
