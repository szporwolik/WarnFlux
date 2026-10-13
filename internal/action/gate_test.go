package action

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
	_ "modernc.org/sqlite"
)

// fakeGateStore is the gate's storage seam, scripted per test.
type fakeGateStore struct {
	blocked    bool
	superseded bool
	verdict    storage.HazardVerdict
	err        error
	// asked records which lookups the gate performed.
	asked []string
}

func (f *fakeGateStore) LifecycleBlocks(context.Context, string, string, int64) (bool, error) {
	f.asked = append(f.asked, "blocks")
	return f.blocked, f.err
}

func (f *fakeGateStore) LifecycleSuperseded(context.Context, string, string, int64) (bool, error) {
	f.asked = append(f.asked, "superseded")
	return f.superseded, f.err
}

func (f *fakeGateStore) HazardActive(context.Context, string, string, time.Time) (storage.HazardVerdict, error) {
	f.asked = append(f.asked, "active")
	return f.verdict, f.err
}

func notifyRequest() ActionRequest {
	return ActionRequest{ID: "emcom:net/log", Event: dispatch.Event{
		Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{
			Key: "emcom:net", Source: "emcom", Publisher: "pub-1", ChangeID: 9,
			Type: dispatch.TransitionExpired, Notify: true,
			Hazard: dispatch.Hazard{EventKey: "emcom:net", Severity: "severe"},
		},
	}}
}

func hazardRequest() ActionRequest {
	return ActionRequest{ID: "imgw:1/log", Event: dispatch.Event{
		Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{
			Key: "imgw:1", Source: "imgw", Publisher: "pub-1", ChangeID: 3,
			Hazard: dispatch.Hazard{EventKey: "imgw:1"},
		},
	}}
}

// TestLifecycleGateStopsSupersededNotice pins the reported P1 on the
// gate itself: a queued stand-down notice must not go out once a NEWER
// version of the same message is recorded (the network was raised
// again), while the terminal status of the notice's own version — the
// whole reason it is being sent — must not hold it back.
func TestLifecycleGateStopsSupersededNotice(t *testing.T) {
	store := &fakeGateStore{superseded: true, verdict: storage.HazardInactive}
	gate := NewLifecycleGate(store, nil)

	if gate(context.Background(), notifyRequest()) {
		t.Fatal("a stand-down notice superseded by a newer version was allowed through")
	}
	for _, want := range []string{"superseded"} {
		found := false
		for _, got := range store.asked {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Errorf("the gate never asked for %q (asked %v)", want, store.asked)
		}
	}

	// The notice is the current version and terminal: it must go out,
	// even though the hazard itself is no longer active.
	fresh := &fakeGateStore{verdict: storage.HazardInactive}
	if !NewLifecycleGate(fresh, nil)(context.Background(), notifyRequest()) {
		t.Fatal("the current stand-down notice was suppressed")
	}
	for _, got := range fresh.asked {
		if got == "active" {
			t.Fatal("a Notify job must not be judged by the hazard's active state")
		}
	}
}

// TestLifecycleGateRules pins the alert path unchanged: superseded
// versions and terminal states are held back, an inactive hazard is held
// back, and every read failure fails open.
func TestLifecycleGateRules(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store *fakeGateStore
		want  bool
	}{
		{"active hazard passes", &fakeGateStore{verdict: storage.HazardActive}, true},
		{"inactive hazard is held back", &fakeGateStore{verdict: storage.HazardInactive}, false},
		{"unknown hazard passes", &fakeGateStore{verdict: storage.HazardUnknown}, true},
		{"superseded version is held back", &fakeGateStore{blocked: true}, false},
		{"read failure fails open", &fakeGateStore{err: errors.New("db down")}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := NewLifecycleGate(tc.store, nil)(context.Background(), hazardRequest()); got != tc.want {
				t.Fatalf("gate = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestLifecycleGateAgainstTheStore is the reported scenario end to end
// against the real ledger: a cancellation queued at version 9, then the
// network raised again at version 10. The queued notice is held back,
// and the very same notice sent while it is still the current version
// goes out.
func TestLifecycleGateAgainstTheStore(t *testing.T) {
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "gate.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	gate := NewLifecycleGate(store, nil)

	// Version 9: the network stands down. This is the notice in the queue.
	if err := store.RecordLifecycle(ctx, "pub-1", "emcom:net", 9, "cancelled"); err != nil {
		t.Fatal(err)
	}
	if !gate(ctx, notifyRequest()) {
		t.Fatal("the current stand-down notice (version 9) was suppressed")
	}

	// Version 10: the operator raises the network again. The queued
	// version 9 notice must now be held back.
	if err := store.RecordLifecycle(ctx, "pub-1", "emcom:net", 10, "active"); err != nil {
		t.Fatal(err)
	}
	if gate(ctx, notifyRequest()) {
		t.Fatal("the superseded stand-down notice reached the channels although version 10 is active")
	}

	// A notice that belongs to the newer version still goes out.
	next := notifyRequest()
	next.Event.Hazard.ChangeID = 11
	if err := store.RecordLifecycle(ctx, "pub-1", "emcom:net", 11, "cancelled"); err != nil {
		t.Fatal(err)
	}
	if !gate(ctx, next) {
		t.Fatal("the current stand-down notice (version 11) was suppressed")
	}
}

// saveComposeTransition records one panel-issued communication through
// the real compose path and returns the stamped transition the panel
// would hand to the live queue.
func saveComposeTransition(t *testing.T, store *sqlite.Store, headline string) dispatch.Event {
	t.Helper()
	ev := dispatch.Event{
		Kind:   dispatch.EventHazardTransition,
		Origin: dispatch.Origin{Type: "web", ReceiverID: "compose"},
		Hazard: &dispatch.HazardTransition{
			Type:   dispatch.TransitionNew,
			Key:    "compose:net",
			Source: "compose",
			Hazard: dispatch.Hazard{EventKey: "compose:net", Severity: "severe", Headline: headline},
		},
	}
	_, stamped, err := store.SaveComposeHazardAndEnqueue(context.Background(), storage.ComposeHazard{
		EventKey: "compose:net", State: []byte(`{"headline":"` + headline + `"}`),
		Status: "active", UpdatedAt: time.Now(),
	}, ev)
	if err != nil {
		t.Fatalf("SaveComposeHazardAndEnqueue: %v", err)
	}
	return stamped
}

// TestComposeCorrectionStopsTheReplacedText is the reported P1 on the
// panel path, end to end: "ewakuacja na północ" is saved, then corrected
// to "ewakuacja na południe" while the first text is still queued. The
// correction goes out; the text it replaced is held back before
// transmission.
func TestComposeCorrectionStopsTheReplacedText(t *testing.T) {
	store, _, err := sqlite.Open(filepath.Join(t.TempDir(), "compose-gate.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	gate := NewLifecycleGate(store, nil)

	north := saveComposeTransition(t, store, "Ewakuacja na północ")
	south := saveComposeTransition(t, store, "Ewakuacja na południe")

	// The panel must be able to tell the two apart: the version is
	// allocated per save and recorded under the publisher that owns it.
	if north.Hazard.Publisher == "" || north.Hazard.ChangeID == 0 {
		t.Fatalf("first save carries no version identity: %+v", north.Hazard)
	}
	if south.Hazard.ChangeID <= north.Hazard.ChangeID {
		t.Fatalf("correction version %d is not newer than %d", south.Hazard.ChangeID, north.Hazard.ChangeID)
	}

	if !gate(ctx, ActionRequest{ID: "compose:net/log", Event: south}) {
		t.Fatal("the correction was held back")
	}
	if gate(ctx, ActionRequest{ID: "compose:net/log", Event: north}) {
		t.Fatal("the replaced text still passed the gate and would be transmitted")
	}

	// Expiring the corrected message keeps blocking that same text.
	expire := dispatch.Event{
		Kind:   dispatch.EventHazardTransition,
		Origin: dispatch.Origin{Type: "web", ReceiverID: "compose"},
		Hazard: &dispatch.HazardTransition{
			Type:   dispatch.TransitionExpired,
			Key:    south.Hazard.Key,
			Source: "compose",
			Hazard: dispatch.Hazard{EventKey: south.Hazard.Key, Severity: "severe", Headline: "Ewakuacja na południe"},
		},
	}
	_, expired, err := store.SaveComposeHazardAndEnqueue(ctx, storage.ComposeHazard{
		EventKey: south.Hazard.Key, State: []byte(`{}`), Status: "expired", UpdatedAt: time.Now(),
	}, expire)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if gate(ctx, ActionRequest{ID: "compose:net/log", Event: expired}) {
		t.Fatal("an expired message still passed the gate")
	}
	if gate(ctx, ActionRequest{ID: "compose:net/log", Event: south}) {
		t.Fatal("the expired message's earlier text passed the gate")
	}
}

// legacyComposeStore builds a database in the state an OLD station leaves
// behind: a panel communication whose lifecycle row has no version-aware
// owner (the empty publisher), and the schema rolled back so the adoption
// migration runs on the next open — the real upgrade path. counter is
// the version counter the upgrade continues from (it was seeded ABOVE
// the legacy wall-clock versions when the counter was introduced).
func legacyComposeStore(t *testing.T, key string, version, counter int64, status string) *sqlite.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "legacy-compose.db")
	created, _, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// The database has an instance identity long before the ledger gains
	// an owner: every ingest stamps it (v24).
	if _, err := created.InstanceID(context.Background()); err != nil {
		t.Fatalf("instance id: %v", err)
	}
	if err := created.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Write the pre-upgrade rows as the old binary left them, then roll
	// the schema version back so opening runs the migration again.
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.Exec(`INSERT INTO compose_hazards(event_key, hazard_json, status, updated_at_ms)
		VALUES(?, '{}', 'active', 0)`, key); err != nil {
		t.Fatalf("seed compose row: %v", err)
	}
	if _, err := raw.Exec(`INSERT INTO message_lifecycle(publisher, event_key, version, status, updated_at_ms)
		VALUES('', ?, ?, ?, 0)`, key, version, status); err != nil {
		t.Fatalf("seed legacy lifecycle: %v", err)
	}
	if _, err := raw.Exec(`UPDATE lifecycle_versions SET next = ?`, counter); err != nil {
		t.Fatalf("seed the version counter: %v", err)
	}
	// The version counter is a contract: the database must genuinely be
	// at the v58 SCHEMA, not merely carry the number, or the reopened
	// upgrade would re-run a step whose column is already there. v60 adds
	// the producer column of the events table, so it is dropped again here
	// (a migration that changes the schema must be mirrored here too).
	if _, err := raw.Exec(`ALTER TABLE events DROP COLUMN producer`); err != nil {
		t.Fatalf("drop the producer column: %v", err)
	}
	if _, err := raw.Exec(`PRAGMA user_version = 58`); err != nil {
		t.Fatalf("roll back the schema version: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}
	upgraded, info, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { upgraded.Close() })
	if info.From != 58 {
		t.Fatalf("schema upgrade started at %d, want 58", info.From)
	}
	return upgraded
}

// expireCompose records the retirement of one communication through the
// same path the panel uses.
func expireCompose(t *testing.T, store *sqlite.Store, key string) dispatch.Event {
	t.Helper()
	expire := dispatch.Event{
		Kind:   dispatch.EventHazardTransition,
		Origin: dispatch.Origin{Type: "web", ReceiverID: "compose"},
		Hazard: &dispatch.HazardTransition{
			// The payload of the job that was queued BEFORE the upgrade:
			// it carries no publisher identity.
			Type:   dispatch.TransitionExpired,
			Key:    key,
			Source: "compose",
			Hazard: dispatch.Hazard{EventKey: key, Severity: "severe"},
		},
	}
	_, stamped, err := store.SaveComposeHazardAndEnqueue(context.Background(), storage.ComposeHazard{
		EventKey: key, State: []byte(`{}`), Status: "expired", UpdatedAt: time.Now(),
	}, expire)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	return stamped
}

// TestLegacyComposeJobFollowsTheAdoptedLifecycle pins the reported P1 on
// the delivery gate with the real storage: a job queued before the ledger
// had an owner (publisher "") must follow the version the INSTANCE now
// records. Reading the old row let a communication that was retired in
// the meantime pass the gate and go out with its stale text.
func TestLegacyComposeJobFollowsTheAdoptedLifecycle(t *testing.T) {
	ctx := context.Background()
	const key = "compose:legacy"
	store := legacyComposeStore(t, key, 100, 100, "active")
	gate := NewLifecycleGate(store, nil)

	legacyJob := ActionRequest{ID: key + "/log", Event: dispatch.Event{
		Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{
			Type: dispatch.TransitionNew, Key: key, Source: "compose",
			Publisher: "", ChangeID: 100, // queued before the upgrade
			Hazard: dispatch.Hazard{EventKey: key, Severity: "severe", Headline: "Ewakuacja na północ"},
		},
	}}
	// While the communication is live the queued job may still go out.
	if !gate(ctx, legacyJob) {
		t.Fatal("an active communication was held back")
	}

	expireCompose(t, store, key)
	if gate(ctx, legacyJob) {
		t.Fatal("a retired communication passed the delivery gate: the legacy job read the old lifecycle row")
	}

	// A second, untouched communication keeps its legacy job deliverable:
	// the compatibility rule must not block what is still live.
	other := "compose:legacy-active"
	store2 := legacyComposeStore(t, other, 200, 200, "active")
	gate2 := NewLifecycleGate(store2, nil)
	live := ActionRequest{ID: other + "/log", Event: dispatch.Event{
		Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{
			Type: dispatch.TransitionNew, Key: other, Source: "compose",
			Publisher: "", ChangeID: 200,
			Hazard: dispatch.Hazard{EventKey: other, Severity: "severe"},
		},
	}}
	if !gate2(ctx, live) {
		t.Fatal("an untouched legacy communication was held back")
	}
}

// TestLegacyComposeJobIsJudgedByTheCurrentRecord covers the harder half of
// the same report: a database whose version counter does NOT continue the
// old numbering (the legacy row is newer than anything the ledger will
// allocate). The version comparison alone cannot hold such a job back, so
// the current state of the local panel record decides.
func TestLegacyComposeJobIsJudgedByTheCurrentRecord(t *testing.T) {
	ctx := context.Background()
	const key = "compose:legacy-domain"
	// Counter below the legacy version: the adoption keeps the old
	// version, and nothing the station records later can outrank it.
	store := legacyComposeStore(t, key, 1_700_000_000_000, 1, "active")
	gate := NewLifecycleGate(store, nil)

	legacyJob := ActionRequest{ID: key + "/log", Event: dispatch.Event{
		Kind: dispatch.EventHazardTransition,
		Hazard: &dispatch.HazardTransition{
			Type: dispatch.TransitionNew, Key: key, Source: "compose",
			Publisher: "", ChangeID: 1_700_000_000_000,
			Hazard: dispatch.Hazard{EventKey: key, Severity: "severe", Headline: "Ewakuacja na północ"},
		},
	}}
	if !gate(ctx, legacyJob) {
		t.Fatal("an active communication was held back")
	}
	expireCompose(t, store, key)

	// The ledger row cannot represent the retirement (its version is
	// higher than anything the counter allocates), so the local record is
	// what keeps the old job out.
	if blocked, err := store.LifecycleBlocks(ctx, "", key, 1_700_000_000_000); err != nil || !blocked {
		t.Fatalf("LifecycleBlocks(legacy) = (%v, %v), want (true, nil)", blocked, err)
	}
	if gate(ctx, legacyJob) {
		t.Fatal("a retired communication passed the delivery gate")
	}
}
