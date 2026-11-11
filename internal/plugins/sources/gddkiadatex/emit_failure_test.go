package gddkiadatex

import (
	"bytes"
	"context"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// fakeEmit is the smallest emitter a snapshot poll needs: it records what the
// source handed over and can refuse selected events, which is exactly the
// transient write failure that must not become a false all-clear.
type fakeEmit struct {
	mu        sync.Mutex
	refuse    func(core.HazardEvent) bool
	emitted   []core.HazardEvent
	active    []core.HazardEvent
	loadError error
}

func (f *fakeEmit) Emit(_ context.Context, ev core.HazardEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refuse != nil && f.refuse(ev) {
		return os.ErrDeadlineExceeded
	}
	f.emitted = append(f.emitted, ev)
	return nil
}

func (f *fakeEmit) EmitInformation(context.Context, core.InformationMessage) error { return nil }

func (f *fakeEmit) ListSourceActiveEvents(context.Context, string) ([]core.HazardEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loadError != nil {
		return nil, f.loadError
	}
	return append([]core.HazardEvent(nil), f.active...), nil
}

func (f *fakeEmit) cancellations() []core.HazardEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []core.HazardEvent
	for _, ev := range f.emitted {
		if ev.Status == core.StatusCancelled {
			out = append(out, ev)
		}
	}
	return out
}

// fixtureSource is a Source wired to the real parser and the recorded snapshot,
// with the area wide enough that the fixture lands inside it.
func fixtureSource(t *testing.T) *Source {
	t.Helper()
	body, err := os.ReadFile("testdata/snapshot.xml")
	if err != nil {
		t.Fatal(err)
	}
	return &Source{
		cfg:       Config{Endpoint: "https://kpd.example.test/snapshotPull"},
		centerLat: 50.03, centerLon: 20.2, radiusKM: 400,
		fetch: func(context.Context) ([]byte, error) { return body, nil },
	}
}

// firstSnapshotEvent is the identity the fixture's first in-area record gets.
func firstSnapshotEvent(t *testing.T, src *Source) core.HazardEvent {
	t.Helper()
	body, err := src.fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	doc, err := parseResponse(body)
	if err != nil {
		t.Fatal(err)
	}
	for _, sit := range doc.Payload.Situations {
		for _, rec := range sit.Records {
			ev, err := normalize(rec, src.cfg.Endpoint)
			if err != nil {
				continue
			}
			if ev.Key() != "" {
				return ev
			}
		}
	}
	t.Fatal("fixture has no usable record")
	return core.HazardEvent{}
}

// TestFailedUpdateDoesNotCancelALiveAlert pins WF-REV-01: the provider still
// lists the alert, only the local write failed. Reconciliation must never read
// that as "it disappeared" — a transient storage error would otherwise cancel a
// warning that is still in force, and the poll would even report success.
func TestFailedUpdateDoesNotCancelALiveAlert(t *testing.T) {
	src := fixtureSource(t)
	live := firstSnapshotEvent(t, src)
	live.Status = core.StatusActive

	emit := &fakeEmit{
		active: []core.HazardEvent{live},
		refuse: func(ev core.HazardEvent) bool {
			return ev.Key() == live.Key() && ev.Status == core.StatusActive
		},
	}

	err := src.pollOnce(context.Background(), emit)
	if err == nil {
		t.Errorf("pollOnce reported success although a record was not persisted")
	}
	if got := emit.cancellations(); len(got) != 0 {
		t.Errorf("failed update cancelled a live alert: %v", keysOf(got))
	}
}

// TestRealDisappearanceStillCancels is the other half of WF-REV-01: a record the
// provider really dropped from a healthy snapshot must still be cancelled.
func TestRealDisappearanceStillCancels(t *testing.T) {
	src := fixtureSource(t)
	gone := firstSnapshotEvent(t, src)
	gone.Status = core.StatusActive
	// Same source and shape, but an identity the snapshot does not contain.
	gone.SourceID = strings.ReplaceAll(gone.SourceID, "-", "") + "-GONE"

	emit := &fakeEmit{active: []core.HazardEvent{gone}}

	if err := src.pollOnce(context.Background(), emit); err != nil {
		t.Fatalf("pollOnce = %v", err)
	}
	if got := emit.cancellations(); len(got) != 1 || got[0].Key() != gone.Key() {
		t.Errorf("cancellations = %v, want exactly the disappeared event", keysOf(got))
	}
}

// TestReconcileFailureIsReported pins the second half of the ticket: a
// reconciliation that cannot read the station's own view is a failed poll, not
// a quiet success.
func TestReconcileFailureIsReported(t *testing.T) {
	src := fixtureSource(t)
	emit := &fakeEmit{loadError: os.ErrDeadlineExceeded}

	if err := src.pollOnce(context.Background(), emit); err == nil {
		t.Errorf("pollOnce hid a reconciliation failure")
	}
}

func keysOf(evs []core.HazardEvent) []string {
	out := make([]string, 0, len(evs))
	for _, ev := range evs {
		out = append(out, ev.Key())
	}
	return out
}

var _ plugin.Emitter = (*fakeEmit)(nil)

// TestMissingGeometryDoesNotWithdrawALiveAlert is WF-R3-03: the provider still
// lists the record as active, but the next snapshot carries no point geometry
// for it. That is a gap in OUR reading, not a withdrawal — the live alert must
// stay, and the poll must say the feed is degraded instead of reporting a clean
// run.
func TestMissingGeometryDoesNotWithdrawALiveAlert(t *testing.T) {
	src := fixtureSource(t)
	live := firstSnapshotEvent(t, src)
	live.Status = core.StatusActive

	// The next snapshot repeats the same records and statuses but without any
	// point geometry at all.
	body, err := src.fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	stripped := stripPointGeometry(t, body)
	src.fetch = func(context.Context) ([]byte, error) { return stripped, nil }

	emit := &fakeEmit{active: []core.HazardEvent{live}}
	err = src.pollOnce(context.Background(), emit)
	if err == nil {
		t.Errorf("pollOnce reported a clean poll although active records were unreadable")
	}
	if got := emit.cancellations(); len(got) != 0 {
		t.Errorf("missing geometry cancelled a live alert: %v", keysOf(got))
	}
}

// stripPointGeometry removes every pointByCoordinates element from a snapshot,
// leaving ids, statuses and everything else intact — the provider shape the
// review used. The fixture is a single-line document, so this works on the text.
func stripPointGeometry(t *testing.T, body []byte) []byte {
	t.Helper()
	re := regexp.MustCompile(`(?s)<(?:[A-Za-z0-9]+:)?pointByCoordinates\b[^>]*>.*?</(?:[A-Za-z0-9]+:)?pointByCoordinates>`)
	out := re.ReplaceAll(body, nil)
	if bytes.Equal(out, body) {
		t.Fatalf("the fixture carries no pointByCoordinates to strip")
	}
	return out
}
