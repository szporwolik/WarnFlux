package rso

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// fakeEmitter implements every capability the rso source may use.
type fakeEmitter struct {
	mu       sync.Mutex
	emitted  []core.HazardEvent
	active   []core.HazardEvent
	healthy  int
	degraded int
}

func (f *fakeEmitter) Emit(_ context.Context, ev core.HazardEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.emitted = append(f.emitted, ev)
	return nil
}

func (f *fakeEmitter) EmitInformation(context.Context, core.InformationMessage) error { return nil }

func (f *fakeEmitter) ListSourceActiveEvents(_ context.Context, source string) ([]core.HazardEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []core.HazardEvent
	for _, ev := range f.active {
		if ev.Source == source {
			out = append(out, ev)
		}
	}
	return out, nil
}

func (f *fakeEmitter) ReportSourceHealthy() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.healthy++
}

func (f *fakeEmitter) ReportSourceDegraded(error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.degraded++
}

func (f *fakeEmitter) cancelledKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, ev := range f.emitted {
		if ev.Status == core.StatusCancelled {
			out = append(out, ev.Key())
		}
	}
	return out
}

func (f *fakeEmitter) activeKeys() map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]bool{}
	for _, ev := range f.emitted {
		if ev.Status == core.StatusActive {
			out[ev.Key()] = true
		}
	}
	return out
}

func (f *fakeEmitter) health() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.healthy, f.degraded
}

// noReaderEmitter implements plugin.Emitter but deliberately lacks
// SourceActiveEventReader.
type noReaderEmitter struct{}

func (f *noReaderEmitter) Emit(context.Context, core.HazardEvent) error { return nil }
func (f *noReaderEmitter) EmitInformation(context.Context, core.InformationMessage) error {
	return nil
}

func testSource(t *testing.T, baseURL string, voivodeships ...string) *Source {
	t.Helper()
	return &Source{
		cfg: Config{
			PollInterval:   50 * time.Millisecond,
			RequestTimeout: 2 * time.Second,
			BaseURL:        baseURL,
			Voivodeships:   voivodeships,
		},
		client: NewClient(baseURL, 2*time.Second),
	}
}

func rsoEvent(id string, expires *time.Time) core.HazardEvent {
	ev := core.HazardEvent{
		Source:    sourceRSO,
		SourceID:  id,
		Event:     "Komunikat",
		Severity:  "unknown",
		Headline:  "Komunikat",
		ExpiresAt: expires,
		Status:    core.StatusActive,
	}
	ev.Normalize()
	return ev
}

// rsoServer serves regional feeds from a map slug→XML body.
func rsoServer(t *testing.T, feeds map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
		if len(parts) < 2 || parts[0] != "komunikatyxml" {
			http.NotFound(w, r)
			return
		}
		body, ok := feeds[parts[1]]
		if !ok {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, body)
	}))
}

// newsXML builds one <news> element.
func newsXML(id, title, provincesXML string) string {
	return `<news>` +
		`<id>` + id + `</id><title>` + title + `</title>` +
		`<shortcut>s</shortcut><content>c</content>` +
		`<valid_from>2026-09-22 10:00:00</valid_from><valid_to>2026-09-23 10:00:00</valid_to>` +
		`<provinces>` + provincesXML + `</provinces></news>`
}

// newsesXML builds a full page-0 document with consistent pagination info.
func newsesXML(items ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `<newses><pagination_info totalItems="%d" itemsPerPage="20"></pagination_info>`, len(items))
	for _, it := range items {
		b.WriteString(it)
	}
	b.WriteString(`</newses>`)
	return b.String()
}

func provinceXML(slug string) string {
	return `<province id="1" slug="` + slug + `">x</province>`
}

// TestConfigDefaultsAndValidation covers defaults, canonicalization,
// exclusivity, duplicates and bounds.
func TestConfigDefaultsAndValidation(t *testing.T) {
	p, err := New(nil)
	if err != nil {
		t.Fatalf("default config: %v", err)
	}
	s := p.(*Source)
	if s.cfg.PollInterval != defaultPollInterval || s.cfg.RequestTimeout != defaultRequestTimeout {
		t.Errorf("defaults = %s/%s", s.cfg.PollInterval, s.cfg.RequestTimeout)
	}
	if len(s.cfg.Voivodeships) != 1 || s.cfg.Voivodeships[0] != "wszystkie" {
		t.Errorf("default voivodeships = %v, want [wszystkie]", s.cfg.Voivodeships)
	}
	if s.cfg.BaseURL != defaultBaseURL {
		t.Errorf("default base url = %q", s.cfg.BaseURL)
	}

	// Display names canonicalize to official slugs.
	node, err := decodeNode(`voivodeships: [małopolskie, śląskie]`)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := New(node)
	if err != nil {
		t.Fatalf("display names: %v", err)
	}
	if got := s2.(*Source).cfg.Voivodeships; len(got) != 2 || got[0] != "malopolskie" || got[1] != "slaskie" {
		t.Errorf("canonical voivodeships = %v", got)
	}

	cases := []string{
		"voivodeships: [bogus]\n",
		"voivodeships: [malopolskie, malopolskie]\n",
		"voivodeships: [wszystkie, malopolskie]\n",
		"poll_interval: 30s\n",
		"poll_interval: 2h\n",
		"request_timeout: -5s\n",
		"request_timeout: 2m\n",
		"base_url: not-a-url\n",
	}
	for _, c := range cases {
		node, err := decodeNode(c)
		if err != nil {
			t.Fatalf("yaml %q: %v", c, err)
		}
		if _, err := New(node); err == nil {
			t.Errorf("config %q must be rejected", strings.TrimSpace(c))
		}
	}
}

func decodeNode(y string) (*yaml.Node, error) {
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(y), &node); err != nil {
		return nil, err
	}
	return &node, nil
}

// TestMultiRegionMerge: A/B from malopolskie and B/C from slaskie merge
// into A, B, C with B emitted once and its areas unioned.
func TestMultiRegionMerge(t *testing.T) {
	em := &fakeEmitter{}
	srv := rsoServer(t, map[string]string{
		"malopolskie": newsesXML(
			newsXML("A", "Alert A", provinceXML("malopolskie")),
			newsXML("B", "Alert B", provinceXML("malopolskie")),
		),
		"slaskie": newsesXML(
			newsXML("B", "Alert B", provinceXML("slaskie")),
			newsXML("C", "Alert C", provinceXML("slaskie")),
		),
	})
	defer srv.Close()

	s := testSource(t, srv.URL, "malopolskie", "slaskie")
	s.pollOnce(context.Background(), em, em)

	keys := em.activeKeys()
	for _, want := range []string{sourceRSO + ":A", sourceRSO + ":B", sourceRSO + ":C"} {
		if !keys[want] {
			t.Errorf("missing merged identity %s (keys %v)", want, keys)
		}
	}
	em.mu.Lock()
	var bAreas []string
	bCount := 0
	for _, ev := range em.emitted {
		if ev.Status == core.StatusActive && ev.SourceID == "B" {
			bCount++
			bAreas = ev.Areas
		}
	}
	em.mu.Unlock()
	if bCount != 1 {
		t.Errorf("B emitted %d times, want exactly once", bCount)
	}
	if len(bAreas) != 2 || bAreas[0] != "wojewodztwo:malopolskie" || bAreas[1] != "wojewodztwo:slaskie" {
		t.Errorf("B areas = %v, want the merged sorted union", bAreas)
	}
	if h, d := em.health(); h != 1 || d != 0 {
		t.Errorf("health = (%d, %d), want healthy", h, d)
	}
}

// TestPartialFailure: a failing regional feed never looks like a mass
// withdrawal; valid items from successful regions may still be emitted.
func TestPartialFailure(t *testing.T) {
	future := time.Now().Add(time.Hour)
	em := &fakeEmitter{active: []core.HazardEvent{rsoEvent("A", &future), rsoEvent("B", &future)}}
	srv := rsoServer(t, map[string]string{
		"malopolskie": `<newses><pagination_info totalItems="1" itemsPerPage="20"></pagination_info><news>` +
			`<id>C</id><title>Nowy komunikat</title><shortcut>s</shortcut><content>c</content>` +
			`<valid_from>2026-09-22 10:00:00</valid_from><valid_to>2026-09-23 10:00:00</valid_to></news></newses>`,
		// slaskie is simply missing from the map → HTTP 500
	})
	defer srv.Close()

	s := testSource(t, srv.URL, "malopolskie", "slaskie")
	s.pollOnce(context.Background(), em, em)
	keys := em.activeKeys()
	if !keys[sourceRSO+":C"] {
		t.Error("valid malopolskie item was not emitted")
	}
	if got := em.cancelledKeys(); len(got) != 0 {
		t.Fatalf("partial regional failure cancelled warnings: %v", got)
	}
	if h, d := em.health(); h != 0 || d != 1 {
		t.Errorf("health = (%d, %d), want degraded", h, d)
	}
}

// TestConflictingDuplicateNotEmitted: the same provider ID with different
// content in two regional feeds is ambiguous — never emitted, no
// reconciliation.
func TestConflictingDuplicateNotEmitted(t *testing.T) {
	future := time.Now().Add(time.Hour)
	em := &fakeEmitter{active: []core.HazardEvent{rsoEvent("B", &future)}}
	srv := rsoServer(t, map[string]string{
		"malopolskie": newsesXML(newsXML("B", "Alert X", provinceXML("malopolskie"))),
		"slaskie":     newsesXML(newsXML("B", "Completely Different Alert", provinceXML("slaskie"))),
	})
	defer srv.Close()

	s := testSource(t, srv.URL, "malopolskie", "slaskie")
	s.pollOnce(context.Background(), em, em)
	if em.activeKeys()[sourceRSO+":B"] {
		t.Fatal("conflicting duplicate was arbitrarily emitted")
	}
	if got := em.cancelledKeys(); len(got) != 0 {
		t.Fatalf("conflicting snapshot cancelled warnings: %v", got)
	}
	if h, d := em.health(); h != 0 || d != 1 {
		t.Errorf("health = (%d, %d), want degraded", h, d)
	}
}

// TestFailedFetchDoesNotCancel: any provider failure must never look like
// a mass cancellation (malformed XML, wrong root, HTTP 500).
func TestFailedFetchDoesNotCancel(t *testing.T) {
	future := time.Now().Add(time.Hour)
	bodies := []string{
		`<html><body>error</body></html>`,
		`<newses><news>`,
		`<provinces></provinces>`,
	}
	for i, body := range bodies {
		em := &fakeEmitter{active: []core.HazardEvent{rsoEvent("A", &future), rsoEvent("B", &future), rsoEvent("C", &future)}}
		srv := rsoServer(t, map[string]string{"malopolskie": body})
		s := testSource(t, srv.URL, "malopolskie")
		s.pollOnce(context.Background(), em, em)
		srv.Close()
		if got := em.cancelledKeys(); len(got) != 0 {
			t.Fatalf("body %d caused cancellations: %v", i, got)
		}
		if h, d := em.health(); h != 0 || d != 1 {
			t.Errorf("body %d health = (%d, %d), want degraded", i, h, d)
		}
	}
}

// TestEmptySnapshotCancels: a structurally valid empty regional snapshot
// withdraws non-expired active communications; expired ones stay with the
// expiration worker.
func TestEmptySnapshotCancels(t *testing.T) {
	future := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Hour)
	em := &fakeEmitter{active: []core.HazardEvent{
		rsoEvent("A", &future),
		rsoEvent("B", &future),
		rsoEvent("E", &past),
	}}
	srv := rsoServer(t, map[string]string{
		"malopolskie": `<newses><pagination_info totalItems="0" itemsPerPage="20"></pagination_info></newses>`,
	})
	defer srv.Close()

	s := testSource(t, srv.URL, "malopolskie")
	s.pollOnce(context.Background(), em, em)
	got := em.cancelledKeys()
	if len(got) != 2 {
		t.Fatalf("cancelled keys = %v, want exactly A and B", got)
	}
	seen := map[string]bool{got[0]: true, got[1]: true}
	if !seen[sourceRSO+":A"] || !seen[sourceRSO+":B"] {
		t.Errorf("cancelled keys = %v, want A and B", got)
	}
	if seen[sourceRSO+":E"] {
		t.Error("expired event was cancelled by reconciliation")
	}
	if h, d := em.health(); h != 1 || d != 0 {
		t.Errorf("health = (%d, %d), want healthy for a valid empty snapshot", h, d)
	}
}

// TestProcessRestartReconciliation: a fresh plugin process with no local
// memory cancels an active RSO communication that disappeared from a
// complete snapshot (authoritative state comes from SQLite).
func TestProcessRestartReconciliation(t *testing.T) {
	future := time.Now().Add(time.Hour)
	em := &fakeEmitter{active: []core.HazardEvent{rsoEvent("Gone", &future)}}
	srv := rsoServer(t, map[string]string{
		"wszystkie": `<newses><pagination_info totalItems="0" itemsPerPage="20"></pagination_info></newses>`,
	})
	defer srv.Close()

	s := testSource(t, srv.URL, "wszystkie")
	s.pollOnce(context.Background(), em, em)
	if got := em.cancelledKeys(); len(got) != 1 || got[0] != sourceRSO+":Gone" {
		t.Errorf("cancelled keys = %v, want [rso:Gone]", got)
	}
}

// TestPaginationMismatchIsIncomplete: items are still emitted but the
// snapshot is incomplete (no reconciliation).
func TestPaginationMismatchIsIncomplete(t *testing.T) {
	future := time.Now().Add(time.Hour)
	em := &fakeEmitter{active: []core.HazardEvent{rsoEvent("Ghost", &future)}}
	srv := rsoServer(t, map[string]string{
		"malopolskie": `<newses><pagination_info totalItems="5" itemsPerPage="20"></pagination_info>` +
			newsXML("A", "Alert A", provinceXML("malopolskie")) + `</newses>`,
	})
	defer srv.Close()

	s := testSource(t, srv.URL, "malopolskie")
	s.pollOnce(context.Background(), em, em)
	if !em.activeKeys()[sourceRSO+":A"] {
		t.Error("item A was not emitted despite the count mismatch")
	}
	if got := em.cancelledKeys(); len(got) != 0 {
		t.Fatalf("count-mismatch snapshot cancelled warnings: %v", got)
	}
	if h, d := em.health(); h != 0 || d != 1 {
		t.Errorf("health = (%d, %d), want degraded", h, d)
	}
}

// TestRunImmediatePollAndCancellation + capability requirement.
func TestRunRequiresReaderCapability(t *testing.T) {
	s := testSource(t, "http://127.0.0.1:1", "malopolskie")
	if err := s.Run(context.Background(), &noReaderEmitter{}); err == nil {
		t.Fatal("Run must fail when the emitter lacks the active-event reader")
	}
}

func TestRunImmediatePollAndCancellation(t *testing.T) {
	em := &fakeEmitter{}
	srv := rsoServer(t, map[string]string{
		"malopolskie": `<newses><pagination_info totalItems="1" itemsPerPage="20"></pagination_info><news>` +
			`<id>A</id><title>Komunikat</title><shortcut>s</shortcut><content>c</content>` +
			`<valid_from>2026-09-22 10:00:00</valid_from><valid_to>2026-09-23 10:00:00</valid_to></news></newses>`,
	})
	defer srv.Close()

	s := testSource(t, srv.URL, "malopolskie")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := s.Run(ctx, em); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	waitFor(t, 3*time.Second, func() bool { return len(em.activeKeys()) > 0 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}
