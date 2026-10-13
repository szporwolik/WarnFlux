package burzedzis

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/burzedzis"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/severity"
)

// fakeEmitter implements every capability the source may use and records
// what it received.
type fakeEmitter struct {
	mu       sync.Mutex
	emitted  []core.HazardEvent
	active   []core.HazardEvent
	healthy  int
	degraded []error
	stats    []string
	filtered int
}

var _ plugin.Emitter = (*fakeEmitter)(nil)

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

func (f *fakeEmitter) ReportSourceDegraded(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.degraded = append(f.degraded, err)
}

func (f *fakeEmitter) ReportSourceStats(summary string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stats = append(f.stats, summary)
}

func (f *fakeEmitter) ReportSourceFiltered(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.filtered += n
}

// events returns what was emitted, optionally filtered by source.
func (f *fakeEmitter) events(source string) []core.HazardEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []core.HazardEvent
	for _, ev := range f.emitted {
		if source == "" || ev.Source == source {
			out = append(out, ev)
		}
	}
	return out
}

func (f *fakeEmitter) lastStats() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.stats) == 0 {
		return ""
	}
	return f.stats[len(f.stats)-1]
}

// noReaderEmitter deliberately lacks SourceActiveEventReader.
type noReaderEmitter struct{}

func (*noReaderEmitter) Emit(context.Context, core.HazardEvent) error { return nil }
func (*noReaderEmitter) EmitInformation(context.Context, core.InformationMessage) error {
	return nil
}

// providerStub answers the three operations deterministically: Niepołomice
// (y=50.02) carries a third-degree storm warning and lightning around it,
// Kraków (y=50.03) is quiet, and every other name is a locality the
// directory does not know (0/0 — which the warnings endpoint would read as
// "return test data").
func providerStub(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		text := strings.TrimSpace(string(body))
		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		switch {
		case strings.Contains(text, "<miejscowosc "):
			switch {
			case strings.Contains(text, "<nazwa>Niepołomice</nazwa>"):
				_, _ = io.WriteString(w, cityFixture(50.02, 20.13))
			case strings.Contains(text, "<nazwa>Kraków</nazwa>"):
				_, _ = io.WriteString(w, cityFixture(50.03, 19.57))
			default:
				_, _ = io.WriteString(w, cityFixture(0, 0))
			}
		case strings.Contains(text, "<ostrzezenia_pogodowe "):
			if strings.Contains(text, "<y>50.02</y>") {
				_, _ = io.WriteString(w, warningsFixture(3))
			} else {
				_, _ = io.WriteString(w, warningsFixture(0))
			}
		case strings.Contains(text, "<szukaj_burzy "):
			if strings.Contains(text, "<y>50.02</y>") {
				_, _ = io.WriteString(w, stormFixture(999, 4.99, "NW", 15))
			} else {
				_, _ = io.WriteString(w, stormFixture(0, 0, "", 20))
			}
		default:
			t.Errorf("unexpected SOAP body: %s", text)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
}

func number(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

func cityFixture(lat, lon float64) string {
	return `<?xml version="1.0" encoding="UTF-8"?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Body>` +
		`<ns1:miejscowoscResponse xmlns:ns1="https://burze.dzis.net/soap.php"><return><y>` + number(lat) + `</y><x>` + number(lon) + `</x>` +
		`</return></ns1:miejscowoscResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`
}

// warningsFixture renders the provider's warning envelope with every
// phenomenon at the given degree and one fixed UTC window.
func warningsFixture(degree int) string {
	if degree == 0 {
		return `<?xml version="1.0" encoding="UTF-8"?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Body>` +
			`<ns1:ostrzezenia_pogodoweResponse xmlns:ns1="https://burze.dzis.net/soap.php"><return>` +
			level("mroz", 0) + level("upal", 0) + level("wiatr", 0) +
			level("opad", 0) + level("burza", 0) + level("traba", 0) +
			`</return></ns1:ostrzezenia_pogodoweResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`
	}
	return `<?xml version="1.0" encoding="UTF-8"?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Body>` +
		`<ns1:ostrzezenia_pogodoweResponse xmlns:ns1="https://burze.dzis.net/soap.php"><return>` +
		level("mroz", degree) + level("upal", degree) + level("wiatr", degree) +
		level("opad", degree) + level("burza", degree) + level("traba", degree) +
		`</return></ns1:ostrzezenia_pogodoweResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`
}

func level(key string, degree int) string {
	from, to := "0", "0"
	if degree > 0 {
		from, to = "2026-10-13 14:00:00", "2026-10-13 20:00:00"
	}
	return "<" + key + ">" + strconv.Itoa(degree) + "</" + key + ">" +
		"<" + key + "_od_dnia>" + from + "</" + key + "_od_dnia>" +
		"<" + key + "_do_dnia>" + to + "</" + key + "_do_dnia>"
}

func stormFixture(count int, distance float64, direction string, window int) string {
	return `<?xml version="1.0" encoding="UTF-8"?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Body>` +
		`<ns1:szukaj_burzyResponse xmlns:ns1="https://burze.dzis.net/soap.php"><return>` +
		"<liczba>" + strconv.Itoa(count) + "</liczba><odleglosc>" + number(distance) + "</odleglosc>" +
		"<kierunek>" + direction + "</kierunek><okres>" + strconv.Itoa(window) + "</okres>" +
		`</return></ns1:szukaj_burzyResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`
}

// newSource builds a Source through the real factory from a YAML document,
// so the tests exercise the configuration surface the operator writes.
func newSource(t *testing.T, baseURL, extra string) *Source {
	t.Helper()
	return sourceFromYAML(t, "api_key: test-key\nbase_url: "+baseURL+"\nlocations: [Niepołomice, Kraków]\n"+extra)
}

func sourceFromYAML(t *testing.T, text string) *Source {
	t.Helper()
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(text), &node); err != nil {
		t.Fatalf("parse test config: %v", err)
	}
	p, err := New(&node)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src, ok := p.(*Source)
	if !ok {
		t.Fatalf("New returned %T", p)
	}
	return src
}

func phenomenonOf(t *testing.T, key string) burzedzis.Phenomenon {
	t.Helper()
	for _, ph := range burzedzis.Phenomena {
		if ph.Key == key {
			return ph
		}
	}
	t.Fatalf("no phenomenon %q", key)
	return burzedzis.Phenomenon{}
}

func point(name string, lat, lon float64) burzedzis.Locality {
	return burzedzis.Locality{Name: name, Lat: lat, Lon: lon}
}

func warningOf(t *testing.T, key string, degree int, from, to time.Time, at burzedzis.Locality) burzedzis.Warning {
	t.Helper()
	return burzedzis.Warning{Phenomenon: phenomenonOf(t, key), Degree: degree, From: from, To: to, Point: at}
}

func stormAt(t *testing.T, distance float64) burzedzis.Storm {
	t.Helper()
	return burzedzis.Storm{Locality: "Niepołomice", Lat: 50.02, Lon: 20.13, RadiusKM: 25, Count: 12, DistanceKM: distance, Direction: "NW", WindowMins: 15}
}

func TestConfigValidation(t *testing.T) {
	if _, err := New(nil); err == nil || !strings.Contains(err.Error(), "api_key") {
		t.Errorf("New(nil) = %v, want a missing-key error", err)
	}

	cases := map[string]struct{ yaml, want string }{
		"key and key file":     {"api_key: k\napi_key_file: /tmp/k\n", "mutually exclusive"},
		"non-http base url":    {"api_key: k\nbase_url: burze.dzis.net\n", "http(s)"},
		"wind floor too low":   {"api_key: k\nstorm_min_severity: minor\n", "never routed below severe"},
		"unknown degree sev":   {"api_key: k\ndegree2_severity: huge\n", "unknown severity"},
		"unknown rain floor":   {"api_key: k\nrain_min_severity: tiny\n", "unknown rain_min_severity"},
		"poll too short":       {"api_key: k\npoll_interval: 1s\n", "poll_interval"},
		"storm km inverted":    {"api_key: k\nstorm_tracking: true\nstorm_near_km: 3\nstorm_direct_km: 10\n", "storm_direct_km"},
		"storm severities dip": {"api_key: k\nstorm_tracking: true\nstorm_severity: severe\nstorm_near_severity: minor\n", "must not decrease"},
		"update beyond ttl":    {"api_key: k\nstorm_tracking: true\nstorm_ttl: 10m\nstorm_update_interval: 20m\n", "storm_update_interval"},
		"locality too long":    {"api_key: k\nlocations: [" + strings.Repeat("x", 80) + "]\n", "maximum 64"},
	}
	for name, c := range cases {
		var node yaml.Node
		if err := yaml.Unmarshal([]byte(c.yaml), &node); err != nil {
			t.Fatalf("%s: parse: %v", name, err)
		}
		_, err := New(&node)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: New = %v, want an error mentioning %q", name, err, c.want)
		}
	}

	// The defaults must be the documented ones.
	src := newSource(t, "https://burze.dzis.net", "storm_tracking: true\n")
	if src.cfg.PollInterval != defaultPollInterval || src.cfg.StormTTL != defaultStormTTL {
		t.Errorf("defaults = poll %s, ttl %s", src.cfg.PollInterval, src.cfg.StormTTL)
	}
	if src.cfg.StormRadiusKM != 25 || src.cfg.StormMinLightnings != 1 || src.cfg.StormUpdateInterval != defaultStormUpdate {
		t.Errorf("storm defaults = %+v", src.cfg)
	}
	if src.cfg.StormSeverity != severity.Moderate || src.cfg.StormNearSeverity != severity.Severe || src.cfg.StormDirectSeverity != severity.Extreme {
		t.Errorf("lightning severities = %s/%s/%s", src.cfg.StormSeverity, src.cfg.StormNearSeverity, src.cfg.StormDirectSeverity)
	}
	if got := src.cfg.Locations; len(got) != 2 || got[0] != "Niepołomice" || got[1] != "Kraków" {
		t.Errorf("locations = %v", got)
	}
	if src.cfg.LocalityRefresh != defaultLocalityRefresh || src.cfg.RequestTimeout != defaultRequestTimeout {
		t.Errorf("defaults = %+v", src.cfg)
	}
}

func TestNormalizeLocationsBound(t *testing.T) {
	names := make([]string, 0, MaxLocalities+1)
	for i := 0; i <= MaxLocalities; i++ {
		names = append(names, fmt.Sprintf("Town %d", i))
	}
	if _, err := NormalizeLocations(names); err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Errorf("NormalizeLocations with %d names = %v, want the bound error", len(names), err)
	}
	if got, err := NormalizeLocations(names[:MaxLocalities]); err != nil || len(got) != MaxLocalities {
		t.Errorf("NormalizeLocations with %d names = %d, %v", MaxLocalities, len(got), err)
	}
}

func TestNormalizeLocations(t *testing.T) {
	got, err := NormalizeLocations([]string{"  Niepołomice ", "Niepołomice", "Kraków", "", "Nowy   Sącz"})
	if err != nil {
		t.Fatalf("NormalizeLocations: %v", err)
	}
	want := []string{"Niepołomice", "Kraków", "Nowy Sącz"}
	if len(got) != len(want) {
		t.Fatalf("locations = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("locations[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestWarningSeverity pins the routing priorities: a thunderstorm or a
// strong-wind warning is at least severe at every degree and extreme at the
// third, while rain, heat and frost follow the plain degree scale.
func TestWarningSeverity(t *testing.T) {
	src := newSource(t, "https://burze.dzis.net", "")
	cases := []struct {
		phenomenon string
		degree     int
		want       string
	}{
		{"storm", 1, severity.Severe},
		{"storm", 2, severity.Severe},
		{"storm", 3, severity.Extreme},
		{"wind", 1, severity.Severe},
		{"wind", 3, severity.Extreme},
		{"tornado", 1, severity.Severe},
		{"tornado", 3, severity.Extreme},
		{"rain", 1, severity.Moderate},
		{"rain", 2, severity.Severe},
		{"rain", 3, severity.Extreme},
		{"frost", 1, severity.Moderate},
		{"heat", 2, severity.Severe},
	}
	for _, c := range cases {
		ph := phenomenonOf(t, c.phenomenon)
		if got := src.warningSeverity(ph, c.degree); got != c.want {
			t.Errorf("%s degree %d = %s, want %s", c.phenomenon, c.degree, got, c.want)
		}
	}
}

// TestStormSeverity pins the lightning escalation: farther than the near
// threshold is the base severity, close is the near severity and a discharge
// right over the locality is the direct one.
func TestStormSeverity(t *testing.T) {
	src := newSource(t, "https://burze.dzis.net", "storm_tracking: true\n")
	cases := []struct {
		distance float64
		want     string
	}{
		{25, severity.Moderate},
		{10.1, severity.Moderate},
		{10, severity.Severe},
		{3.1, severity.Severe},
		{3, severity.Extreme},
		{0.5, severity.Extreme},
		{0, severity.Moderate}, // the provider reported no distance
	}
	for _, c := range cases {
		if got := src.stormSeverity(stormAt(t, c.distance)); got != c.want {
			t.Errorf("distance %v km = %s, want %s", c.distance, got, c.want)
		}
	}
}

func TestWarningEventNamesTheLocality(t *testing.T) {
	src := newSource(t, "https://burze.dzis.net", "")
	at := point("Niepołomice", 50.02, 20.13)
	from := time.Date(2026, 10, 13, 14, 0, 0, 0, time.UTC)
	in := warningOf(t, "storm", 2, from, from.Add(6*time.Hour), at)
	ev := src.warningEvent(in)

	if ev.Source != Type || ev.Severity != severity.Severe {
		t.Errorf("source/severity = %s/%s", ev.Source, ev.Severity)
	}
	if ev.Event != "Thunderstorm warning" {
		t.Errorf("event type = %q (it must stay the canonical English name other installations share)", ev.Event)
	}
	if len(ev.Areas) != 1 || ev.Areas[0] != "Niepołomice" {
		t.Errorf("areas = %v, want the locality", ev.Areas)
	}
	if !strings.Contains(ev.Headline, "Niepołomice") || !strings.Contains(ev.Headline, "Burza") || !strings.Contains(ev.Headline, "II stopień") {
		t.Errorf("headline = %q", ev.Headline)
	}
	if !strings.Contains(ev.Description, "Niepołomice") || !strings.Contains(ev.Description, "UTC") {
		t.Errorf("description = %q", ev.Description)
	}
	if ev.Instruction != "" {
		t.Errorf("instruction must stay empty (the provider publishes none), got %q", ev.Instruction)
	}
	if ev.ProviderSeverity != "2" {
		t.Errorf("provider severity = %q, want the degree", ev.ProviderSeverity)
	}
	if ev.Latitude == nil || *ev.Latitude != 50.02 || ev.ExpiresAt == nil || !ev.ExpiresAt.Equal(from.Add(6*time.Hour)) {
		t.Errorf("coordinates/expiry = %v/%v", ev.Latitude, ev.ExpiresAt)
	}
	if err := ev.Validate(); err != nil {
		t.Errorf("event does not validate: %v", err)
	}

	// The identity is stable for the same window and changes when the
	// forecast window moves, so a re-issue cannot silently overwrite it.
	same := src.warningEvent(warningOf(t, "storm", 3, from, from.Add(8*time.Hour), at))
	if same.SourceID != ev.SourceID {
		t.Errorf("a degree change within the same window must keep the identity: %q vs %q", same.SourceID, ev.SourceID)
	}
	moved := src.warningEvent(warningOf(t, "storm", 2, from.Add(time.Hour), from.Add(7*time.Hour), at))
	if moved.SourceID == ev.SourceID {
		t.Errorf("a shifted window must change the identity, both are %q", moved.SourceID)
	}
}

func TestStormEventIsStableWithinItsBucket(t *testing.T) {
	src := newSource(t, "https://burze.dzis.net", "storm_tracking: true\n")
	st := stormAt(t, 4.99)

	now := time.Date(2026, 10, 13, 15, 7, 0, 0, time.UTC)
	first := src.stormEvent(st, now)
	later := src.stormEvent(st, now.Add(3*time.Minute))
	if first.Source != TypeStorm {
		t.Errorf("storm source = %q, want %q", first.Source, TypeStorm)
	}
	if first.Event != "Lightning nearby" {
		t.Errorf("storm event type = %q", first.Event)
	}
	if !strings.Contains(first.Headline, "Niepołomice") || !strings.Contains(first.Headline, "12 wyładowań") {
		t.Errorf("storm headline = %q", first.Headline)
	}
	if !strings.Contains(first.Description, "Najbliższe wyładowanie: 5.0 km, kierunek północny zachód") {
		t.Errorf("storm description = %q", first.Description)
	}
	if first.Instruction != "" {
		t.Errorf("storm instruction must stay empty, got %q", first.Instruction)
	}
	// Same facts inside one validity bucket must produce the same
	// fingerprint: a fingerprint change is a notification.
	if core.Fingerprint(first) != core.Fingerprint(later) {
		t.Errorf("fingerprint changed within one validity bucket:\n%s\n%s", core.Fingerprint(first), core.Fingerprint(later))
	}
	if first.Severity != severity.Severe {
		t.Errorf("4.99 km severity = %s, want severe", first.Severity)
	}
	if err := first.Validate(); err != nil {
		t.Errorf("storm event does not validate: %v", err)
	}

	// An escalating storm keeps one identity: the operator cites one
	// message per locality per storm.
	closer := st
	closer.DistanceKM = 1.2
	if src.stormEvent(closer, now).SourceID != first.SourceID {
		t.Error("an escalating storm must keep its identity")
	}
}

func TestPollOncePublishesAndCancels(t *testing.T) {
	srv := providerStub(t)
	defer srv.Close()
	src := newSource(t, srv.URL, "storm_tracking: true\n")

	// A warning the provider no longer publishes: reconciliation must
	// cancel it instead of letting it hang until its window ends.
	withdrawn := src.warningEvent(warningOf(t, "rain", 2,
		time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC),
		time.Now().Add(3*time.Hour), point("Kraków", 50.03, 19.57)))
	emit := &fakeEmitter{active: []core.HazardEvent{withdrawn}}

	if err := src.pollOnce(context.Background(), emit); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}

	published, cancelled := 0, 0
	for _, ev := range emit.events(Type) {
		if ev.Status == core.StatusCancelled {
			cancelled++
			if ev.Key() != withdrawn.Key() {
				t.Errorf("cancelled %q, want the withdrawn %q", ev.Key(), withdrawn.Key())
			}
			continue
		}
		published++
		if !strings.Contains(ev.Headline, "Niepołomice") {
			t.Errorf("warning %q does not name the locality", ev.Headline)
		}
		if ev.Severity != severity.Severe && ev.Severity != severity.Extreme {
			t.Errorf("third-degree warning severity = %s, want at least severe", ev.Severity)
		}
	}
	if published != 6 {
		t.Errorf("published %d warnings, want 6 (every phenomenon of the storm warning)", published)
	}
	if cancelled != 1 {
		t.Errorf("cancelled %d warnings, want 1", cancelled)
	}

	storms := emit.events(TypeStorm)
	if len(storms) != 1 {
		t.Fatalf("published %d lightning events, want 1 (Kraków is quiet)", len(storms))
	}
	if !strings.Contains(storms[0].Headline, "Niepołomice") || storms[0].Severity != severity.Severe {
		t.Errorf("lightning event = %q (%s)", storms[0].Headline, storms[0].Severity)
	}
	if got := emit.lastStats(); !strings.Contains(got, "2 localities") || !strings.Contains(got, "6 warnings") || !strings.Contains(got, "1 lightning") {
		t.Errorf("stats summary = %q", got)
	}

	// A second poll with unchanged facts and the same validity bucket must
	// not re-publish the detection.
	before := len(emit.events(TypeStorm))
	if err := src.pollOnce(context.Background(), emit); err != nil {
		t.Fatalf("second pollOnce: %v", err)
	}
	if after := len(emit.events(TypeStorm)); after != before {
		t.Errorf("an unchanged detection was re-published (%d → %d)", before, after)
	}
}

// TestPollOnceReportsUnknownLocality: the provider answers 0/0 for a
// mistyped town, and 0/0 is its "return test data" switch — the source must
// report the gap instead of swallowing it or, worse, publishing the sample
// alerts.
func TestPollOnceReportsUnknownLocality(t *testing.T) {
	srv := providerStub(t)
	defer srv.Close()
	src := sourceFromYAML(t, "api_key: test-key\nbase_url: "+srv.URL+"\nlocations: [Niepolomice, Niepołomice]\n")
	emit := &fakeEmitter{}
	if err := src.pollOnce(context.Background(), emit); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if got := emit.events(""); len(got) != 6 {
		t.Errorf("emitted %d events, want only Niepołomice's six", len(got))
	}
	if got := emit.lastStats(); !strings.Contains(got, "unknown: Niepolomice") {
		t.Errorf("stats must name the unresolved locality: %q", got)
	}
}

// TestPollOnceSkipsReconciliationOnIncompleteSnapshot: a failed locality
// query must not look like "the provider withdrew everything".
func TestPollOnceSkipsReconciliationOnIncompleteSnapshot(t *testing.T) {
	var fail bool
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		text := strings.TrimSpace(string(body))
		mu.Lock()
		broken := fail
		mu.Unlock()
		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		switch {
		case strings.Contains(text, "<miejscowosc "):
			_, _ = io.WriteString(w, cityFixture(50.02, 20.13))
		case strings.Contains(text, "<ostrzezenia_pogodowe "):
			if broken {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, _ = io.WriteString(w, warningsFixture(0))
		default:
			_, _ = io.WriteString(w, stormFixture(0, 0, "", 20))
		}
	}))
	defer srv.Close()

	src := newSource(t, srv.URL, "storm_tracking: true\n")
	active := src.warningEvent(warningOf(t, "storm", 2, time.Now().Add(time.Hour), time.Now().Add(4*time.Hour), point("Niepołomice", 50.02, 20.13)))
	emit := &fakeEmitter{active: []core.HazardEvent{active}}
	mu.Lock()
	fail = true
	mu.Unlock()
	if err := src.pollOnce(context.Background(), emit); err == nil {
		t.Fatal("a failed warnings pass must be reported")
	}
	for _, ev := range emit.events("") {
		if ev.Status == core.StatusCancelled {
			t.Errorf("an incomplete snapshot cancelled %q", ev.Key())
		}
	}

	// A complete pass then cancels it: nothing is in force for that point.
	mu.Lock()
	fail = false
	mu.Unlock()
	if err := src.pollOnce(context.Background(), emit); err != nil {
		t.Fatalf("second pollOnce: %v", err)
	}
	cancelled := 0
	for _, ev := range emit.events(Type) {
		if ev.Status == core.StatusCancelled {
			cancelled++
		}
	}
	if cancelled != 1 {
		t.Errorf("the complete quiet snapshot cancelled %d warnings, want 1", cancelled)
	}
}

func TestNoLocalitiesStaysIdle(t *testing.T) {
	src := sourceFromYAML(t, "api_key: test-key\nlocations: []\n")
	emit := &fakeEmitter{}
	if err := src.pollOnce(context.Background(), emit); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if len(emit.emitted) != 0 {
		t.Errorf("an instance without localities emitted %d events", len(emit.emitted))
	}
	if got := emit.lastStats(); !strings.Contains(got, "no localities") {
		t.Errorf("stats = %q, want the idle note", got)
	}
}

func TestRunRequiresReaderCapability(t *testing.T) {
	src := newSource(t, "http://127.0.0.1:1", "")
	if err := src.Run(context.Background(), &noReaderEmitter{}); err == nil {
		t.Fatal("Run must fail when the emitter lacks the active-event reader")
	}
}

func TestSlugKeepsEventIDsUsable(t *testing.T) {
	for name, want := range map[string]string{
		"Niepołomice":    "niepolomice",
		"Nowy Sącz":      "nowy-sacz",
		"Kraków":         "krakow",
		"  Łódź  ":       "lodz",
		"Żory":           "zory",
		"":               "locality",
		"---":            "locality",
		"Gdańsk/Stogi 2": "gdansk-stogi-2",
	} {
		if got := slug(name); got != want {
			t.Errorf("slug(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestRoman(t *testing.T) {
	for degree, want := range map[int]string{1: "I", 2: "II", 3: "III", 4: "4"} {
		if got := roman(degree); got != want {
			t.Errorf("roman(%d) = %q, want %q", degree, got, want)
		}
	}
}

// flakyDirectory is the provider's directory with a switch: it answers
// normally, or with a transient failure (HTTP 503 / a maintenance page), or
// with the 0/0 the provider uses for a name it does not know.
type flakyDirectory struct {
	mu      sync.Mutex
	mode    string
	unknown map[string]bool
}

// refuse makes the directory answer 0/0 for that name (its "I do not know
// this locality" answer), whatever the global mode is.
func (d *flakyDirectory) refuse(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.unknown == nil {
		d.unknown = make(map[string]bool)
	}
	d.unknown[name] = true
}

func (d *flakyDirectory) set(mode string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mode = mode
}

func (d *flakyDirectory) refused(name string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.unknown[name]
}

func (d *flakyDirectory) get() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.mode == "" {
		return "ok"
	}
	return d.mode
}

func (d *flakyDirectory) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		text := strings.TrimSpace(string(body))
		switch {
		case strings.Contains(text, "<miejscowosc "):
			name := ""
			if i := strings.Index(text, "<nazwa>"); i >= 0 {
				rest := text[i+len("<nazwa>"):]
				name = rest[:strings.Index(rest, "</nazwa>")]
			}
			if d.refused(name) {
				w.Header().Set("Content-Type", "text/xml; charset=utf-8")
				_, _ = io.WriteString(w, cityFixture(0, 0))
				return
			}
			switch d.get() {
			case "503":
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			case "maintenance":
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				_, _ = io.WriteString(w, "<html><body>maintenance</body></html>")
				return
			case "unknown":
				w.Header().Set("Content-Type", "text/xml; charset=utf-8")
				_, _ = io.WriteString(w, cityFixture(0, 0))
				return
			}
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			_, _ = io.WriteString(w, cityFixture(50.02, 20.13))
		case strings.Contains(text, "<ostrzezenia_pogodowe "):
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			_, _ = io.WriteString(w, warningsFixture(3))
		case strings.Contains(text, "<szukaj_burzy "):
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			_, _ = io.WriteString(w, stormFixture(0, 0, "", 20))
		default:
			t.Errorf("unexpected SOAP body: %s", text)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestResolveSurvivesProviderFailure pins the reported P1: a transient
// directory failure (HTTP 503) used to be classified as "unknown locality",
// which wiped the resolved coordinates AND marked the cache fresh — 24 hours
// without monitoring. A failure must keep the last good coordinates, report
// the source as degraded and be retried on the next poll.
func TestResolveSurvivesProviderFailure(t *testing.T) {
	dir := &flakyDirectory{}
	srv := dir.start(t)
	src := sourceFromYAML(t, "api_key: test-key\nbase_url: "+srv.URL+
		"\nlocations: [Niepołomice]\nlocality_refresh: 30m\n")

	// 1. A healthy pass resolves the locality.
	emit := &fakeEmitter{}
	if err := src.pollOnce(context.Background(), emit); err != nil {
		t.Fatalf("healthy pollOnce: %v", err)
	}
	if got := emit.events(Type); len(got) != 6 {
		t.Fatalf("healthy pass emitted %d warnings, want 6", len(got))
	}
	src.mu.Lock()
	refreshed := src.refreshed
	resolved := len(src.points)
	src.mu.Unlock()
	if refreshed.IsZero() || resolved != 1 {
		t.Fatalf("healthy pass did not cache the locality: refreshed=%v points=%d", refreshed, resolved)
	}

	// 2. The directory fails the next refresh (as it does when the refresh
	//    window has elapsed): the station must keep watching.
	staleStamp := time.Now().Add(-src.cfg.LocalityRefresh - time.Minute) // refresh is due
	src.mu.Lock()
	src.refreshed = staleStamp
	src.mu.Unlock()
	dir.set("503")

	emit = &fakeEmitter{}
	err := src.pollOnce(context.Background(), emit)
	if err == nil {
		t.Fatal("a failed locality lookup must be reported (the source is degraded)")
	}
	if got := emit.events(Type); len(got) != 6 {
		t.Errorf("a directory outage stopped the warnings: %d events, want 6", len(got))
	}
	src.mu.Lock()
	kept, hasCoords := src.points["Niepołomice"]
	isUnknown := src.unknown["Niepołomice"]
	retry, advanced := src.retry["Niepołomice"], src.refreshed.After(staleStamp)
	src.mu.Unlock()
	if !hasCoords || isUnknown {
		t.Errorf("a 503 was classified as an unknown locality: hasCoords=%v unknown=%v kept=%v", hasCoords, isUnknown, kept)
	}
	if !retry {
		t.Error("the failed lookup was not queued for the next poll")
	}
	if advanced {
		t.Error("a pass with a failure was marked as a completed refresh (the retry would wait a full interval)")
	}
	if got, _ := src.snapshotPoints(); len(got) != 1 {
		t.Errorf("the locality disappeared from the point list: %v", got)
	}

	// 3. The provider recovers: the very next poll resolves and watches
	//    again, without a restart and without waiting for the refresh.
	dir.set("ok")
	emit = &fakeEmitter{}
	if err := src.pollOnce(context.Background(), emit); err != nil {
		t.Fatalf("pollOnce after recovery: %v", err)
	}
	if got := emit.events(Type); len(got) != 6 {
		t.Errorf("after recovery the pass emitted %d warnings, want 6", len(got))
	}
	src.mu.Lock()
	retry, fresh := src.retry["Niepołomice"], !src.refreshed.IsZero()
	src.mu.Unlock()
	if retry || !fresh {
		t.Errorf("recovery did not clear the retry queue (retry=%v refreshed=%v)", retry, fresh)
	}
}

// TestResolveDistinguishesUnknownFromUnreachable: only the provider's 0/0
// answer makes a name unknown; a transient failure for a name that already
// has coordinates keeps them, so both kinds of trouble are reported for what
// they are.
func TestResolveDistinguishesUnknownFromUnreachable(t *testing.T) {
	dir := &flakyDirectory{}
	srv := dir.start(t)
	src := sourceFromYAML(t, "api_key: test-key\nbase_url: "+srv.URL+
		"\nlocations: [Niepołomice, Kraków]\nlocality_refresh: 30m\n")

	// A healthy pass resolves both.
	if err := src.pollOnce(context.Background(), &fakeEmitter{}); err != nil {
		t.Fatalf("healthy pollOnce: %v", err)
	}
	if got, _ := src.snapshotPoints(); len(got) != 2 {
		t.Fatalf("resolved points = %v, want 2", got)
	}

	// The directory stops knowing one name and fails (503) on the other.
	dir.refuse("Kraków")
	dir.set("503")
	src.mu.Lock()
	src.refreshed = time.Now().Add(-src.cfg.LocalityRefresh - time.Minute)
	src.mu.Unlock()
	if err := src.pollOnce(context.Background(), &fakeEmitter{}); err == nil {
		t.Fatal("the transient failure must be reported (the source is degraded)")
	}

	points, unknown := src.snapshotPoints()
	if len(points) != 1 || points[0].Name != "Niepołomice" {
		t.Errorf("points = %v, want only the name that has known coordinates", points)
	}
	if len(unknown) != 1 || unknown[0] != "Kraków" {
		t.Errorf("unknown = %v, want only the name the directory refused", unknown)
	}
}

// TestUnreadableAnswersCancelNothing pins the reported P1 at the source level:
// the provider (or a proxy in front of it) answers HTTP 200 with a maintenance
// page, which parses as valid XML with nothing inside. Reading that as "nothing
// is in force" would reconcile every live warning away, so the pass must fail
// and the reconciliation must be skipped.
func TestUnreadableAnswersCancelNothing(t *testing.T) {
	broken := map[string]bool{}
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		text := strings.TrimSpace(string(body))
		mu.Lock()
		warnDown, stormDown := broken["warnings"], broken["storm"]
		mu.Unlock()
		switch {
		case strings.Contains(text, "<miejscowosc "):
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			_, _ = io.WriteString(w, cityFixture(50.02, 20.13))
		case strings.Contains(text, "<ostrzezenia_pogodowe "):
			if warnDown {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				_, _ = io.WriteString(w, "<!DOCTYPE html><html><body>maintenance</body></html>")
				return
			}
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			_, _ = io.WriteString(w, warningsFixture(0))
		case strings.Contains(text, "<szukaj_burzy "):
			if stormDown {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				_, _ = io.WriteString(w, "<!DOCTYPE html><html><body>maintenance</body></html>")
				return
			}
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			_, _ = io.WriteString(w, stormFixture(0, 0, "", 20))
		default:
			t.Errorf("unexpected SOAP body: %s", text)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	src := newSource(t, srv.URL, "storm_tracking: true\n")
	now := time.Now()
	liveWarning := src.warningEvent(warningOf(t, "storm", 2, now.Add(-time.Hour), now.Add(3*time.Hour), point("Niepołomice", 50.02, 20.13)))
	liveStorm := src.stormEvent(stormAt(t, 4), now)
	emit := &fakeEmitter{active: []core.HazardEvent{liveWarning, liveStorm}}

	mu.Lock()
	broken["warnings"] = true
	broken["storm"] = true
	mu.Unlock()

	if err := src.pollOnce(context.Background(), emit); err == nil {
		t.Fatal("an unreadable answer must be reported (the source is degraded)")
	}
	for _, ev := range emit.events("") {
		if ev.Status == core.StatusCancelled {
			t.Errorf("an unreadable answer cancelled %q", ev.Key())
		}
	}

	// The provider recovers: the live warning stays in force (it was never
	// cancelled) and the pass reports healthy again.
	mu.Lock()
	broken["warnings"] = false
	broken["storm"] = false
	mu.Unlock()
	if err := src.pollOnce(context.Background(), emit); err != nil {
		t.Fatalf("pollOnce after recovery: %v", err)
	}
	cancelled := 0
	for _, ev := range emit.events("") {
		if ev.Status == core.StatusCancelled {
			cancelled++
		}
	}
	// The warning was withdrawn by the provider's own quiet snapshot, so ONE
	// cancellation is correct now — and the storm event goes the same way.
	if cancelled != 2 {
		t.Errorf("cancellations after the quiet snapshot = %d, want 2 (the live warning and the storm)", cancelled)
	}
}

// TestUnreadableWindowKeepsTheAlert pins the reported P1: a phenomenon that
// carries a degree but no readable window is UNKNOWN, not withdrawn. The alert
// already published for it must survive the cycle (and the source must report
// the incomplete answer), while a genuinely complete quiet snapshot still
// cancels it.
func TestUnreadableWindowKeepsTheAlert(t *testing.T) {
	// A complete envelope whose storm window cannot be read.
	badWindow := `<?xml version="1.0" encoding="UTF-8"?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Body>` +
		`<ns1:ostrzezenia_pogodoweResponse xmlns:ns1="https://burze.dzis.net/soap.php"><return>` +
		level("mroz", 0) + level("upal", 0) + level("wiatr", 0) + level("opad", 0) +
		"<burza>2</burza><burza_od_dnia>brak</burza_od_dnia><burza_do_dnia>0</burza_do_dnia>" +
		level("traba", 0) +
		`</return></ns1:ostrzezenia_pogodoweResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`

	var mu sync.Mutex
	broken := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		text := strings.TrimSpace(string(body))
		mu.Lock()
		bad := broken
		mu.Unlock()
		switch {
		case strings.Contains(text, "<miejscowosc "):
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			_, _ = io.WriteString(w, cityFixture(50.02, 20.13))
		case strings.Contains(text, "<ostrzezenia_pogodowe "):
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			if bad {
				_, _ = io.WriteString(w, badWindow)
				return
			}
			_, _ = io.WriteString(w, warningsFixture(0))
		case strings.Contains(text, "<szukaj_burzy "):
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			_, _ = io.WriteString(w, stormFixture(0, 0, "", 20))
		default:
			t.Errorf("unexpected SOAP body: %s", text)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	src := newSource(t, srv.URL, "storm_tracking: true\n")
	now := time.Now()
	// The alert the station published for an EARLIER storm window: its key is
	// not part of this cycle's snapshot, so only the protection keeps it.
	live := src.warningEvent(warningOf(t, "storm", 2, now.Add(-2*time.Hour), now.Add(2*time.Hour), point("Niepołomice", 50.02, 20.13)))
	// A second live alert for a DIFFERENT phenomenon, which the provider
	// withdraws in the same cycle: it must still be cancelled.
	withdrawn := src.warningEvent(warningOf(t, "rain", 2, now.Add(-time.Hour), now.Add(3*time.Hour), point("Niepołomice", 50.02, 20.13)))
	emit := &fakeEmitter{active: []core.HazardEvent{live, withdrawn}}

	err := src.pollOnce(context.Background(), emit)
	if err == nil {
		t.Fatal("an incomplete answer must be reported (the source is degraded)")
	}
	cancelled := map[string]bool{}
	kept := 0
	for _, ev := range emit.events(Type) {
		if ev.Status == core.StatusCancelled {
			cancelled[ev.Key()] = true
			continue
		}
		kept++
	}
	if cancelled[live.Key()] {
		t.Error("an unreadable window cancelled the alert already in force")
	}
	if !cancelled[withdrawn.Key()] {
		t.Error("a genuine withdrawal in the same cycle was lost: the reconciliation must still run")
	}
	if kept != 0 {
		t.Errorf("the unreadable storm warning must not be published as an alert, got %d events", kept)
	}
	if got := emit.lastStats(); !strings.Contains(got, "incomplete answer: storm") {
		t.Errorf("the status line does not report the incomplete answer: %q", got)
	}

	// A complete, quiet snapshot withdraws the alert: the protection only
	// applies to the cycle that could not read the window.
	mu.Lock()
	broken = false
	mu.Unlock()
	if err := src.pollOnce(context.Background(), emit); err != nil {
		t.Fatalf("pollOnce after the provider recovered: %v", err)
	}
	if !cancelled[live.Key()] {
		stillCancelled := false
		for _, ev := range emit.events(Type) {
			if ev.Key() == live.Key() && ev.Status == core.StatusCancelled {
				stillCancelled = true
			}
		}
		if !stillCancelled {
			t.Error("the complete quiet snapshot did not withdraw the alert")
		}
	}
}

// TestReturningStormIsPublishedAfterItsCancellation pins the reported P2:
// after "discharges → silence → discharges" the returning storm carries the
// same severity and the same validity bucket as the detection that was
// cancelled, so the dedup memory of the first one must not swallow it — the
// alarm has to be published (active) in the FIRST cycle of the return, not
// after the update interval.
func TestReturningStormIsPublishedAfterItsCancellation(t *testing.T) {
	var mu sync.Mutex
	storming := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		text := strings.TrimSpace(string(body))
		mu.Lock()
		storm := storming
		mu.Unlock()
		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		switch {
		case strings.Contains(text, "<miejscowosc "):
			_, _ = io.WriteString(w, cityFixture(50.02, 20.13))
		case strings.Contains(text, "<ostrzezenia_pogodowe "):
			_, _ = io.WriteString(w, warningsFixture(0))
		case strings.Contains(text, "<szukaj_burzy "):
			if storm {
				_, _ = io.WriteString(w, stormFixture(999, 4.99, "NW", 15))
				return
			}
			_, _ = io.WriteString(w, stormFixture(0, 0, "", 20))
		default:
			t.Errorf("unexpected SOAP body: %s", text)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	src := sourceFromYAML(t, "api_key: test-key\nbase_url: "+srv.URL+"\nlocations: [Niepołomice]\nstorm_tracking: true\nstorm_update_interval: 15m\n")
	// The emitter behaves like the store: what was published is the active
	// state the next pass reconciles against, and a cancellation takes it
	// out of that state.
	emit := newActiveEmitter()

	// The storm is over the locality: published and in force.
	if err := src.pollOnce(context.Background(), emit); err != nil {
		t.Fatalf("first pollOnce: %v", err)
	}
	first := emit.activeOf(TypeStorm)
	if len(first) != 1 {
		t.Fatalf("first poll published %d lightning events, want 1", len(first))
	}

	// The sky goes quiet: the nowcast is cancelled.
	mu.Lock()
	storming = false
	mu.Unlock()
	if err := src.pollOnce(context.Background(), emit); err != nil {
		t.Fatalf("second pollOnce: %v", err)
	}
	if live := emit.activeOf(TypeStorm); len(live) != 0 {
		t.Fatalf("the quiet pass kept %d lightning events in force", len(live))
	}
	if _, ok := src.storm[first[0].SourceID]; ok {
		t.Error("the dedup memory of the cancelled storm survived its cancellation")
	}

	// The discharges come back with the SAME severity and inside the same
	// validity bucket, well inside the update interval: the alarm must be
	// active again.
	mu.Lock()
	storming = true
	mu.Unlock()
	if err := src.pollOnce(context.Background(), emit); err != nil {
		t.Fatalf("third pollOnce: %v", err)
	}
	live := emit.activeOf(TypeStorm)
	if len(live) != 1 {
		t.Fatalf("the returning storm left %d active lightning events, want 1", len(live))
	}
	if live[0].Key() != first[0].Key() {
		t.Errorf("the returning storm is %q, want the same identity %q", live[0].Key(), first[0].Key())
	}
	if live[0].Severity != first[0].Severity || !live[0].ExpiresAt.Equal(*first[0].ExpiresAt) {
		t.Errorf("the returning storm is %s/%v, want the same facts as %s/%v",
			live[0].Severity, live[0].ExpiresAt, first[0].Severity, first[0].ExpiresAt)
	}
}

// activeEmitter is a fake emitter that keeps the active state it is given,
// the way the store does: an emit updates it (a cancelled event leaves it).
type activeEmitter struct {
	fakeEmitter
}

func newActiveEmitter() *activeEmitter { return &activeEmitter{} }

func (f *activeEmitter) Emit(ctx context.Context, ev core.HazardEvent) error {
	f.mu.Lock()
	kept := f.active[:0]
	for _, prev := range f.active {
		if prev.Key() != ev.Key() {
			kept = append(kept, prev)
		}
	}
	f.active = kept
	if ev.Status == core.StatusActive {
		f.active = append(f.active, ev)
	}
	f.mu.Unlock()
	return f.fakeEmitter.Emit(ctx, ev)
}

// activeOf returns the recorded active events of one source.
func (f *activeEmitter) activeOf(source string) []core.HazardEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []core.HazardEvent
	for _, ev := range f.active {
		if ev.Source == source {
			out = append(out, ev)
		}
	}
	return out
}

// TestStormValidityOutlivesTheBucketBoundary pins the reported P2: a
// detection made one second before its validity bucket ends must not expire
// a second later, before the station checks again. Every emitted detection
// is valid until the next measurement AND the next scheduled re-publish, and
// a storm that simply continues keeps one window (so it neither lapses nor
// re-notifies merely because the bucket boundary was crossed).
func TestStormValidityOutlivesTheBucketBoundary(t *testing.T) {
	src := sourceFromYAML(t, "api_key: test-key\nbase_url: https://burze.dzis.net\nlocations: [Niepołomice]\n"+
		"storm_tracking: true\nstorm_ttl: 30m\nstorm_update_interval: 15m\npoll_interval: 5m\n")
	st := stormAt(t, 4.99)
	margin := src.cfg.StormUpdateInterval + src.cfg.PollInterval

	// The worst case: one second before the bucket ends (12:30 with a 30m
	// TTL), i.e. where the bucket alone granted a one-second validity.
	edge := time.Date(2026, 10, 13, 12, 29, 59, 0, time.UTC)
	first := src.stormEvent(st, edge)
	if first.ExpiresAt == nil {
		t.Fatal("the detection carries no expiry")
	}
	if want := edge.Add(margin); first.ExpiresAt.Before(want) {
		t.Errorf("a detection at %v is valid to %v, want at least %v: it would lapse before the next poll",
			edge, first.ExpiresAt, want)
	}

	// The window does slide when it has to stay ahead of the next poll —
	// that is not news, so it must not re-publish within the update
	// interval: only a severity change or the interval itself does.
	slid := src.stormEvent(st, edge.Add(11*time.Minute))
	if slid.ExpiresAt.Equal(*first.ExpiresAt) {
		t.Fatalf("the fixture no longer crosses an extension point (both windows end at %v)", first.ExpiresAt)
	}
	if core.Fingerprint(slid) == core.Fingerprint(first) {
		t.Error("a slid window must change the fingerprint (the core records content changes by it)")
	}
	state := stormState{severity: first.Severity, at: edge}
	seen := true
	if src.shouldPublish(state, seen, slid, edge.Add(11*time.Minute)) {
		t.Error("a window that slid to stay valid was re-published inside the update interval")
	}
	if !src.shouldPublish(state, seen, slid, edge.Add(src.cfg.StormUpdateInterval)) {
		t.Error("the update interval must re-publish the detection")
	}
	escalated := src.stormEvent(stormAt(t, 1.2), edge.Add(11*time.Minute))
	if !src.shouldPublish(state, seen, escalated, edge.Add(time.Minute)) {
		t.Error("an escalating storm must be published without waiting for the interval")
	}
	if !src.shouldPublish(stormState{}, false, first, edge) {
		t.Error("the first detection of a locality must be published")
	}

	// Whatever the moment, the alert outlives the next cycle: a continuous
	// storm can never be gone from the station's active state between two
	// polls.
	for at := edge; at.Before(edge.Add(2 * time.Hour)); at = at.Add(time.Minute) {
		ev := src.stormEvent(st, at)
		if !ev.ExpiresAt.After(at.Add(margin)) {
			t.Fatalf("a detection at %v is valid only to %v, want later than %v", at, ev.ExpiresAt, at.Add(margin))
		}
		if ev.EffectiveAt == nil || ev.EffectiveAt.After(at) {
			t.Fatalf("a detection at %v claims to be effective from %v", at, ev.EffectiveAt)
		}
		if err := ev.Validate(); err != nil {
			t.Fatalf("a detection at %v does not validate: %v", at, err)
		}
	}
}
