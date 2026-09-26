package giosaq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// levelsFixture builds the paginated-history fixture with dates relative
// to now (the provider stamps local wall-clock times), so the max-age
// filter can never rot over calendar time.
func levelsFixture(now time.Time) string {
	loc, _ := time.LoadLocation("Europe/Warsaw")
	fresh1 := now.In(loc).Format("2006-01-02 15:04")
	fresh2 := now.In(loc).Add(-2 * time.Hour).Format("2006-01-02 15:04")
	fresh3 := now.In(loc).Add(-3 * time.Hour).Format("2006-01-02 15:04")
	stale := now.In(loc).Add(-48 * time.Hour).Format("2006-01-02 15:04")
	return fmt.Sprintf(`{
  "totalPages": 199,
  "Przekroczenia": [
    {
      "Typ normy": "Próg Informowania (PI) - Ochrona Zdrowia (OZ) / S1 > 180 ug/m3",
      "Strefa": "PL1203 strefa małopolska",
      "Stanowisko pomiarowe": "MpSzarowSpok-O3-1g",
      "Data i godzina": %q,
      "Czas trwania": "1godziny ",
      "Liczba mieszkańców": 252202,
      "Wartość maksymalnego stężenia (µg/m3)": 189.2,
      "Odnośnik do strony internetowej z informacjami": "https://powietrze.gios.gov.pl/pjp/rwms/6/overruns/0",
      "Zalecane środki ostrożności": "ograniczenie przebywania na zewnątrz"
    },
    {
      "Typ normy": "Poziom alarmowy (PA) - Ochrona Zdrowia (OZ) / S1 > 240 ug/m3",
      "Strefa": "PL1201 aglomeracja krakowska",
      "Stanowisko pomiarowe": "MpKrakBulwar-PM10-2h",
      "Data i godzina": %q,
      "Czas trwania": "2godziny",
      "Liczba mieszkańców": 800000,
      "Wartość maksymalnego stężenia (µg/m3)": 260.4
    },
    {
      "Typ normy": "Poziom dopuszczalny (PD)",
      "Strefa": "PL2602 strefa świętokrzyska",
      "Stanowisko pomiarowe": "SkKielce-PM10-1h",
      "Data i godzina": %q,
      "Czas trwania": "1godziny"
    },
    {
      "Typ normy": "Próg Informowania (PI)",
      "Strefa": "PL1203 strefa małopolska",
      "Stanowisko pomiarowe": "MpStaryZabytek-O3-1g",
      "Data i godzina": %q,
      "Czas trwania": "1godziny"
    }
  ]
}`, fresh1, fresh2, fresh3, stale)
}

const stationsFixture = `{
  "Lista stacji pomiarowych": [
    {"Identyfikator stacji": 301, "Kod stacji": "MpSzarowSpok", "Nazwa stacji": "Szarów, ul. Spokojna", "WGS84 φ N": "50.010000", "WGS84 λ E": "20.200000", "Gmina": "Kłaj", "Powiat": "wielicki", "Województwo": "MAŁOPOLSKIE"},
    {"Identyfikator stacji": 302, "Kod stacji": "MpKrakBulwar", "Nazwa stacji": "Kraków, Bulwarowa", "WGS84 φ N": "50.050000", "WGS84 λ E": "19.950000", "Gmina": "Kraków", "Powiat": "krakowski", "Województwo": "MAŁOPOLSKIE"}
  ]
}`

const aqIndexFixture = `{"AqIndex":{
  "Identyfikator stacji pomiarowej": 301,
  "Data wykonania obliczeń indeksu": "2026-09-26 16:25:16",
  "Wartość indeksu": 2,
  "Nazwa kategorii indeksu": "Umiarkowany",
  "Wartość indeksu dla wskaźnika PM10": 2,
  "Nazwa kategorii indeksu dla wskaźnika PM10": "Umiarkowany",
  "Wartość indeksu dla wskaźnika PM2.5": 1,
  "Nazwa kategorii indeksu dla wskaźnika PM2.5": "Dobry"
}}`

func giosServer(t *testing.T, levels, stations string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/rest/levels/getInformationAboutExceeding":
			_, _ = w.Write([]byte(levels))
		case r.URL.Path == "/v1/rest/station/findAll":
			_, _ = w.Write([]byte(stations))
		case strings.HasPrefix(r.URL.Path, "/v1/rest/aqindex/getIndex/"):
			_, _ = w.Write([]byte(aqIndexFixture))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func testSource(t *testing.T, srv *httptest.Server) *Source {
	t.Helper()
	var n yaml.Node
	cfg := fmt.Sprintf("base_url: %q\nzones: [\"strefa małopolska\", \"aglomeracja krakowska\"]\nmax_age: 24h\npoll_interval: 5m\n", srv.URL)
	if err := yaml.Unmarshal([]byte(cfg), &n); err != nil {
		t.Fatal(err)
	}
	p, err := New(&n, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p.(*Source)
}

type recordingEmitter struct {
	events []core.HazardEvent
	info   []core.InformationMessage
}

func (r *recordingEmitter) Emit(_ context.Context, ev core.HazardEvent) error {
	r.events = append(r.events, ev)
	return nil
}

func (r *recordingEmitter) EmitInformation(_ context.Context, m core.InformationMessage) error {
	r.info = append(r.info, m)
	return nil
}

// TestPollOnceFilteringEmits pins the whole pipeline: zone filter, max-age
// cutoff, severity mapping, station geocoding and stable identities.
func TestPollOnceFilteringEmits(t *testing.T) {
	srv := giosServer(t, levelsFixture(time.Now()), stationsFixture)
	s := testSource(t, srv)
	emit := &recordingEmitter{}

	if err := s.pollOnce(context.Background(), emit); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}

	// Records 1 (PI małopolska) and 2 (PA kraków) pass; record 3 is a
	// foreign zone, record 4 is older than max_age.
	if len(emit.events) != 2 {
		t.Fatalf("emitted = %d events, want 2:\n%+v", len(emit.events), emit.events)
	}
	first, second := emit.events[0], emit.events[1]
	if first.Source != sourceName || first.SourceID == "" {
		t.Errorf("first identity = %s/%s", first.Source, first.SourceID)
	}
	if first.Severity != "moderate" || second.Severity != "severe" {
		t.Errorf("severities = %s / %s, want moderate (PI) / severe (PA)", first.Severity, second.Severity)
	}
	if first.Latitude == nil || *first.Latitude != 50.01 || *first.Longitude != 20.2 {
		t.Errorf("first coordinates = %v/%v, want the Szarów station", first.Latitude, first.Longitude)
	}
	if !strings.Contains(first.Event, "O3") || !strings.Contains(second.Event, "PM10") {
		t.Errorf("pollutants = %q / %q, want O3 / PM10", first.Event, second.Event)
	}
	if !strings.Contains(first.Description, "Stacja pomiarowa: Szarów, ul. Spokojna") {
		t.Errorf("description missing station name: %q", first.Description)
	}
	if len(first.Areas) != 1 || first.Areas[0] != "strefa małopolska" {
		t.Errorf("areas = %v, want [strefa małopolska]", first.Areas)
	}
	if first.Key() == second.Key() {
		t.Errorf("two records share one identity")
	}

	// Re-polling the same feed keeps the identities stable (no duplicates).
	emit2 := &recordingEmitter{}
	if err := s.pollOnce(context.Background(), emit2); err != nil {
		t.Fatalf("second pollOnce: %v", err)
	}
	if len(emit2.events) != 2 || emit2.events[0].Key() != first.Key() || emit2.events[1].Key() != second.Key() {
		t.Errorf("second poll identities changed:\n%v", emit2.events)
	}
}

func TestNewDefaultsAndValidation(t *testing.T) {
	var n yaml.Node
	p, err := New(&n, nil)
	if err != nil {
		t.Fatalf("New with empty config: %v", err)
	}
	s := p.(*Source)
	if s.cfg.PollInterval != defaultPollInterval || s.cfg.MaxAge != defaultMaxAge || s.cfg.RequestTimeout != defaultRequestTimeout {
		t.Errorf("defaults = %+v", s.cfg)
	}
	if s.sev != (SeverityConfig{Alarm: "severe", Info: "moderate", Limit: "minor"}) {
		t.Errorf("default severities = %+v", s.sev)
	}
	if s.client.levelsURL != defaultLevelsURL || s.client.stationsURL != defaultStationsURL {
		t.Errorf("client urls = %s / %s", s.client.levelsURL, s.client.stationsURL)
	}

	cases := []struct {
		name, cfg, want string
	}{
		{"bad severity", "alarm_severity: nuclear\n", "unknown severity"},
		{"bad interval", "poll_interval: 10s\n", "poll_interval"},
		{"bad url", "base_url: ftp://x\n", "http(s)"},
	}
	for _, c := range cases {
		var node yaml.Node
		if err := yaml.Unmarshal([]byte(c.cfg), &node); err != nil {
			t.Fatal(err)
		}
		if _, err := New(&node, nil); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want mention of %q", c.name, err, c.want)
		}
	}
}

// TestNormSeverity pins the keyword mapping.
func TestNormSeverity(t *testing.T) {
	cases := []struct{ norm, want string }{
		{"Próg Informowania (PI) - Ochrona Zdrowia (OZ)", "moderate"},
		{"Poziom alarmowy (PA)", "severe"},
		{"Poziom dopuszczalny (PD)", "minor"},
		{"Poziom docelowy", "minor"},
		{"coś innego", "minor"},
	}
	sev := SeverityConfig{Alarm: "severe", Info: "moderate", Limit: "minor"}
	for _, c := range cases {
		if got := normSeverity(c.norm, sev.Alarm, sev.Info, sev.Limit); got != c.want {
			t.Errorf("normSeverity(%q) = %s, want %s", c.norm, got, c.want)
		}
	}
}

// TestNormalizeIdentityAndPollutant pins pollutant extraction and the
// stable identity hash.
func TestNormalizeIdentityAndPollutant(t *testing.T) {
	sev := SeverityConfig{Alarm: "severe", Info: "moderate", Limit: "minor"}
	rec := exceedance{
		NormType: "Poziom alarmowy (PA)",
		Zone:     "PL1201 aglomeracja krakowska",
		Station:  "MpKrakBulwar-PM10-2h",
		DateTime: "2026-09-26 10:00",
	}
	ev, err := normalize(rec, sev, "https://example.test")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Severity != "severe" || !strings.Contains(ev.Event, "PM10") {
		t.Errorf("event = %+v", ev)
	}
	ev2, err := normalize(rec, sev, "https://example.test")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Key() != ev2.Key() {
		t.Errorf("identity unstable: %s vs %s", ev.Key(), ev2.Key())
	}
	rec2 := rec
	rec2.DateTime = "2026-09-26 11:00"
	ev3, err := normalize(rec2, sev, "https://example.test")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Key() == ev3.Key() {
		t.Errorf("different hours share one identity")
	}
	if _, err := normalize(exceedance{NormType: "X", DateTime: "not a date"}, sev, ""); err == nil {
		t.Error("unparseable timestamp accepted")
	}
}

var _ plugin.SourcePlugin = (*Source)(nil)

// TestPollStationsPublishesAirQuality pins the informational station layer:
// only stations inside the hub area get an air_quality snapshot, with the
// station coordinates and the official index levels.
func TestPollStationsPublishesAirQuality(t *testing.T) {
	srv := giosServer(t, levelsFixture(time.Now()), stationsFixture)
	lat, lon := 50.0212, 20.2075
	hub, err := aprs.NewHub(aprs.HubConfig{
		Enabled: true, Callsign: "SP9TST-10", GridSquare: "KO00BA",
		Latitude: &lat, Longitude: &lon, RadiusKM: 30,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	var n yaml.Node
	cfg := fmt.Sprintf("base_url: %q\nair_index: true\nstation_poll_interval: 5m\nzones: [\"strefa małopolska\"]\n", srv.URL)
	if err := yaml.Unmarshal([]byte(cfg), &n); err != nil {
		t.Fatal(err)
	}
	p, err := New(&n, hub)
	if err != nil {
		t.Fatal(err)
	}
	s := p.(*Source)
	if !s.airIndex {
		t.Fatal("air_index not enabled")
	}

	emit := &recordingEmitter{}
	if err := s.pollStations(context.Background(), emit); err != nil {
		t.Fatalf("pollStations: %v", err)
	}
	if len(emit.info) != 2 {
		t.Fatalf("station snapshots = %d, want 2 (both fixture stations inside 30 km)", len(emit.info))
	}
	for _, m := range emit.info {
		if m.Kind != "air_quality" || m.Source != sourceName || m.Key == "" {
			t.Errorf("message identity = kind %s source %s key %q", m.Kind, m.Source, m.Key)
		}
		var payload aqPayload
		if err := json.Unmarshal(m.Payload, &payload); err != nil {
			t.Fatalf("payload: %v", err)
		}
		if payload.Latitude == 0 || payload.IndexLevelName == "" || payload.StationCode == "" {
			t.Errorf("payload incomplete: %+v", payload)
		}
	}

	// The station directory refreshed once; a second poll reuses it and
	// publishes the same two snapshots (stable keys).
	emit2 := &recordingEmitter{}
	if err := s.pollStations(context.Background(), emit2); err != nil {
		t.Fatalf("second pollStations: %v", err)
	}
	if len(emit2.info) != 2 || emit2.info[0].Key != emit.info[0].Key {
		t.Errorf("second poll changed identities: %+v", emit2.info)
	}
}
