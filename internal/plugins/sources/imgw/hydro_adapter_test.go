package imgw

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/core"
)

const hydroDroughtFixture = `[
  {
    "opublikowano": "2026-05-17 08:45:07",
    "stopień": "-1",
    "data_od": "2026-05-17 08:45:56",
    "data_do": "9999-12-31 23:59:59",
    "prawdopodobienstwo": "90",
    "numer": "31",
    "biuro": "Biuro Prognoz Hydrologicznych w Krakowie",
    "zdarzenie": "Susza hydrologiczna",
    "przebieg": "W zlewniach dopływów Wisły obserwuje się suszę hydrologiczną.",
    "komentarz": "",
    "obszary": [
      {
        "wojewodztwo": "wielkopolskie",
        "opis": "Kanał Mosiński, susza hydrologiczna",
        "kod_zlewni": ["Z_P_WP_1856"]
      }
    ]
  }
]`

func hydroItems(t *testing.T, fixture string) []hydroWarning {
	t.Helper()
	var items []hydroWarning
	if err := json.Unmarshal([]byte(fixture), &items); err != nil {
		t.Fatalf("unmarshal hydro fixture: %v", err)
	}
	return items
}

func TestNormalizeHydroDrought(t *testing.T) {
	items := hydroItems(t, hydroDroughtFixture)
	if len(items) != 1 {
		t.Fatalf("fixture items = %d, want 1", len(items))
	}
	ev, err := normalizeHydro(items[0], "https://danepubliczne.imgw.pl/api/data/warningshydro")
	if err != nil {
		t.Fatalf("normalizeHydro: %v", err)
	}
	if ev.Source != sourceHydro {
		t.Errorf("source = %q, want %q", ev.Source, sourceHydro)
	}
	if !strings.HasPrefix(ev.SourceID, "hydro:") || len(ev.SourceID) != len("hydro:")+64 {
		t.Errorf("source id = %q, want hydro:<64 hex>", ev.SourceID)
	}
	if ev.Event != "Susza hydrologiczna" || ev.Headline != "Susza hydrologiczna" {
		t.Errorf("event/headline = %q/%q", ev.Event, ev.Headline)
	}
	// -1 is the documented ungraded drought: severity unknown, never a
	// fake level.
	if ev.Severity != "unknown" {
		t.Errorf("severity = %q, want unknown (ungraded drought)", ev.Severity)
	}
	// year-9999 sentinel → indefinite warning, no expiry.
	if ev.ExpiresAt != nil {
		t.Errorf("expires_at = %v, want nil for the 9999 sentinel", ev.ExpiresAt)
	}
	if ev.EffectiveAt == nil || ev.EffectiveAt.Format("2006-01-02 15:04:05 -07:00") != "2026-05-17 08:45:56 +02:00" {
		t.Errorf("effective_at = %v, want 2026-05-17 08:45:56 +02:00", ev.EffectiveAt)
	}
	wantAreas := []string{
		"obszar:wielkopolskie, Kanał Mosiński, susza hydrologiczna",
		"wojewodztwo:wielkopolskie",
		"zlewnia:Z_P_WP_1856",
	}
	if len(ev.Areas) != 3 {
		t.Fatalf("areas = %v, want %v", ev.Areas, wantAreas)
	}
	for i := range wantAreas {
		if ev.Areas[i] != wantAreas[i] {
			t.Errorf("areas[%d] = %q, want %q", i, ev.Areas[i], wantAreas[i])
		}
	}
	if !strings.Contains(ev.Description, "Prawdopodobieństwo IMGW: 90%.") {
		t.Errorf("description lacks probability: %q", ev.Description)
	}
	if !strings.Contains(ev.Description, "suszę hydrologiczną") {
		t.Errorf("description lacks przebieg text: %q", ev.Description)
	}
	if ev.Status != core.StatusActive {
		t.Errorf("status = %q, want active", ev.Status)
	}
	if err := ev.Validate(); err != nil {
		t.Errorf("normalized event fails core validation: %v", err)
	}
}

func TestHydroSeverityMapping(t *testing.T) {
	for level, want := range map[string]string{"1": "moderate", "2": "severe", "3": "extreme", "-1": "unknown", "7": "unknown", "": "unknown"} {
		item := hydroItems(t, hydroDroughtFixture)[0]
		item.Stopien = level
		ev, err := normalizeHydro(item, "u")
		if err != nil {
			t.Fatalf("level %q: %v", level, err)
		}
		if ev.Severity != want {
			t.Errorf("level %q → severity %q, want %q", level, ev.Severity, want)
		}
	}
}

func TestHydroFiniteExpiry(t *testing.T) {
	item := hydroItems(t, hydroDroughtFixture)[0]
	item.DataDo = "2026-05-20 12:00:00"
	ev, err := normalizeHydro(item, "u")
	if err != nil {
		t.Fatalf("normalizeHydro: %v", err)
	}
	if ev.ExpiresAt == nil || ev.ExpiresAt.Format("2006-01-02 15:04:05 -07:00") != "2026-05-20 12:00:00 +02:00" {
		t.Errorf("expires_at = %v, want 2026-05-20 12:00 +02:00", ev.ExpiresAt)
	}
	// Empty data_do → indefinite.
	item.DataDo = ""
	ev, err = normalizeHydro(item, "u")
	if err != nil || ev.ExpiresAt != nil {
		t.Errorf("empty data_do: expires_at = %v (err %v), want nil", ev.ExpiresAt, err)
	}
}

// TestHydroIdentityStable: ordinary content updates must not change the
// derived identity — only the identity fields (year of data_od, numer,
// normalized biuro) participate.
func TestHydroIdentityStable(t *testing.T) {
	base := hydroItems(t, hydroDroughtFixture)[0]
	baseID, err := hydroSourceID(base)
	if err != nil {
		t.Fatalf("base identity: %v", err)
	}

	// Mutations that must NOT change identity.
	variants := []func(*hydroWarning){
		func(w *hydroWarning) { w.Przebieg = "Zupełnie inny przebieg zdarzenia." },
		func(w *hydroWarning) { w.Komentarz = "Nowy komentarz biura." },
		func(w *hydroWarning) { w.Prawdopodobienstwo = "55" },
		func(w *hydroWarning) { w.DataDo = "2026-06-01 00:00:00" },
		func(w *hydroWarning) { w.Obszary = []hydroArea{{Wojewodztwo: "małopolskie"}} },
	}
	for i, mut := range variants {
		w := base
		mut(&w)
		id, err := hydroSourceID(w)
		if err != nil {
			t.Fatalf("variant %d: %v", i, err)
		}
		if id != baseID {
			t.Errorf("variant %d changed the identity: %q → %q", i, baseID, id)
		}
	}

	// Biuro normalization: case and whitespace must not matter.
	w := base
	w.Biuro = "  BIURO PROGNOZ   HYDROLOGICZNYCH W KRAKOWIE "
	id, err := hydroSourceID(w)
	if err != nil || id != baseID {
		t.Errorf("normalized biuro changed identity: %q (err %v)", id, err)
	}

	// Identity fields DO change the identity.
	if id, _ := hydroSourceID(func() hydroWarning { w := base; w.Numer = "32"; return w }()); id == baseID {
		t.Error("changed numer did not change the identity")
	}
	if id, _ := hydroSourceID(func() hydroWarning { w := base; w.DataOd = "2027-05-17 08:45:56"; return w }()); id == baseID {
		t.Error("changed year did not change the identity")
	}
	if id, _ := hydroSourceID(func() hydroWarning { w := base; w.Biuro = "Inne Biuro"; return w }()); id == baseID {
		t.Error("changed biuro did not change the identity")
	}
}

func TestHydroIdentityRequiresFields(t *testing.T) {
	cases := []func(*hydroWarning){
		func(w *hydroWarning) { w.DataOd = "banana" },
		func(w *hydroWarning) { w.Numer = " " },
		func(w *hydroWarning) { w.Biuro = "   " },
	}
	for i, mut := range cases {
		w := hydroItems(t, hydroDroughtFixture)[0]
		mut(&w)
		if _, err := hydroSourceID(w); err == nil {
			t.Errorf("case %d: missing/malformed identity fields accepted", i)
		}
	}
}
