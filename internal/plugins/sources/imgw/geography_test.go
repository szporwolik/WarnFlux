package imgw

import (
	"testing"

	"github.com/szporwolik/WarnFlux/internal/geo"
)

func targetGeography(t *testing.T, include ...string) *geography {
	t.Helper()
	g, err := buildGeography(&GeographyConfig{
		Enabled: true,
		Include: include,
		// The local water keywords of THIS installation's test fixture;
		// the same values live in build/config.yaml.
		HydroLocalKeywords: []string{
			`niepolomic\w*`, `podlez\w*`, `wieliczk\w*`, `wielick\w*`, `krakow\w*`,
			`bochn\w*`, `klaj\w*`, `targowisko`, `szarow\w*`, `brzezie`, `gdow\w*`,
			`staniatk\w*`, `drwinka`, `seraf\w*`,
		},
	})
	if err != nil {
		t.Fatalf("buildGeography: %v", err)
	}
	return g
}

func TestBuildGeographyValidation(t *testing.T) {
	if _, err := buildGeography(nil); err != nil {
		t.Errorf("nil geography must be valid (disabled): %v", err)
	}
	if _, err := buildGeography(&GeographyConfig{Enabled: false}); err != nil {
		t.Errorf("disabled geography must be valid: %v", err)
	}
	if _, err := buildGeography(&GeographyConfig{Enabled: true}); err == nil {
		t.Error("enabled geography without include entries must fail")
	}
	for _, bad := range []string{"niepolomice", "gmina:bogus", "powiat:niepolomice", "miasto:wielicki"} {
		if _, err := buildGeography(&GeographyConfig{Enabled: true, Include: []string{bad}}); err == nil {
			t.Errorf("include %q accepted, want failure", bad)
		}
	}
	if _, err := buildGeography(&GeographyConfig{Enabled: true, Include: []string{"gmina:niepolomice", "gmina:niepolomice"}}); err == nil {
		t.Error("duplicate include accepted")
	}
}

func TestMeteoHierarchicalMatching(t *testing.T) {
	cases := []struct {
		name    string
		include []string
		teryt   []string
		want    bool
	}{
		// Configured gmina:niepolomice; warning for powiat wielicki covers it.
		{"ancestor warning matches", []string{"gmina:niepolomice"}, []string{"1219"}, true},
		// Exact gmina match.
		{"exact gmina", []string{"gmina:niepolomice"}, []string{"1219043"}, true},
		// The observed regression: powiat tatrzański is NOT relevant.
		{"tatrzański irrelevant", []string{"gmina:niepolomice", "miasto:krakow", "powiat:wielicki", "powiat:bochenski"}, []string{"1217"}, false},
		// Kraków target matches Kraków TERYT.
		{"krakow exact", []string{"miasto:krakow"}, []string{"1261011"}, true},
		// One relevant area among several wins.
		{"mixed areas", []string{"gmina:niepolomice"}, []string{"1217", "1219"}, true},
		// Unknown-only area never matches.
		{"unknown only", []string{"gmina:niepolomice"}, []string{"9999999"}, false},
		// Broad target includes descendants: powiat wielicki target with a
		// gmina-level warning inside it.
		{"descendant warning", []string{"powiat:wielicki"}, []string{"1219043"}, true},
	}
	for _, c := range cases {
		g := targetGeography(t, c.include...)
		got, _, err := g.matchesMeteo(c.teryt)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s: matchesMeteo = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestMeteoAreaEnrichment(t *testing.T) {
	g := targetGeography(t, "gmina:niepolomice")
	_, areas, err := g.matchesMeteo([]string{"1219", "1217", "1219", "9999999"})
	if err != nil {
		t.Fatalf("matchesMeteo: %v", err)
	}
	want := []string{
		"powiat:tatrzanski", "powiat:wielicki", "teryt:1217", "teryt:1219", "teryt:9999999",
	}
	if len(areas) != len(want) {
		t.Fatalf("areas = %v, want %v", areas, want)
	}
	for i := range want {
		if areas[i] != want[i] {
			t.Errorf("areas = %v, want %v", areas, want)
			break
		}
	}
}

func TestMeteoDuplicateTerytDeduplicated(t *testing.T) {
	g := targetGeography(t, "powiat:wielicki")
	_, areas, err := g.matchesMeteo([]string{"1219", "1219", "1219"})
	if err != nil {
		t.Fatalf("matchesMeteo: %v", err)
	}
	if len(areas) != 2 { // teryt:1219 + powiat:wielicki
		t.Fatalf("areas = %v, want 2 deduplicated entries", areas)
	}
}

func TestHydroMatching(t *testing.T) {
	g := targetGeography(t, "gmina:niepolomice", "miasto:krakow", "powiat:wielicki", "powiat:bochenski")
	cases := []struct {
		areas []string
		want  bool
	}{
		{[]string{"obszar:małopolskie, zlewnia Drwinki, Niepołomice"}, true},
		{[]string{"obszar:małopolskie, Kraków - Wisła"}, true},
		{[]string{"wojewodztwo:małopolskie", "obszar:zlewnia Dunajca, Nowy Targ"}, false},
		{[]string{"obszar:małopolskie"}, false}, // regional only: not sufficient
	}
	for _, c := range cases {
		if got := g.matchesHydro(c.areas); got != c.want {
			t.Errorf("matchesHydro(%v) = %v, want %v", c.areas, got, c.want)
		}
	}
}

// TestHydroPassesWithoutKeywords: an installation without
// hydro_local_keywords must not silently swallow hydrological warnings.
func TestHydroPassesWithoutKeywords(t *testing.T) {
	g, err := buildGeography(&GeographyConfig{Enabled: true, Include: []string{"powiat:tarnowski"}})
	if err != nil {
		t.Fatal(err)
	}
	if !g.matchesHydro([]string{"obszar:małopolskie, zlewnia Dunajca, Nowy Targ"}) {
		t.Error("hydro warning without configured keywords must pass through")
	}
}

// TestHydroRegionDerivedFromVoivodeshipTarget: the regional pattern is
// derived from the included voivodeship unit, never hardcoded.
func TestHydroRegionDerivedFromVoivodeshipTarget(t *testing.T) {
	g := targetGeography(t, "wojewodztwo:malopolskie")
	if g.matchesHydro([]string{"obszar:małopolskie"}) {
		t.Error("a plain voivodeship mention must not count as local")
	}
	if !g.matchesHydro([]string{"obszar:małopolskie, Niepołomice"}) {
		t.Error("a local keyword next to the voivodeship must match")
	}
}

// TestObservedRegressionSeverityAloneMustNotDeliver pins the real-world
// problem: "Intensywne opady deszczu, moderate, teryt:1217" (powiat
// tatrzański) must never enter the accepted snapshot for the
// Niepołomice/Kraków/Wieliczka/Bochnia target, and its human-readable
// resolution must remain available.
func TestObservedRegressionSeverityAloneMustNotDeliver(t *testing.T) {
	item := meteoWarning{
		ID:                 "Sk20260921094811163",
		NazwaZdarzenia:     "Intensywne opady deszczu",
		Stopien:            "1",
		ObowiazujeOd:       "2026-09-21 18:00:00",
		ObowiazujeDo:       "2026-09-23 00:00:00",
		Prawdopodobienstwo: "80",
		Tresc:              "Prognozowane są intensywne opady deszczu.",
		Teryt:              []string{"1217"},
	}
	ev, err := normalizeMeteo(item, "https://example.test/warningsmeteo")
	if err != nil {
		t.Fatalf("normalizeMeteo: %v", err)
	}
	if ev.Severity != "moderate" {
		t.Fatalf("severity = %q, want moderate", ev.Severity)
	}
	g := targetGeography(t, "gmina:niepolomice", "miasto:krakow", "powiat:wielicki", "powiat:bochenski")
	match, areas, err := g.matchesMeteo(item.Teryt)
	if err != nil {
		t.Fatalf("matchesMeteo: %v", err)
	}
	if match {
		t.Fatal("tatrzański moderate warning matched the local target geography")
	}
	// Human-readable resolution is available for diagnostics and display.
	if got := geo.Display("teryt:1217"); got != "powiat tatrzański (TERYT 1217)" {
		t.Errorf("Display(teryt:1217) = %q", got)
	}
	_ = areas
}
