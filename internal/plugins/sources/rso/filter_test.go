package rso

import (
	"testing"
)

// defaultPolicy mirrors buildFilterConfig's defaults for the high-signal
// target configuration.
func defaultPolicy() filterConfig {
	p, err := buildFilterConfig(&FileFilterConfig{HighSignalOnly: boolPtr(true)})
	if err != nil {
		panic(err)
	}
	return *p
}

func boolPtr(b bool) *bool { return &b }

func item(title, shortcut, content string) newsItem {
	return newsItem{ID: "1", Title: title, Shortcut: shortcut, Content: content}
}

func TestRCBExcluded(t *testing.T) {
	p := defaultPolicy()
	for _, title := range []string{
		"ALERT RCB",
		"Alert RCB",
		"ALERT-RCB",
		"ALERT -RCB",
		"RCB",
	} {
		if d := p.decide(item(title, "", "")); d.emit {
			t.Errorf("RCB title %q emitted", title)
		}
	}
	// RCB present only in content.
	if d := p.decide(item("Uwaga", "", "Treść komunikatu RCB dla województwa")); d.emit {
		t.Error("RCB in content emitted")
	}
	// RCB filtering is unconditional even for a serious event.
	if d := p.decide(item("Alert RCB: poważne zagrożenie życia", "", "ewakuacja")); d.emit {
		t.Error("serious RCB message emitted")
	}
}

func TestAirQualityExcluded(t *testing.T) {
	p := defaultPolicy()
	cases := []string{
		"Przekroczony poziom PM10",
		"Informacja o pyłach PM2.5",
		"Alert smogowy",
		"Zła jakość powietrza",
		"Poziom informowania dla pyłu zawieszonego",
		"Poziom alarmowy PM10",
	}
	for _, c := range cases {
		if d := p.decide(item(c, "", "")); d.emit {
			t.Errorf("air-quality %q emitted", c)
		}
	}
}

func TestAirQualityDoesNotHideCivilProtection(t *testing.T) {
	p := defaultPolicy()
	for _, c := range []string{
		"Pożar hali przemysłowej, toksyczny dym nad okolicą",
		"Uwolnienie substancji chemicznej, zagrożenie dla mieszkańców",
	} {
		d := p.decide(item(c, "", "Ewakuacja mieszkańców"))
		if !d.emit || d.severity != "severe" {
			t.Errorf("civil-protection %q: emit=%v severity=%q, want emitted severe", c, d.emit, d.severity)
		}
	}
}

func TestRSOAlarmNeverChangesSeverity(t *testing.T) {
	p := defaultPolicy()
	base := item("Brak przydatności wody do spożycia w gminie Niepołomice", "", "")
	alarm1 := base
	alarm1.RSOAlarm = "1"
	base.RSOAlarm = "0"
	if d1, d2 := p.decide(alarm1), p.decide(base); d1.severity != d2.severity {
		t.Errorf("rso_alarm changed severity: %q vs %q", d1.severity, d2.severity)
	}
}

func TestWarningDegreeMapping(t *testing.T) {
	cases := map[string]string{
		"Ostrzeżenie pierwszego stopnia dla województwa małopolskiego": "moderate",
		"Ostrzeżenie 1 stopnia":                "moderate",
		"stopień: 1":                           "moderate",
		"stopień zagrożenia: 1":                "moderate",
		"Ostrzeżenie drugiego stopnia":         "severe",
		"2 stopień zagrożenia":                 "severe",
		"stopień: 2":                           "severe",
		"Ostrzeżenie trzeciego stopnia":        "extreme",
		"3 stopień zagrożenia hydrologicznego": "extreme",
		"stopień: 3":                           "extreme",
	}
	for text, want := range cases {
		if got := classifySeverity(normalizeMatchText(text)); got != want {
			t.Errorf("classifySeverity(%q) = %q, want %q", text, got, want)
		}
	}
}

func TestUnrelatedNumbersAreNotDegrees(t *testing.T) {
	for _, text := range []string{
		"Utrudnienia na ulicy Krakowskiej 1/2",
		"Awaria na linii nr 2, opóźnienia do 20 minut",
		"Temperatura spadnie do -2 stopni",
		"Trwa modernizacja odcinka 3,4 km",
		"20 stopień zasilania energetycznego",
	} {
		switch got := classifySeverity(normalizeMatchText(text)); got {
		case "moderate", "severe", "extreme":
			t.Errorf("classifySeverity(%q) = %q, want unknown/minor (unrelated number)", text, got)
		}
	}
}

func TestRoadSeverity(t *testing.T) {
	cases := []struct {
		text string
		want string
	}{
		{"Wypadek na A4, 467 km, Bochnia, droga zablokowana", "severe"},
		{"A4 435 km, zablokowany jeden pas", "moderate"},
		{"A4 zablokowana, 97 km, kierunek Wrocław", "severe"}, // severity regardless of geography
		{"DK75 Niepołomice, ruch wstrzymany po wypadku", "severe"},
		{"DW964 zamknięta w obu kierunkach z powodu remontu", "severe"}, // complete closure wins
		{"A4 Balice-Targowisko, koszenie traw, zajęty jeden pas", "minor"},
		{"Droga gminna zamknięta w obu kierunkach, objazd", "moderate"},
		{"Kolizja dwóch aut na wjeździe do Krakowa", "moderate"},
	}
	for _, c := range cases {
		if got := classifySeverity(normalizeMatchText(c.text)); got != c.want {
			t.Errorf("classifySeverity(%q) = %q, want %q", c.text, got, c.want)
		}
	}
}

func TestWaterSeverity(t *testing.T) {
	cases := map[string]string{
		"brak przydatności wody do spożycia":                     "severe",
		"woda nieprzydatna do spożycia":                          "severe",
		"woda niezdatna do spożycia":                             "severe",
		"zanieczyszczenie mikrobiologiczne wody":                 "severe",
		"woda nadaje się do spożycia wyłącznie po przegotowaniu": "severe",
		"warunkowa przydatność wody do spożycia":                 "moderate",
		"przekroczenie parametrów jakości wody":                  "moderate",
	}
	for text, want := range cases {
		if got := classifySeverity(normalizeMatchText(text)); got != want {
			t.Errorf("classifySeverity(%q) = %q, want %q", text, got, want)
		}
	}
}

func TestHydrologySeverity(t *testing.T) {
	cases := map[string]string{
		"stan ostrzegawczy na Wiśle":                  "moderate",
		"przekroczony stan alarmowy na Rabie":         "severe",
		"Zagrożenie powodziowe dla gminy Niepołomice": "severe",
		"susza hydrologiczna":                         "unknown", // low signal, filtered by threshold
	}
	for text, want := range cases {
		if got := classifySeverity(normalizeMatchText(text)); got != want {
			t.Errorf("classifySeverity(%q) = %q, want %q", text, got, want)
		}
	}
}

func TestIMGWDuplicateSuppression(t *testing.T) {
	p := defaultPolicy()
	p.suppressIMGWDupes = true
	if d := p.decide(item("IMGW-PIB wydał ostrzeżenie meteorologiczne", "", "Prognozowane burze z gradem.")); d.emit {
		t.Error("plain IMGW duplicate emitted")
	}
	// A distinct civil-protection consequence mentioning IMGW stays.
	d := p.decide(item("IMGW ostrzega przed burzami", "", "Zalecana ewakuacja mieszkańców zagrożonych terenów"))
	if !d.emit {
		t.Error("IMGW mention with evacuation consequence suppressed")
	}
}

func TestGeographicScopeAndThresholds(t *testing.T) {
	p := defaultPolicy()
	cases := []struct {
		text string
		sev  string
		want bool
	}{
		// Core area: moderate passes.
		{"Wypadek w Niepołomicach, droga zablokowana", "moderate", true},
		{"Utrudnienia w ruchu w Podłężu", "moderate", true},
		{"Prace drogowe w Staniątkach", "minor", false},
		// Cities.
		{"Awaria wodociągu w Krakowie", "severe", true},
		{"Utrudnienia w ruchu w Wieliczce", "moderate", true},
		{"Ćwiczenia straży pożarnej w Bochni", "minor", false},
		// Corridor.
		{"A4 zablokowana, 467 km, Bochnia-Brzesko", "severe", true},
		{"A4 435 km, zablokowany jeden pas", "moderate", true},
		{"A4 zablokowana, 97 km, kierunek Wrocław", "severe", false},
		// Whole voivodeship: only severe+.
		{"Ostrzeżenie pierwszego stopnia dla województwa małopolskiego", "moderate", false},
		{"Wyciek gazu, zagrożenie dla mieszkańców całego województwa", "severe", true},
		{"Test syren w całej Małopolsce", "minor", false},
		// Regional severe passes, regional moderate does not.
		{"Wypadek w Nowym Targu, ruch wstrzymany", "severe", true},
	}
	for _, c := range cases {
		d := p.decide(item(c.text, "", ""))
		// The classifier decides severity; only assert emit where the
		// table pins severity explicitly.
		if d.severity != c.sev {
			t.Errorf("%q: severity = %q, want %q", c.text, d.severity, c.sev)
			continue
		}
		if d.emit != c.want {
			t.Errorf("%q: emit = %v, want %v (reason %q)", c.text, d.emit, c.want, d.reason)
		}
	}
}

func TestLocalPlacesEmit(t *testing.T) {
	p := defaultPolicy()
	for _, place := range []string{
		"Niepołomice", "Podłęże", "Staniątki", "Wola Batorska",
		"Wieliczka", "Bochnia", "Kraków",
	} {
		text := "Zdarzenie w " + place + ", utrudnienia w ruchu"
		d := p.decide(item(text, "", ""))
		if !d.emit {
			t.Errorf("%q not emitted (severity %q)", text, d.severity)
		}
	}
}

func TestInformationalMessagesClassifyNaturally(t *testing.T) {
	p := defaultPolicy()
	for _, text := range []string{
		"Test syren alarmowych w gminie",
		"Ćwiczenia obronne",
		"Szczepienie lisów przeciwko wściekliźnie",
		"Uwaga hałas w związku z próbą systemu alarmowania",
	} {
		d := p.decide(item(text, "", ""))
		if d.severity != "minor" && d.severity != "unknown" {
			t.Errorf("%q classified as %q, want minor/unknown", text, d.severity)
		}
		if d.emit {
			t.Errorf("%q emitted despite low severity", text)
		}
	}
}

func TestAreaEnrichment(t *testing.T) {
	p := defaultPolicy()
	d := p.decide(item("Wypadek na A4, 467 km, Bochnia-Brzesko, droga zablokowana", "", ""))
	want := []string{"corridor:a4-balice-tarnow", "droga:a4", "miasto:bochnia"}
	for _, w := range want {
		found := false
		for _, a := range d.areas {
			if a == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("areas %v missing %q", d.areas, w)
		}
	}

	d2 := p.decide(item("Brak przydatności wody do spożycia w Niepołomicach", "", ""))
	if d2.severity != "severe" || !d2.emit {
		t.Fatalf("water event: severity=%q emit=%v", d2.severity, d2.emit)
	}
	for _, w := range []string{"gmina:niepolomice", "powiat:wielicki"} {
		found := false
		for _, a := range d2.areas {
			if a == w {
				found = true
			}
		}
		if !found {
			t.Errorf("water event areas %v missing %q", d2.areas, w)
		}
	}
}

func TestCategoryClassification(t *testing.T) {
	cases := map[string]string{
		"Wypadek na A4, droga zablokowana":     "road",
		"Brak przydatności wody do spożycia":   "water",
		"Przekroczony stan alarmowy na Wiśle":  "hydrology",
		"IMGW ostrzega przed burzami z gradem": "weather",
		"Pożar składowiska odpadów, ewakuacja": "civil-protection",
		"Spotkanie mieszkańców z burmistrzem":  "",
	}
	for text, want := range cases {
		if got := classifyCategory(normalizeMatchText(text)); got != want {
			t.Errorf("classifyCategory(%q) = %q, want %q", text, got, want)
		}
	}
}

func TestKilometreExtraction(t *testing.T) {
	got := extractKilometres("A4 435 km, dalej 435,6 km, 437.5 km, km 442+600, 450+200 i 97 km kierunek Wrocław")
	want := []int{97, 435, 437, 442, 450}
	if len(got) != len(want) {
		t.Fatalf("extractKilometres = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("extractKilometres = %v, want %v", got, want)
			break
		}
	}
}
