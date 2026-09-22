package imgw

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/core"
)

const meteoFixture = `[
  {
    "id": "Sk20260921094811163",
    "nazwa_zdarzenia": "Intensywne opady deszczu",
    "stopien": "1",
    "prawdopodobienstwo": "80",
    "obowiazuje_do": "2026-09-23 00:00:00",
    "obowiazuje_od": "2026-09-21 18:00:00",
    "opublikowano": "2026-09-21 11:48:00",
    "tresc": "Prognozowane są   intensywne opady deszczu.",
    "komentarz": "Brak.",
    "biuro": "Biuro Prognoz Meteorologicznych w Krakowie",
    "teryt": ["1261", "1217", "1261"]
  }
]`

func meteoItems(t *testing.T, fixture string) []meteoWarning {
	t.Helper()
	var items []meteoWarning
	if err := json.Unmarshal([]byte(fixture), &items); err != nil {
		t.Fatalf("unmarshal meteo fixture: %v", err)
	}
	return items
}

func TestNormalizeMeteo(t *testing.T) {
	items := meteoItems(t, meteoFixture)
	if len(items) != 1 {
		t.Fatalf("fixture items = %d, want 1", len(items))
	}
	ev, err := normalizeMeteo(items[0], "https://danepubliczne.imgw.pl/api/data/warningsmeteo")
	if err != nil {
		t.Fatalf("normalizeMeteo: %v", err)
	}
	if ev.Source != sourceMeteo {
		t.Errorf("source = %q, want %q", ev.Source, sourceMeteo)
	}
	if ev.SourceID != "Sk20260921094811163" {
		t.Errorf("provider ID not preserved: %q", ev.SourceID)
	}
	if ev.Category != "met" || ev.Event != "Intensywne opady deszczu" || ev.Headline != "Intensywne opady deszczu" {
		t.Errorf("category/event/headline = %q/%q/%q", ev.Category, ev.Event, ev.Headline)
	}
	if ev.Severity != "moderate" {
		t.Errorf("severity = %q, want moderate", ev.Severity)
	}
	// Europe/Warsaw parsing with DST (September → CEST +02:00).
	if ev.EffectiveAt == nil || ev.EffectiveAt.Format("2006-01-02 15:04:05 -07:00") != "2026-09-21 18:00:00 +02:00" {
		t.Errorf("effective_at = %v, want 2026-09-21 18:00 +02:00", ev.EffectiveAt)
	}
	if ev.ExpiresAt == nil || ev.ExpiresAt.Format("2006-01-02 15:04:05 -07:00") != "2026-09-23 00:00:00 +02:00" {
		t.Errorf("expires_at = %v, want 2026-09-23 00:00 +02:00", ev.ExpiresAt)
	}
	wantAreas := []string{"teryt:1217", "teryt:1261"}
	if len(ev.Areas) != 2 || ev.Areas[0] != wantAreas[0] || ev.Areas[1] != wantAreas[1] {
		t.Errorf("areas = %v, want sorted + deduplicated %v", ev.Areas, wantAreas)
	}
	// Deterministic description: collapsed whitespace, probability present,
	// placeholder comment omitted.
	wantDesc := "Prognozowane są intensywne opady deszczu.\n\nPrawdopodobieństwo IMGW: 80%."
	if ev.Description != wantDesc {
		t.Errorf("description = %q, want %q", ev.Description, wantDesc)
	}
	if ev.Instruction != "" || ev.Urgency != "" || ev.Certainty != "" {
		t.Errorf("invented semantics: instruction=%q urgency=%q certainty=%q", ev.Instruction, ev.Urgency, ev.Certainty)
	}
	if ev.Status != core.StatusActive {
		t.Errorf("status = %q, want active", ev.Status)
	}
	if err := ev.Validate(); err != nil {
		t.Errorf("normalized event fails core validation: %v", err)
	}
}

func TestMeteoSeverityMapping(t *testing.T) {
	for level, want := range map[string]string{"1": "moderate", "2": "severe", "3": "extreme", "9": "unknown", "": "unknown"} {
		item := meteoItems(t, meteoFixture)[0]
		item.Stopien = level
		ev, err := normalizeMeteo(item, "u")
		if err != nil {
			t.Fatalf("level %q: %v", level, err)
		}
		if ev.Severity != want {
			t.Errorf("level %q → severity %q, want %q", level, ev.Severity, want)
		}
	}
}

func TestParseLocalTimeDST(t *testing.T) {
	summer, err := parseLocalTime("2026-07-15 12:00:00")
	if err != nil || summer.Format("-07:00") != "+02:00" {
		t.Errorf("summer = %v (%v), want CEST +02:00", summer, err)
	}
	winter, err := parseLocalTime("2026-01-15 12:00:00")
	if err != nil || winter.Format("-07:00") != "+01:00" {
		t.Errorf("winter = %v (%v), want CET +01:00", winter, err)
	}
	if _, err := parseLocalTime("not-a-time"); err == nil {
		t.Error("malformed timestamp must not silently become a time")
	}
}

func TestNormalizeMeteoMalformed(t *testing.T) {
	base := meteoItems(t, meteoFixture)[0]
	cases := []struct {
		name string
		mut  func(*meteoWarning)
	}{
		{"empty id", func(w *meteoWarning) { w.ID = "  " }},
		{"malformed effective", func(w *meteoWarning) { w.ObowiazujeOd = "banana" }},
		{"malformed expiry", func(w *meteoWarning) { w.ObowiazujeDo = "banana" }},
		{"empty event name", func(w *meteoWarning) { w.NazwaZdarzenia = "  " }},
		{"oversized teryt", func(w *meteoWarning) { w.Teryt = []string{strings.Repeat("1", 64)} }},
	}
	for _, c := range cases {
		item := base
		c.mut(&item)
		if _, err := normalizeMeteo(item, "u"); err == nil {
			t.Errorf("%s: expected rejection", c.name)
		}
	}
}
