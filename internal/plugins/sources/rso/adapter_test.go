package rso

import (
	"encoding/xml"
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// rsoFixture is a sanitized fixture based on the CURRENT live RSO XML
// schema (komunikaty.tvp.pl/komunikatyxml/...).
const rsoFixture = `<?xml version="1.0" encoding="utf-8"?>
<newses>
  <pagination_info totalItems="2" itemsPerPage="20"></pagination_info>
  <news>
    <id>23356508</id>
    <title>UTRUDNIENIA na S5</title>
    <shortcut>Krótki opis</shortcut>
    <content>Pełny opis utrudnień na drodze S5.</content>
    <rso_alarm>0</rso_alarm>
    <rso_icon></rso_icon>
    <valid_from>2026-09-22 16:46:00</valid_from>
    <valid_to>2026-09-22 19:46:00</valid_to>
    <repetition></repetition>
    <type></type>
    <created_at>2026-09-22 16:52:29</created_at>
    <updated_at>2026-09-22 16:52:29</updated_at>
    <provinces>
      <province id="2" slug="kujawsko-pomorskie" city="">Kujawsko-Pomorskie</province>
    </provinces>
  </news>
  <news>
    <id>23352653</id>
    <title>Intensywne opady deszczu</title>
    <shortcut>Woj. małopolskie, ostrzeżenie pierwszego stopnia</shortcut>
    <content>Województwo małopolskie powiaty: tatrzański.</content>
    <rso_alarm>0</rso_alarm>
    <valid_from>2026-09-21 11:50:00</valid_from>
    <valid_to>2026-09-23 00:00:00</valid_to>
    <provinces>
      <province id="6" slug="malopolskie">małopolskie</province>
    </provinces>
  </news>
</newses>`

func parseFixture(t *testing.T, fixture string) []newsItem {
	t.Helper()
	var list newsList
	if err := xml.Unmarshal([]byte(fixture), &list); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	return list.News
}

func TestNormalizeNews(t *testing.T) {
	items := parseFixture(t, rsoFixture)
	ev, err := normalizeNews(items[0], "https://komunikaty.tvp.pl/komunikatyxml/wszystkie/wszystkie/0?_format=xml", "wszystkie")
	if err != nil {
		t.Fatalf("normalizeNews: %v", err)
	}
	if ev.Source != sourceRSO {
		t.Errorf("source = %q, want %q", ev.Source, sourceRSO)
	}
	if ev.SourceID != "23356508" {
		t.Errorf("provider ID not preserved: %q", ev.SourceID)
	}
	if ev.Event != "UTRUDNIENIA na S5" || ev.Headline != "UTRUDNIENIA na S5" {
		t.Errorf("event/headline = %q/%q", ev.Event, ev.Headline)
	}
	// The fuller provider text (content) is preferred over the shortcut.
	if ev.Description != "Pełny opis utrudnień na drodze S5." {
		t.Errorf("description = %q", ev.Description)
	}
	if ev.Severity != "unknown" {
		t.Errorf("severity = %q, want unknown (RSO exposes no severity semantics)", ev.Severity)
	}
	if ev.Urgency != "" || ev.Certainty != "" || ev.Instruction != "" || ev.Category != "" {
		t.Errorf("invented semantics: urgency=%q certainty=%q instruction=%q category=%q", ev.Urgency, ev.Certainty, ev.Instruction, ev.Category)
	}
	// Naive Polish local timestamps parsed in Europe/Warsaw (DST-aware).
	if ev.EffectiveAt == nil || ev.EffectiveAt.Format("2006-01-02 15:04:05 -07:00") != "2026-09-22 16:46:00 +02:00" {
		t.Errorf("effective_at = %v, want 2026-09-22 16:46 +02:00", ev.EffectiveAt)
	}
	if ev.ExpiresAt == nil || ev.ExpiresAt.Format("2006-01-02 15:04:05 -07:00") != "2026-09-22 19:46:00 +02:00" {
		t.Errorf("expires_at = %v, want 2026-09-22 19:46 +02:00", ev.ExpiresAt)
	}
	wantAreas := []string{"wojewodztwo:kujawsko-pomorskie"}
	if len(ev.Areas) != 1 || ev.Areas[0] != wantAreas[0] {
		t.Errorf("areas = %v, want %v", ev.Areas, wantAreas)
	}
	if ev.Status != core.StatusActive {
		t.Errorf("status = %q, want active", ev.Status)
	}
	if !strings.Contains(ev.SourceURL, "komunikatyxml") {
		t.Errorf("source url = %q", ev.SourceURL)
	}
	if err := ev.Validate(); err != nil {
		t.Errorf("normalized event fails core validation: %v", err)
	}
}

func TestNormalizeNewsFallbackArea(t *testing.T) {
	items := parseFixture(t, rsoFixture)
	noProvinces := items[0]
	noProvinces.Provinces = nil
	ev, err := normalizeNews(noProvinces, "u", "malopolskie")
	if err != nil {
		t.Fatal(err)
	}
	if len(ev.Areas) != 1 || ev.Areas[0] != "wojewodztwo:malopolskie" {
		t.Errorf("fallback areas = %v, want [wojewodztwo:malopolskie]", ev.Areas)
	}
	// The national wildcard is never used as an affected area.
	ev, err = normalizeNews(noProvinces, "u", "wszystkie")
	if err != nil || len(ev.Areas) != 0 {
		t.Errorf("wszystkie fallback areas = %v (err %v), want none", ev.Areas, err)
	}
}

func TestNormalizeNewsMalformed(t *testing.T) {
	base := parseFixture(t, rsoFixture)[0]
	cases := []struct {
		name string
		mut  func(*newsItem)
	}{
		{"empty id", func(n *newsItem) { n.ID = "  " }},
		{"empty title", func(n *newsItem) { n.Title = "  " }},
		{"bad valid_from", func(n *newsItem) { n.ValidFrom = "banana" }},
		{"bad valid_to", func(n *newsItem) { n.ValidTo = "banana" }},
	}
	for _, c := range cases {
		item := base
		c.mut(&item)
		if _, err := normalizeNews(item, "u", "wszystkie"); err == nil {
			t.Errorf("%s: expected rejection", c.name)
		}
	}
	// Empty valid_to → indefinite (no expiry).
	item := base
	item.ValidTo = ""
	ev, err := normalizeNews(item, "u", "wszystkie")
	if err != nil || ev.ExpiresAt != nil {
		t.Errorf("empty valid_to: expires_at = %v (err %v), want nil", ev.ExpiresAt, err)
	}
}

// TestContentSignatureIgnoresAreas: area differences between regional
// feeds must not look like content conflicts.
func TestContentSignatureIgnoresAreas(t *testing.T) {
	items := parseFixture(t, rsoFixture)
	a := items[0]
	b := items[0]
	b.Provinces = []province{{ID: "6", Slug: "malopolskie", Name: "małopolskie"}}
	if contentSignature(a) != contentSignature(b) {
		t.Error("area-only differences changed the content signature")
	}
	b.Title = "Zupełnie inny tytuł"
	if contentSignature(a) == contentSignature(b) {
		t.Error("title differences did not change the content signature")
	}
}

// TestRSOXMLStructureValidation: wrong roots, malformed XML, pagination
// mismatches are never valid snapshots.
func TestRSOXMLStructureValidation(t *testing.T) {
	bad := []string{
		`<html><body>error</body></html>`,
		`<provinces></provinces>`,
		`<newses><news>`,
		``,
		`{"not":"xml"}`,
	}
	for _, body := range bad {
		var list newsList
		if err := xml.Unmarshal([]byte(body), &list); err == nil {
			t.Errorf("body %q decoded without error", body)
		}
	}
	// Valid empty snapshot: correct root + zero items + consistent count.
	empty := `<newses><pagination_info totalItems="0" itemsPerPage="20"></pagination_info></newses>`
	var list newsList
	if err := xml.Unmarshal([]byte(empty), &list); err != nil {
		t.Fatalf("valid empty snapshot rejected: %v", err)
	}
	if list.PaginationInfo.TotalItems != 0 || len(list.News) != 0 {
		t.Errorf("empty snapshot decoded as total=%d items=%d", list.PaginationInfo.TotalItems, len(list.News))
	}
}
