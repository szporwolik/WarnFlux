package i18n

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestT(t *testing.T) {
	if got := T(LangEN, "nav.dashboard"); got != "Dashboard" {
		t.Fatalf("T(en, nav.dashboard) = %q", got)
	}
	if got := T(LangPL, "nav.dashboard"); got != "Pulpit" {
		t.Fatalf("T(pl, nav.dashboard) = %q", got)
	}
	// Unknown language falls back to English.
	if got := T("xx", "nav.dashboard"); got != "Dashboard" {
		t.Fatalf("T(xx, nav.dashboard) = %q", got)
	}
	// Unknown key falls back to the key itself.
	if got := T(LangPL, "no.such.key"); got != "no.such.key" {
		t.Fatalf("T(pl, missing) = %q", got)
	}
	// Formatted keys keep their verbs.
	if got := T(LangEN, "emcom.level"); got != "level %d" {
		t.Fatalf("T(en, emcom.level) = %q", got)
	}
	if got := T(LangPL, "emcom.level"); got != "poziom %d" {
		t.Fatalf("T(pl, emcom.level) = %q", got)
	}
}

func TestSupported(t *testing.T) {
	for _, code := range []string{LangEN, LangPL} {
		if !Supported(code) {
			t.Fatalf("Supported(%q) = false", code)
		}
	}
	for _, code := range []string{"de", "xx", "en-US", ""} {
		if Supported(code) {
			t.Fatalf("Supported(%q) = true", code)
		}
	}
}

func TestFromRequest(t *testing.T) {
	cases := []struct {
		name           string
		cookie         string
		acceptLanguage string
		want           string
	}{
		{name: "default english", want: LangEN},
		{name: "cookie pl", cookie: LangPL, want: LangPL},
		{name: "cookie en over header pl", cookie: LangEN, acceptLanguage: "pl", want: LangEN},
		{name: "cookie invalid falls to header", cookie: "de", acceptLanguage: "pl-PL", want: LangPL},
		{name: "header pl", acceptLanguage: "pl-PL,pl;q=0.9,en;q=0.8", want: LangPL},
		{name: "header en first", acceptLanguage: "en-US,en;q=0.9", want: LangEN},
		{name: "header other falls back", acceptLanguage: "de-DE", want: LangEN},
		{name: "header wildcard", acceptLanguage: "*", want: LangEN},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.cookie != "" {
				r.AddCookie(&http.Cookie{Name: "wf_lang", Value: tc.cookie})
			}
			if tc.acceptLanguage != "" {
				r.Header.Set("Accept-Language", tc.acceptLanguage)
			}
			if got := FromRequest(r); got != tc.want {
				t.Fatalf("FromRequest() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestEventName pins the canonical event-type translation: a built-in
// producer stamps one machine identity ("Road works"), and every channel
// that shows it to a human renders it in that reader's language.
func TestEventName(t *testing.T) {
	cases := []struct {
		name      string
		lang      string
		canonical string
		want      string
	}{
		{"english", "en", "Road works", "Road works"},
		{"polish", "pl", "Road works", "Roboty drogowe"},
		{"case insensitive", "pl", "  road closure ", "Droga zamknięta"},
		{"traffic", "pl", "Alternating traffic", "Ruch wahadłowy"},
		{"accident", "pl", "Industrial accident", "Awaria przemysłowa"},
		{"chemical", "pl", "Chemical release", "Uwolnienie substancji chemicznej"},
		{"qualifier kept", "pl", "Air quality exceedance (PM10)", "Przekroczenie jakości powietrza (PM10)"},
		{"unknown unchanged", "pl", "Burze z gradem", "Burze z gradem"},
		{"operator text unchanged", "pl", "Powódź na Wiśle", "Powódź na Wiśle"},
		{"empty", "pl", "", ""},
		{"unsupported language falls back", "de", "Road works", "Road works"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := EventName(tc.lang, tc.canonical); got != tc.want {
				t.Errorf("EventName(%q, %q) = %q, want %q", tc.lang, tc.canonical, got, tc.want)
			}
		})
	}
}

// TestEventSuggestionsAreTranslated pins the compose form's suggestions: the
// names offered there are the ones every channel already translates, so a
// suggestion an operator picks never reaches a Polish reader in English (an
// event type typed by hand does — that is how "Brak" once went out).
func TestEventSuggestionsAreTranslated(t *testing.T) {
	pl := EventSuggestions("pl")
	if len(pl) < 10 {
		t.Fatalf("suggestions = %d, want a usable vocabulary", len(pl))
	}
	seen := map[string]bool{}
	for _, s := range pl {
		if s.Value == "" || s.Label == "" {
			t.Errorf("suggestion %+v is empty", s)
		}
		if seen[s.Value] {
			t.Errorf("suggestion %q is offered twice", s.Value)
		}
		seen[s.Value] = true
		if !Supported("pl") {
			continue
		}
		// The label is the translation, so it differs from the canonical
		// name — except for the names that are deliberately identical.
		if s.Value != "EMCOM" && s.Value != "Test" && s.Label == s.Value {
			t.Errorf("suggestion %q has no Polish rendering", s.Value)
		}
		if got := EventName("pl", s.Value); got != s.Label {
			t.Errorf("suggestion %q label = %q, want EventName %q", s.Value, s.Label, got)
		}
	}
	// Sorted by the label the operator reads.
	for i := 1; i < len(pl); i++ {
		if pl[i-1].Label > pl[i].Label {
			t.Errorf("suggestions are not sorted: %q before %q", pl[i-1].Label, pl[i].Label)
		}
	}
	// The compose vocabulary is part of it: a message composed from a
	// template must read in Polish everywhere.
	for _, name := range []string{"Information", "Hazard", "Weather alert", "Test"} {
		if !seen[name] {
			t.Errorf("compose vocabulary %q is missing from the suggestions", name)
		}
	}
}

// TestCanonicalEventTypesHaveTranslations pins the producer contract: every
// event type a built-in producer stamps on a hazard — listed with the file
// that stamps it — must have a translation entry, so no canonical English
// type can leak into a Polish mail, SMS, mesh, Discord message or page.
// (Provider text such as IMGW/RSO names is deliberately passed through.)
func TestCanonicalEventTypesHaveTranslations(t *testing.T) {
	producers := map[string]string{
		"Road works":             "internal/plugins/sources/gddkia/adapter.go",
		"Road closure":           "internal/plugins/sources/gddkia/adapter.go",
		"Alternating traffic":    "internal/plugins/sources/gddkia/adapter.go",
		"Industrial accident":    "internal/plugins/sources/gios/adapter.go",
		"Chemical release":       "internal/plugins/sources/gios/adapter.go",
		"Air quality exceedance": "internal/plugins/sources/giosaq/adapter.go",
		"EMCOM":                  "internal/web/emcom.go",
		"SMS alert":              "internal/gsm/commands.go",
		"Alert RCB":              "internal/gsm/commands.go",
		"APRS alert":             "internal/aprs/hub.go",
		"Information":            "internal/web/compose.go (template prefill)",
		"Hazard":                 "internal/web/compose.go (template prefill)",
		"Weather alert":          "internal/web/compose.go (template prefill)",
		"Test":                   "internal/web/compose.go (template prefill)",
	}
	for name, producer := range producers {
		if _, ok := eventNameKeys[strings.ToLower(name)]; !ok {
			t.Errorf("event type %q (%s) has no translation entry", name, producer)
			continue
		}
		if got := EventName("pl", name); got == "" {
			t.Errorf("event type %q (%s) has no Polish rendering", name, producer)
		}
		// Names that are identical in both languages on purpose.
		if name != "EMCOM" && name != "Alert RCB" && name != "Test" {
			if pl, en := EventName("pl", name), EventName("en", name); pl == en {
				t.Errorf("event type %q (%s) is not translated", name, producer)
			}
		}
	}
}
