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
		if name != "EMCOM" && name != "Alert RCB" {
			if pl, en := EventName("pl", name), EventName("en", name); pl == en {
				t.Errorf("event type %q (%s) is not translated", name, producer)
			}
		}
	}
}
