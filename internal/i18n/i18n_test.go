package i18n

import (
	"net/http"
	"net/http/httptest"
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
