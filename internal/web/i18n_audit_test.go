package web

import (
	"bytes"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/i18n"
)

// TestTemplateKeysTranslated pins the server-side translation coverage:
// every literal key used through tr/trf/trh in any template must resolve
// in BOTH supported languages (a missing key would leak the raw key into
// the page).
func TestTemplateKeysTranslated(t *testing.T) {
	keyRe := regexp.MustCompile(`tr[fh]?\s+(?:\.Lang|\$\.Lang|\$)?\s*"([a-zA-Z0-9._-]+)"`)
	seen := map[string]bool{}
	entries, err := templatesFS.ReadDir("templates")
	if err != nil {
		t.Fatalf("read templates dir: %v", err)
	}
	for _, e := range entries {
		name := "templates/" + e.Name()
		src, err := templatesFS.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, m := range keyRe.FindAllSubmatch(src, -1) {
			seen[string(m[1])] = true
		}
	}
	if len(seen) < 100 {
		t.Fatalf("suspiciously few template keys extracted: %d", len(seen))
	}
	for key := range seen {
		if i18n.T(i18n.LangEN, key) == key {
			t.Errorf("template key %q missing in the EN catalog", key)
		}
		if i18n.T(i18n.LangPL, key) == key {
			t.Errorf("template key %q missing in the PL catalog", key)
		}
	}

	// The dynamic printf-built keys must have concrete variants in both
	// catalogs. %d bases carry numbered variants (0-4); %s bases carry
	// the known concrete values.
	dynRe := regexp.MustCompile(`printf "([a-zA-Z0-9._-]+)\.(%[sd])"`)
	strVariants := map[string][]string{
		"compose.status": {"active", "expired"},
		"notif.outcome":  {"delivered", "failed", "skipped", "submitted", "unconfirmed"},
		"notif.status":   {"saved", "running", "failed", "succeeded", "accepted", "confirmed", "unconfirmed", "expired"},
		"config.mqtt":    {"events", "active", "info", "status", "save"},
		"lang":           {"en", "pl"},
	}
	for _, e := range entries {
		name := "templates/" + e.Name()
		src, err := templatesFS.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, m := range dynRe.FindAllSubmatch(src, -1) {
			base, spec := string(m[1]), string(m[2])
			for _, lang := range []string{i18n.LangEN, i18n.LangPL} {
				found := false
				variants := []string{}
				if spec == "%d" {
					for i := 0; i <= 4; i++ {
						variants = append(variants, strconv.Itoa(i))
					}
				} else {
					variants = strVariants[base]
				}
				for _, v := range variants {
					if i18n.T(lang, base+"."+v) != base+"."+v {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("dynamic key base %q (%s) has no concrete variants in %s", base, spec, lang)
				}
			}
		}
	}
}

// TestClientDictParity pins the client-side I18N tables in app.js: the
// en and pl dictionaries must carry the SAME key set, and every tr()/trf()
// call with a literal key must resolve in the dictionary (a missing key
// would leak the raw key into the UI).
func TestClientDictParity(t *testing.T) {
	src, err := os.ReadFile("static/app.js")
	if err != nil {
		t.Fatalf("read app.js: %v", err)
	}
	text := string(src)

	iEn := strings.Index(text, "en: {")
	iPl := strings.Index(text, "pl: {")
	iEnd := strings.Index(text[iPl:], "\n  };") + iPl
	if iEn < 0 || iPl < 0 || iEnd <= iPl {
		t.Fatalf("I18N dict boundaries not found (en=%d pl=%d end=%d)", iEn, iPl, iEnd)
	}
	keyRe := regexp.MustCompile(`(?m)^\s*"([a-z0-9._-]+)":\s*"`)
	collect := func(block string) map[string]bool {
		out := map[string]bool{}
		for _, m := range keyRe.FindAllSubmatch([]byte(block), -1) {
			out[string(m[1])] = true
		}
		return out
	}
	en := collect(text[iEn:iPl])
	pl := collect(text[iPl:iEnd])
	if len(en) < 50 || len(pl) < 50 {
		t.Fatalf("suspiciously small client dicts: en=%d pl=%d", len(en), len(pl))
	}
	for k := range en {
		if !pl[k] {
			t.Errorf("client key %q missing in the pl dict", k)
		}
	}
	for k := range pl {
		if !en[k] {
			t.Errorf("client key %q missing in the en dict", k)
		}
	}

	// Every literal tr/trf key must exist in the en dict.
	useRe := regexp.MustCompile(`trf?\("([a-z0-9._-]+)"\)`)
	for _, m := range useRe.FindAllSubmatch(src, -1) {
		key := string(m[1])
		if !en[key] {
			t.Errorf("client code uses %q but neither dict defines it", key)
		}
	}
}

// TestClientHelpersAreNotShadowed pins the other half of the tr()/trf()
// contract: a local (or parameter) named tr/trf hides the global helper, so
// every "tr(...)" in that scope throws a TypeError at runtime instead of
// returning a string. That is exactly how the MQTT browser broke: a row
// variable named "tr" in the render loop made the first cell throw, the
// promise chain rejected and the panel showed only "browse failed" — for
// every non-empty result, on dev and on production.
func TestClientHelpersAreNotShadowed(t *testing.T) {
	src, err := os.ReadFile("static/app.js")
	if err != nil {
		t.Fatalf("read app.js: %v", err)
	}
	shadowRe := regexp.MustCompile(`(?m)\b(var|let|const)\s+(tr|trf)\s*=`)
	for _, m := range shadowRe.FindAllSubmatchIndex(src, -1) {
		line := 1 + bytes.Count(src[:m[0]], []byte("\n"))
		t.Errorf("static/app.js:%d declares %q, which shadows the tr()/trf() helper and throws a TypeError in that scope: %s",
			line, string(src[m[4]:m[5]]), strings.TrimSpace(string(src[m[0]:m[1]])))
	}
	// A parameter shadowing the helper is just as fatal.
	paramRe := regexp.MustCompile(`function\s*\([^)]*\b(tr|trf)\b[^)]*\)`)
	for _, m := range paramRe.FindAllSubmatchIndex(src, -1) {
		line := 1 + bytes.Count(src[:m[0]], []byte("\n"))
		t.Errorf("static/app.js:%d takes a parameter named %q, which shadows the tr()/trf() helper: %s",
			line, string(src[m[2]:m[3]]), strings.TrimSpace(string(src[m[0]:m[1]])))
	}
}
