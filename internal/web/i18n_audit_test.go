package web

import (
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
		"notif.outcome":  {"delivered", "failed", "skipped", "submitted"},
		"config.mqtt":    {"events", "active", "info", "status", "save"},
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
