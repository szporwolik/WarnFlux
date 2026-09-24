package geo

import "testing"

func TestResolveKnownCodes(t *testing.T) {
	cases := []struct {
		code string
		slug string
		typ  string
		name string
	}{
		{"1219", "wielicki", "powiat", "powiat wielicki"},
		{"1219043", "niepolomice", "gmina", "gmina Niepołomice"},
		{"1261011", "krakow", "miasto", "Kraków"},
		{"1201", "bochenski", "powiat", "powiat bocheński"},
		{"1201011", "bochnia", "miasto", "Bochnia"},
		{"1219053", "wieliczka", "gmina", "gmina Wieliczka"},
		{"1217", "tatrzanski", "powiat", "powiat tatrzański"},
		{"12", "malopolskie", "wojewodztwo", "województwo małopolskie"},
	}
	for _, c := range cases {
		a, ok := Resolve(c.code)
		if !ok {
			t.Errorf("code %s not resolved", c.code)
			continue
		}
		if a.Slug != c.slug || a.Type != c.typ || a.Name != c.name {
			t.Errorf("code %s -> %+v, want slug %s type %s name %q", c.code, a, c.slug, c.typ, c.name)
		}
	}
}

func TestLookupBySlug(t *testing.T) {
	for slug, want := range map[string]string{
		"niepolomice": "1219043",
		"krakow":      "1261011",
		"wielicki":    "1219",
		"bochenski":   "1201",
		"wieliczka":   "1219053",
		"krakowski":   "1206",
		"miechowski":  "1208",
		"myslenicki":  "1209",
		"proszowicki": "1214",
	} {
		a, ok := Lookup(slug)
		if !ok || a.Code != want {
			t.Errorf("Lookup(%q) = %+v (%v), want code %s", slug, a, ok, want)
		}
	}
	if _, ok := Lookup("bogus"); ok {
		t.Error("unknown slug must not resolve")
	}
}

func TestHierarchyIntersection(t *testing.T) {
	niepolomice, _ := Resolve("1219043")
	wielicki, _ := Resolve("1219")
	tatrzanski, _ := Resolve("1217")
	krakow, _ := Resolve("1261011")
	malopolskie, _ := Resolve("12")

	cases := []struct {
		name  string
		x, y  Area
		match bool
	}{
		// Warning area is an ancestor of the target.
		{"powiat wielicki vs gmina Niepołomice", wielicki, niepolomice, true},
		// Warning area is the exact target.
		{"gmina Niepołomice vs itself", niepolomice, niepolomice, true},
		// Warning area is a descendant of an included broader target.
		{"gmina Niepołomice vs powiat wielicki", niepolomice, wielicki, true},
		// Unrelated units in the same voivodeship do not match.
		{"powiat tatrzański vs gmina Niepołomice", tatrzanski, niepolomice, false},
		{"powiat tatrzański vs powiat wielicki", tatrzanski, wielicki, false},
		{"Kraków vs gmina Niepołomice", krakow, niepolomice, false},
		// Voivodeship is an ancestor of everything.
		{"województwo małopolskie vs Niepołomice", malopolskie, niepolomice, true},
	}
	for _, c := range cases {
		if got := Intersects(c.x, c.y); got != c.match {
			t.Errorf("%s: Intersects = %v, want %v", c.name, got, c.match)
		}
	}
}

func TestDisplay(t *testing.T) {
	cases := map[string]string{
		"teryt:1217":                "powiat tatrzański (TERYT 1217)",
		"teryt:1219043":             "gmina Niepołomice (TERYT 1219043)",
		"powiat:tatrzanski":         "powiat tatrzański",
		"gmina:niepolomice":         "gmina Niepołomice",
		"miasto:krakow":             "Kraków",
		"wojewodztwo:malopolskie":   "województwo małopolskie",
		"droga:a4":                  "droga:a4",
		"teryt:9999999":             "TERYT 9999999",
		"obszar:małopolskie, Wisła": "obszar:małopolskie, Wisła",
	}
	for in, want := range cases {
		if got := Display(in); got != want {
			t.Errorf("Display(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDisplayAreasKeepsUnknown(t *testing.T) {
	got := DisplayAreas([]string{"teryt:1217", "powiat:tatrzanski"})
	want := []string{"powiat tatrzański (TERYT 1217)", "powiat tatrzański"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("DisplayAreas = %v, want %v", got, want)
	}
}
