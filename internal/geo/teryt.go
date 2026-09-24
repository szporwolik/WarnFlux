// Package geo holds the small, static TERYT geography used for IMGW
// geographic filtering and for human-readable area display across the
// application. It is deliberately NOT a generic GIS engine: it contains
// only the territorial units needed by this installation, with explicit
// parent hierarchy metadata.
//
// Codes are the official GUS TERYT identifiers (verified):
//
//	12      województwo małopolskie
//	1201    powiat bocheński
//	1202    powiat brzeski
//	1206    powiat krakowski
//	1208    powiat miechowski
//	1209    powiat myślenicki
//	1214    powiat proszowicki
//	1216    powiat tarnowski
//	1217    powiat tatrzański (example of known-but-irrelevant geography)
//	1219    powiat wielicki
//	1201011 miasto Bochnia
//	1201012 gmina Bochnia (wiejska)
//	1219043 gmina Niepołomice
//	1219044 miasto Niepołomice
//	1219045 gmina Niepołomice — obszar wiejski
//	1219053 gmina Wieliczka
//	1219054 miasto Wieliczka
//	1261011 miasto Kraków
package geo

import (
	"fmt"
	"regexp"
	"strings"
)

// Area describes one territorial unit from the bundled mapping.
type Area struct {
	// Code is the official TERYT identifier.
	Code string
	// Type is one of "wojewodztwo", "powiat", "gmina", "miasto".
	Type string
	// Slug is the canonical ASCII identifier, e.g. "niepolomice".
	Slug string
	// Name is the human-readable Polish name.
	Name string
	// Parents lists ancestor slugs, closest first.
	Parents []string
}

// areas is the bundled mapping. Keep it easy to inspect and update.
var areas = []Area{
	{Code: "12", Type: "wojewodztwo", Slug: "malopolskie", Name: "województwo małopolskie"},

	{Code: "1201", Type: "powiat", Slug: "bochenski", Name: "powiat bocheński", Parents: []string{"malopolskie"}},
	{Code: "1202", Type: "powiat", Slug: "brzeski", Name: "powiat brzeski", Parents: []string{"malopolskie"}},
	{Code: "1206", Type: "powiat", Slug: "krakowski", Name: "powiat krakowski", Parents: []string{"malopolskie"}},
	{Code: "1208", Type: "powiat", Slug: "miechowski", Name: "powiat miechowski", Parents: []string{"malopolskie"}},
	{Code: "1209", Type: "powiat", Slug: "myslenicki", Name: "powiat myślenicki", Parents: []string{"malopolskie"}},
	{Code: "1214", Type: "powiat", Slug: "proszowicki", Name: "powiat proszowicki", Parents: []string{"malopolskie"}},
	{Code: "1216", Type: "powiat", Slug: "tarnowski", Name: "powiat tarnowski", Parents: []string{"malopolskie"}},
	{Code: "1217", Type: "powiat", Slug: "tatrzanski", Name: "powiat tatrzański", Parents: []string{"malopolskie"}},
	{Code: "1219", Type: "powiat", Slug: "wielicki", Name: "powiat wielicki", Parents: []string{"malopolskie"}},

	{Code: "1201011", Type: "miasto", Slug: "bochnia", Name: "Bochnia", Parents: []string{"bochenski", "malopolskie"}},
	{Code: "1201012", Type: "gmina", Slug: "bochnia-wies", Name: "gmina Bochnia", Parents: []string{"bochenski", "malopolskie"}},

	{Code: "1219043", Type: "gmina", Slug: "niepolomice", Name: "gmina Niepołomice", Parents: []string{"wielicki", "malopolskie"}},
	{Code: "1219044", Type: "miasto", Slug: "niepolomice-miasto", Name: "Niepołomice", Parents: []string{"niepolomice", "wielicki", "malopolskie"}},
	{Code: "1219045", Type: "gmina", Slug: "niepolomice-obszar", Name: "gmina Niepołomice (obszar wiejski)", Parents: []string{"niepolomice", "wielicki", "malopolskie"}},

	{Code: "1219053", Type: "gmina", Slug: "wieliczka", Name: "gmina Wieliczka", Parents: []string{"wielicki", "malopolskie"}},
	{Code: "1219054", Type: "miasto", Slug: "wieliczka-miasto", Name: "Wieliczka", Parents: []string{"wieliczka", "wielicki", "malopolskie"}},

	{Code: "1261011", Type: "miasto", Slug: "krakow", Name: "Kraków", Parents: []string{"malopolskie"}},
}

var (
	byCode map[string]Area
	bySlug map[string]Area
)

func init() {
	byCode = make(map[string]Area, len(areas))
	bySlug = make(map[string]Area, len(areas))
	for _, a := range areas {
		byCode[a.Code] = a
		bySlug[a.Slug] = a
	}
}

// Resolve returns the bundled unit for an official TERYT code.
func Resolve(code string) (Area, bool) {
	a, ok := byCode[strings.TrimSpace(code)]
	return a, ok
}

// Lookup returns the bundled unit for a canonical slug (configuration).
func Lookup(slug string) (Area, bool) {
	a, ok := bySlug[strings.ToLower(strings.TrimSpace(slug))]
	return a, ok
}

// Ancestors returns the set of the area's slug plus all ancestor slugs,
// expanding parent chains through the table so a single declared parent
// transitively reaches its own parents.
func Ancestors(a Area) map[string]bool {
	out := map[string]bool{a.Slug: true}
	var walk func(string)
	walk = func(slug string) {
		if out[slug] {
			return
		}
		out[slug] = true
		if p, ok := bySlug[slug]; ok {
			for _, gp := range p.Parents {
				walk(gp)
			}
		}
	}
	for _, p := range a.Parents {
		walk(p)
	}
	return out
}

// Intersects reports whether two units overlap according to the explicit
// parent hierarchy: equal units, one being the other's ancestor, or one
// being a descendant of an explicitly included broader unit.
func Intersects(x, y Area) bool {
	if x.Slug == y.Slug {
		return true
	}
	return Ancestors(x)[y.Slug] || Ancestors(y)[x.Slug]
}

// Register merges installation-specific territorial units (from the
// top-level geo.areas configuration) into the bundled table, so an
// installation in any region works without code changes. Errors cover
// malformed entries, duplicate slugs/codes and unknown parents.
func Register(extra []Area) error {
	validType := map[string]bool{
		"wojewodztwo": true, "powiat": true, "gmina": true, "miasto": true,
	}
	slugRe := regexp.MustCompile(`^[a-z0-9-]+$`)
	codeRe := regexp.MustCompile(`^[0-9]{1,7}$`)

	seenSlugs := map[string]bool{}
	seenCodes := map[string]bool{}
	for i, a := range extra {
		a.Slug = strings.ToLower(strings.TrimSpace(a.Slug))
		a.Name = strings.TrimSpace(a.Name)
		a.Code = strings.TrimSpace(a.Code)
		if !validType[a.Type] {
			return fmt.Errorf("geo.areas[%d]: type %q must be wojewodztwo, powiat, gmina or miasto", i, a.Type)
		}
		if !slugRe.MatchString(a.Slug) {
			return fmt.Errorf("geo.areas[%d]: slug %q must match %s", i, a.Slug, slugRe)
		}
		if !codeRe.MatchString(a.Code) {
			return fmt.Errorf("geo.areas[%d]: code %q must be a 1-7 digit TERYT code", i, a.Code)
		}
		if a.Name == "" {
			return fmt.Errorf("geo.areas[%d]: name must not be empty", i)
		}
		if _, dup := bySlug[a.Slug]; dup || seenSlugs[a.Slug] {
			return fmt.Errorf("geo.areas[%d]: slug %q already exists in the geography table", i, a.Slug)
		}
		if _, dup := byCode[a.Code]; dup || seenCodes[a.Code] {
			return fmt.Errorf("geo.areas[%d]: code %q already exists in the geography table", i, a.Code)
		}
		for _, p := range a.Parents {
			pn := strings.ToLower(strings.TrimSpace(p))
			if _, ok := bySlug[pn]; ok {
				continue
			}
			if seenSlugs[pn] {
				continue
			}
			return fmt.Errorf("geo.areas[%d]: parent slug %q is unknown (declare parents before children or use a bundled slug)", i, p)
		}
		seenSlugs[a.Slug] = true
		seenCodes[a.Code] = true
	}

	for _, a := range extra {
		a.Slug = strings.ToLower(strings.TrimSpace(a.Slug))
		a.Code = strings.TrimSpace(a.Code)
		parents := make([]string, 0, len(a.Parents))
		for _, p := range a.Parents {
			parents = append(parents, strings.ToLower(strings.TrimSpace(p)))
		}
		a.Parents = parents
		areas = append(areas, a)
		bySlug[a.Slug] = a
		byCode[a.Code] = a
	}
	return nil
}

// Display renders one normalized area token for human consumption. Known
// bundled units resolve through the table; unknown tokens pass through.
// TERYT codes and known type:slug tokens become Polish names; everything
// else is preserved verbatim.
func Display(token string) string {
	t := strings.TrimSpace(token)
	if rest, ok := strings.CutPrefix(t, "teryt:"); ok {
		if a, ok := Resolve(rest); ok {
			return fmt.Sprintf("%s (TERYT %s)", a.Name, a.Code)
		}
		return "TERYT " + rest
	}
	kind, slug, ok := strings.Cut(t, ":")
	if !ok {
		return t
	}
	if a, ok := Lookup(slug); ok {
		switch kind {
		case "gmina", "miasto":
			return a.Name
		case "powiat":
			return a.Name
		case "wojewodztwo":
			return a.Name
		}
	}
	return t
}

// DisplayAreas renders every token via Display, preserving order.
func DisplayAreas(tokens []string) []string {
	out := make([]string, 0, len(tokens))
	for _, t := range tokens {
		out = append(out, Display(t))
	}
	return out
}
