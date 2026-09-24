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

// Ancestors returns the set of the area's slug plus all ancestor slugs.
func Ancestors(a Area) map[string]bool {
	out := map[string]bool{a.Slug: true}
	for _, p := range a.Parents {
		out[p] = true
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

// Display renders one normalized area token for human consumption. Known
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
