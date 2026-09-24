package imgw

import (
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/szporwolik/WarnFlux/internal/geo"
)

// errGeographicallyFiltered marks an event that is well-formed but outside
// the configured target geography. It never marks the provider snapshot
// incomplete.
var errGeographicallyFiltered = errors.New("imgw event outside configured geography")

// GeographyConfig is the optional geographic policy block.
type GeographyConfig struct {
	Enabled bool     `yaml:"enabled"`
	Include []string `yaml:"include"`
	// HydroLocalKeywords makes a hydrological warning locally relevant
	// when any fragment appears in its geographic description (plain
	// words work; regexp fragments like "niepolomic\w*" are allowed).
	// Empty means no local keyword matching: hydro warnings then pass
	// through (a missing keyword list must never silently swallow them).
	HydroLocalKeywords []string `yaml:"hydro_local_keywords"`
}

// geography is the runtime-ready policy: the resolved target units and the
// compiled hydro keyword patterns.
type geography struct {
	enabled     bool
	targets     []geo.Area
	hydroLocal  *regexp.Regexp
	hydroRegion *regexp.Regexp // derived from the included voivodeships
}

// keywordRE joins the configured fragments into one folded-text pattern.
func keywordRE(fragments []string) *regexp.Regexp {
	cleaned := make([]string, 0, len(fragments))
	for _, f := range fragments {
		if f = strings.TrimSpace(f); f != "" {
			cleaned = append(cleaned, f)
		}
	}
	if len(cleaned) == 0 {
		return nil
	}
	return regexp.MustCompile(`\b(?:` + strings.Join(cleaned, "|") + `)\b`)
}

// buildGeography validates the configured include list. Unknown names fail
// configuration rather than silently matching nothing.
func buildGeography(cfg *GeographyConfig) (*geography, error) {
	if cfg == nil || !cfg.Enabled {
		return &geography{}, nil
	}
	if len(cfg.Include) == 0 {
		return nil, errors.New("geography.enabled requires at least one include entry")
	}
	g := &geography{enabled: true}
	seen := map[string]bool{}
	for _, raw := range cfg.Include {
		kind, slug, ok := strings.Cut(strings.TrimSpace(raw), ":")
		if !ok || slug == "" {
			return nil, fmt.Errorf("geography.include %q must be <type>:<slug>", raw)
		}
		a, ok := geo.Lookup(slug)
		if !ok {
			return nil, fmt.Errorf("geography.include %q: unknown geographic name %q", raw, slug)
		}
		if !validAreaType(a.Type, kind) {
			return nil, fmt.Errorf("geography.include %q: type %q does not match unit %q (type %s)", raw, kind, slug, a.Type)
		}
		if seen[slug] {
			return nil, fmt.Errorf("duplicate geography.include %q", raw)
		}
		seen[slug] = true
		g.targets = append(g.targets, a)
	}

	// Hydro matching: local keywords come from the configuration; the
	// regional pattern derives from the included voivodeship units so the
	// operator never has to spell it out.
	g.hydroLocal = keywordRE(cfg.HydroLocalKeywords)
	var regionFrags []string
	for _, t := range g.targets {
		if t.Type == "wojewodztwo" {
			regionFrags = append(regionFrags, strings.TrimSuffix(t.Slug, "ie")+`\w*`)
		}
	}
	g.hydroRegion = keywordRE(regionFrags)
	return g, nil
}

// validAreaType accepts the type spellings used in configuration.
func validAreaType(unitType, kind string) bool {
	switch kind {
	case "powiat":
		return unitType == "powiat"
	case "gmina":
		return unitType == "gmina"
	case "miasto":
		return unitType == "miasto"
	case "wojewodztwo":
		return unitType == "wojewodztwo"
	}
	return false
}

// matchesMeteo reports whether AT LEAST ONE provider TERYT unit intersects
// the configured target geography (hierarchical matching). Unknown codes
// never match by themselves. It also returns the enriched, deterministic
// area list: raw teryt:<code> tokens plus resolved <type>:<slug> tokens.
func (g *geography) matchesMeteo(teryt []string) (match bool, areas []string, err error) {
	enabled := g != nil && g.enabled
	for _, raw := range teryt {
		code := strings.TrimSpace(raw)
		if code == "" {
			continue
		}
		if len(code) > 32 {
			return false, nil, fmt.Errorf("TERYT code %q is implausibly long", code)
		}
		areas = append(areas, "teryt:"+code)
		a, ok := geo.Resolve(code)
		if !ok {
			// Provider evolution: preserve, do not invent, do not match.
			slog.Debug("unknown IMGW TERYT code preserved", "teryt", code)
			continue
		}
		areas = append(areas, a.Type+":"+a.Slug)
		if enabled {
			for _, t := range g.targets {
				if geo.Intersects(a, t) {
					match = true
					break
				}
			}
		}
	}
	if !enabled {
		match = true
	}
	return match, sortedUnique(areas), nil
}

// foldDiacritics maps Polish letters to ASCII so text matching works on
// both diacritic and folded spellings.
var foldDiacritics = strings.NewReplacer(
	"ą", "a", "ć", "c", "ę", "e", "ł", "l", "ń", "n", "ó", "o",
	"ś", "s", "ź", "z", "ż", "z",
	"Ą", "A", "Ć", "C", "Ę", "E", "Ł", "L", "Ń", "N", "Ó", "O",
	"Ś", "S", "Ź", "Z", "Ż", "Z",
)

// matchesHydro applies the conservative hydrological policy: the warning
// passes when its normalized geographic description clearly intersects the
// local area. A plain voivodeship mention without any configured local
// keyword does not pass. (Full basin-level GIS resolution is documented as
// a limitation.) With no hydro_local_keywords configured the warnings pass
// through: an empty keyword list must not silently hide everything.
func (g *geography) matchesHydro(areas []string) bool {
	if g == nil || !g.enabled {
		return true
	}
	if g.hydroLocal == nil {
		return true
	}
	var local bool
	for _, area := range areas {
		folded := foldDiacritics.Replace(strings.ToLower(area))
		if g.hydroLocal.MatchString(folded) {
			local = true
		}
		if g.hydroRegion != nil && g.hydroRegion.MatchString(folded) {
			// regional mention alone is insufficient; keep scanning.
			continue
		}
	}
	return local
}
