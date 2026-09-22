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
}

// geography is the runtime-ready policy: the resolved target units.
type geography struct {
	enabled bool
	targets []geo.Area
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

// hydroLocalPatterns recognizes the Niepołomice-area surface waters and
// locations that make a hydrological warning locally relevant. Broad
// rivers (Wisła, Raba) alone are deliberately not sufficient: their
// basins span far beyond the target area.
var hydroLocalPatterns = regexp.MustCompile(`\b(niepolomic\w*|podlez\w*|wieliczk\w*|wielick\w*|krakow\w*|bochn\w*|klaj\w*|targowisko|szarow\w*|brzezie|gdow\w*|staniatk\w*|drwinka|seraf\w*)\b`)

// hydroMalopolskie detects the folded voivodeship names.
var hydroMalopolskie = regexp.MustCompile(`malopolsk\w*|ma[łl]opolsk\w*`)

// matchesHydro applies the conservative hydrological policy: the warning
// passes when its normalized geographic description clearly intersects the
// local area. A plain "małopolskie" without any local reference does not
// pass. (Full basin-level GIS resolution is documented as a limitation.)
func (g *geography) matchesHydro(areas []string) bool {
	if g == nil || !g.enabled {
		return true
	}
	var local bool
	for _, area := range areas {
		folded := foldDiacritics.Replace(strings.ToLower(area))
		if hydroLocalPatterns.MatchString(folded) {
			local = true
		}
		if hydroMalopolskie.MatchString(folded) {
			// regional mention alone is insufficient; keep scanning.
			continue
		}
	}
	return local
}
