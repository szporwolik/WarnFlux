package rso

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/snapshotutil"
)

// normalizeText trims and collapses internal whitespace deterministically.
func normalizeText(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// rsoAreas builds deterministic area labels from the provider provinces.
// When the item carries no provinces, the queried regional slug is the
// fallback (never "wszystkie").
func rsoAreas(provinces []province, queriedSlug string) []string {
	var areas []string
	for _, p := range provinces {
		slug := strings.TrimSpace(p.Slug)
		if slug != "" && slug != "wszystkie" {
			areas = append(areas, "wojewodztwo:"+slug)
		}
	}
	if len(areas) == 0 && queriedSlug != "" && queriedSlug != "wszystkie" {
		areas = append(areas, "wojewodztwo:"+queriedSlug)
	}
	sort.Strings(areas)
	out := areas[:0]
	var prev string
	for i, a := range areas {
		if i > 0 && a == prev {
			continue
		}
		out = append(out, a)
		prev = a
	}
	return out
}

// normalizeNews converts one RSO communication into a canonical
// HazardEvent. The provider <id> is the stable identity. A non-nil error
// means the item is not safely identifiable (snapshot incomplete).
func normalizeNews(item newsItem, sourceURL, queriedSlug string) (core.HazardEvent, error) {
	id := strings.TrimSpace(item.ID)
	if id == "" {
		return core.HazardEvent{}, fmt.Errorf("RSO news item has an empty id")
	}
	title := normalizeText(item.Title)
	if title == "" {
		return core.HazardEvent{}, fmt.Errorf("RSO news %q has no title", id)
	}
	// Prefer the fuller provider text when the list response carries it.
	body := normalizeText(item.Content)
	if body == "" {
		body = normalizeText(item.Shortcut)
	}

	effective, err := snapshotutil.ParseWarsawLocal(item.ValidFrom)
	if err != nil {
		return core.HazardEvent{}, fmt.Errorf("RSO news %q: %w", id, err)
	}
	var expires *time.Time
	if strings.TrimSpace(item.ValidTo) != "" {
		t, err := snapshotutil.ParseWarsawLocal(item.ValidTo)
		if err != nil {
			return core.HazardEvent{}, fmt.Errorf("RSO news %q: %w", id, err)
		}
		expires = &t
	}

	ev := core.HazardEvent{
		Source:      sourceRSO,
		SourceID:    id,
		Event:       title,
		Severity:    "unknown",
		Headline:    title,
		Description: body,
		EffectiveAt: &effective,
		ExpiresAt:   expires,
		Areas:       rsoAreas(item.Provinces, queriedSlug),
		Status:      core.StatusActive,
		SourceURL:   sourceURL,
	}
	if err := ev.Validate(); err != nil {
		return core.HazardEvent{}, fmt.Errorf("RSO news %q is oversized/invalid: %w", id, err)
	}
	return ev, nil
}

// contentSignature is the non-area content identity used to detect
// conflicting duplicates across regional feeds: title, validity window and
// body. Areas are deliberately excluded (they legitimately differ per
// filtered endpoint).
func contentSignature(item newsItem) string {
	return strings.Join([]string{
		normalizeText(item.Title),
		strings.TrimSpace(item.ValidFrom),
		strings.TrimSpace(item.ValidTo),
		normalizeText(item.Content),
	}, "\x00")
}
