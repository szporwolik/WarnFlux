package gddkia

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/severity"
)

// sourceName is the normalized source namespace for routing and display.
const sourceName = "gddkia"

// parseFeedTime parses the feed's RFC3339 timestamps; the feed writes
// offsets without a colon ("+0200"), so both layouts are accepted.
func parseFeedTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05-0700"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unparseable timestamp %q", s)
}

// normalize converts one feed entry into a canonical HazardEvent.
// The feed carries no unique IDs, so the identity is a stable hash of the
// identifying content (road, kilometre, section, creation date, position
// and difficulty code): the same obstruction keeps one event key across
// polls, and a changed entry becomes a new identity.
func normalize(u utr, sourceURL string) (core.HazardEvent, error) {
	road := strings.TrimSpace(u.NrDrogi)
	if road == "" {
		return core.HazardEvent{}, fmt.Errorf("entry has no road number")
	}
	lat, err := strconv.ParseFloat(strings.TrimSpace(u.GeoLat), 64)
	if err != nil {
		return core.HazardEvent{}, fmt.Errorf("entry has no usable latitude")
	}
	lon, err := strconv.ParseFloat(strings.TrimSpace(u.GeoLong), 64)
	if err != nil {
		return core.HazardEvent{}, fmt.Errorf("entry has no usable longitude")
	}

	sum := sha256.Sum256([]byte(strings.Join([]string{
		road, strings.TrimSpace(u.KM), strings.TrimSpace(u.NazwaOdcinka),
		strings.TrimSpace(u.DataPowstania), strings.TrimSpace(u.GeoLat),
		strings.TrimSpace(u.GeoLong), strings.TrimSpace(u.Rodzaj.Poz),
	}, "\x00")))
	sourceID := hex.EncodeToString(sum[:12])

	effective, err := parseFeedTime(u.DataPowstania)
	if err != nil {
		effective = time.Time{}
	}
	var expires *time.Time
	if s := strings.TrimSpace(u.DataLikwidacji); s != "" {
		if t, err := parseFeedTime(s); err == nil {
			expires = &t
		}
	}

	section := strings.TrimSpace(u.NazwaOdcinka)
	headline := road + " km " + strings.TrimSpace(u.KM)
	if section != "" {
		headline += " — " + section
	}

	desc := strings.TrimSpace(u.Objazd)
	if v := strings.TrimSpace(u.OgrPredkosc); v != "" && v != "0" {
		limit := "Speed limit: " + v + " km/h."
		if desc != "" {
			desc += "\n\n" + limit
		} else {
			desc = limit
		}
	}

	// Provider severity: road closures are severe, alternating traffic is
	// moderate, everything else (works, mowing, ...) is minor.
	sev := severity.Minor
	event := "Road works"
	switch strings.TrimSpace(strings.ToLower(u.DrogaZamknieta)) {
	case "true", "1", "tak":
		sev, event = severity.Severe, "Road closure"
	default:
		switch strings.TrimSpace(strings.ToLower(u.RuchWahadlowy)) {
		case "true", "1", "tak":
			sev, event = severity.Moderate, "Alternating traffic"
		}
	}

	ev := core.HazardEvent{
		Source:           sourceName,
		SourceID:         sourceID,
		Category:         "road",
		Event:            event,
		Severity:         sev,
		ProviderSeverity: strings.TrimSpace(u.Rodzaj.Poz),
		Headline:         headline,
		Description:      desc,
		Latitude:         &lat,
		Longitude:        &lon,
		Areas:            []string{roadArea(road)},
		Status:           core.StatusActive,
		SourceURL:        sourceURL,
	}
	if !effective.IsZero() {
		ev.EffectiveAt = &effective
	}
	ev.ExpiresAt = expires
	if err := ev.Validate(); err != nil {
		return core.HazardEvent{}, fmt.Errorf("road entry is oversized/invalid: %w", err)
	}
	return ev, nil
}

// roadArea normalizes a road number into a deterministic area token
// (e.g. "A4" -> "droga:a4", "94g" -> "droga:94g").
func roadArea(road string) string {
	return "droga:" + strings.ToLower(strings.TrimSpace(road))
}
