package adsb

import (
	"encoding/json"
	"math"
	"sort"
	"time"
)

// Canonical aircraft snapshot schema (also the MQTT wire document):
// one document per poll covering every target in the area of interest.
const (
	schemaVersion = 1
	wireType      = "aircraft"
)

// maxTrailPoints bounds one aircraft's trail (3-5 minutes at 10s polls is
// ~30 points; the cap protects against pathological poll intervals).
const maxTrailPoints = 60

// trackPoint is one recorded position of one aircraft (internal; the
// wire form uses unix seconds for At).
type trackPoint struct {
	Latitude  float64
	Longitude float64
	At        time.Time
}

// wireTrailPoint is one trail position in the wire document.
type wireTrailPoint struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	At        int64   `json:"at"` // unix seconds
}

// wireAircraft is one aircraft inside the snapshot document.
type wireAircraft struct {
	Icao24         string           `json:"icao24"`
	Callsign       string           `json:"callsign,omitempty"`
	Category       string           `json:"category,omitempty"`
	Latitude       float64          `json:"latitude"`
	Longitude      float64          `json:"longitude"`
	AltitudeM      *float64         `json:"altitude_m,omitempty"`
	OnGround       bool             `json:"on_ground"`
	SpeedKmh       *float64         `json:"speed_kmh,omitempty"`
	TrackDeg       *float64         `json:"track_deg,omitempty"`
	VerticalRateMS *float64         `json:"vertical_rate_m_s,omitempty"`
	SeenAt         int64            `json:"seen_at"` // unix seconds
	Trail          []wireTrailPoint `json:"trail,omitempty"`
}

// wireSnapshot is the canonical aircraft document.
type wireSnapshot struct {
	Type          string         `json:"type"`
	SchemaVersion int            `json:"schema_version"`
	GeneratedAt   time.Time      `json:"generated_at"`
	Provider      string         `json:"provider"`
	CenterLat     float64        `json:"center_latitude"`
	CenterLon     float64        `json:"center_longitude"`
	RadiusKM      float64        `json:"radius_km"`
	Aircraft      []wireAircraft `json:"aircraft"`
}

// tracker keeps the per-aircraft position history used for the minute
// vector and the 3-5 minute trail.
type tracker struct {
	window time.Duration
	byHex  map[string][]trackPoint
}

func newTracker(window time.Duration) *tracker {
	return &tracker{window: window, byHex: make(map[string][]trackPoint)}
}

// observe records one target position and prunes stale history. The
// latest point must remain even when older than the window.
func (t *tracker) observe(hex string, lat, lon float64, at time.Time) []trackPoint {
	pts := t.byHex[hex]
	if n := len(pts); n > 0 {
		last := pts[n-1]
		if !at.After(last.At) {
			return pts // stale or duplicate timestamp — keep history as-is
		}
		if math.Abs(last.Latitude-lat) < 1e-6 && math.Abs(last.Longitude-lon) < 1e-6 {
			return pts // stationary — no new trail point
		}
	}
	pts = append(pts, trackPoint{Latitude: lat, Longitude: lon, At: at})
	cutoff := at.Add(-t.window)
	i := 0
	for i < len(pts)-1 && pts[i].At.Before(cutoff) {
		i++
	}
	if i > 0 {
		pts = append([]trackPoint(nil), pts[i:]...)
	}
	if len(pts) > maxTrailPoints {
		pts = pts[len(pts)-maxTrailPoints:]
	}
	t.byHex[hex] = pts
	return pts
}

// prune drops aircraft not seen for the window (plus slack) so the
// tracker does not grow without bound.
func (t *tracker) prune(now time.Time) {
	cutoff := now.Add(-t.window - time.Minute)
	for hex, pts := range t.byHex {
		if len(pts) > 0 && pts[len(pts)-1].At.Before(cutoff) {
			delete(t.byHex, hex)
		}
	}
}

// headingBetween computes the initial bearing (degrees, 0 = north) and
// the ground speed (km/h) between two positions.
func headingBetween(a, b trackPoint) (deg, kmh float64, ok bool) {
	dt := b.At.Sub(a.At).Seconds()
	if dt < 1 {
		return 0, 0, false
	}
	la1 := a.Latitude * math.Pi / 180
	la2 := b.Latitude * math.Pi / 180
	dLon := (b.Longitude - a.Longitude) * math.Pi / 180
	y := math.Sin(dLon) * math.Cos(la2)
	x := math.Cos(la1)*math.Sin(la2) - math.Sin(la1)*math.Cos(la2)*math.Cos(dLon)
	deg = math.Mod(math.Atan2(y, x)*180/math.Pi+360, 360)

	dLat := (b.Latitude - a.Latitude) * math.Pi / 180
	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(la1)*math.Cos(la2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	distKm := 2 * 6371 * math.Asin(math.Min(1, math.Sqrt(h)))
	kmh = distKm / (dt / 3600)
	return deg, kmh, true
}

// fallbackVector derives the vector from the last two trail points when
// the provider reports no track/ground speed (e.g. mlat-only targets).
func fallbackVector(pts []trackPoint) (deg, kmh *float64) {
	if len(pts) < 2 {
		return nil, nil
	}
	d, s, ok := headingBetween(pts[len(pts)-2], pts[len(pts)-1])
	if !ok {
		return nil, nil
	}
	return &d, &s
}

// buildSnapshot converts the current provider targets plus the tracker
// state into the canonical wire document.
func buildSnapshot(provider string, centerLat, centerLon, radiusKm float64, targets []ProviderTarget, track *tracker, now time.Time) wireSnapshot {
	snap := wireSnapshot{
		Type:          wireType,
		SchemaVersion: schemaVersion,
		GeneratedAt:   now.UTC(),
		Provider:      provider,
		CenterLat:     centerLat,
		CenterLon:     centerLon,
		RadiusKM:      radiusKm,
		Aircraft:      make([]wireAircraft, 0, len(targets)),
	}
	for _, t := range targets {
		w := wireAircraft{
			Icao24:    t.Hex,
			Callsign:  t.Flight,
			Category:  t.Category,
			Latitude:  t.Lat,
			Longitude: t.Lon,
			OnGround:  t.OnGround,
			SeenAt:    t.SeenAt.Unix(),
			Trail:     nil,
		}
		if t.AltBaroFt != 0 {
			m := t.AltBaroFt * 0.3048
			w.AltitudeM = &m
		}
		if !math.IsNaN(t.GSKt) && t.GSKt > 0 {
			s := t.GSKt * 1.852
			w.SpeedKmh = &s
		}
		if !math.IsNaN(t.TrackDeg) && t.TrackDeg >= 0 {
			d := t.TrackDeg
			w.TrackDeg = &d
		}
		if !math.IsNaN(t.BaroRateFPM) && t.BaroRateFPM != 0 {
			v := t.BaroRateFPM * 0.00508
			w.VerticalRateMS = &v
		}
		// Record the current point first: the vector fallback reads the
		// history INCLUDING this observation.
		pts := track.observe(t.Hex, t.Lat, t.Lon, now)
		if w.SpeedKmh == nil || w.TrackDeg == nil {
			d, s := fallbackVector(pts)
			if w.TrackDeg == nil {
				w.TrackDeg = d
			}
			if w.SpeedKmh == nil {
				w.SpeedKmh = s
			}
		}
		if len(pts) > 1 {
			w.Trail = make([]wireTrailPoint, 0, len(pts))
			for _, p := range pts {
				w.Trail = append(w.Trail, wireTrailPoint{Latitude: p.Latitude, Longitude: p.Longitude, At: p.At.Unix()})
			}
		}
		snap.Aircraft = append(snap.Aircraft, w)
	}
	// Deterministic order: by callsign, then hex.
	sort.Slice(snap.Aircraft, func(i, j int) bool {
		a, b := snap.Aircraft[i], snap.Aircraft[j]
		if a.Callsign != b.Callsign {
			return a.Callsign < b.Callsign
		}
		return a.Icao24 < b.Icao24
	})
	return snap
}

// marshalSnapshot serializes the canonical document (the MQTT payload).
func marshalSnapshot(s wireSnapshot) (json.RawMessage, error) {
	return json.Marshal(s)
}
