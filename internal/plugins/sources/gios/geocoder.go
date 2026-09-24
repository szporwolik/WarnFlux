package gios

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultGeocoderURL = "https://services.gugik.gov.pl/uug/"
	// The ULDK endpoint has no public reverse geocode, so the one-shot
	// home-position → voivodeship lookup uses Nominatim (1 request per
	// process, cached). Config-provided wojewodztwo skips it entirely.
	defaultReverseURL = "https://nominatim.openstreetmap.org/reverse"
	maxGeoBodyBytes   = 1 << 20 // 1 MiB
)

// geoPoint is one geocoded position (lat/lon).
type geoPoint struct {
	Lat float64
	Lon float64
}

// uldkResponse is the GUGiK ULDK GetAddress answer. The "results" object
// is keyed "1".."N"; x is longitude and y latitude (verified live).
type uldkResponse struct {
	Results map[string]struct {
		X string `json:"x"`
		Y string `json:"y"`
	} `json:"results"`
}

// nominatimResponse is the minimal reverse-geocode answer surface.
type nominatimResponse struct {
	Address struct {
		State  string `json:"state"`
		County string `json:"county"`
	} `json:"address"`
}

// Geocoder geocodes localities through GUGiK ULDK (forward) and resolves
// the home position to its voivodeship through Nominatim (reverse, one
// shot). Successful lookups are cached for the process lifetime.
type Geocoder struct {
	geocoderURL string
	reverseURL  string
	http        *http.Client

	mu      sync.Mutex
	towns   map[string]geoPoint
	reverse map[[2]float64]adminScope // position key → resolved scope
}

// adminScope is the auto-derived administrative scope.
type adminScope struct {
	Wojewodztwo string
	Powiat      string
}

// NewGeocoder builds a geocoder with validated service URLs.
func NewGeocoder(geocoderURL, reverseURL string, timeout time.Duration) *Geocoder {
	return &Geocoder{
		geocoderURL: geocoderURL,
		reverseURL:  reverseURL,
		http:        &http.Client{Timeout: timeout},
		towns:       make(map[string]geoPoint),
		reverse:     make(map[[2]float64]adminScope),
	}
}

// Geocode resolves one locality name to its centroid. Results are cached
// per locality so a national listing only costs one lookup per distinct
// town.
func (g *Geocoder) Geocode(ctx context.Context, miejscowosc string) (geoPoint, error) {
	key := strings.ToLower(strings.TrimSpace(miejscowosc))
	if key == "" {
		return geoPoint{}, fmt.Errorf("geocode: empty locality")
	}
	g.mu.Lock()
	if p, ok := g.towns[key]; ok {
		g.mu.Unlock()
		return p, nil
	}
	g.mu.Unlock()

	q := url.Values{}
	q.Set("request", "GetAddress")
	q.Set("location", strings.TrimSpace(miejscowosc))
	q.Set("result", "point")
	q.Set("srid", "4326")
	q.Set("maxresults", "1")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.geocoderURL+"?"+q.Encode(), nil)
	if err != nil {
		return geoPoint{}, fmt.Errorf("build geocode request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "WarnFlux gios/0.1")

	resp, err := g.http.Do(req)
	if err != nil {
		return geoPoint{}, fmt.Errorf("geocode fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return geoPoint{}, fmt.Errorf("geocode status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxGeoBodyBytes+1))
	if err != nil {
		return geoPoint{}, fmt.Errorf("geocode read: %w", err)
	}
	if len(data) > maxGeoBodyBytes {
		return geoPoint{}, fmt.Errorf("geocode response exceeds %d bytes", maxGeoBodyBytes)
	}

	var doc uldkResponse
	if err := json.Unmarshal(data, &doc); err != nil {
		return geoPoint{}, fmt.Errorf("geocode parse: %w", err)
	}
	first, ok := doc.Results["1"]
	if !ok {
		return geoPoint{}, fmt.Errorf("geocode: locality %q not found", miejscowosc)
	}
	lon, err := strconv.ParseFloat(strings.TrimSpace(first.X), 64)
	if err != nil {
		return geoPoint{}, fmt.Errorf("geocode: longitude %q unparseable", first.X)
	}
	lat, err := strconv.ParseFloat(strings.TrimSpace(first.Y), 64)
	if err != nil {
		return geoPoint{}, fmt.Errorf("geocode: latitude %q unparseable", first.Y)
	}
	if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		return geoPoint{}, fmt.Errorf("geocode: out-of-range position (%v, %v)", lat, lon)
	}
	p := geoPoint{Lat: lat, Lon: lon}

	g.mu.Lock()
	g.towns[key] = p
	g.mu.Unlock()
	return p, nil
}

// Reverse resolves a position to its voivodeship (and powiat when the
// answer carries it) via Nominatim. One request per distinct position,
// cached for the process lifetime. Nominatim's usage policy (proper UA,
// minimal traffic) is respected: this is a one-shot lookup per process
// when the configuration does not name the voivodeship explicitly.
func (g *Geocoder) Reverse(ctx context.Context, lat, lon float64) (adminScope, error) {
	posKey := [2]float64{lat, lon}
	g.mu.Lock()
	if s, ok := g.reverse[posKey]; ok {
		g.mu.Unlock()
		return s, nil
	}
	g.mu.Unlock()

	q := url.Values{}
	q.Set("lat", strconv.FormatFloat(lat, 'f', 6, 64))
	q.Set("lon", strconv.FormatFloat(lon, 'f', 6, 64))
	q.Set("format", "jsonv2")
	q.Set("zoom", "10")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.reverseURL+"?"+q.Encode(), nil)
	if err != nil {
		return adminScope{}, fmt.Errorf("build reverse request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "WarnFlux gios/0.1 (APRS SP9SPM-10; one-shot geocoding)")

	resp, err := g.http.Do(req)
	if err != nil {
		return adminScope{}, fmt.Errorf("reverse fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return adminScope{}, fmt.Errorf("reverse status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxGeoBodyBytes+1))
	if err != nil {
		return adminScope{}, fmt.Errorf("reverse read: %w", err)
	}
	if len(data) > maxGeoBodyBytes {
		return adminScope{}, fmt.Errorf("reverse response exceeds %d bytes", maxGeoBodyBytes)
	}

	var doc nominatimResponse
	if err := json.Unmarshal(data, &doc); err != nil {
		return adminScope{}, fmt.Errorf("reverse parse: %w", err)
	}

	woj := normalizeWojewodztwo(doc.Address.State)
	if !wojewodztwa[woj] {
		return adminScope{}, fmt.Errorf("reverse geocode did not yield a known voivodeship (state %q)", doc.Address.State)
	}
	scope := adminScope{Wojewodztwo: woj}
	if c := normalizePowiat(doc.Address.County); c != "" {
		scope.Powiat = c
	}

	g.mu.Lock()
	g.reverse[posKey] = scope
	g.mu.Unlock()
	return scope, nil
}

// normalizeWojewodztwo trims the "województwo " prefix Nominatim prepends
// (e.g. "województwo małopolskie" → "małopolskie").
func normalizeWojewodztwo(s string) string {
	out := strings.ToLower(strings.TrimSpace(s))
	for _, prefix := range []string{"województwo ", "woj. "} {
		out = strings.TrimPrefix(out, prefix)
	}
	return out
}

// normalizePowiat trims the "powiat " prefix (e.g. "powiat wielicki" →
// "wielicki"). Empty input yields an empty string.
func normalizePowiat(s string) string {
	out := strings.ToLower(strings.TrimSpace(s))
	return strings.TrimPrefix(out, "powiat ")
}
