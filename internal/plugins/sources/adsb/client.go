// Package adsb is an informational source for aircraft (ADS-B) traffic in
// the configured area of interest. Two providers are supported:
//
//   - adsblol: the free, keyless https://api.adsb.lol API (point/radius
//     query) for the internet-connected case,
//   - tar1090: a LOCAL readsb/tar1090 receiver's /data/aircraft.json
//     (e.g. a Raspberry Pi with an SDR on the same LAN) — this keeps
//     aircraft visibility working when the internet uplink is gone.
//
// Every poll builds one canonical aircraft snapshot: per-aircraft
// position, altitude, speed, track, vertical rate, the minute vector and
// a 3-5 minute trail. The snapshot is published as an informational MQTT
// message (kind "aircraft") and mirrored into the web map layer.
package adsb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxBodyBytes bounds one provider response.
const maxBodyBytes = 4 << 20 // 4 MiB (a busy tar1090 receiver can be large)

// rateLimitError reports a provider-side 429 with the advertised
// Retry-After window; the run loop backs the next poll off by it.
type rateLimitError struct {
	retryAfter time.Duration
}

func (e *rateLimitError) Error() string {
	return fmt.Sprintf("rate limited, retry after %s", e.retryAfter)
}

// parseRetryAfter reads the Retry-After header (seconds or HTTP date),
// clamped to [1s, 5m]; unparseable values fall back to 30s.
func parseRetryAfter(v string) time.Duration {
	if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && secs > 0 {
		return clampRetry(time.Duration(secs) * time.Second)
	}
	if t, err := http.ParseTime(strings.TrimSpace(v)); err == nil {
		return clampRetry(time.Until(t))
	}
	return 30 * time.Second
}

func clampRetry(d time.Duration) time.Duration {
	if d < time.Second {
		return time.Second
	}
	if d > 5*time.Minute {
		return 5 * time.Minute
	}
	return d
}

// Providers.
const (
	ProviderAdsbLol = "adsblol"
	ProviderTar1090 = "tar1090"
)

// ProviderTarget is one aircraft as reported by a provider, normalized to
// the fields WarnFlux uses. Units are the provider's raw units; the
// adapter converts to canonical units.
type ProviderTarget struct {
	Hex         string
	Flight      string
	Type        string
	Category    string
	Lat         float64
	Lon         float64
	AltBaroFt   float64
	GSKt        float64
	TrackDeg    float64
	BaroRateFPM float64
	OnGround    bool
	SeenAt      time.Time
}

// Client fetches aircraft from one provider.
type Client struct {
	provider string
	baseURL  string
	http     *http.Client
}

// NewClient builds a provider client. baseURL must already be validated.
func NewClient(provider, baseURL string, timeout time.Duration) *Client {
	return &Client{
		provider: provider,
		baseURL:  strings.TrimRight(baseURL, "/"),
		http:     &http.Client{Timeout: timeout},
	}
}

// Fetch queries the provider for aircraft around (lat, lon) within
// radiusKm and returns them plus the provider's "now" timestamp.
func (c *Client) Fetch(ctx context.Context, lat, lon, radiusKm float64) ([]ProviderTarget, time.Time, error) {
	switch c.provider {
	case ProviderAdsbLol:
		return c.fetchAdsbLol(ctx, lat, lon, radiusKm)
	case ProviderTar1090:
		return c.fetchTar1090(ctx)
	default:
		return nil, time.Time{}, fmt.Errorf("unknown provider %q", c.provider)
	}
}

func (c *Client) getJSON(ctx context.Context, u string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "WarnFlux adsb/0.1")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, &rateLimitError{retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"))}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if len(data) > maxBodyBytes {
		return nil, fmt.Errorf("response exceeds %d bytes", maxBodyBytes)
	}
	return data, nil
}

// fetchAdsbLol queries the point/radius endpoint. The radius is nautical
// miles server-side, clamped to the API's allowed range.
func (c *Client) fetchAdsbLol(ctx context.Context, lat, lon, radiusKm float64) ([]ProviderTarget, time.Time, error) {
	nm := radiusKm / 1.852
	if nm < 1 {
		nm = 1
	}
	if nm > 250 {
		nm = 250
	}
	u := fmt.Sprintf("%s/v2/point/%.4f/%.4f/%.1f", c.baseURL, lat, lon, nm)
	data, err := c.getJSON(ctx, u)
	if err != nil {
		return nil, time.Time{}, err
	}
	var resp struct {
		AC    []adsbLolAC `json:"ac"`
		Now   int64       `json:"now"` // epoch milliseconds
		Msg   string      `json:"msg"`
		Total int         `json:"total"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, time.Time{}, fmt.Errorf("parse JSON: %w", err)
	}
	if resp.Msg != "" && resp.Msg != "No error" {
		return nil, time.Time{}, fmt.Errorf("provider error: %s", resp.Msg)
	}
	now := time.Now().UTC()
	if resp.Now > 0 {
		now = time.UnixMilli(resp.Now).UTC()
	}
	out := make([]ProviderTarget, 0, len(resp.AC))
	for _, ac := range resp.AC {
		t, ok := ac.target(now)
		if !ok {
			continue
		}
		out = append(out, t)
	}
	return out, now, nil
}

// fetchTar1090 queries a local readsb/tar1090 receiver. The receiver
// reports its whole coverage; the caller filters to the area of interest.
func (c *Client) fetchTar1090(ctx context.Context) ([]ProviderTarget, time.Time, error) {
	u := c.baseURL + "/data/aircraft.json"
	data, err := c.getJSON(ctx, u)
	if err != nil {
		return nil, time.Time{}, err
	}
	var resp struct {
		Aircraft []tar1090AC `json:"aircraft"`
		Now      float64     `json:"now"` // epoch seconds
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, time.Time{}, fmt.Errorf("parse JSON: %w", err)
	}
	now := time.Now().UTC()
	if resp.Now > 0 {
		now = time.Unix(int64(resp.Now), 0).UTC()
	}
	out := make([]ProviderTarget, 0, len(resp.Aircraft))
	for _, ac := range resp.Aircraft {
		t, ok := ac.target(now)
		if !ok {
			continue
		}
		out = append(out, t)
	}
	return out, now, nil
}

// flexFloat decodes a JSON number that the provider occasionally sends
// as a string (api.adsb.lol does this for alt_baro and other numeric
// fields). "null", missing values and non-numeric sentinel strings (the
// provider sends e.g. "ground" for grounded aircraft) decode as 0.
type flexFloat float64

func (f *flexFloat) UnmarshalJSON(data []byte) error {
	var n float64
	if err := json.Unmarshal(data, &n); err == nil {
		*f = flexFloat(n)
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("adsb: expected a number or string, got %s", data)
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		// Sentinel strings like "ground" are not numbers: treat them
		// like a missing value instead of dropping the whole snapshot.
		*f = 0
		return nil
	}
	*f = flexFloat(n)
	return nil
}

func (f flexFloat) Float() float64 { return float64(f) }

// adsbLolAC mirrors the ac[] entries of api.adsb.lol /v2/point.
type adsbLolAC struct {
	Hex      string    `json:"hex"`
	Flight   string    `json:"flight"`
	Type     string    `json:"t"`
	Category string    `json:"category"`
	Lat      flexFloat `json:"lat"`
	Lon      flexFloat `json:"lon"`
	AltBaro  flexFloat `json:"alt_baro"`
	GS       flexFloat `json:"gs"`
	Track    flexFloat `json:"track"`
	BaroRate flexFloat `json:"baro_rate"`
	Seen     flexFloat `json:"seen"` // seconds ago
}

// target converts one adsb.lol entry; entries without a position or
// without a hex code are skipped.
func (a adsbLolAC) target(now time.Time) (ProviderTarget, bool) {
	lat, lon := a.Lat.Float(), a.Lon.Float()
	alt, gs, track, baro, seen := a.AltBaro.Float(), a.GS.Float(), a.Track.Float(), a.BaroRate.Float(), a.Seen.Float()
	if a.Hex == "" || lat == 0 && lon == 0 {
		return ProviderTarget{}, false
	}
	return ProviderTarget{
		Hex:         a.Hex,
		Flight:      strings.TrimSpace(a.Flight),
		Type:        a.Type,
		Category:    a.Category,
		Lat:         lat,
		Lon:         lon,
		AltBaroFt:   alt,
		GSKt:        gs,
		TrackDeg:    track,
		BaroRateFPM: baro,
		OnGround:    alt == 0,
		SeenAt:      now.Add(-time.Duration(seen * float64(time.Second))),
	}, true
}

// tar1090AC mirrors /data/aircraft.json entries (readsb/tar1090 format).
type tar1090AC struct {
	Hex      string  `json:"hex"`
	Flight   string  `json:"flight"`
	Category string  `json:"category"`
	Lat      float64 `json:"lat"`
	Lon      float64 `json:"lon"`
	AltBaro  float64 `json:"alt_baro"`
	GS       float64 `json:"gs"`
	Track    float64 `json:"track"`
	BaroRate float64 `json:"baro_rate"`
	Seen     float64 `json:"seen"` // epoch seconds
}

func (a tar1090AC) target(now time.Time) (ProviderTarget, bool) {
	if a.Hex == "" || a.Lat == 0 && a.Lon == 0 {
		return ProviderTarget{}, false
	}
	seen := now
	if a.Seen > 0 {
		seen = time.Unix(int64(a.Seen), 0).UTC()
	}
	return ProviderTarget{
		Hex:         a.Hex,
		Flight:      strings.TrimSpace(a.Flight),
		Category:    a.Category,
		Lat:         a.Lat,
		Lon:         a.Lon,
		AltBaroFt:   a.AltBaro,
		GSKt:        a.GS,
		TrackDeg:    a.Track,
		BaroRateFPM: a.BaroRate,
		OnGround:    a.AltBaro == 0,
		SeenAt:      seen,
	}, true
}
