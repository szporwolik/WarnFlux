// Package metar is an informational weather source for aviation METAR
// reports served by the NOAA Aviation Weather Center
// (https://aviationweather.gov/api/data/metar). Each configured station
// becomes a canonical weather snapshot carrying the observation position
// (the API reports lat/lon itself), so the weather tab shows an airport
// pin with the current condition and temperature.
package metar

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// maxBodyBytes bounds one API response.
const maxBodyBytes = 1 << 20 // 1 MiB

// metarRecord is the relevant surface of one JSON METAR entry.
type metarRecord struct {
	IcaoID     string  `json:"icaoId"`
	Name       string  `json:"name"`
	Lat        float64 `json:"lat"`
	Lon        float64 `json:"lon"`
	Elev       float64 `json:"elev"`
	ObsTime    int64   `json:"obsTime"` // unix seconds
	ReportTime string  `json:"reportTime"`
	Temp       float64 `json:"temp"`
	Dewp       float64 `json:"dewp"`
	Wdir       float64 `json:"wdir"`
	Wspd       float64 `json:"wspd"` // knots
	Wgst       float64 `json:"wgst"` // knots
	Altim      float64 `json:"altim"`
	RawOb      string  `json:"rawOb"`
	WxString   string  `json:"wxString"`
	Clouds     []struct {
		Cover string  `json:"cover"`
		Base  float64 `json:"base"`
	} `json:"clouds"`
}

// Client fetches METARs from the NOAA AWC API.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient builds an API client. baseURL must already be validated.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: timeout},
	}
}

// Fetch downloads the current METARs for the given station IDs (one
// batched request). Stations without a current report are simply absent
// from the response.
func (c *Client) Fetch(ctx context.Context, ids []string) ([]metarRecord, error) {
	q := url.Values{}
	q.Set("ids", strings.Join(ids, ","))
	q.Set("format", "json")
	u := c.baseURL + "/metar?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "WarnFlux metar/0.1")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()

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

	var out []metarRecord
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no METAR records in the response")
	}
	return out, nil
}
