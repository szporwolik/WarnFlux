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
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// maxBodyBytes bounds one API response.
const maxBodyBytes = 1 << 20 // 1 MiB

// flexFloat decodes a JSON number that the provider occasionally sends
// as a string (the AWC API reports "VRB" for variable wind direction
// and similar sentinels for other observation fields). Non-numeric
// sentinels decode as NaN so the adapter omits the field instead of
// failing the whole poll.
type flexFloat float64

func (f *flexFloat) UnmarshalJSON(data []byte) error {
	var n float64
	if err := json.Unmarshal(data, &n); err == nil {
		*f = flexFloat(n)
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("metar: expected a number or string, got %s", data)
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		*f = flexFloat(math.NaN())
		return nil
	}
	*f = flexFloat(n)
	return nil
}

func (f flexFloat) Float() float64 { return float64(f) }

// metarRecord is the relevant surface of one JSON METAR entry. The
// observation fields are flexible: the provider occasionally sends
// non-numeric sentinels where a number is expected.
type metarRecord struct {
	IcaoID     string    `json:"icaoId"`
	Name       string    `json:"name"`
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	Elev       float64   `json:"elev"`
	ObsTime    int64     `json:"obsTime"` // unix seconds
	ReportTime string    `json:"reportTime"`
	Temp       flexFloat `json:"temp"`
	Dewp       flexFloat `json:"dewp"`
	Wdir       flexFloat `json:"wdir"`
	Wspd       flexFloat `json:"wspd"` // knots
	Wgst       flexFloat `json:"wgst"` // knots
	Altim      flexFloat `json:"altim"`
	RawOb      string    `json:"rawOb"`
	WxString   string    `json:"wxString"`
	Clouds     []struct {
		Cover string    `json:"cover"`
		Base  flexFloat `json:"base"`
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
