package giosaq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// maxBodyBytes bounds one API response (station/findAll?size=500 is a few
// hundred KB; anything beyond that is a transport risk).
const maxBodyBytes = 4 << 20 // 4 MiB

// defaultLevelsURL is the official exceedance endpoint (current-state
// history, newest first). It is rate-limited to 2 requests/minute.
const defaultLevelsURL = "https://api.gios.gov.pl/pjp-api/v1/rest/levels/getInformationAboutExceeding"

// defaultStationsURL is the station directory (one request with size=500
// covers the whole country).
const defaultStationsURL = "https://api.gios.gov.pl/pjp-api/v1/rest/station/findAll?size=500"

// Client fetches the two GIOŚ PJP endpoints the plugin uses.
type Client struct {
	baseURL     string
	http        *http.Client
	levelsURL   string
	stationsURL string
}

// NewClient builds an API client. baseURL must already be validated; it
// points at the API root (https://api.gios.gov.pl/pjp-api) so tests can
// substitute an httptest server.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL:     baseURL,
		http:        &http.Client{Timeout: timeout},
		levelsURL:   baseURL + "/v1/rest/levels/getInformationAboutExceeding",
		stationsURL: baseURL + "/v1/rest/station/findAll?size=500",
	}
}

func (c *Client) getJSON(ctx context.Context, u string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	// The GIOŚ PJP API answers 406 to "Accept: application/json" (its
	// content negotiation expects */*), so the header is omitted.
	req.Header.Set("User-Agent", "WarnFlux giosaq/0.1")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}
	if len(data) > maxBodyBytes {
		return fmt.Errorf("response exceeds %d bytes", maxBodyBytes)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("parse JSON: %w", err)
	}
	return nil
}

// FetchLevels downloads the first page of exceedance records.
func (c *Client) FetchLevels(ctx context.Context) ([]exceedance, int, error) {
	var doc levelsDoc
	if err := c.getJSON(ctx, c.levelsURL, &doc); err != nil {
		return nil, 0, err
	}
	return doc.Records, doc.TotalPages, nil
}

// FetchStations downloads the station directory (code -> coordinates).
func (c *Client) FetchStations(ctx context.Context) (map[string]stationRef, error) {
	var doc stationsDoc
	if err := c.getJSON(ctx, c.stationsURL, &doc); err != nil {
		return nil, err
	}
	out := make(map[string]stationRef, len(doc.Stations))
	for _, st := range doc.Stations {
		ref, ok := stationRefOf(st)
		if !ok || st.Code == "" {
			continue
		}
		out[st.Code] = ref
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("station directory is empty")
	}
	return out, nil
}

// FetchAQIndex downloads the official air-quality index for one station.
func (c *Client) FetchAQIndex(ctx context.Context, stationID int64) (aqIndex, error) {
	u := fmt.Sprintf("%s/v1/rest/aqindex/getIndex/%d", c.baseURL, stationID)
	var doc aqIndexDoc
	if err := c.getJSON(ctx, u, &doc); err != nil {
		return aqIndex{}, err
	}
	return doc.Index, nil
}
