// Package gios is a hazard source for the GIOŚ serious-industrial-
// accident register ("poważne awarie", https://dane.gios.gov.pl, API
// /api/powazne-awarie). Accidents of the current year are filtered to the
// configured administrative area (voivodeship, optionally powiat) and
// then geocoded to the locality centroid via the GUGiK ULDK service, so
// every event carries coordinates the web UI can draw on the map.
//
// Event types: INDUSTRIAL_ACCIDENT (most records) and CHEMICAL_RELEASE
// (rodzajZdarzenia "emisja (wyciek)").
package gios

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// pageSize is the maximum provider page size (50). maxPages bounds the
// paging loop so a provider quirk can never spin forever.
const (
	pageSize     = 50
	maxPages     = 20
	maxBodyBytes = 4 << 20 // 4 MiB
)

// Client fetches and parses the GIOŚ poważne-awarie API.
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

// FetchAwarie pages through /v1/powazne-awarie and returns the complete
// matching record set. rok filters the register (data available from
// 2017); wojewodztwo/powiat are passed verbatim as administrative scope.
func (c *Client) FetchAwarie(ctx context.Context, rok int, wojewodztwo, powiat string) ([]awariaRekord, error) {
	var out []awariaRekord
	for page := 0; page < maxPages; page++ {
		q := url.Values{}
		if rok > 0 {
			q.Set("rok", fmt.Sprintf("%d", rok))
		}
		if w := wojewodztwo; w != "" {
			q.Set("wojewodztwo", w)
		}
		if p := powiat; p != "" {
			q.Set("powiat", p)
		}
		q.Set("numerStrony", fmt.Sprintf("%d", page))
		q.Set("liczbaElementowNaStronie", fmt.Sprintf("%d", pageSize))

		u := c.baseURL + "/v1/powazne-awarie?" + q.Encode()
		recs, err := c.fetchPage(ctx, u)
		if err != nil {
			return nil, fmt.Errorf("page %d: %w", page, err)
		}
		out = append(out, recs...)
		if len(recs) < pageSize {
			return out, nil
		}
	}
	return nil, fmt.Errorf("gios: paging exceeded %d pages", maxPages)
}

// fetchPage downloads and decodes one response page.
func (c *Client) fetchPage(ctx context.Context, u string) ([]awariaRekord, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "WarnFlux gios/0.1")

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

	var doc paResponse
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}
	return doc.Dane.Strona, nil
}
