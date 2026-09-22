package rso

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	// maxResponseBytes bounds one provider response (8 MiB).
	maxResponseBytes = 8 << 20
	// maxItemsPerFeed bounds one regional response; exceeding it makes the
	// combined snapshot incomplete.
	maxItemsPerFeed = 4096
	// userAgent identifies WarnFlux to the provider.
	userAgent = "WarnFlux rso-source (github.com/szporwolik/WarnFlux)"
)

// Client is a minimal, bounded RSO public XML HTTP client.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient builds a client for the given base URL.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: timeout},
	}
}

// listURL builds the documented list URL for one voivodeship:
// /komunikatyxml/{voivodeship}/wszystkie/0?_format=xml (page 0 = all
// matching items, no pagination).
func listURL(baseURL, voivodeship string) string {
	return baseURL + "/komunikatyxml/" + voivodeship + "/wszystkie/0?_format=xml"
}

// Fetch performs one bounded GET for the voivodeship's communication list.
func (c *Client) Fetch(ctx context.Context, voivodeship string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, listURL(c.baseURL, voivodeship), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/xml")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	if len(body) > maxResponseBytes {
		return nil, fmt.Errorf("provider response exceeds %d bytes", maxResponseBytes)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("provider returned HTTP %d", resp.StatusCode)
	}
	return body, nil
}
