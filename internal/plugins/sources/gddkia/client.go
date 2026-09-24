package gddkia

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"time"
)

// maxFeedBytes bounds the downloaded XML document; the feed is a national
// listing, so anything beyond this is treated as a transport/parsing risk.
const maxFeedBytes = 4 << 20 // 4 MiB

// Client fetches and parses the GDDKiA difficulties feed.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient builds a feed client. baseURL must already be validated.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: timeout},
	}
}

// Fetch downloads and parses the current feed document. The feed is a
// COMPLETE current-state snapshot (like IMGW/RSO): disappearances are
// reconciled against it by the caller.
func (c *Client) Fetch(ctx context.Context) (*feed, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/xml, text/xml")
	req.Header.Set("User-Agent", "WarnFlux gddkia/0.1")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxFeedBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if len(data) > maxFeedBytes {
		return nil, fmt.Errorf("feed exceeds %d bytes", maxFeedBytes)
	}

	var f feed
	if err := xml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse XML: %w", err)
	}
	if len(f.Utr) == 0 {
		return nil, fmt.Errorf("feed contains no entries")
	}
	return &f, nil
}
