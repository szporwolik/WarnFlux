package imgw

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// maxResponseBytes bounds one provider feed response (4 MiB).
	maxResponseBytes = 4 << 20
	// maxWarningsPerFeed bounds one provider snapshot; exceeding it makes
	// the snapshot incomplete (no disappearance reconciliation).
	maxWarningsPerFeed = 4096
	// userAgent identifies WarnFlux to the provider.
	userAgent = "WarnFlux imgw-source (github.com/szporwolik/WarnFlux)"
)

// Client is a minimal, bounded IMGW danepubliczne HTTP client.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient builds a client for the given data API base URL.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: timeout},
	}
}

// endpointFor maps a configured feed name to its documented endpoint path.
func endpointFor(feed string) string {
	switch feed {
	case feedHydro:
		return "warningshydro"
	default:
		return "warningsmeteo"
	}
}

// Fetch performs one bounded GET for the feed endpoint. The documented
// empty-warning condition — HTTP 404 with a {"status":false,...} body — is
// returned as a successful empty snapshot ("[]"); any other non-200 status
// is a provider failure.
func (c *Client) Fetch(ctx context.Context, feed string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/"+endpointFor(feed), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")

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

	switch resp.StatusCode {
	case http.StatusOK:
		// The meteo endpoint reports "no warnings" with HTTP 200 and an
		// OBJECT {"message":"Brak ostrzeżeń meteorologicznych"} instead of
		// an array — the same condition the 404 path handles below. Only
		// that exact payload is an empty successful snapshot.
		var msg providerMessage
		if err := json.Unmarshal(body, &msg); err == nil &&
			strings.TrimSpace(msg.Message) == "Brak ostrzeżeń meteorologicznych" {
			return []byte("[]"), nil
		}
		return body, nil
	case http.StatusNotFound:
		// IMGW reports "no warnings currently" as HTTP 404 with the EXACT
		// payload {"status":false,"message":"No products were found"}.
		// ONLY that exact condition is an empty successful snapshot; any
		// other 404 (missing status, wrong message, status true, malformed
		// JSON) is a provider failure — a provider error must never look
		// like an empty warning set.
		var st providerStatus
		if err := json.Unmarshal(body, &st); err == nil &&
			st.Status != nil && !*st.Status &&
			strings.TrimSpace(st.Message) == "No products were found" {
			return []byte("[]"), nil
		}
		return nil, fmt.Errorf("provider returned HTTP 404 without the documented no-products payload")
	default:
		return nil, fmt.Errorf("provider returned HTTP %d", resp.StatusCode)
	}
}

// requireJSONArray ensures the response body's top-level JSON value is an
// ARRAY. json.Unmarshal([]byte("null"), &slice) silently succeeds in Go,
// so a provider returning null/object/scalar with HTTP 200 could otherwise
// look like an empty warning set and trigger mass cancellation.
func requireJSONArray(body []byte) error {
	var raw json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return fmt.Errorf("response is not valid JSON: %w", err)
	}
	if len(raw) == 0 || raw[0] != '[' {
		return fmt.Errorf("response top-level value is not a JSON array")
	}
	return nil
}
