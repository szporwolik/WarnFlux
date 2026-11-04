package gddkiadatex

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"
)

// soapAction is the operation the pull service exposes; the service rejects a
// call without it.
const soapAction = "http://datex2.eu/wsdl/snapshotPull/2020/pullData"

// maxResponseBytes bounds one download. The national snapshot is ~2 MB today;
// the bound exists so a misbehaving endpoint cannot exhaust memory.
const maxResponseBytes = 64 << 20

// Client asks the KPD snapshot-pull service for the complete set of current
// obstructions.
type Client struct {
	url    string
	signer *signer
	http   *http.Client
}

// NewClient builds the client. The endpoint is validated by the caller.
func NewClient(endpoint string, s *signer, timeout time.Duration) *Client {
	return &Client{
		url:    endpoint,
		signer: s,
		http: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					// The client certificate is self-signed: it identifies the
					// caller, it is not part of a chain the server would verify.
					MinVersion: tls.VersionTLS12,
				},
				MaxIdleConns:        2,
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 15 * time.Second,
			},
		},
	}
}

// Fetch performs one signed pull and returns the raw SOAP response body.
func (c *Client) Fetch(ctx context.Context) ([]byte, error) {
	body, err := c.signer.signedEnvelope()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("kpd: build request: %w", err)
	}
	req.Header.Set("Content-Type", "text/xml; charset=UTF-8")
	req.Header.Set("SOAPAction", soapAction)
	req.Header.Set("Accept", "text/xml")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kpd: request failed: %w", err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("kpd: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kpd: HTTP %d: %s", resp.StatusCode, soapFault(payload))
	}
	return payload, nil
}

// soapFault extracts the faultstring from a SOAP fault so the log line says
// what the service complained about instead of dumping the envelope.
func soapFault(body []byte) string {
	const open, close = "<faultstring", "</faultstring>"
	start := bytes.Index(body, []byte(open))
	if start < 0 {
		if len(body) > 300 {
			body = body[:300]
		}
		return string(bytes.TrimSpace(body))
	}
	gt := bytes.IndexByte(body[start:], '>')
	if gt < 0 {
		return "unparseable SOAP fault"
	}
	rest := body[start+gt+1:]
	end := bytes.Index(rest, []byte(close))
	if end < 0 {
		return "unparseable SOAP fault"
	}
	return string(bytes.TrimSpace(rest[:end]))
}
