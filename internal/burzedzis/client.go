// Package burzedzis is a client for the burze.dzis.net storm and severe
// weather API (SOAP over POST /soap.php): locality coordinates, the
// official weather warnings issued for one point and the lightning
// discharge search around it.
//
// The provider republishes the official Polish warnings, so the degrees it
// returns are the ones operators already know: 0 means "not in force",
// 1..3 are the first, second and third degree. Every date it returns is
// UTC, and every answer is bound to a POINT — the client asks about
// coordinates, never about a warning area.
//
// Two provider quirks shape this package. An unknown locality answers 0/0
// instead of failing, and 0/0 is at the same time the provider's "return
// test data" switch (a full set of third-degree warnings) — so a mistyped
// town would turn into fake severe alerts. Coordinates therefore always
// come from Locality(), which rejects 0/0 with ErrUnknownLocality, and the
// warnings endpoint is never queried with a point it did not resolve.
package burzedzis

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// DefaultBaseURL is the provider's API host.
	DefaultBaseURL = "https://burze.dzis.net"

	soapPath       = "/soap.php"
	soapNS         = "https://burze.dzis.net/soap.php"
	soapEnvelopeNS = "http://schemas.xmlsoap.org/soap/envelope/"
	userAgent      = "WarnFlux burzedzis/0.1"

	// maxBodyBytes bounds one SOAP response: the envelopes are a few
	// hundred bytes, anything beyond this is a transport risk.
	maxBodyBytes = 256 << 10

	// DefaultStormRadiusKM is the provider's own default search radius.
	DefaultStormRadiusKM = 25
)

// ErrUnknownLocality reports a locality the provider's directory does not
// know; it answers 0/0 for those, which the warnings endpoint reads as
// "return test data" (a complete set of third-degree warnings).
var ErrUnknownLocality = errors.New("locality unknown to burze.dzis.net")

// faultError is a SOAP fault: the provider refusing the request (bad or
// missing key, exhausted quota, unusable coordinates).
type faultError struct {
	Text string
}

func (e *faultError) Error() string {
	return "burze.dzis.net refused the request: " + e.Text
}

// IsFault reports whether err is a refusal by the provider rather than a
// transport or decoding problem. The text is the provider's own.
func IsFault(err error) bool {
	var f *faultError
	return errors.As(err, &f)
}

// errNoPayload reports an answer that parsed as XML but does not carry the
// operation's own payload — an empty SOAP body, a slightly different
// document, or the maintenance page the provider (and any proxy in front of
// it) returns with HTTP 200.
//
// This matters more here than anywhere else: the warnings answer is a
// COMPLETE snapshot per point, so reading an empty document as "nothing is in
// force" would reconcile every live warning away. An unreadable answer has to
// be an error, never an empty snapshot.
func errNoPayload(operation string) error {
	return fmt.Errorf("the answer carries no %s/return payload (empty SOAP body, maintenance page or another service?)", operation)
}

// Client talks to one burze.dzis.net installation.
type Client struct {
	baseURL string
	key     string
	http    *http.Client
}

// NewClient builds a client. baseURL must already be validated and points
// at the API root (https://burze.dzis.net) so tests can substitute an
// httptest server.
func NewClient(baseURL, apiKey string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		key:     strings.TrimSpace(apiKey),
		http:    &http.Client{Timeout: timeout},
	}
}

// Locality resolves a locality name into coordinates. The directory is
// diacritic-sensitive ("Niepołomice" resolves, "Niepolomice" does not).
func (c *Client) Locality(ctx context.Context, name string) (Locality, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Locality{}, fmt.Errorf("locality name is empty")
	}
	var doc cityResponse
	if err := c.call(ctx, cityRequest{NS: soapNS, Name: name, Key: c.key}, &doc); err != nil {
		return Locality{}, err
	}
	ret := doc.Body.City.Return
	if ret == nil || ret.CoordY == nil || ret.CoordX == nil {
		return Locality{}, errNoPayload("miejscowosc")
	}
	lat, lon := *ret.CoordY, *ret.CoordX
	if lat == 0 && lon == 0 {
		return Locality{}, fmt.Errorf("%q: %w", name, ErrUnknownLocality)
	}
	return Locality{Name: name, Lat: lat, Lon: lon}, nil
}

// Warnings fetches the warnings in force for one point. The answer is a
// COMPLETE snapshot for that point: every phenomenon appears, a degree of
// 0 meaning "not in force", so disappearances can be reconciled.
func (c *Client) Warnings(ctx context.Context, at Locality) (Warnings, error) {
	req := warningsRequest{NS: soapNS, Y: at.Lat, X: at.Lon, Key: c.key}
	var doc warningsResponse
	if err := c.call(ctx, req, &doc); err != nil {
		return Warnings{}, err
	}
	if doc.Body.Return == nil {
		return Warnings{}, errNoPayload("ostrzezenia_pogodowe")
	}
	return doc.Body.Return.snapshot(at)
}

// Storm searches for lightning discharges around one point within radiusKM
// kilometres (0 = the provider's default), over the provider's rolling
// window (10, 15 or 20 minutes).
func (c *Client) Storm(ctx context.Context, at Locality, radiusKM int) (Storm, error) {
	req := stormRequest{NS: soapNS, Y: at.Lat, X: at.Lon, Radius: radiusKM, Key: c.key}
	var doc stormResponse
	if err := c.call(ctx, req, &doc); err != nil {
		return Storm{}, err
	}
	ret := doc.Body.Return
	if ret == nil || ret.Count == nil || ret.Distance == nil || ret.Window == nil {
		return Storm{}, errNoPayload("szukaj_burzy")
	}
	return Storm{
		Locality:   at.Name,
		Lat:        at.Lat,
		Lon:        at.Lon,
		RadiusKM:   radiusKM,
		Count:      *ret.Count,
		DistanceKM: *ret.Distance,
		Direction:  strings.TrimSpace(ret.Direction),
		WindowMins: *ret.Window,
	}, nil
}

// call posts one SOAP body and decodes the answer, unwrapping a fault into
// a readable error.
func (c *Client) call(ctx context.Context, inner any, out any) error {
	payload, err := xml.Marshal(inner)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	body := append([]byte(xml.Header), []byte("<Envelope xmlns=\""+soapEnvelopeNS+"\"><Body>")...)
	body = append(body, payload...)
	body = append(body, []byte("</Body></Envelope>")...)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+soapPath, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "text/xml; charset=utf-8")
	req.Header.Set("User-Agent", userAgent)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("post SOAP: %w", err)
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
	if text := faultText(data); text != "" {
		return &faultError{Text: text}
	}
	if err := xml.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode SOAP response: %w", err)
	}
	return nil
}

// faultText extracts a SOAP fault message, if the document carries one. It
// runs before the typed decode because a fault shares no element with any
// operation's response shape.
func faultText(data []byte) string {
	var doc struct {
		Body struct {
			Fault struct {
				Text string `xml:"faultstring"`
			} `xml:"Fault"`
		} `xml:"Body"`
	}
	if err := xml.Unmarshal(data, &doc); err != nil {
		return ""
	}
	text := strings.TrimSpace(doc.Body.Fault.Text)
	if text == "" {
		return ""
	}
	// The provider repeats every message in Polish and English; the first
	// part is the Polish one, which is what the operator reads.
	if i := strings.Index(text, " / "); i > 0 {
		text = text[:i]
	}
	return text
}
