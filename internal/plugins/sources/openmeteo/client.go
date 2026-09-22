package openmeteo

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// defaultBaseURL is the public Open-Meteo forecast endpoint.
	defaultBaseURL = "https://api.open-meteo.com/v1/forecast"
	// customerBaseURL is used automatically when an API key is configured
	// and no base_url override is set.
	customerBaseURL = "https://customer-api.open-meteo.com/v1/forecast"
	// maxResponseBytes bounds one provider response body (1 MiB).
	maxResponseBytes = 1 << 20
	// userAgent identifies WarnFlux to the provider.
	userAgent = "WarnFlux openmeteo-source (github.com/szporwolik/WarnFlux)"
	// maxRetryAfter bounds how long a rate-limit backoff may delay the
	// poll loop: honoring Retry-After must not stall the source forever.
	maxRetryAfter = 5 * time.Minute
)

// currentVariables are the requested current-weather fields.
var currentVariables = []string{
	"temperature_2m",
	"relative_humidity_2m",
	"apparent_temperature",
	"is_day",
	"precipitation",
	"rain",
	"showers",
	"snowfall",
	"weather_code",
	"cloud_cover",
	"pressure_msl",
	"surface_pressure",
	"wind_speed_10m",
	"wind_direction_10m",
	"wind_gusts_10m",
}

// hourlyVariables are the requested hourly forecast fields.
var hourlyVariables = []string{
	"temperature_2m",
	"relative_humidity_2m",
	"apparent_temperature",
	"precipitation_probability",
	"precipitation",
	"weather_code",
	"cloud_cover",
	"pressure_msl",
	"wind_speed_10m",
	"wind_direction_10m",
	"wind_gusts_10m",
}

// dailyVariables are the requested daily forecast fields.
var dailyVariables = []string{
	"weather_code",
	"temperature_2m_max",
	"temperature_2m_min",
	"apparent_temperature_max",
	"apparent_temperature_min",
	"precipitation_probability_max",
	"precipitation_sum",
	"wind_speed_10m_max",
	"wind_gusts_10m_max",
	"wind_direction_10m_dominant",
	"sunrise",
	"sunset",
}

// Client is a minimal, bounded Open-Meteo HTTP client.
type Client struct {
	baseURL string
	apiKey  string // never logged
	client  *http.Client
}

// NewClient builds a client for the given endpoint. The API key is only
// attached to provider requests and never appears in logs or errors.
func NewClient(baseURL, apiKey string, timeout time.Duration) *Client {
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		client:  &http.Client{Timeout: timeout},
	}
}

// retryAfterError signals a transient, rate-limited provider response and
// carries the requested backoff delay.
type retryAfterError struct {
	after time.Duration
	msg   string
}

func (e *retryAfterError) Error() string { return e.msg }

// retryAfter returns the parsed Retry-After delay of an error, if any.
func retryAfter(err error) (time.Duration, bool) {
	re, ok := err.(*retryAfterError)
	if !ok || re.after <= 0 {
		return 0, false
	}
	return re.after, true
}

// Fetch performs one bounded forecast request and decodes the response.
func (c *Client) Fetch(ctx context.Context, loc Location, forecastHours, forecastDays int) (*ProviderResponse, error) {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}
	q := u.Query()
	q.Set("latitude", strconv.FormatFloat(loc.Latitude, 'f', 4, 64))
	q.Set("longitude", strconv.FormatFloat(loc.Longitude, 'f', 4, 64))
	q.Set("timezone", "auto")
	q.Set("forecast_hours", strconv.Itoa(forecastHours))
	q.Set("forecast_days", strconv.Itoa(forecastDays))
	q.Set("temperature_unit", "celsius")
	q.Set("wind_speed_unit", "kmh")
	q.Set("precipitation_unit", "mm")
	q.Set("current", strings.Join(currentVariables, ","))
	q.Set("hourly", strings.Join(hourlyVariables, ","))
	q.Set("daily", strings.Join(dailyVariables, ","))
	if c.apiKey != "" {
		q.Set("apikey", c.apiKey)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		// Transport errors may embed the request URL, which carries the
		// API key query parameter. Redact it before the error is logged
		// or surfaced anywhere.
		return nil, fmt.Errorf("request failed: %s", redactKey(err.Error(), c.apiKey))
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return nil, rateLimited(resp)
	default:
		return nil, fmt.Errorf("provider returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	if len(body) > maxResponseBytes {
		return nil, fmt.Errorf("provider response exceeds %d bytes", maxResponseBytes)
	}

	parsed, err := parseResponse(body)
	if err != nil {
		return nil, err
	}
	return parsed, nil
}

// redactKey replaces every occurrence of the API key (and its URL-encoded
// form) in a string, so provider URLs never leak credentials into logs or
// errors.
func redactKey(s, key string) string {
	if key == "" {
		return s
	}
	s = strings.ReplaceAll(s, key, "[redacted]")
	s = strings.ReplaceAll(s, url.QueryEscape(key), "[redacted]")
	return s
}

// rateLimited builds a retryAfterError from a 429/503 response, honoring a
// Retry-After header in seconds or HTTP-date form when present.
func rateLimited(resp *http.Response) error {
	status := resp.StatusCode
	msg := fmt.Sprintf("provider rate limited: HTTP %d", status)
	if v := resp.Header.Get("Retry-After"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil {
			return &retryAfterError{after: time.Duration(secs) * time.Second, msg: fmt.Sprintf("%s (Retry-After %ss)", msg, v)}
		}
		if t, err := http.ParseTime(v); err == nil {
			after := time.Until(t)
			return &retryAfterError{after: after, msg: fmt.Sprintf("%s (Retry-After %s)", msg, v)}
		}
	}
	return &retryAfterError{msg: msg}
}
