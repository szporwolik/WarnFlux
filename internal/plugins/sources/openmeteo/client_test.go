package openmeteo

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestFetchRequestParameters(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("latitude") != "50.0000" || q.Get("longitude") != "20.0000" {
			t.Errorf("coordinates = (%s, %s), want (50.0000, 20.0000)", q.Get("latitude"), q.Get("longitude"))
		}
		if q.Get("timezone") != "auto" {
			t.Errorf("timezone = %q, want auto", q.Get("timezone"))
		}
		if q.Get("forecast_hours") != "48" || q.Get("forecast_days") != "7" {
			t.Errorf("forecast limits = (%s, %s), want (48, 7)", q.Get("forecast_hours"), q.Get("forecast_days"))
		}
		if q.Get("temperature_unit") != "celsius" || q.Get("wind_speed_unit") != "kmh" || q.Get("precipitation_unit") != "mm" {
			t.Errorf("units = (%s, %s, %s)", q.Get("temperature_unit"), q.Get("wind_speed_unit"), q.Get("precipitation_unit"))
		}
		if q.Get("apikey") != "" {
			t.Errorf("apikey must be absent without a configured key")
		}
		for name, vars := range map[string]string{
			"current": q.Get("current"),
			"hourly":  q.Get("hourly"),
			"daily":   q.Get("daily"),
		} {
			wantCur, wantHour, wantDay := queryVars()
			want := map[string]string{"current": wantCur, "hourly": wantHour, "daily": wantDay}[name]
			if vars != want {
				t.Errorf("%s variables = %q, want %q", name, vars, want)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, validResponseJSON())
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", time.Second)
	if _, err := c.Fetch(context.Background(), Location{ID: "home", Latitude: 50, Longitude: 20}, 48, 7); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
}

func TestFetchSendsAPIKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("apikey") != "secret-key" {
			t.Errorf("apikey = %q, want secret-key", r.URL.Query().Get("apikey"))
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, validResponseJSON())
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "secret-key", time.Second)
	if _, err := c.Fetch(context.Background(), Location{ID: "home", Latitude: 50, Longitude: 20}, 48, 7); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
}

func TestFetchStatusErrors(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		headers map[string]string
		wantRet bool
	}{
		{"500", http.StatusInternalServerError, nil, false},
		{"429 no retry-after", http.StatusTooManyRequests, nil, false},
		{"429 retry-after seconds", http.StatusTooManyRequests, map[string]string{"Retry-After": "120"}, true},
		{"503 retry-after date", http.StatusServiceUnavailable, map[string]string{"Retry-After": time.Now().Add(time.Minute).UTC().Format(http.TimeFormat)}, true},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for k, v := range c.headers {
				w.Header().Set(k, v)
			}
			http.Error(w, "nope", c.status)
		}))
		client := NewClient(srv.URL, "", time.Second)
		_, err := client.Fetch(context.Background(), Location{ID: "home", Latitude: 50, Longitude: 20}, 48, 7)
		srv.Close()
		if err == nil {
			t.Errorf("%s: expected error, got nil", c.name)
			continue
		}
		if c.wantRet {
			if after, ok := retryAfter(err); !ok || after <= 0 {
				t.Errorf("%s: expected Retry-After backoff, got %v", c.name, err)
			}
		} else if _, ok := retryAfter(err); ok {
			t.Errorf("%s: unexpected Retry-After backoff", c.name)
		}
	}
}

func TestFetchOversizedResponseRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"pad":"`)
		for i := 0; i < maxResponseBytes/64; i++ {
			fmt.Fprint(w, strings.Repeat("a", 64))
		}
		fmt.Fprint(w, `"}`)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", time.Second)
	if _, err := c.Fetch(context.Background(), Location{ID: "home", Latitude: 50, Longitude: 20}, 48, 7); err == nil {
		t.Fatal("oversized response must be rejected")
	}
}

func TestFetchErrorNeverLeaksAPIKey(t *testing.T) {
	// Point the client at a dead endpoint: the transport error contains the
	// URL including the apikey parameter and must be redacted.
	c := NewClient("http://127.0.0.1:1/forecast", "super-secret-key", 50*time.Millisecond)
	_, err := c.Fetch(context.Background(), Location{ID: "home", Latitude: 50, Longitude: 20}, 48, 7)
	if err == nil {
		t.Fatal("expected transport error")
	}
	if strings.Contains(err.Error(), "super-secret-key") {
		t.Fatalf("error leaks the API key: %v", err)
	}
	if strings.Contains(err.Error(), url.QueryEscape("super-secret-key")) {
		t.Fatalf("error leaks the URL-encoded API key: %v", err)
	}
}

func TestFetchMalformedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, "{not json")
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", time.Second)
	if _, err := c.Fetch(context.Background(), Location{ID: "home", Latitude: 50, Longitude: 20}, 48, 7); err == nil {
		t.Fatal("malformed provider JSON must be rejected")
	}
}

// TestRateLimitedHTTPDateUsesInjectedClock: HTTP-date Retry-After values
// are interpreted against the client's injectable clock, so scheduling
// tests never depend on real wall-clock timing.
func TestRateLimitedHTTPDateUsesInjectedClock(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	c := NewClient("https://example.invalid", "", time.Second)
	c.now = func() time.Time { return now }

	// Future HTTP-date → a positive bounded delay.
	future := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{},
	}
	future.Header.Set("Retry-After", now.Add(90*time.Second).Format(http.TimeFormat))
	if after, ok := retryAfter(c.rateLimited(future)); !ok || after != 90*time.Second {
		t.Errorf("future HTTP-date delay = %v (%v), want exactly 90s", after, ok)
	}

	// Past HTTP-date → no positive delay (a stale header must not create
	// backoff).
	past := &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Header:     http.Header{},
	}
	past.Header.Set("Retry-After", now.Add(-10*time.Second).Format(http.TimeFormat))
	if after, ok := retryAfter(c.rateLimited(past)); ok || after > 0 {
		t.Errorf("past HTTP-date produced delay %v, want none", after)
	}

	// Delta-seconds form stays primary and is unaffected by the clock.
	delta := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{},
	}
	delta.Header.Set("Retry-After", "120")
	if after, ok := retryAfter(c.rateLimited(delta)); !ok || after != 2*time.Minute {
		t.Errorf("delta-seconds delay = %v (%v), want exactly 2m", after, ok)
	}

	// Garbage Retry-After header → error without a delay (no hot loop).
	garbage := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{},
	}
	garbage.Header.Set("Retry-After", "not-a-date")
	if after, ok := retryAfter(c.rateLimited(garbage)); ok || after != 0 {
		t.Errorf("garbage Retry-After produced delay %v, want none", after)
	}
}
