package imgw

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFetchOKAndRequestShape(t *testing.T) {
	var gotPath, gotUA, gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotUA, gotAccept = r.URL.Path, r.Header.Get("User-Agent"), r.Header.Get("Accept")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[{"id":"x"}]`)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, time.Second)
	body, err := c.Fetch(context.Background(), feedMeteo)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(body) != `[{"id":"x"}]` {
		t.Errorf("body = %s", body)
	}
	if gotPath != "/warningsmeteo" {
		t.Errorf("path = %q, want /warningsmeteo", gotPath)
	}
	if gotUA != userAgent || gotAccept != "application/json" {
		t.Errorf("headers = UA %q Accept %q", gotUA, gotAccept)
	}

	if _, err := c.Fetch(context.Background(), feedHydro); err != nil {
		t.Fatalf("Fetch hydro: %v", err)
	}
	if gotPath != "/warningshydro" {
		t.Errorf("hydro path = %q, want /warningshydro", gotPath)
	}
}

func TestFetchNoProducts404IsEmptySuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"status":false,"message":"No products were found"}`)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, time.Second)
	body, err := c.Fetch(context.Background(), feedMeteo)
	if err != nil {
		t.Fatalf("documented no-products 404 must be a successful empty snapshot: %v", err)
	}
	if string(body) != "[]" {
		t.Errorf("body = %s, want []", body)
	}
}

// TestFetchMeteoNoWarningsObjectIsEmptySuccess: the meteo endpoint
// reports "no warnings" with HTTP 200 and an OBJECT body; only that exact
// payload is an empty successful snapshot (any other object is a
// provider failure, so mass cancellation can never masquerade as calm).
func TestFetchMeteoNoWarningsObjectIsEmptySuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"message":"Brak ostrzeżeń meteorologicznych"}`)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, time.Second)
	body, err := c.Fetch(context.Background(), feedMeteo)
	if err != nil {
		t.Fatalf("documented meteo no-warnings object must be an empty snapshot: %v", err)
	}
	if string(body) != "[]" {
		t.Errorf("body = %s, want []", body)
	}
}

func TestFetchFailures(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"unexpected 404", http.StatusNotFound, `{"status":true}`},
		{"plain 404", http.StatusNotFound, `not found`},
		{"500", http.StatusInternalServerError, `boom`},
		{"503", http.StatusServiceUnavailable, `boom`},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(c.status)
			fmt.Fprint(w, c.body)
		}))
		client := NewClient(srv.URL, time.Second)
		_, err := client.Fetch(context.Background(), feedHydro)
		srv.Close()
		if err == nil {
			t.Errorf("%s: expected a provider failure", c.name)
		}
	}
}

func TestFetchOversizedResponseRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, strings.Repeat("a", maxResponseBytes+1))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, time.Second)
	if _, err := c.Fetch(context.Background(), feedMeteo); err == nil {
		t.Fatal("oversized response must be rejected")
	}
}

func TestFetchRespectsContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	c := NewClient(srv.URL, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Fetch(ctx, feedMeteo); err == nil {
		t.Fatal("cancelled context must fail the request")
	}
}

// TestFetch404StrictRecognition: a 404 whose body says "no warnings" is a
// successful empty snapshot; every other 404 is a provider failure, because a
// failure read as "no warnings" would cancel active alarms.
func TestFetch404StrictRecognition(t *testing.T) {
	// The provider has used both wordings for the same condition: the
	// documented English one and the Polish one it returns today (production
	// reported the station as degraded for hours because only the English
	// string was accepted).
	for _, payload := range []string{
		`{"status":false,"message":"No products were found"}`,
		`{"status":false,"message":"Brak ostrzeżeń hydrologicznych"}`,
		`{"status":false,"message":"Brak ostrzeżeń meteorologicznych"}`,
		`{"status":false,"message":"brak ostrzezen"}`,
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, payload)
		}))
		body, err := NewClient(srv.URL, time.Second).Fetch(context.Background(), feedHydro)
		srv.Close()
		if err != nil || string(body) != "[]" {
			t.Fatalf("no-warnings payload %s: body=%s err=%v", payload, body, err)
		}
	}

	reject := []string{
		`{}`,
		`{"status":false}`,
		`{"message":"No products were found"}`,
		`{"status":false,"message":"Database unavailable"}`,
		`{"status":true,"message":"No products were found"}`,
		`{"status":false,"message":"Not Found"}`,
		`{"status":false,"message":"Brak danych"}`,
		`{"status":false,"message":"  "}`,
		`not json`,
	}
	for _, payload := range reject {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, payload)
		}))
		client := NewClient(srv.URL, time.Second)
		_, err := client.Fetch(context.Background(), feedHydro)
		srv.Close()
		if err == nil {
			t.Errorf("404 payload %s must be a provider failure", payload)
		}
	}
}

// TestFetch200NoWarningsObject: the meteo endpoint says "no warnings" with a
// plain object instead of an array — the same wording rule applies there.
func TestFetch200NoWarningsObject(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"message":"Brak ostrzeżeń meteorologicznych"}`)
	}))
	defer srv.Close()

	body, err := NewClient(srv.URL, time.Second).Fetch(context.Background(), feedMeteo)
	if err != nil || string(body) != "[]" {
		t.Fatalf("no-warnings object: body=%s err=%v", body, err)
	}

	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"message":"Service temporarily unavailable"}`)
	}))
	defer other.Close()
	if _, err := NewClient(other.URL, time.Second).Fetch(context.Background(), feedMeteo); err != nil {
		t.Fatalf("an unrecognised object must be passed on to the parser, not failed here: %v", err)
	}
}

// TestRequireJSONArray: HTTP 200 with a non-array top-level value must be
// incomplete — json.Unmarshal(null, &slice) silently yields an empty slice
// in Go, which must never look like an empty warning set.
func TestRequireJSONArray(t *testing.T) {
	for _, good := range []string{`[]`, `[{"id":"x"}]`, ` [ ] `} {
		if err := requireJSONArray([]byte(good)); err != nil {
			t.Errorf("array %q rejected: %v", good, err)
		}
	}
	for _, bad := range []string{`null`, `{}`, `false`, `123`, `"foo"`, ``, `[`} {
		if err := requireJSONArray([]byte(bad)); err == nil {
			t.Errorf("non-array %q accepted", bad)
		}
	}
}
