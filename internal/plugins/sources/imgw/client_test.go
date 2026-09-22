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

// TestFetch404StrictRecognition: ONLY the exact documented payload
// {"status":false,"message":"No products were found"} is a successful
// empty snapshot; every other 404 is a provider failure.
func TestFetch404StrictRecognition(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"status":false,"message":"No products were found"}`)
	}))
	body, err := NewClient(ok.URL, time.Second).Fetch(context.Background(), feedMeteo)
	ok.Close()
	if err != nil || string(body) != "[]" {
		t.Fatalf("exact no-products payload: body=%s err=%v", body, err)
	}

	reject := []string{
		`{}`,
		`{"status":false}`,
		`{"message":"No products were found"}`,
		`{"status":false,"message":"Database unavailable"}`,
		`{"status":true,"message":"No products were found"}`,
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
