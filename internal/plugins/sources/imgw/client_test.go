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
