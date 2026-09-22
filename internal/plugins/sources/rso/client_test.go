package rso

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestListURL(t *testing.T) {
	got := listURL("https://komunikaty.tvp.pl", "malopolskie")
	want := "https://komunikaty.tvp.pl/komunikatyxml/malopolskie/wszystkie/0?_format=xml"
	if got != want {
		t.Errorf("listURL = %q, want %q", got, want)
	}
}

func TestFetchRequestShape(t *testing.T) {
	var gotPath, gotQuery, gotUA, gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery, gotUA, gotAccept = r.URL.Path, r.URL.RawQuery, r.Header.Get("User-Agent"), r.Header.Get("Accept")
		fmt.Fprint(w, `<newses><pagination_info totalItems="0" itemsPerPage="20"></pagination_info></newses>`)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, time.Second)
	if _, err := c.Fetch(context.Background(), "slaskie"); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if gotPath != "/komunikatyxml/slaskie/wszystkie/0" || gotQuery != "_format=xml" {
		t.Errorf("request = %s?%s, want /komunikatyxml/slaskie/wszystkie/0?_format=xml", gotPath, gotQuery)
	}
	if gotUA != userAgent || gotAccept != "application/xml" {
		t.Errorf("headers = UA %q Accept %q", gotUA, gotAccept)
	}
}

func TestFetchFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, time.Second)
	if _, err := c.Fetch(context.Background(), "malopolskie"); err == nil {
		t.Fatal("HTTP 500 must be a failure")
	}
}

func TestFetchOversizedRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, strings.Repeat("a", maxResponseBytes+1))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, time.Second)
	if _, err := c.Fetch(context.Background(), "malopolskie"); err == nil {
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
	if _, err := c.Fetch(ctx, "malopolskie"); err == nil {
		t.Fatal("cancelled context must fail the request")
	}
}
