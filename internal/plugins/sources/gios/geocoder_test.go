package gios

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// geocoderServer serves ULDK-style GetAddress answers from a fixed table.
func geocoderServer(t *testing.T, towns map[string][2]float64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("request") != "GetAddress" {
			http.Error(w, "unknown request", http.StatusBadRequest)
			return
		}
		loc := r.URL.Query().Get("location")
		coords, ok := towns[strings.ToLower(loc)]
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"type":"city","found objects":0,"results":null}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"city","results":{"1":{"x":"` +
			formatFloat(coords[1]) + `","y":"` + formatFloat(coords[0]) + `"}}}`))
	}))
}

func formatFloat(v float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.6f", v), "0"), ".")
}

func TestGeocodeAndCache(t *testing.T) {
	towns := map[string][2]float64{
		"oświęcim": {50.0358, 19.2100},
	}
	srv := geocoderServer(t, towns)
	defer srv.Close()

	g := NewGeocoder(srv.URL, "http://127.0.0.1:1/reverse", 0)
	got, err := g.Geocode(context.Background(), "OŚWIĘCIM")
	if err != nil {
		t.Fatalf("Geocode: %v", err)
	}
	if got.Lat < 50.03 || got.Lat > 50.04 || got.Lon < 19.20 || got.Lon > 19.22 {
		t.Errorf("geocoded point = %+v", got)
	}

	// Cache hit: the server is closed and the second lookup still works.
	srv.Close()
	again, err := g.Geocode(context.Background(), "oświęcim")
	if err != nil {
		t.Fatalf("cached Geocode: %v", err)
	}
	if again != got {
		t.Errorf("cache mismatch: %+v vs %+v", again, got)
	}
}

func TestGeocodeNotFound(t *testing.T) {
	srv := geocoderServer(t, map[string][2]float64{})
	defer srv.Close()

	g := NewGeocoder(srv.URL, "http://127.0.0.1:1/reverse", 0)
	if _, err := g.Geocode(context.Background(), "Nicosia"); err == nil {
		t.Fatal("unknown locality accepted")
	}
	if _, err := g.Geocode(context.Background(), "   "); err == nil {
		t.Fatal("empty locality accepted")
	}
}

func TestReverseNormalization(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("format") != "jsonv2" {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"address":{"state":"województwo małopolskie","county":"powiat wielicki"}}`))
	}))
	defer srv.Close()

	g := NewGeocoder("http://127.0.0.1:1/uug", srv.URL, 0)
	scope, err := g.Reverse(context.Background(), 50.0212, 20.2075)
	if err != nil {
		t.Fatalf("Reverse: %v", err)
	}
	if scope.Wojewodztwo != "małopolskie" || scope.Powiat != "wielicki" {
		t.Errorf("scope = %+v", scope)
	}
}

func TestReverseUnknownState(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"address":{"state":"Middle-earth"}}`))
	}))
	defer srv.Close()

	g := NewGeocoder("http://127.0.0.1:1/uug", srv.URL, 0)
	if _, err := g.Reverse(context.Background(), 0, 0); err == nil {
		t.Fatal("unknown state accepted as a voivodeship")
	}
}
