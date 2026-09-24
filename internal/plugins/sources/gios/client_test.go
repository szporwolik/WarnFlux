package gios

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// apiServer serves a fake GIOŚ PA API from a paged record set.
func apiServer(t *testing.T, pages [][]awariaRekord) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/powazne-awarie" {
			http.NotFound(w, r)
			return
		}
		page, err := strconv.Atoi(r.URL.Query().Get("numerStrony"))
		if err != nil {
			t.Errorf("numerStrony: %v", err)
			page = 0
		}
		if page >= len(pages) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(paResponse{Dane: paDane{Strona: []awariaRekord{}}})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(paResponse{Dane: paDane{
			Strona:         pages[page],
			LiczbaRekordow: len(pages[page]),
		}})
	}))
}

func sampleRecords() []awariaRekord {
	return []awariaRekord{
		{
			Data:                "2026-06-21",
			Wojewodztwo:         "małopolskie",
			Powiat:              "oświęcimski",
			Gmina:               "Oświęcim",
			KodPocztowy:         "32-600",
			Miejscowosc:         "Oświęcim",
			MiejsceZdarzenia:    "ZAKŁAD",
			KlasyfikacjaZakladu: "ZDR",
			RodzajZdarzenia:     "pożar",
			ZrodloZdarzenia:     "proces przemysłowy",
			OpisZdarzenia:       "Pożar na terenie zakładu chemicznego.",
		},
		{
			Data:             "2026-01-07",
			Wojewodztwo:      "małopolskie",
			Powiat:           "krakowski",
			Gmina:            "Skawina",
			Miejscowosc:      "Skawina",
			MiejsceZdarzenia: "ZAKŁAD",
			RodzajZdarzenia:  "emisja (wyciek)",
			ZrodloZdarzenia:  "proces przemysłowy",
			OpisZdarzenia:    "Wyciek substancji niebezpiecznej.",
		},
	}
}

func TestClientFetchAwariePaging(t *testing.T) {
	// Page 0 must be full (50 records) or the client stops early; the
	// second page carries the distinguishing record.
	full := make([]awariaRekord, pageSize)
	for i := range full {
		full[i] = awariaRekord{
			Data:            "2026-05-01",
			Wojewodztwo:     "małopolskie",
			Powiat:          "wielicki",
			Gmina:           "Niepołomice",
			Miejscowosc:     "Niepołomice",
			RodzajZdarzenia: "wybuch",
			OpisZdarzenia:   string(rune('a' + i%26)),
		}
	}
	pages := [][]awariaRekord{
		full,
		{{
			Data:            "2026-06-21",
			Wojewodztwo:     "małopolskie",
			Powiat:          "oświęcimski",
			Gmina:           "Oświęcim",
			Miejscowosc:     "Oświęcim",
			RodzajZdarzenia: "pożar",
		}},
	}
	srv := apiServer(t, pages)
	defer srv.Close()

	c := NewClient(srv.URL, 0)
	recs, err := c.FetchAwarie(context.Background(), 2026, "małopolskie", "")
	if err != nil {
		t.Fatalf("FetchAwarie: %v", err)
	}
	if len(recs) != pageSize+1 {
		t.Fatalf("got %d records, want %d", len(recs), pageSize+1)
	}
	if recs[pageSize].Miejscowosc != "Oświęcim" {
		t.Errorf("last record = %+v", recs[pageSize])
	}
}

func TestClientFetchAwarieEmpty(t *testing.T) {
	srv := apiServer(t, [][]awariaRekord{{}})
	defer srv.Close()

	c := NewClient(srv.URL, 0)
	recs, err := c.FetchAwarie(context.Background(), 2026, "małopolskie", "wielicki")
	if err != nil {
		t.Fatalf("FetchAwarie: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("got %d records, want 0", len(recs))
	}
}

func TestClientFetchAwarieStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 0)
	if _, err := c.FetchAwarie(context.Background(), 2026, "", ""); err == nil {
		t.Fatal("non-200 status accepted")
	}
}
