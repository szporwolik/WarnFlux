package gddkia

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fixtureXML mirrors the real feed shape with entries inside and outside
// the test area.
const fixtureXML = `<?xml version="1.0" encoding="UTF-8"?>
<utrudnienia gen="2026-09-24T22:28:01+0200">
<utr><typ>U</typ><nr_drogi>94g</nr_drogi><woj>malopolskie</woj><km>13.200</km><dl>1.5</dl>
<geo_lat>49.9850</geo_lat><geo_long>20.1895</geo_long><nazwa_odcinka>Wieliczka - Bochnia</nazwa_odcinka>
<data_powstania>2026-09-12T00:00:00+0200</data_powstania><data_likwidacji>2026-10-10T23:59:00+0200</data_likwidacji>
<objazd>Prace remontowe na jezdni.</objazd><objazd_mapy><mapa/></objazd_mapy><rodzaj><poz>U33</poz></rodzaj>
<skutki/><ogr_nosnosc/><ogr_nacisk/><ogr_skrajnia_pozioma/><ogr_skrajnia_pionowa/><ogr_szerokosc/>
<ogr_predkosc>80</ogr_predkosc><ruch_wahadlowy>false</ruch_wahadlowy><sygnalizacja_swietlna>false</sygnalizacja_swietlna>
<awaria_mostu>false</awaria_mostu><ruch_2_kierunkowy>true</ruch_2_kierunkowy><droga_zamknieta>false</droga_zamknieta>
<czasy_oczekiwania><czas_oczekiwania><kierunek>O</kierunek><czas_od>00:00</czas_od><czas_do>00:00</czas_do>
<pn_pt>00:00</pn_pt><so>00:00</so><ni>00:00</ni></czas_oczekiwania></czasy_oczekiwania></utr>
<utr><typ>U</typ><nr_drogi>A4</nr_drogi><woj>malopolskie</woj><km>400.872</km><dl>2.0</dl>
<geo_lat>50.0853</geo_lat><geo_long>19.8008</geo_long><nazwa_odcinka>w. Balice I - w. Targowisko</nazwa_odcinka>
<data_powstania>2026-09-20T00:00:00+0200</data_powstania><data_likwidacji>2026-09-25T23:59:00+0200</data_likwidacji>
<objazd>Mechaniczne i ręczne koszenie traw.</objazd><objazd_mapy><mapa/></objazd_mapy><rodzaj><poz>U01</poz></rodzaj>
<skutki/><ogr_nosnosc/><ogr_nacisk/><ogr_skrajnia_pozioma/><ogr_skrajnia_pionowa/><ogr_szerokosc/>
<ogr_predkosc/><ruch_wahadlowy>true</ruch_wahadlowy><sygnalizacja_swietlna>false</sygnalizacja_swietlna>
<awaria_mostu>false</awaria_mostu><ruch_2_kierunkowy>true</ruch_2_kierunkowy><droga_zamknieta>false</droga_zamknieta>
<czasy_oczekiwania/></utr>
<utr><typ>U</typ><nr_drogi>A1</nr_drogi><woj>pomorskie</woj><km>13.370</km><dl>7.23</dl>
<geo_lat>54.134239</geo_lat><geo_long>18.673217</geo_long><nazwa_odcinka>PPO Rusocin - w. Swarożyn</nazwa_odcinka>
<data_powstania>2026-09-12T00:00:00+0200</data_powstania><data_likwidacji>2026-10-10T23:59:00+0200</data_likwidacji>
<objazd>Prace remontowe na jezdni.</objazd><objazd_mapy><mapa/></objazd_mapy><rodzaj><poz>U33</poz></rodzaj>
<skutki/><ogr_nosnosc/><ogr_nacisk/><ogr_skrajnia_pozioma/><ogr_skrajnia_pionowa/><ogr_szerokosc/>
<ogr_predkosc/><ruch_wahadlowy>false</ruch_wahadlowy><sygnalizacja_swietlna>false</sygnalizacja_swietlna>
<awaria_mostu>false</awaria_mostu><ruch_2_kierunkowy>true</ruch_2_kierunkowy><droga_zamknieta>false</droga_zamknieta>
<czasy_oczekiwania/></utr>
<utr><typ>U</typ><nr_drogi>75</nr_drogi><woj>malopolskie</woj><km>17.195</km><dl>0.4</dl>
<geo_lat>49.9504</geo_lat><geo_long>20.5906</geo_long><nazwa_odcinka>Okocim - Wytrzyszczka</nazwa_odcinka>
<data_powstania>2026-09-21T00:00:00+0200</data_powstania><data_likwidacji>2026-09-30T23:59:00+0200</data_likwidacji>
<objazd>Koszenie traw.</objazd><objazd_mapy><mapa/></objazd_mapy><rodzaj><poz>U01</poz></rodzaj>
<skutki/><ogr_nosnosc/><ogr_nacisk/><ogr_skrajnia_pozioma/><ogr_skrajnia_pionowa/><ogr_szerokosc/>
<ogr_predkosc/><ruch_wahadlowy>false</ruch_wahadlowy><sygnalizacja_swietlna>false</sygnalizacja_swietlna>
<awaria_mostu>false</awaria_mostu><ruch_2_kierunkowy>false</ruch_2_kierunkowy><droga_zamknieta>true</droga_zamknieta>
<czasy_oczekiwania/></utr>
</utrudnienia>`

func testServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "utrdane.xml") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(fixtureXML))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestClientFetchParses(t *testing.T) {
	srv := testServer(t)
	c := NewClient(srv.URL+"/dane/zima_html/utrdane.xml", 5*time.Second)
	doc, err := c.Fetch(t.Context())
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(doc.Utr) != 4 {
		t.Fatalf("entries = %d, want 4", len(doc.Utr))
	}
	if doc.Gen == "" {
		t.Error("generation timestamp empty")
	}
}

func TestNormalizeRoadEntry(t *testing.T) {
	// The first fixture entry: works, no closure.
	ev, err := normalize(fixtureEntry(t, 0), "https://example/utrdane.xml")
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if ev.Source != "gddkia" || ev.Category != "road" {
		t.Errorf("source/category = %q/%q", ev.Source, ev.Category)
	}
	if ev.Severity != "minor" || ev.Event != "Road works" {
		t.Errorf("severity/event = %q/%q, want minor/Road works", ev.Severity, ev.Event)
	}
	if ev.Headline != "94g km 13.200 — Wieliczka - Bochnia" {
		t.Errorf("headline = %q", ev.Headline)
	}
	if ev.Latitude == nil || *ev.Latitude != 49.9850 || ev.Longitude == nil || *ev.Longitude != 20.1895 {
		t.Errorf("position = (%v, %v)", ev.Latitude, ev.Longitude)
	}
	if len(ev.Areas) != 1 || ev.Areas[0] != "droga:94g" {
		t.Errorf("areas = %v", ev.Areas)
	}
	if ev.ExpiresAt == nil || ev.ExpiresAt.Year() != 2026 {
		t.Errorf("expires = %v", ev.ExpiresAt)
	}
	if !strings.Contains(ev.Description, "Speed limit: 80 km/h") {
		t.Errorf("description missing speed limit: %q", ev.Description)
	}

	// Identity is stable across normalizations.
	ev2, err := normalize(fixtureEntry(t, 0), "https://example/utrdane.xml")
	if err != nil {
		t.Fatalf("normalize again: %v", err)
	}
	if ev.Key() != ev2.Key() {
		t.Errorf("identity unstable: %q vs %q", ev.Key(), ev2.Key())
	}

	// Closure entry maps to severe/Road closure.
	closed, err := normalize(fixtureEntry(t, 3), "https://example/utrdane.xml")
	if err != nil {
		t.Fatalf("normalize closure: %v", err)
	}
	if closed.Severity != "severe" || closed.Event != "Road closure" {
		t.Errorf("closure severity/event = %q/%q, want severe/Road closure", closed.Severity, closed.Event)
	}

	// Alternating traffic maps to moderate.
	alt, err := normalize(fixtureEntry(t, 1), "https://example/utrdane.xml")
	if err != nil {
		t.Fatalf("normalize alternating: %v", err)
	}
	if alt.Severity != "moderate" || alt.Event != "Alternating traffic" {
		t.Errorf("alternating severity/event = %q/%q, want moderate/Alternating traffic", alt.Severity, alt.Event)
	}
}

// fixtureEntry parses the i-th fixture entry through the client so tests
// exercise the exact feed shape.
func fixtureEntry(t *testing.T, i int) utr {
	t.Helper()
	srv := testServer(t)
	c := NewClient(srv.URL+"/dane/zima_html/utrdane.xml", 5*time.Second)
	f, err := c.Fetch(t.Context())
	if err != nil {
		t.Fatalf("fetch fixture: %v", err)
	}
	return f.Utr[i]
}
