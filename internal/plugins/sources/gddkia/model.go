package gddkia

import (
	"encoding/xml"
)

// utr is one road-difficulty entry ("utrudnienie") from the GDDKiA
// winter/current national-roads feed.
//
// Field semantics (source: archiwum.gddkia.gov.pl/dane/zima_html/utrdane.xml):
//   - typ: U (utrudnienie) / I (informacja) / W (wypadek) — U dominates.
//   - nr_drogi: road number as the GDDKiA uses it, e.g. "A4", "75", "94g".
//   - km / dl: kilometre marker and length of the affected stretch.
//   - geo_lat / geo_long: the event's geographic position.
//   - data_powstania / data_likwidacji: RFC3339 creation / liquidation.
//   - objazd: free-text description of the obstruction and diversions.
type utr struct {
	Typ            string `xml:"typ"`
	NrDrogi        string `xml:"nr_drogi"`
	Woj            string `xml:"woj"`
	KM             string `xml:"km"`
	DL             string `xml:"dl"`
	GeoLat         string `xml:"geo_lat"`
	GeoLong        string `xml:"geo_long"`
	NazwaOdcinka   string `xml:"nazwa_odcinka"`
	DataPowstania  string `xml:"data_powstania"`
	DataLikwidacji string `xml:"data_likwidacji"`
	Objazd         string `xml:"objazd"`
	Rodzaj         rodzaj `xml:"rodzaj"`
	OgrPredkosc    string `xml:"ogr_predkosc"`
	RuchWahadlowy  string `xml:"ruch_wahadlowy"`
	Ruch2Kier      string `xml:"ruch_2_kierunkowy"`
	DrogaZamknieta string `xml:"droga_zamknieta"`
}

// rodzaj carries the provider's difficulty-code block (poz, e.g. "U33").
type rodzaj struct {
	Poz string `xml:"poz"`
}

// feed is the root XML document of the difficulties feed.
type feed struct {
	XMLName xml.Name `xml:"utrudnienia"`
	Gen     string   `xml:"gen,attr"`
	Utr     []utr    `xml:"utr"`
}
