package giosaq

import "time"

// levelsDoc is the GIOŚ PJP API response for
// /v1/rest/levels/getInformationAboutExceeding: a paginated archive of
// exceedance records, newest first. The plugin reads only the first page
// and applies its own recency filter.
type levelsDoc struct {
	TotalPages int          `json:"totalPages"`
	Records    []exceedance `json:"Przekroczenia"`
}

// exceedance is one official air-quality exceedance record. The provider
// uses Polish field names; the struct keeps them verbatim so the wire
// format stays obvious.
type exceedance struct {
	NormType    string  `json:"Typ normy"`
	Zone        string  `json:"Strefa"`
	Station     string  `json:"Stanowisko pomiarowe"`
	DateTime    string  `json:"Data i godzina"`
	Duration    string  `json:"Czas trwania"`
	Residents   int     `json:"Liczba mieszkańców"`
	MaxValue    float64 `json:"Wartość maksymalnego stężenia (µg/m3)"`
	InfoLink    string  `json:"Odnośnik do strony internetowej z informacjami"`
	Causes      string  `json:"Przyczyny przekroczenia"`
	Forecast    string  `json:"Prognoza zmian stężeń"`
	RiskGroups  string  `json:"Informacje w sprawie grup ludności objętych ryzykiem"`
	Precautions string  `json:"Zalecane środki ostrożności"`
}

// stationsDoc is the response of /v1/rest/station/findAll (one page; the
// client requests size=500 which covers the whole country today).
type stationsDoc struct {
	Stations []station `json:"Lista stacji pomiarowych"`
}

// station is one measurement station with its coordinates.
type station struct {
	ID          int64  `json:"Identyfikator stacji"`
	Code        string `json:"Kod stacji"`
	Name        string `json:"Nazwa stacji"`
	Lat         string `json:"WGS84 φ N"`
	Lon         string `json:"WGS84 λ E"`
	Gmina       string `json:"Gmina"`
	Powiat      string `json:"Powiat"`
	Voivodeship string `json:"Województwo"`
}

// stationRef is the geocoding-relevant part of a station.
type stationRef struct {
	Name string
	Lat  float64
	Lon  float64
}

// parsedExceedance is one record after the raw fields were interpreted.
type parsedExceedance struct {
	rec         exceedance
	effectiveAt time.Time
	pollutant   string
}
