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
	ID   int64
	Code string
	Name string
	Lat  float64
	Lon  float64
}

// aqIndexDoc is the response of /v1/rest/aqindex/getIndex/{stationId}:
// the official air-quality index for one station, overall and per
// pollutant.
type aqIndexDoc struct {
	Index aqIndex `json:"AqIndex"`
}

// aqIndex is one station's official air-quality index.
type aqIndex struct {
	StationID      int64  `json:"Identyfikator stacji pomiarowej"`
	CalculatedAt   string `json:"Data wykonania obliczeń indeksu"`
	IndexLevelID   *int   `json:"Wartość indeksu"`
	IndexLevelName string `json:"Nazwa kategorii indeksu"`
	SO2LevelID     *int   `json:"Wartość indeksu dla wskaźnika SO2"`
	SO2LevelName   string `json:"Nazwa kategorii indeksu dla wskażnika SO2"`
	NO2LevelID     *int   `json:"Wartość indeksu dla wskaźnika NO2"`
	NO2LevelName   string `json:"Nazwa kategorii indeksu dla wskaźnika NO2"`
	PM10LevelID    *int   `json:"Wartość indeksu dla wskaźnika PM10"`
	PM10LevelName  string `json:"Nazwa kategorii indeksu dla wskaźnika PM10"`
	PM25LevelID    *int   `json:"Wartość indeksu dla wskaźnika PM2.5"`
	PM25LevelName  string `json:"Nazwa kategorii indeksu dla wskaźnika PM2.5"`
	O3LevelID      *int   `json:"Wartość indeksu dla wskaźnika O3"`
	O3LevelName    string `json:"Nazwa kategorii indeksu dla wskaźnika O3"`
}

// aqPayload is the canonical information document published on the
// air_quality topics and parsed by the web map layer.
type aqPayload struct {
	SchemaVersion  int           `json:"schema_version"`
	StationCode    string        `json:"station_code"`
	StationName    string        `json:"station_name"`
	Latitude       float64       `json:"latitude"`
	Longitude      float64       `json:"longitude"`
	IndexLevelID   *int          `json:"index_level_id,omitempty"`
	IndexLevelName string        `json:"index_level_name"`
	GeneratedAt    string        `json:"generated_at"`
	Pollutants     []aqPollutant `json:"pollutants,omitempty"`
}

// aqPollutant is one pollutant's index level within a station snapshot.
type aqPollutant struct {
	Code      string `json:"code"`
	LevelID   *int   `json:"level_id,omitempty"`
	LevelName string `json:"level_name"`
}

// parsedExceedance is one record after the raw fields were interpreted.
type parsedExceedance struct {
	rec         exceedance
	effectiveAt time.Time
	pollutant   string
}
