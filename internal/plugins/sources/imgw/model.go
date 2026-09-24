// Package imgw implements the first real hazard source: IMGW-PIB public
// warnings (meteorological warningsmeteo and hydrological warningshydro).
//
// Data source: Instytut Meteorologii i Gospodarki Wodnej – Państwowy
// Instytut Badawczy (IMGW-PIB), https://danepubliczne.imgw.pl/apiinfo.
// See README.md for the required attribution wording.
package imgw

// meteoWarning is one item of the warningsmeteo feed. IMGW currently
// returns numeric-like values (stopien, prawdopodobienstwo) as STRINGS;
// they are parsed explicitly, never assumed to be JSON numbers.
type meteoWarning struct {
	ID                 string   `json:"id"`
	NazwaZdarzenia     string   `json:"nazwa_zdarzenia"`
	Stopien            string   `json:"stopien"`
	Prawdopodobienstwo string   `json:"prawdopodobienstwo"`
	ObowiazujeDo       string   `json:"obowiazuje_do"`
	ObowiazujeOd       string   `json:"obowiazuje_od"`
	Opublikowano       string   `json:"opublikowano"`
	Tresc              string   `json:"tresc"`
	Komentarz          string   `json:"komentarz"`
	Biuro              string   `json:"biuro"`
	Teryt              []string `json:"teryt"`
}

// hydroWarning is one item of the warningshydro feed. Note the provider
// field names differ from warningsmeteo (stopień vs stopien).
type hydroWarning struct {
	Opublikowano       string      `json:"opublikowano"`
	Stopien            string      `json:"stopień"`
	DataOd             string      `json:"data_od"`
	DataDo             string      `json:"data_do"`
	Prawdopodobienstwo string      `json:"prawdopodobienstwo"`
	Numer              string      `json:"numer"`
	Biuro              string      `json:"biuro"`
	Zdarzenie          string      `json:"zdarzenie"`
	Przebieg           string      `json:"przebieg"`
	Komentarz          string      `json:"komentarz"`
	Obszary            []hydroArea `json:"obszary"`
}

// hydroArea is one affected region of a hydrological warning.
type hydroArea struct {
	Wojewodztwo string   `json:"wojewodztwo"`
	Opis        string   `json:"opis"`
	KodZlewni   []string `json:"kod_zlewni"`
}

// providerStatus is the documented empty-warning payload shape used by
// both endpoints. Status is a pointer so a MISSING status field is
// distinguishable from an explicit false.
type providerStatus struct {
	Status  *bool  `json:"status"`
	Message string `json:"message"`
}

// providerMessage is the meteo endpoint's HTTP-200 no-warnings shape:
// a plain object with a Polish message, no status field.
type providerMessage struct {
	Message string `json:"message"`
}
