package gios

// paResponse is the API envelope:
//
//	{"dane":{"strona":[...],"liczbaRekordow":65},"wynik":{...}}
type paResponse struct {
	Dane paDane `json:"dane"`
}

type paDane struct {
	Strona         []awariaRekord `json:"strona"`
	LiczbaRekordow int            `json:"liczbaRekordow"`
}

// awariaRekord is one register entry. There are NO coordinates in the
// provider data — only the administrative address — so the plugin
// geocodes miejscowosc to a locality centroid through the GUGiK ULDK
// service.
type awariaRekord struct {
	Data                string `json:"data"` // YYYY-MM-DD
	Wojewodztwo         string `json:"wojewodztwo"`
	Powiat              string `json:"powiat"`
	Gmina               string `json:"gmina"`
	KodPocztowy         string `json:"kodPocztowy"`
	Miejscowosc         string `json:"miejscowosc"`
	MiejsceZdarzenia    string `json:"miejsceZdarzenia"`
	RodzajTransportu    string `json:"rodzajTransportu"`
	KlasyfikacjaZakladu string `json:"klasyfikacjaZakladu"`
	RodzajZdarzenia     string `json:"rodzajZdarzenia"`
	ZrodloZdarzenia     string `json:"zrodloZdarzenia"`
	OpisZdarzenia       string `json:"opisZdarzenia"`
}

// wojewodztwa is the closed provider vocabulary (the OpenAPI
// WojewodztwoEnum); config-provided values and reverse-geocoded names are
// validated against it so a typo cannot silently empty the feed.
var wojewodztwa = map[string]bool{
	"dolnośląskie":        true,
	"kujawsko-pomorskie":  true,
	"lubelskie":           true,
	"lubuskie":            true,
	"łódzkie":             true,
	"małopolskie":         true,
	"mazowieckie":         true,
	"opolskie":            true,
	"podkarpackie":        true,
	"podlaskie":           true,
	"pomorskie":           true,
	"śląskie":             true,
	"świętokrzyskie":      true,
	"warmińsko-mazurskie": true,
	"wielkopolskie":       true,
	"zachodniopomorskie":  true,
}
