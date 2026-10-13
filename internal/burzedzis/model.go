package burzedzis

import (
	"encoding/xml"
	"fmt"
	"strings"
	"time"
)

// Locality is one point the station follows: the name the operator typed
// (it ends up in every alert) plus the provider's coordinates for it.
type Locality struct {
	Name string
	Lat  float64
	Lon  float64
}

// Phenomenon is one warning type of the provider: the XML elements that
// carry its degree and its UTC window, the canonical English event type
// WarnFlux stamps on it (shared with the broker and webhook consumers) and
// the Polish name used in the headline.
type Phenomenon struct {
	// Key is the stable short identity, used in event ids and configuration.
	Key string
	// Title is the canonical English event type ("Thunderstorm warning").
	Title string
	// Label is the Polish name of the phenomenon ("Burza").
	Label string
	// Wind marks the wind family (funnel, storm, wind): the warnings the
	// station never routes below severe, whatever degree the provider
	// publishes.
	Wind bool
	// level/from/to are the provider's element names.
	level, from, to string
}

// Phenomena lists every warning type the provider publishes, most severe
// first (the order the operator sees them in).
var Phenomena = []Phenomenon{
	{Key: "tornado", Title: "Tornado warning", Label: "Trąba powietrzna", Wind: true, level: "traba", from: "traba_od_dnia", to: "traba_do_dnia"},
	{Key: "storm", Title: "Thunderstorm warning", Label: "Burza", Wind: true, level: "burza", from: "burza_od_dnia", to: "burza_do_dnia"},
	{Key: "wind", Title: "Strong wind warning", Label: "Silny wiatr", Wind: true, level: "wiatr", from: "wiatr_od_dnia", to: "wiatr_do_dnia"},
	{Key: "rain", Title: "Heavy rain warning", Label: "Intensywne opady", level: "opad", from: "opad_od_dnia", to: "opad_do_dnia"},
	{Key: "heat", Title: "Heat warning", Label: "Upał", level: "upal", from: "upal_od_dnia", to: "upal_do_dnia"},
	{Key: "frost", Title: "Frost warning", Label: "Mróz", level: "mroz", from: "mroz_od_dnia", to: "mroz_do_dnia"},
}

// Warning is one warning in force for a point, already decoded: the degree
// (1..3) and its UTC window.
type Warning struct {
	Phenomenon Phenomenon
	// Degree is the provider's warning degree (1, 2 or 3).
	Degree int
	// From / To bound the warning in UTC.
	From time.Time
	To   time.Time
	// Point is the locality the warning was reported for.
	Point Locality
}

// Warnings is the complete snapshot of one point: a phenomenon with a
// degree of 0 is simply not in force. Rejected names the phenomena that
// carried a degree but no readable window — the provider's own data defect,
// reported instead of guessed at.
type Warnings struct {
	Point    Locality
	InForce  []Warning
	Rejected []string
}

// Storm is one lightning search result: how many discharges were recorded
// in the rolling window, how far the nearest one was and where.
type Storm struct {
	Locality   string
	Lat        float64
	Lon        float64
	RadiusKM   int
	Count      int
	DistanceKM float64
	Direction  string
	WindowMins int
}

// Detected reports whether the search found any discharge at all.
func (s Storm) Detected() bool { return s.Count > 0 }

// Window returns the observation window as a duration (0 when the provider
// did not report one).
func (s Storm) Window() time.Duration {
	if s.WindowMins <= 0 {
		return 0
	}
	return time.Duration(s.WindowMins) * time.Minute
}

// DirectionName renders the provider's compass code in Polish, the language
// of the alert bodies ("NW" becomes "północny zachód").
func DirectionName(code string) string {
	switch strings.ToUpper(strings.TrimSpace(code)) {
	case "N":
		return "północ"
	case "NE":
		return "północny wschód"
	case "E":
		return "wschód"
	case "SE":
		return "południowy wschód"
	case "S":
		return "południe"
	case "SW":
		return "południowy zachód"
	case "W":
		return "zachód"
	case "NW":
		return "północny zachód"
	default:
		return strings.TrimSpace(code)
	}
}

// providerTimeLayout is the provider's UTC timestamp
// ("2026-10-13 13:38:00").
const providerTimeLayout = "2006-01-02 15:04:05"

// ParseUTC parses one provider timestamp. The documented zone is UTC, which
// the live feed confirms (a warning published at 15:38 Warsaw time arrives
// as 13:38).
func ParseUTC(s string) (time.Time, error) {
	t, err := time.ParseInLocation(providerTimeLayout, strings.TrimSpace(s), time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("malformed provider timestamp %q", s)
	}
	return t, nil
}

// SOAP request bodies. encoding/xml renders each element name from its tag,
// so every operation is its own type; the namespace attribute is declared
// explicitly because the provider always replies with one.

type cityRequest struct {
	XMLName xml.Name `xml:"miejscowosc"`
	NS      string   `xml:"xmlns,attr"`
	Name    string   `xml:"nazwa"`
	Key     string   `xml:"klucz,omitempty"`
}

type warningsRequest struct {
	XMLName xml.Name `xml:"ostrzezenia_pogodowe"`
	NS      string   `xml:"xmlns,attr"`
	Y       float64  `xml:"y"`
	X       float64  `xml:"x"`
	Key     string   `xml:"klucz,omitempty"`
}

type stormRequest struct {
	XMLName xml.Name `xml:"szukaj_burzy"`
	NS      string   `xml:"xmlns,attr"`
	Y       float64  `xml:"y"`
	X       float64  `xml:"x"`
	Radius  int      `xml:"promien,omitempty"`
	Key     string   `xml:"klucz,omitempty"`
}

// SOAP answers. The provider prefixes every element (SOAP-ENV:, ns1:);
// encoding/xml matches on local names, so the tags stay unprefixed.

type cityResponse struct {
	Body struct {
		City struct {
			Return *cityPoint `xml:"return"`
		} `xml:"miejscowoscResponse"`
	} `xml:"Body"`
}

// cityPoint is one directory answer. The coordinates are pointers so a
// missing element is distinguishable from a legitimate 0.
type cityPoint struct {
	CoordY *float64 `xml:"y"`
	CoordX *float64 `xml:"x"`
}

type warningsResponse struct {
	Body struct {
		Return *warningLevels `xml:"ostrzezenia_pogodoweResponse>return"`
	} `xml:"Body"`
}

// warningLevels mirrors the provider's warning envelope: one degree and one
// window per phenomenon, all of them always present.
type warningLevels struct {
	Frost      *int    `xml:"mroz"`
	FrostFrom  *string `xml:"mroz_od_dnia"`
	FrostTo    *string `xml:"mroz_do_dnia"`
	Heat       *int    `xml:"upal"`
	HeatFrom   *string `xml:"upal_od_dnia"`
	HeatTo     *string `xml:"upal_do_dnia"`
	Wind       *int    `xml:"wiatr"`
	WindFrom   *string `xml:"wiatr_od_dnia"`
	WindTo     *string `xml:"wiatr_do_dnia"`
	Rain       *int    `xml:"opad"`
	RainFrom   *string `xml:"opad_od_dnia"`
	RainTo     *string `xml:"opad_do_dnia"`
	Storm      *int    `xml:"burza"`
	StormFrom  *string `xml:"burza_od_dnia"`
	StormTo    *string `xml:"burza_do_dnia"`
	Funnel     *int    `xml:"traba"`
	FunnelFrom *string `xml:"traba_od_dnia"`
	FunnelTo   *string `xml:"traba_do_dnia"`
}

// phenomenon returns the raw degree and window of one phenomenon, and refuses
// an envelope that does not carry them. The provider always ships all six
// phenomena (a degree of 0 means "not in force"), so a missing element proves
// the document is NOT the operation's payload — a maintenance page parses as
// an empty envelope, and reading that as "nothing is in force" would reconcile
// every live warning away.
func (l *warningLevels) phenomenon(key string) (degree int, from, to string, err error) {
	var d *int
	var f, t *string
	switch key {
	case "tornado":
		d, f, t = l.Funnel, l.FunnelFrom, l.FunnelTo
	case "storm":
		d, f, t = l.Storm, l.StormFrom, l.StormTo
	case "wind":
		d, f, t = l.Wind, l.WindFrom, l.WindTo
	case "rain":
		d, f, t = l.Rain, l.RainFrom, l.RainTo
	case "heat":
		d, f, t = l.Heat, l.HeatFrom, l.HeatTo
	case "frost":
		d, f, t = l.Frost, l.FrostFrom, l.FrostTo
	default:
		return 0, "", "", fmt.Errorf("unknown phenomenon %q", key)
	}
	if d == nil || f == nil || t == nil {
		return 0, "", "", fmt.Errorf("the %s elements are missing from the warning envelope", key)
	}
	return *d, *f, *t, nil
}

// snapshot converts the raw envelope into the warnings in force for one
// point. A phenomenon that carries a degree but no readable window is
// rejected by name instead of being guessed at: an alert with invented
// validity would be worse than no alert.
func (l *warningLevels) snapshot(at Locality) (Warnings, error) {
	out := Warnings{Point: at}
	for _, ph := range Phenomena {
		degree, from, to, err := l.phenomenon(ph.Key)
		if err != nil {
			return Warnings{}, fmt.Errorf("incomplete warning answer: %w", err)
		}
		if degree <= 0 {
			continue
		}
		start, err := ParseUTC(from)
		if err != nil {
			out.Rejected = append(out.Rejected, ph.Key+" (from "+strings.TrimSpace(from)+")")
			continue
		}
		end, err := ParseUTC(to)
		if err != nil {
			out.Rejected = append(out.Rejected, ph.Key+" (to "+strings.TrimSpace(to)+")")
			continue
		}
		if !end.After(start) {
			out.Rejected = append(out.Rejected, ph.Key+" (window ends before it starts)")
			continue
		}
		out.InForce = append(out.InForce, Warning{Phenomenon: ph, Degree: degree, From: start, To: end, Point: at})
	}
	return out, nil
}

type stormResponse struct {
	Body struct {
		Return *stormAnswer `xml:"szukaj_burzyResponse>return"`
	} `xml:"Body"`
}

// stormAnswer is one lightning search result. The numbers are pointers: the
// provider always sends them, so a missing one means the document is not this
// operation's payload.
type stormAnswer struct {
	Count     *int     `xml:"liczba"`
	Distance  *float64 `xml:"odleglosc"`
	Direction string   `xml:"kierunek"`
	Window    *int     `xml:"okres"`
}
