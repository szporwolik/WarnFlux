package gddkiadatex

import (
	"encoding/xml"
	"fmt"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
)

// messageContainer is the DATEX II envelope every KPD response carries: one
// SituationPublication with the complete set of current obstructions.
type messageContainer struct {
	XMLName xml.Name `xml:"messageContainer"`
	Payload struct {
		Type       string      `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`
		Situations []situation `xml:"situation"`
	} `xml:"payload"`
}

type situation struct {
	ID      string   `xml:"id,attr"`
	Records []record `xml:"situationRecord"`
}

// record is one obstruction. Only the fields the station uses are decoded; the
// response is ~2 MB of them and everything else is ignored on purpose.
type record struct {
	ID   string `xml:"id,attr"`
	Type string `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`

	CreationTime string `xml:"situationRecordCreationTime"`
	VersionTime  string `xml:"situationRecordVersionTime"`
	Severity     string `xml:"severity"`
	// Probability is the provider's certainty ("certain", "probable", …).
	Probability string `xml:"probabilityOfOccurrence"`
	// The subtype of the record type: roadMaintenanceType on maintenance works,
	// accidentType on accidents, obstructionType on obstructions.
	MaintenanceType string `xml:"roadMaintenanceType"`
	AccidentType    string `xml:"accidentType"`
	ObstructionType string `xml:"obstructionType"`

	Validity struct {
		Status string `xml:"validityStatus"`
		Spec   struct {
			Start string `xml:"overallStartTime"`
			End   string `xml:"overallEndTime"`
		} `xml:"validityTimeSpecification"`
	} `xml:"validity"`

	Comments []struct {
		Text struct {
			Values []localised `xml:"values>value"`
		} `xml:"comment"`
		Type string `xml:"commentType"`
	} `xml:"generalPublicComment"`

	Location struct {
		Type  string `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`
		Point struct {
			Coordinates struct {
				Lat float64 `xml:"latitude"`
				Lon float64 `xml:"longitude"`
			} `xml:"pointCoordinates"`
		} `xml:"pointByCoordinates"`
		// OpenLR localises the stretch: the first and last reference point
		// bound the segment, which is what the map can draw as a line.
		OpenLR struct {
			First struct {
				Coordinates struct {
					Lat float64 `xml:"latitude"`
					Lon float64 `xml:"longitude"`
				} `xml:"openlrCoordinates"`
			} `xml:"openlrLocationReferencePoint"`
			Last struct {
				Coordinates struct {
					Lat float64 `xml:"latitude"`
					Lon float64 `xml:"longitude"`
				} `xml:"openlrCoordinates"`
			} `xml:"openlrLastLocationReferencePoint"`
		} `xml:"openlrPointLocationReference"`
		AlongLinear struct {
			Linear struct {
				RoadNumber string `xml:"roadNumber"`
				RoadName   struct {
					Values []localised `xml:"values>value"`
				} `xml:"roadName"`
				// StartPoint carries the kilometre on the reference model
				// ("Pikietaż"): the number operators recognise from the road.
				StartPoint struct {
					Description struct {
						Values []localised `xml:"values>value"`
					} `xml:"referentDescription"`
				} `xml:"startPointOfLinearElement"`
			} `xml:"linearElement"`
			Distance struct {
				Along float64 `xml:"distanceAlong"`
			} `xml:"distanceAlongLinearElement"`
		} `xml:"pointAlongLinearElement"`
	} `xml:"locationReference"`
}

// localised is a <value lang="PL">…</value> pair.
type localised struct {
	Lang  string `xml:"lang,attr"`
	Value string `xml:",chardata"`
}

// text returns the Polish value when present, else the first non-empty one.
func (r record) text() string {
	for _, c := range r.Comments {
		var fallback string
		for _, v := range c.Text.Values {
			s := strings.TrimSpace(v.Value)
			if s == "" {
				continue
			}
			if strings.EqualFold(v.Lang, "pl") {
				return s
			}
			if fallback == "" {
				fallback = s
			}
		}
		if fallback != "" {
			return fallback
		}
	}
	return ""
}

// segment returns the stretch the record describes, when the provider
// localised it with OpenLR. Two points at most: OpenLR gives the first and last
// reference point, and the map draws the straight line between them — enough to
// see WHICH stretch is affected, without inventing intermediate geometry.
func (r record) segment() []core.GeoPoint {
	first := r.Location.OpenLR.First.Coordinates
	last := r.Location.OpenLR.Last.Coordinates
	if !validCoord(first.Lat, first.Lon) || !validCoord(last.Lat, last.Lon) {
		return nil
	}
	if first.Lat == last.Lat && first.Lon == last.Lon {
		return nil
	}
	return []core.GeoPoint{{Lat: first.Lat, Lon: first.Lon}, {Lat: last.Lat, Lon: last.Lon}}
}

// validCoord reports whether a coordinate pair is inside the valid ranges.
func validCoord(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && !(lat == 0 && lon == 0)
}

// road returns the road designation the record sits on ("9", "S19", "A4").
func (r record) road() string {
	if n := strings.TrimSpace(r.Location.AlongLinear.Linear.RoadNumber); n != "" {
		return n
	}
	for _, v := range r.Location.AlongLinear.Linear.RoadName.Values {
		if s := strings.TrimSpace(v.Value); s != "" {
			return s
		}
	}
	return ""
}

// kilometre returns the reference kilometre ("425.798") the record sits at, as
// the provider writes it (kilometre.metres).
func (r record) kilometre() string {
	for _, v := range r.Location.AlongLinear.Linear.StartPoint.Description.Values {
		if s := strings.TrimSpace(v.Value); s != "" {
			return s
		}
	}
	return ""
}

// subtype names the record's specific kind ("maintenanceWork", "accident", …).
func (r record) subtype() string {
	for _, v := range []string{r.MaintenanceType, r.AccidentType, r.ObstructionType} {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

// coordinates returns the record's position. Records without a point (linear
// references) cannot be placed on the map and are reported as such.
func (r record) coordinates() (lat, lon float64, ok bool) {
	c := r.Location.Point.Coordinates
	if c.Lat == 0 && c.Lon == 0 {
		return 0, 0, false
	}
	if c.Lat < -90 || c.Lat > 90 || c.Lon < -180 || c.Lon > 180 {
		return 0, 0, false
	}
	return c.Lat, c.Lon, true
}

// soapEnvelope is the transport wrapper the service answers with. The
// publication sits inside Body (or a Fault, when the call is rejected).
type soapEnvelope struct {
	XMLName xml.Name `xml:"Envelope"`
	Body    struct {
		Container *messageContainer `xml:"messageContainer"`
		Fault     *struct {
			Code   string `xml:"faultcode"`
			String string `xml:"faultstring"`
		} `xml:"Fault"`
	} `xml:"Body"`
}

// parseResponse decodes the SOAP body into the publication. It accepts both the
// wrapped answer and a bare messageContainer, so a proxy that unwraps the
// envelope keeps working.
func parseResponse(body []byte) (*messageContainer, error) {
	var env soapEnvelope
	if err := xml.Unmarshal(body, &env); err == nil {
		if env.Body.Fault != nil {
			return nil, fmt.Errorf("kpd: SOAP fault: %s", env.Body.Fault.String)
		}
		if env.Body.Container != nil {
			return checkPublication(env.Body.Container)
		}
	}
	var doc messageContainer
	if err := xml.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("kpd: parse DATEX response: %w", err)
	}
	return checkPublication(&doc)
}

// checkPublication rejects a payload that is not a situation publication: an
// empty one is legitimate (nothing on the roads), a different one is a change of
// shape and must never be read as "no obstructions".
func checkPublication(doc *messageContainer) (*messageContainer, error) {
	if len(doc.Payload.Situations) == 0 && !strings.Contains(doc.Payload.Type, "SituationPublication") {
		return nil, fmt.Errorf("kpd: response carries no SituationPublication (payload type %q)", doc.Payload.Type)
	}
	return doc, nil
}

// datexTimeLayout is the timestamp form KPD uses: local Polish time, no zone.
const datexTimeLayout = "2006-01-02T15:04:05"

// datexZone is the zone KPD writes its timestamps in. Poland runs a single
// zone, and the offset is left out of the payload, so the answer must come from
// the provider's convention — never from the station's own time.Local, which a
// container may well keep on UTC. Without a zoneinfo database the fallback is
// plain CET: an hour off in summer beats a shifted alert window nobody notices.
var datexZone = func() *time.Location {
	if loc, err := time.LoadLocation("Europe/Warsaw"); err == nil {
		return loc
	}
	return time.FixedZone("CET", 60*60)
}()

// parseTime reads a DATEX timestamp as Polish local time (the service writes
// Polish local time without an offset); an explicit offset is respected.
func parseTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	if t, err := time.ParseInLocation(datexTimeLayout, s, datexZone); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}
	return time.Time{}, false
}
