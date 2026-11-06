package i18n

import (
	"strings"
	"testing"
)

// TestEventNameTranslatesDatexKinds pins the language rule for the road data:
// the event carries the DATEX KIND, so a Polish station names it in Polish and
// an English reader gets English — never an English label in Polish text.
func TestEventNameTranslatesDatexKinds(t *testing.T) {
	for _, tc := range []struct{ code, pl, en string }{
		{"VehicleObstruction", "Przeszkoda — pojazd", "Vehicle obstruction"},
		{"MaintenanceWorks", "Roboty drogowe", "Road works"},
		{"Accident", "Wypadek", "Accident"},
		{"DisabledVehicle", "Unieruchomiony pojazd", "Disabled vehicle"},
		{"WrongWayDriving", "Jazda pod prąd", "Wrong-way driving"},
		{"Queue", "Korek", "Queue"},
	} {
		if got := EventName("pl", tc.code); got != tc.pl {
			t.Errorf("EventName(pl, %q) = %q, want %q", tc.code, got, tc.pl)
		}
		if got := EventName("en", tc.code); got != tc.en {
			t.Errorf("EventName(en, %q) = %q, want %q", tc.code, got, tc.en)
		}
	}
	// An unknown kind still reads as words, never as a code.
	if got := EventName("pl", "Winter driving management"); got != "Winter driving management" {
		t.Errorf("unknown kind mangled: %q", got)
	}
}

// TestEventNameTranslatesLegacyRoadLabels covers the rows stored before the
// event carried the DATEX kind: the label was English text inside the event
// field, and the archive and the detail popup render the type from it. An old
// entry must read in the page language exactly like a fresh one.
func TestEventNameTranslatesLegacyRoadLabels(t *testing.T) {
	for _, label := range []string{
		"Road works", "Obstruction", "Accident", "Infrastructure damage",
		"Vehicle obstruction", "Disabled vehicle", "Lane management",
		"Abnormal traffic", "Traffic", "Weather conditions", "Road conditions",
		"Environment", "Signal fault", "Public event", "Diversion",
		"Speed management", "Wrong-way driving", "Vehicle fire", "Queue",
	} {
		pl := EventName("pl", label)
		if pl == label {
			t.Errorf("legacy label %q is not translated to Polish", label)
		}
		if pl == "" || strings.HasPrefix(pl, "event.") {
			t.Errorf("legacy label %q rendered as %q", label, pl)
		}
		if en := EventName("en", label); en != label {
			t.Errorf("legacy label %q reads %q in English", label, en)
		}
	}
}
