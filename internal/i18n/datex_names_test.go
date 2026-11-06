package i18n

import "testing"

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
