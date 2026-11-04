package alerttext

import "testing"

// TestCreditOnlyForBorrowedData pins the short-channel attribution: data we
// republish from a provider carry its short name, our own observations carry
// nothing.
func TestCreditOnlyForBorrowedData(t *testing.T) {
	for _, source := range []string{"gddkia-datex", "GDDKiA-DATEX ", "gddkia"} {
		if got := Credit(source); got != "GDDKiA" {
			t.Errorf("Credit(%q) = %q, want GDDKiA", source, got)
		}
	}
	for _, source := range []string{"", "imgw-meteo", "compose", "emcom"} {
		if got := Credit(source); got != "" {
			t.Errorf("Credit(%q) = %q, want empty", source, got)
		}
	}
}

// TestIDSuffix pins the radio suffix: message ID first, credit after, and no
// dangling separator when only one of them applies.
func TestIDSuffix(t *testing.T) {
	for _, tc := range []struct {
		id, source, want string
	}{
		{"WF-1104114004", "gddkia-datex", "ID:WF-1104114004 GDDKiA"},
		{"WF-1104114004", "imgw-meteo", "ID:WF-1104114004"},
		{"", "gddkia-datex", "GDDKiA"},
		{"", "imgw-meteo", ""},
		{"  ", "  ", ""},
	} {
		if got := IDSuffix(tc.id, tc.source); got != tc.want {
			t.Errorf("IDSuffix(%q, %q) = %q, want %q", tc.id, tc.source, got, tc.want)
		}
	}
}
