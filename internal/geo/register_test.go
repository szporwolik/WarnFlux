package geo

import (
	"strings"
	"testing"
)

// TestRegister covers the config-driven extension of the bundled table:
// valid additions, normalization, lookup/display integration and every
// validation failure. The registered units are package-global; they use
// a dedicated "test-" slug/code space that no other test touches.
func TestRegister(t *testing.T) {
	if err := Register(nil); err != nil {
		t.Fatalf("Register(nil): %v", err)
	}

	// Valid addition: parent before child, mixed-case input normalized.
	if err := Register([]Area{
		{Code: "9999901", Type: "powiat", Slug: " TEST-Powiat ", Name: " Test Powiat ", Parents: []string{" MALOPOLSKIE "}},
		{Code: "9999902", Type: "gmina", Slug: "Test-Gmina", Name: "Test Gmina", Parents: []string{"TEST-POWIAT"}},
	}); err != nil {
		t.Fatalf("valid Register: %v", err)
	}

	a, ok := Lookup("test-gmina")
	if !ok || a.Code != "9999902" || a.Name != "Test Gmina" {
		t.Fatalf("Lookup(test-gmina) = %+v, %v; want code 9999902", a, ok)
	}
	if !Intersects(a, Area{Slug: "test-powiat"}) {
		t.Fatalf("Intersects(test-gmina, test-powiat) = false; want true")
	}
	// The transitive chain resolves through the declared parent.
	if !Intersects(a, Area{Slug: "malopolskie"}) {
		t.Fatalf("Intersects(test-gmina, malopolskie) = false; want true")
	}
	if got := Display("gmina:test-gmina"); got != "Test Gmina" {
		t.Fatalf("Display(gmina:test-gmina) = %q; want \"Test Gmina\"", got)
	}

	bad := []struct {
		name string
		area Area
		want string
	}{
		{"bad type", Area{Code: "9999903", Type: "osiedle", Slug: "test-a", Name: "X"}, "must be wojewodztwo, powiat, gmina or miasto"},
		{"bad slug", Area{Code: "9999903", Type: "gmina", Slug: "Test A!", Name: "X"}, "must match"},
		{"bad code", Area{Code: "abc", Type: "gmina", Slug: "test-b", Name: "X"}, "must be a 1-7 digit TERYT code"},
		{"empty name", Area{Code: "9999903", Type: "gmina", Slug: "test-c", Name: "  "}, "name must not be empty"},
		{"duplicate slug", Area{Code: "9999903", Type: "gmina", Slug: "test-powiat", Name: "X"}, "already exists"},
		{"duplicate code", Area{Code: "9999901", Type: "gmina", Slug: "test-d", Name: "X"}, "already exists"},
		{"unknown parent", Area{Code: "9999904", Type: "gmina", Slug: "test-orphan", Name: "X", Parents: []string{"no-such-slug"}}, "parent slug"},
	}
	for _, tc := range bad {
		err := Register([]Area{tc.area})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: Register = %v; want error containing %q", tc.name, err, tc.want)
		}
	}
}
