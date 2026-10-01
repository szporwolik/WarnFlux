package mqttpolicy

import (
	"reflect"
	"testing"
)

func TestDefaultMaskAllowsEverything(t *testing.T) {
	for _, c := range List() {
		if !Allowed(c) {
			t.Errorf("category %s blocked by default", Key(c))
		}
	}
	if Mask() != uint32(CatAll) {
		t.Errorf("default mask = %#x, want all bits", Mask())
	}
}

func TestSetAndAllowed(t *testing.T) {
	Set(uint32(CatEvents) | uint32(CatStatus))
	t.Cleanup(func() { Set(uint32(CatAll)) })

	if !Allowed(CatEvents) || !Allowed(CatStatus) {
		t.Error("enabled categories must be allowed")
	}
	for _, c := range List() {
		if c != CatEvents && c != CatStatus && Allowed(c) {
			t.Errorf("disabled category %s still allowed", Key(c))
		}
	}
	if Allowed(0) {
		t.Error("the zero category must never be allowed")
	}
}

func TestParseEnabledKeysRoundTrip(t *testing.T) {
	names := []string{"events", "active", "aprs_bulletins", "meshcore_messages"}
	mask := Parse(names)
	if got := EnabledKeys(mask); !reflect.DeepEqual(got, names) {
		t.Errorf("round trip = %v, want %v", got, names)
	}
	// Unknown keys are ignored, never fail.
	if Parse([]string{"bogus", "status"}) != uint32(CatStatus) {
		t.Error("Parse must ignore unknown keys")
	}
}

func TestKeyCoversEveryCategory(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range List() {
		k := Key(c)
		if k == "" {
			t.Fatalf("category %#x has no config key", uint32(c))
		}
		if seen[k] {
			t.Fatalf("duplicate key %q", k)
		}
		seen[k] = true
	}
}
