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

// TestSetOnChangeHook pins the mask-change notification contract: the
// hook fires with (old, new) after every effective Set and never on a
// no-op, and clearing the hook silences it.
func TestSetOnChangeHook(t *testing.T) {
	old := Mask()
	t.Cleanup(func() {
		SetOnChange(nil)
		Set(old)
	})

	var got [][2]uint32
	SetOnChange(func(o, n uint32) { got = append(got, [2]uint32{o, n}) })

	Set(old) // no-op: no notification
	next := old &^ uint32(CatActive)
	Set(next)
	Set(old)
	if len(got) != 2 {
		t.Fatalf("hook fired %d times, want 2", len(got))
	}
	if got[0][0] != old || got[0][1] != next {
		t.Errorf("first hook = %#x → %#x, want %#x → %#x", got[0][0], got[0][1], old, next)
	}
	if got[1][0] != next || got[1][1] != old {
		t.Errorf("second hook = %#x → %#x, want %#x → %#x", got[1][0], got[1][1], next, old)
	}

	SetOnChange(nil)
	Set(next)
	Set(old)
	if len(got) != 2 {
		t.Errorf("hook fired after clearing, want no more notifications")
	}
}

func TestParseEnabledKeysRoundTrip(t *testing.T) {
	names := []string{"events", "active", "aprs_bulletins", "meshtastic_messages"}
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
