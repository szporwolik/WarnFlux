package web

import (
	"testing"
)

func TestEmcomSlugify(t *testing.T) {
	cases := map[string]string{
		"SP9MOA EMCOM":        "sp9moa-emcom",
		"  Małopolska   NET ": "malopolska-net",
		"Łączność Kryzysowa":  "lacznosc-kryzysowa",
		"ŚLĄSK 2.0":           "slask-2-0",
		"----":                "",
		"":                    "",
	}
	for in, want := range cases {
		if got := emcomSlugify(in); got != want {
			t.Errorf("emcomSlugify(%q) = %q, want %q", in, got, want)
		}
	}
	// Over-length names fold to the 63-char slug cap.
	long := emcomSlugify("abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz")
	if len(long) > 63 {
		t.Errorf("slug too long: %d", len(long))
	}
}

func TestEmcomLevels(t *testing.T) {
	if _, ok := emcomLevelAt(2); !ok {
		t.Fatal("level 2 missing")
	}
	if _, ok := emcomLevelAt(4); ok {
		t.Fatal("level 4 must not exist")
	}
	if emcomLevelClass(0) != "l0" || emcomLevelClass(1) != "l1" ||
		emcomLevelClass(2) != "l2" || emcomLevelClass(3) != "l3" {
		t.Error("level classes wrong")
	}
	if emcomLevelClass(9) != "l0" {
		t.Error("unknown levels must fall back to l0")
	}
	// Anything above monitoring dispatches as severe; monitoring is the
	// only calm level.
	for _, l := range emcomLevels {
		if l.Level == 0 && l.Severity == "severe" {
			t.Error("level 0 must not be severe")
		}
		if l.Level > 0 && l.Severity != "severe" {
			t.Errorf("level %d = %s, want severe", l.Level, l.Severity)
		}
	}
}
