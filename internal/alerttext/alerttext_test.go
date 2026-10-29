package alerttext

import (
	"strings"
	"testing"
)

// TestBodyPrefersTheDescriptionWhenTheHeadlineRepeatsTheEvent pins the case
// production hit: GDDKiA and RSO publish the same string as event and headline,
// so a radio alert read "SEV: Utrudnienia na A4" and never said what happened.
func TestBodyPrefersTheDescriptionWhenTheHeadlineRepeatsTheEvent(t *testing.T) {
	const event = "Utrudnienia na A4"
	const desc = "Jerzmanowice ul. Jurajska, pow. krakowski wypadek autobusu MPK i samochodu osobowego, DK 94 zablokowana (314km)"

	if got := Body(event, event, desc); got == "" || got == event {
		t.Fatalf("body = %q, want the description", got)
	}
	if got := Body(event, "  utrudnienia   NA a4 ", desc); got == event {
		t.Fatalf("body = %q, want the description when only the case differs", got)
	}

	// A headline that adds information wins.
	if got := Body("Wiatr", "Porywy do 120 km/h", desc); got != "Porywy do 120 km/h" {
		t.Fatalf("body = %q, want the headline", got)
	}

	// Nothing informative anywhere.
	if got := Body(event, event, "   "); got != "" {
		t.Fatalf("body = %q, want empty", got)
	}
}

func TestSummaryKeepsAbbreviationsAndDropsTheNoteBlock(t *testing.T) {
	// Polish abbreviations carry periods; cutting at the first one shortened
	// the production message to "Jerzmanowice ul".
	got := Summary("Jerzmanowice ul. Jurajska, pow. krakowski wypadek, DK 94 zablokowana (314km)")
	if !strings.Contains(got, "DK 94 zablokowana") {
		t.Fatalf("summary lost the incident: %q", got)
	}

	// The note block after a blank line is boilerplate, not the incident.
	got = Summary("Prognozuje się gęste mgły, widzialność poniżej 200 m.\n\nIMGW probability: 70%.")
	if strings.Contains(got, "IMGW probability") {
		t.Fatalf("summary kept the provider note: %q", got)
	}

	// A wall of text without punctuation is cut at a word boundary.
	long := strings.Repeat("aaaa bbbb cccc dddd eeee ffff gggg hhhh iiii jjjj kkkk llll mmmm nnnn oooo pppp qqqq rrrr ssss tttt ", 4)
	got = Summary(long)
	if len(got) > summaryCap || len(got) == 0 {
		t.Fatalf("long summary = %d bytes", len(got))
	}
	if strings.HasSuffix(got, " ") {
		t.Fatalf("summary kept a trailing space: %q", got)
	}
}
