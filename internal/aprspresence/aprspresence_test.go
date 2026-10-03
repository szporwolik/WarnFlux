package aprspresence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/aprs"
)

// fakeSend records every announcement.
type fakeSend struct {
	texts []string
}

func (f *fakeSend) send(_ context.Context, text string) error {
	f.texts = append(f.texts, text)
	return nil
}

func doc(call string, mobile bool) aprs.StationDocument {
	d := aprs.StationDocument{Callsign: call}
	if mobile {
		d.SpeedKMH = 40
	}
	return d
}

func docAt(call string, mobile bool, lat, lon float64) aprs.StationDocument {
	d := doc(call, mobile)
	d.Position = &aprs.PositionWire{Latitude: lat, Longitude: lon}
	return d
}

func newWatcher(docs func() []aprs.StationDocument, f *fakeSend) *Watcher {
	return New(Options{
		Channel:  1,
		Send:     f.send,
		Stations: docs,
		Lat:      50.0, Lon: 20.0, RadiusKM: 32,
		Interval: time.Minute,
		Debounce: 10 * time.Minute,
	})
}

// TestBaselineSilentThenTransitions pins the lifecycle: the first scan
// is silent, entries and exits announce, and the opposite event always
// passes the debounce.
func TestBaselineSilentThenTransitions(t *testing.T) {
	f := &fakeSend{}
	state := []aprs.StationDocument{docAt("SP9AAA-9", true, 50.1, 20.1)}
	w := newWatcher(func() []aprs.StationDocument { return state }, f)

	now := time.Now()
	w.scan(context.Background(), now) // baseline
	if len(f.texts) != 0 {
		t.Fatalf("baseline announced: %v", f.texts)
	}

	state = append(state, docAt("SP9BBB-9", true, 50.2, 20.2))
	w.scan(context.Background(), now.Add(31*time.Second))
	if len(f.texts) != 1 || f.texts[0] != "WarnFlux APRS SP9BBB-9 in range" {
		t.Fatalf("entry announcements = %v", f.texts)
	}

	// Same state again: no repeat.
	w.scan(context.Background(), now.Add(62*time.Second))
	if len(f.texts) != 1 {
		t.Fatalf("repeated scan announced: %v", f.texts)
	}

	state = state[:1]
	w.scan(context.Background(), now.Add(93*time.Second))
	if len(f.texts) != 2 || f.texts[1] != "WarnFlux APRS SP9BBB-9 out of range" {
		t.Fatalf("exit announcements = %v", f.texts)
	}

	// Re-entry inside the debounce window: the opposite event always
	// announces.
	state = append(state, docAt("SP9BBB-9", true, 50.2, 20.2))
	w.scan(context.Background(), now.Add(124*time.Second))
	if len(f.texts) != 3 || f.texts[2] != "WarnFlux APRS SP9BBB-9 in range" {
		t.Fatalf("re-entry announcements = %v", f.texts)
	}
}

// TestEntryCarriesComment: a station entering with an APRS comment
// announces it; whitespace collapses and the line stays within the
// mesh text limit.
func TestEntryCarriesComment(t *testing.T) {
	f := &fakeSend{}
	state := []aprs.StationDocument{docAt("SP9AAA-9", true, 50.1, 20.1)}
	w := newWatcher(func() []aprs.StationDocument { return state }, f)
	now := time.Now()
	w.scan(context.Background(), now) // baseline

	withComment := docAt("SP9CCC-9", true, 50.2, 20.2)
	withComment.Comment = "mobile  unit\t patrolling\n the area"
	state = append(state, withComment)
	w.scan(context.Background(), now.Add(31*time.Second))
	if len(f.texts) != 1 || f.texts[0] != "WarnFlux APRS SP9CCC-9 in range: mobile unit patrolling the area" {
		t.Fatalf("entry with comment = %v", f.texts)
	}

	// A very long comment is cut bluntly at the mesh limit, marked with
	// an ellipsis; the callsign and the event stay intact.
	longComment := docAt("SP9DDD-9", true, 50.2, 20.25)
	longComment.Comment = strings.Repeat("x", 300)
	state = append(state, longComment)
	w.scan(context.Background(), now.Add(62*time.Second))
	if len(f.texts) != 2 {
		t.Fatalf("long comment announcements = %v", f.texts)
	}
	got := []rune(f.texts[1])
	if len(got) > announcementMaxRunes {
		t.Fatalf("long comment line = %d runes, over the mesh limit", len(got))
	}
	if !strings.HasPrefix(f.texts[1], "WarnFlux APRS SP9DDD-9 in range: ") || !strings.HasSuffix(f.texts[1], "…") {
		t.Fatalf("long comment line = %q, want the prefix intact and the ellipsis marker", f.texts[1])
	}
}

// TestStaticAndInfrastructureIgnored: fixed stations, digipeaters and
// repeaters never announce.
func TestStaticAndInfrastructureIgnored(t *testing.T) {
	f := &fakeSend{}
	state := []aprs.StationDocument{
		docAt("SP9FIX-1", false, 50.1, 20.1), // static operator
		{SymbolTable: "/", Symbol: "#", Position: &aprs.PositionWire{Latitude: 50.1, Longitude: 20.1}}, // digi
		{SymbolTable: "/", Symbol: "r", Position: &aprs.PositionWire{Latitude: 50.1, Longitude: 20.1}}, // repeater
	}
	w := newWatcher(func() []aprs.StationDocument { return state }, f)
	now := time.Now()
	w.scan(context.Background(), now)
	w.scan(context.Background(), now.Add(31*time.Second))
	if len(f.texts) != 0 {
		t.Fatalf("static/infrastructure announced: %v", f.texts)
	}
}

// TestOutOfRangeIgnored: a mobile outside the operational ring never
// announces (the APRS feed is area-filtered, this is the second layer).
func TestOutOfRangeIgnored(t *testing.T) {
	f := &fakeSend{}
	state := []aprs.StationDocument{docAt("SP9FAR-9", true, 51.0, 21.0)}
	w := newWatcher(func() []aprs.StationDocument { return state }, f)
	now := time.Now()
	w.scan(context.Background(), now)
	state = append(state, docAt("SP9FAR2-9", true, 51.5, 21.5))
	w.scan(context.Background(), now.Add(31*time.Second))
	if len(f.texts) != 0 {
		t.Fatalf("out-of-range announced: %v", f.texts)
	}
}

// TestSameEventDebounced: alternating events always announce (a real
// pass-through reports both), while consecutive identical events within
// the debounce window stay silent.
func TestSameEventDebounced(t *testing.T) {
	f := &fakeSend{}
	state := []aprs.StationDocument{docAt("SP9FLAP-9", true, 50.1, 20.1)}
	w := newWatcher(func() []aprs.StationDocument { return state }, f)
	now := time.Now()
	w.scan(context.Background(), now) // baseline: in range, silent

	state = []aprs.StationDocument{}
	w.scan(context.Background(), now.Add(31*time.Second)) // out
	state = []aprs.StationDocument{docAt("SP9FLAP-9", true, 50.1, 20.1)}
	w.scan(context.Background(), now.Add(62*time.Second)) // in
	state = []aprs.StationDocument{}
	w.scan(context.Background(), now.Add(93*time.Second)) // out
	state = []aprs.StationDocument{docAt("SP9FLAP-9", true, 50.1, 20.1)}
	w.scan(context.Background(), now.Add(124*time.Second)) // in
	got := strings.Join(f.texts, ";")
	for _, want := range []string{
		"SP9FLAP-9 out of range",
		"SP9FLAP-9 in range",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("announcements = %v, missing %q", f.texts, want)
		}
	}
	if len(f.texts) != 4 {
		t.Fatalf("alternating events = %v, want all four announced", f.texts)
	}

	// Consecutive identical events stay silent: the station leaves
	// again (announced) and a further scan while it is still gone adds
	// nothing.
	state = []aprs.StationDocument{}
	w.scan(context.Background(), now.Add(155*time.Second)) // out (announced)
	before := len(f.texts)
	w.scan(context.Background(), now.Add(186*time.Second)) // still out — silent
	if len(f.texts) != before {
		t.Fatalf("consecutive identical event announced: %v", f.texts[before:])
	}
}

// TestDisabledWatcher: without a channel the watcher never announces.
func TestDisabledWatcher(t *testing.T) {
	f := &fakeSend{}
	w := New(Options{Channel: 0, Send: f.send, Stations: func() []aprs.StationDocument {
		return []aprs.StationDocument{docAt("SP9X-9", true, 50.1, 20.1)}
	}})
	if w.Enabled() {
		t.Fatal("watcher without a channel must be disabled")
	}
	w.scan(context.Background(), time.Now())
	if len(f.texts) != 0 {
		t.Fatalf("disabled watcher announced: %v", f.texts)
	}
}
