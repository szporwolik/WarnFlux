package sqlite

import (
	"context"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestEventPathRoundTrip pins the stretch geometry: a road event that
// carries more than one point survives ingest, active listing and a
// plain read, so the map can draw the segment instead of a single pin.
func TestEventPathRoundTrip(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()

	ev := activeEv("stretch-1")
	ev.Path = []core.GeoPoint{
		{Lat: 50.123456, Lon: 19.654321},
		{Lat: 50.223344, Lon: 19.778899},
		{Lat: 50.334455, Lon: 19.889900},
	}
	ingestOne(t, s, ev)

	list, err := s.ListActiveEvents(ctx, "", 10)
	if err != nil {
		t.Fatalf("ListActiveEvents: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("active events = %d, want 1", len(list))
	}
	got := list[0].Path
	if len(got) != len(ev.Path) {
		t.Fatalf("path points = %d, want %d (%v)", len(got), len(ev.Path), got)
	}
	for i, want := range ev.Path {
		if got[i] != want {
			t.Errorf("path[%d] = %+v, want %+v", i, got[i], want)
		}
	}

	// Geometry is not part of the content fingerprint, so a provider that
	// adds or shifts the stretch on an otherwise unchanged record must still
	// update it — silently, without a journal change (no re-notification).
	stored := activeEv("stretch-1")
	stored.Path = []core.GeoPoint{{Lat: 50.5, Lon: 19.5}, {Lat: 50.6, Lon: 19.6}}
	outcome, change := ingestOne(t, s, stored)
	if outcome != storage.OutcomeDuplicate || change != nil {
		t.Fatalf("repeat ingest = %v/%v, want duplicate without change", outcome, change)
	}
	refreshed, err := s.ListActiveEvents(ctx, "", 10)
	if err != nil {
		t.Fatalf("ListActiveEvents: %v", err)
	}
	for _, e := range refreshed {
		if e.SourceID != "stretch-1" {
			continue
		}
		if len(e.Path) != 2 || e.Path[0].Lat != 50.5 || e.Path[1].Lon != 19.6 {
			t.Errorf("silently refreshed path = %+v, want the new stretch", e.Path)
		}
	}

	// An empty incoming path never wipes the stored geometry.
	blank := activeEv("stretch-1")
	blank.Path = nil
	ingestOne(t, s, blank)
	kept, err := s.ListActiveEvents(ctx, "", 10)
	if err != nil {
		t.Fatalf("ListActiveEvents: %v", err)
	}
	for _, e := range kept {
		if e.SourceID == "stretch-1" && len(e.Path) != 2 {
			t.Errorf("blank ingest wiped the path: %+v", e.Path)
		}
	}

	// Point-only events keep an empty path (no regression for the pin map).
	plain := activeEv("point-1")
	plain.Path = nil
	ingestOne(t, s, plain)
	all, err := s.ListActiveEvents(ctx, "", 10)
	if err != nil {
		t.Fatalf("ListActiveEvents: %v", err)
	}
	for _, e := range all {
		if e.SourceID == "point-1" && len(e.Path) != 0 {
			t.Errorf("point event path = %v, want empty", e.Path)
		}
	}
}
