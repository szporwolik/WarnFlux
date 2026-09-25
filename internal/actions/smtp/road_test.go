package smtp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

func TestRoadNumbers(t *testing.T) {
	got := roadNumbers([]string{"droga:79", "gmina:x", "droga:a4", "droga:79", "droga:"})
	want := []string{"79", "A4"}
	if len(got) != len(want) {
		t.Fatalf("roadNumbers = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("roadNumbers = %v, want %v", got, want)
		}
	}
	if rest := nonRoadAreas([]string{"droga:79", "gmina:x"}); len(rest) != 1 || rest[0] != "gmina:x" {
		t.Errorf("nonRoadAreas = %v", rest)
	}
}

func TestGddkiaMessageShowsRoad(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	req := action.ActionRequest{
		Event: dispatch.Event{
			Kind: dispatch.EventHazardTransition,
			Hazard: &dispatch.HazardTransition{
				Type: dispatch.TransitionNew,
				Hazard: dispatch.Hazard{
					Source:   "gddkia",
					EventKey: "gddkia:x",
					Event:    "Road works",
					Severity: "minor",
					Headline: "79 km 369.200 — Krzeszowice",
					Areas:    []string{"droga:79"},
				},
			},
		},
		App: action.AppInfo{Header1: "SOSDEV"},
	}
	msg := buildMessage(context.Background(), Config{From: "a@b.c", To: []string{"c@d.e"}}, req, now)
	s := string(msg)
	if !strings.Contains(s, "Road: 79") {
		t.Errorf("plain body missing the road line: %s", s)
	}
	if !strings.Contains(s, ">79<") {
		t.Errorf("HTML summary missing the road chip: %s", s)
	}
	// The roads-only event must not produce a generic Areas line in the
	// plain summary (the Road line covers it; the technical table keeps
	// the raw tokens for diagnostics).
	if strings.Contains(s, "Areas:") {
		t.Errorf("plain summary leaked a generic areas line: %s", s)
	}
}
