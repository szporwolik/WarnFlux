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
	if !strings.Contains(s, "roads: 79") {
		t.Errorf("plain body missing the road line: %s", s)
	}
	if !strings.Contains(s, ">79<") {
		t.Errorf("HTML summary missing the road chip: %s", s)
	}
	// The roads-only event must not produce a generic areas line in the
	// plain summary (the road line covers it).
	if strings.Contains(s, "areas:") {
		t.Errorf("plain summary leaked a generic areas line: %s", s)
	}
}

// TestHazardInstructionSection pins the always-present instruction
// section: a hazard with its own instruction shows it verbatim, one
// without falls back to the default safety instruction — in both the
// plain and the HTML part.
func TestHazardInstructionSection(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	newReq := func() action.ActionRequest {
		return action.ActionRequest{
			Event: dispatch.Event{
				Kind: dispatch.EventHazardTransition,
				Hazard: &dispatch.HazardTransition{
					Type: dispatch.TransitionNew,
					Hazard: dispatch.Hazard{
						Source:   "rso",
						EventKey: "rso:x",
						Event:    "Ćwiczenie",
						Severity: "minor",
						Headline: "Test łączności",
					},
				},
			},
			App: action.AppInfo{Header1: "SOSDEV"},
		}
	}

	// The hazard's own instruction wins.
	req := newReq()
	req.Event.Hazard.Hazard.Instruction = "Ignore messages."
	msg := string(buildMessage(context.Background(), Config{From: "a@b.c", To: []string{"c@d.e"}}, req, now))
	if !strings.Contains(msg, "instruction: Ignore messages.") {
		t.Errorf("own instruction missing:\n%s", msg)
	}
	if strings.Contains(msg, defaultInstruction) {
		t.Errorf("default instruction leaked into an own-instruction mail:\n%s", msg)
	}

	// No instruction: the default section is still present.
	msg = string(buildMessage(context.Background(), Config{From: "a@b.c", To: []string{"c@d.e"}}, newReq(), now))
	if !strings.Contains(msg, "instruction: "+defaultInstruction) {
		t.Errorf("default instruction missing:\n%s", msg)
	}
}
