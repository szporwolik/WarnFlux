package web_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/storage/sqlite"
)

func archiveEvent(id, headline string) core.HazardEvent {
	return core.HazardEvent{
		Source:   "archive-test",
		SourceID: id,
		Event:    "Test event",
		Severity: "moderate",
		Status:   core.StatusActive,
		Headline: headline,
		Areas:    []string{"gmina:test"},
	}
}

// TestArchiveFragment pins the public archive: newest-first ordering,
// active/ended badges, the 180-day window and the total count line.
func TestArchiveFragment(t *testing.T) {
	clock := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	now := clock
	store, _, err := sqlite.Open(":memory:", sqlite.WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	// One event beyond the 180-day window, two inside it (storm older,
	// flood newest).
	now = clock.AddDate(0, 0, -200)
	if _, _, err := store.Ingest(context.Background(), archiveEvent("old", "Ancient snow"), "fp-old"); err != nil {
		t.Fatal(err)
	}
	now = clock.Add(-2 * time.Hour)
	if _, _, err := store.Ingest(context.Background(), archiveEvent("storm", "Storm warning"), "fp-storm"); err != nil {
		t.Fatal(err)
	}
	now = clock
	if _, _, err := store.Ingest(context.Background(), archiveEvent("flood", "Flood alert"), "fp-flood"); err != nil {
		t.Fatal(err)
	}

	env := newTestEnvWithStore(t, store)
	_, html := env.get("/archive")

	for _, want := range []string{
		"Archive", "1–2 of 2", "Storm warning", "Flood alert",
		`class="badge ok">active`, "Source:", "last seen:",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("archive fragment missing %q:\n%s", want, html)
		}
	}
	if strings.Contains(html, "Ancient snow") {
		t.Errorf("event outside the 180-day window leaked into the archive:\n%s", html)
	}
	if strings.Index(html, "Flood alert") > strings.Index(html, "Storm warning") {
		t.Errorf("archive order = not newest-first:\n%s", html)
	}
}

// TestArchivePagination pins the page math: 30 events across two pages of
// 25 with Prev/Next links and the correct range labels.
func TestArchivePagination(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	store, _, err := sqlite.Open(":memory:", sqlite.WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	for i := 1; i <= 30; i++ {
		now = now.Add(time.Minute)
		if _, _, err := store.Ingest(context.Background(),
			archiveEvent(fmt.Sprintf("ev%02d", i), fmt.Sprintf("Event number %02d", i)), "fp-"+fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}

	env := newTestEnvWithStore(t, store)
	_, page1 := env.get("/archive")
	if !strings.Contains(page1, "1–25 of 30") || !strings.Contains(page1, "Event number 30") {
		t.Errorf("page 1 = missing newest page content:\n%s", page1)
	}

	_, page2 := env.get("/archive?page=2")
	if !strings.Contains(page2, "26–30 of 30") || !strings.Contains(page2, "Page 2 / 2") {
		t.Errorf("page 2 = wrong range labels:\n%s", page2)
	}
	if !strings.Contains(page2, "Event number 05") || strings.Contains(page2, "Event number 06") {
		t.Errorf("page 2 = wrong entries:\n%s", page2)
	}
}

// TestArchiveWithoutStore pins the graceful degradation: a server built
// without an event store renders an empty archive state.
func TestArchiveWithoutStore(t *testing.T) {
	env := newTestEnv(t)
	_, html := env.get("/archive")
	if !strings.Contains(html, "No communications recorded") {
		t.Errorf("empty archive state missing:\n%s", html)
	}
}
