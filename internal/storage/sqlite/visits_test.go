package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// TestSiteVisits pins the visitor analytics roundtrip: one day counter,
// new vs returning bumps, and the newest-first listing.
func TestSiteVisits(t *testing.T) {
	s, _, err := Open(filepath.Join(t.TempDir(), "visits.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	if err := s.RecordSiteVisit(ctx, "2026-10-05", true); err != nil {
		t.Fatalf("record new: %v", err)
	}
	if err := s.RecordSiteVisit(ctx, "2026-10-06", true); err != nil {
		t.Fatalf("record new day2: %v", err)
	}
	if err := s.RecordSiteVisit(ctx, "2026-10-06", false); err != nil {
		t.Fatalf("record returning: %v", err)
	}
	if err := s.RecordSiteVisit(ctx, "2026-10-06", false); err != nil {
		t.Fatalf("record returning again: %v", err)
	}

	rows, err := s.SiteVisits(ctx, 14)
	if err != nil {
		t.Fatalf("SiteVisits: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if rows[0].Day != "2026-10-06" || rows[0].New != 1 || rows[0].Returning != 2 {
		t.Fatalf("newest row = %+v, want 2026-10-06 {1,2}", rows[0])
	}
	if rows[1].Day != "2026-10-05" || rows[1].New != 1 || rows[1].Returning != 0 {
		t.Fatalf("older row = %+v, want 2026-10-05 {1,0}", rows[1])
	}
}

// TestSiteVisitsPrune pins the 365-day retention: days beyond the newest
// keep count are dropped, both opportunistically (on the first visit of
// a new day) and explicitly.
func TestSiteVisitsPrune(t *testing.T) {
	s, _, err := Open(filepath.Join(t.TempDir(), "visits.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	// 370 days of history — the opportunistic prune on each new day
	// already holds the table at the retention bound.
	for i := 0; i < 370; i++ {
		day := time.Unix(0, 0).AddDate(0, 0, -i).UTC().Format("2006-01-02")
		if err := s.RecordSiteVisit(ctx, day, true); err != nil {
			t.Fatalf("record day %s: %v", day, err)
		}
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM site_visits`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != storage.SiteVisitRetentionDays {
		t.Fatalf("rows after recording = %d, want the opportunistic prune to hold %d", total, storage.SiteVisitRetentionDays)
	}

	// The explicit sweep (hourly maintenance) then removes nothing new.
	n, err := s.PruneSiteVisits(ctx, storage.SiteVisitRetentionDays)
	if err != nil {
		t.Fatalf("PruneSiteVisits: %v", err)
	}
	if n != 0 {
		t.Fatalf("prune removed %d rows, want 0 (already bounded)", n)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM site_visits`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != storage.SiteVisitRetentionDays {
		t.Fatalf("rows after explicit prune = %d, want %d", total, storage.SiteVisitRetentionDays)
	}
}
