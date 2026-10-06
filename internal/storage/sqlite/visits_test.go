package sqlite

import (
	"context"
	"path/filepath"
	"testing"
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
