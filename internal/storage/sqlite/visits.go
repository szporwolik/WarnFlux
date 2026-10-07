package sqlite

import (
	"context"
	"fmt"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// RecordSiteVisit bumps the given day's counter (insert-ensure + update
// keeps the upsert race-free without duplicating logic). The first visit
// of a new day opportunistically prunes the table back to the retention
// bound, so the analytics can never grow without limit.
func (s *Store) RecordSiteVisit(ctx context.Context, day string, newVisitor bool) error {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO site_visits(day, new_visitors, returning_visitors) VALUES(?, 0, 0)
		ON CONFLICT(day) DO NOTHING`, day)
	if err != nil {
		return fmt.Errorf("ensure site visit day: %w", err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		if _, err := s.PruneSiteVisits(ctx, storage.SiteVisitRetentionDays); err != nil {
			return fmt.Errorf("prune site visits: %w", err)
		}
	}
	col := "returning_visitors"
	if newVisitor {
		col = "new_visitors"
	}
	// The column is picked from a fixed set above, never from input.
	if _, err := s.db.ExecContext(ctx,
		`UPDATE site_visits SET `+col+` = `+col+` + 1 WHERE day = ?`, day); err != nil {
		return fmt.Errorf("record site visit: %w", err)
	}
	return nil
}

// PruneSiteVisits deletes every daily counter beyond the newest `keep`
// days.
func (s *Store) PruneSiteVisits(ctx context.Context, keep int) (int64, error) {
	if keep < 1 {
		keep = storage.SiteVisitRetentionDays
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM site_visits WHERE day NOT IN (
			SELECT day FROM site_visits ORDER BY day DESC LIMIT ?)`, keep)
	if err != nil {
		return 0, fmt.Errorf("prune site visits: %w", err)
	}
	return res.RowsAffected()
}

// SiteVisits returns the newest `days` daily counters, newest first.
func (s *Store) SiteVisits(ctx context.Context, days int) ([]storage.SiteVisit, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT day, new_visitors, returning_visitors FROM site_visits
		ORDER BY day DESC LIMIT ?`, days)
	if err != nil {
		return nil, fmt.Errorf("list site visits: %w", err)
	}
	defer rows.Close()
	out := make([]storage.SiteVisit, 0, days)
	for rows.Next() {
		var v storage.SiteVisit
		if err := rows.Scan(&v.Day, &v.New, &v.Returning); err != nil {
			return nil, fmt.Errorf("scan site visit: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
