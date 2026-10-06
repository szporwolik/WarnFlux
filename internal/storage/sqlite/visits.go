package sqlite

import (
	"context"
	"fmt"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// RecordSiteVisit bumps the given day's counter (insert-ensure + update
// keeps the upsert race-free without duplicating logic).
func (s *Store) RecordSiteVisit(ctx context.Context, day string, newVisitor bool) error {
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO site_visits(day, new_visitors, returning_visitors) VALUES(?, 0, 0)
		ON CONFLICT(day) DO NOTHING`, day); err != nil {
		return fmt.Errorf("ensure site visit day: %w", err)
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
