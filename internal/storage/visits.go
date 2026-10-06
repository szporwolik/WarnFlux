package storage

import "context"

// SiteVisit is one day of visitor analytics (new vs returning visitors).
type SiteVisit struct {
	Day       string
	New       int
	Returning int
}

// VisitorStore persists per-day visitor analytics. Visitors are counted
// once per day, only after they accepted the site's cookie notice.
type VisitorStore interface {
	// RecordSiteVisit bumps the given day's counter. day is a local
	// calendar date ("2006-01-02") chosen by the caller.
	RecordSiteVisit(ctx context.Context, day string, newVisitor bool) error
	// SiteVisits returns the newest `days` daily counters, newest first.
	SiteVisits(ctx context.Context, days int) ([]SiteVisit, error)
}
