package web

import (
	"net/http"
	"strings"
	"time"
)

// archiveDays is the public archive window: the home-page Archive tab
// browses communications from the last 180 days.
const archiveDays = 180

// archivePageSize bounds one archive page.
const archivePageSize = 25

// archiveEventView is one historical communication shown in the archive.
type archiveEventView struct {
	Severity    string
	Headline    string
	Event       string
	Source      string
	Areas       string
	Status      string // active | expired | cancelled
	EffectiveAt *time.Time
	ExpiresAt   *time.Time
	LastSeenAt  time.Time
}

// archiveView is the public archive page model (shell + list +
// pagination).
type archiveView struct {
	publicChrome
	Lang   string
	Events []archiveEventView
	Page   int
	Pages  int
	From   int
	To     int
	Total  int
	Days   int
}

// handleArchive serves the public archive page: the ENDED
// communications of the last archiveDays days, newest first, paginated.
// Currently active communications are deliberately absent — they are
// already listed on the home page (the alerts section links here).
func (s *Server) handleArchive(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	lang := s.langFor(r)
	view := archiveView{publicChrome: s.publicChromeFor(), Lang: lang, Days: archiveDays}
	s.stampSession(&view.publicChrome, lang, r)
	if s.events == nil {
		s.renderL(w, r, "archive_page", view)
		return
	}

	since := time.Now().AddDate(0, 0, -archiveDays)
	total, err := s.events.CountArchiveEvents(r.Context(), since)
	if err != nil {
		s.logger.Warn("web: archive count failed", "error", err)
		s.renderL(w, r, "archive_page", view)
		return
	}
	view.Total = total
	pages := (total + archivePageSize - 1) / archivePageSize
	if pages < 1 {
		pages = 1
	}
	view.Pages = pages

	page := pageParam(r, "page")
	if page < 1 {
		page = 1
	}
	if page > pages {
		page = pages
	}
	view.Page = page
	if total > 0 {
		view.From = (page-1)*archivePageSize + 1
		view.To = page * archivePageSize
		if view.To > total {
			view.To = total
		}
	}

	stored, err := s.events.ListArchiveEvents(r.Context(), since, (page-1)*archivePageSize, archivePageSize)
	if err != nil {
		s.logger.Warn("web: archive list failed", "error", err)
		s.renderL(w, r, "archive_page", view)
		return
	}
	view.Events = make([]archiveEventView, 0, len(stored))
	for _, se := range stored {
		ev := se.Event
		view.Events = append(view.Events, archiveEventView{
			Severity:    ev.Severity,
			Headline:    ev.Headline,
			Event:       ev.Event,
			Source:      ev.Source,
			Areas:       strings.Join(ev.Areas, ", "),
			Status:      string(ev.Status),
			EffectiveAt: ev.EffectiveAt,
			ExpiresAt:   ev.ExpiresAt,
			LastSeenAt:  se.LastSeenAt,
		})
	}
	s.renderL(w, r, "archive_page", view)
}
