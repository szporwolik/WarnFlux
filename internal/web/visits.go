package web

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// Cookie consent and visitor analytics. The site counts new and
// returning visitors once per day — and only for visitors who accepted
// the cookie notice (a single first-party cookie identifies a visitor;
// nothing else is tracked).
const (
	consentCookie  = "wf_consent"
	visitorCookie  = "wf_uid"
	visitDayCookie = "wf_vday"
	cookieMaxAge   = 365 * 24 * 3600
)

// countVisit records one visit (new or returning) for the current day
// when the visitor accepted cookies. It is called once per rendered
// HTML page, but the wf_vday cookie guarantees at most one count per
// visitor per day.
func (s *Server) countVisit(w http.ResponseWriter, r *http.Request) {
	if s.visits == nil {
		return
	}
	consent, err := r.Cookie(consentCookie)
	if err != nil || consent.Value != "yes" {
		return
	}
	today := time.Now().Format("2006-01-02")

	uid, err := r.Cookie(visitorCookie)
	if err != nil || uid.Value == "" {
		// First visit ever: mint the visitor id and count as new.
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			return
		}
		http.SetCookie(w, &http.Cookie{Name: visitorCookie, Value: hex.EncodeToString(id), Path: "/", MaxAge: cookieMaxAge, HttpOnly: true, SameSite: http.SameSiteLaxMode})
		http.SetCookie(w, &http.Cookie{Name: visitDayCookie, Value: today, Path: "/", MaxAge: cookieMaxAge, HttpOnly: true, SameSite: http.SameSiteLaxMode})
		if err := s.visits.RecordSiteVisit(r.Context(), today, true); err != nil {
			s.logger.Debug("web: record new visitor failed", "error", err)
		}
		return
	}

	if vday, err := r.Cookie(visitDayCookie); err == nil && vday.Value == today {
		return // already counted today
	}
	http.SetCookie(w, &http.Cookie{Name: visitDayCookie, Value: today, Path: "/", MaxAge: cookieMaxAge, HttpOnly: true, SameSite: http.SameSiteLaxMode})
	if err := s.visits.RecordSiteVisit(r.Context(), today, false); err != nil {
		s.logger.Debug("web: record returning visitor failed", "error", err)
	}
}

// handleVisitorStats serves the daily visitor counters for the Traffic
// page's Visitors tab (admin only).
func (s *Server) handleVisitorStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if s.visits == nil {
		_ = json.NewEncoder(w).Encode([]any{})
		return
	}
	days := 14
	if d, err := strconv.Atoi(r.URL.Query().Get("days")); err == nil && d > 0 && d <= 90 {
		days = d
	}
	rows, err := s.visits.SiteVisits(r.Context(), days)
	if err != nil {
		s.logger.Warn("web: site visits failed", "error", err)
		http.Error(w, "stats unavailable", http.StatusInternalServerError)
		return
	}
	if rows == nil {
		rows = []storage.SiteVisit{}
	}
	if err := json.NewEncoder(w).Encode(rows); err != nil {
		s.logger.Warn("web: encode site visits failed", "error", err)
	}
}
