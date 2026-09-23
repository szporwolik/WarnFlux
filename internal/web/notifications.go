package web

import (
	"encoding/json"
	"net/http"

	"github.com/szporwolik/WarnFlux/internal/trail"
)

// notificationsPerPage caps the server-rendered trail list; the page
// poller appends newer trails via the partial endpoint anyway.
const notificationsPerPage = 50

// notificationsView is the full /notifications page model.
type notificationsView struct {
	AppTitle string
	Name     string
	Header1  string
	Header2  string
	Tagline  string
	Version  string
	Commit   string
	RepoURL  string
	CSRF     string
	Username string

	// FocusKey highlights one trail (?key=… deep links from the
	// dashboard warning cards).
	FocusKey string

	Trails []trail.Trail

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavTest          bool
	NavLogs          bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
}

// handleNotificationsPage renders the delivery history: the most recent
// notification trails, newest first. ?key= focuses one trail.
func (s *Server) handleNotificationsPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := notificationsView{
		AppTitle:         s.cfg.Title,
		Name:             s.displayName(),
		Header1:          s.displayHeader1(),
		Header2:          s.cfg.Header2,
		Tagline:          s.cfg.Tagline,
		Version:          s.version,
		Commit:           s.commit,
		RepoURL:          repoURL,
		Username:         sess.username,
		CSRF:             sess.csrf,
		FocusKey:         r.URL.Query().Get("key"),
		Trails:           s.recentTrails(notificationsPerPage),
		NavNotifications: true,
	}
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "notifications", view)
}

// handlePartialNotifications serves the full current trail list as JSON:
// the page poller re-renders on change so new alerts appear without a
// reload. {"trails":[...]}.
func (s *Server) handlePartialNotifications(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"trails": s.recentTrails(notificationsPerPage),
	})
}

func (s *Server) recentTrails(limit int) []trail.Trail {
	if s.trails == nil {
		return []trail.Trail{}
	}
	trails := s.trails.Recent(limit)
	if trails == nil {
		trails = []trail.Trail{}
	}
	return trails
}
