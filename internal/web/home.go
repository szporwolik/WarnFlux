package web

import (
	"html/template"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/severity"
)

// publicHazardView is one active hazard as shown on the public home page:
// severity, headline, source, event, areas and times — no admin surface.
type publicHazardView struct {
	Severity    string
	Headline    string
	Event       string
	Source      string
	Areas       string
	EffectiveAt *time.Time
	ExpiresAt   *time.Time
	UpdatedAt   time.Time
}

// homeView is the PUBLIC home page model. The login form is deliberately
// not part of it: the sign-in lives behind the top-right icon button.
type homeView struct {
	AppTitle string
	Header1  string
	Header2  string
	Tagline  string
	// About is operator-authored content (config file): rendered with
	// line breaks preserved and a deliberately small HTML surface so
	// links work.
	About        template.HTML
	Version      string
	Commit       string
	RepoURL      string
	LoggedIn     bool
	Username     string
	Landing      string
	LandingLabel string

	ActiveCount int
	Hazards     []publicHazardView
}

// handleHome renders the public landing page: header1/header2 plus the
// current active hazards. No session is required; a logged-in operator
// sees a Dashboard entry in the header instead of the sign-in icon.
func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	v := s.buildHomeView()
	if sess := s.sessions.currentSession(r); sess != nil {
		v.LoggedIn = true
		v.Username = sess.username
		v.Landing = "/dashboard"
		v.LandingLabel = "Dashboard"
		if sess.role != "admin" {
			v.Landing = "/compose"
			v.LandingLabel = "Compose"
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "home", v)
}

// handlePartialHome serves the public auto-refresh fragment of the active
// hazard list (the home page polls it).
func (s *Server) handlePartialHome(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "home_alerts_section", s.buildHomeView())
}

// buildHomeView assembles the public view from the mirrored MQTT state,
// most severe first, then newest.
func (s *Server) buildHomeView() homeView {
	v := homeView{
		AppTitle: s.cfg.Title,
		Header1:  s.displayHeader1(),
		Header2:  s.cfg.Header2,
		Tagline:  s.cfg.Tagline,
		About:    template.HTML(s.cfg.About),
		Version:  s.version,
		Commit:   s.commit,
		RepoURL:  repoURL,
	}

	snap := s.st.Snapshot()
	v.ActiveCount = len(snap.Hazards)
	v.Hazards = make([]publicHazardView, 0, len(snap.Hazards))
	for _, h := range snap.Hazards {
		v.Hazards = append(v.Hazards, publicHazardView{
			Severity:    h.Severity,
			Headline:    h.Headline,
			Event:       h.Event,
			Source:      h.Source,
			Areas:       strings.Join(h.Areas, ", "),
			EffectiveAt: h.EffectiveAt,
			ExpiresAt:   h.ExpiresAt,
			UpdatedAt:   h.UpdatedAt,
		})
	}
	// Most severe first; within one severity, newest first.
	sort.Slice(v.Hazards, func(i, j int) bool {
		ri, _ := severity.Rank(v.Hazards[i].Severity)
		rj, _ := severity.Rank(v.Hazards[j].Severity)
		if ri != rj {
			return ri > rj
		}
		return v.Hazards[i].UpdatedAt.After(v.Hazards[j].UpdatedAt)
	})
	return v
}
