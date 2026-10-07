package web

import (
	"html/template"
	"net/http"

	"github.com/szporwolik/WarnFlux/internal/i18n"
)

// publicChrome carries the shared shell fields of every public page
// (home, archive, sources): the header, the offline/disclaimer banners
// and the footer. The page view itself owns the Lang field (the render
// stamper needs a direct, exported field).
type publicChrome struct {
	AppTitle     string
	Header1      string
	Header2      string
	Tagline      string
	Version      string
	Commit       string
	RepoURL      string
	LoggedIn     bool
	Username     string
	Landing      string
	LandingLabel string
	OfflineMode  bool
	ForceTiles   bool
	Disclaimer   string
	// About is the operator-authored system intro (markdown); empty
	// hides both the one-time popup and the header help button.
	About template.HTML
}

// publicChromeFor builds the common public shell for one request.
func (s *Server) publicChromeFor() publicChrome {
	return publicChrome{
		AppTitle:    s.cfg.Title,
		Header1:     s.displayHeader1(),
		Header2:     s.DisplayHeader2(),
		Tagline:     s.DisplayTagline(),
		Version:     s.version,
		Commit:      s.commit,
		RepoURL:     repoURL,
		OfflineMode: s.OfflineMode(),
		ForceTiles:  s.forceTiles.Load(),
		Disclaimer:  s.DisplayDisclaimer(),
		About:       template.HTML(s.DisplayAbout()),
	}
}

// stampSession fills the logged-in header state of one public view: a
// signed-in operator sees a Dashboard entry instead of the sign-in icon.
func (s *Server) stampSession(c *publicChrome, lang string, r *http.Request) {
	if sess := s.sessions.currentSession(r); sess != nil {
		c.LoggedIn = true
		c.Username = sess.username
		c.Landing = "/dashboard"
		c.LandingLabel = i18n.T(lang, "nav.dashboard")
	}
}

// sourcesView is the public /sources page model: the shell plus the
// source feeds and the notification channels.
type sourcesView struct {
	publicChrome
	Lang     string
	Sources  []publicChannelView
	Channels []publicChannelView
}

// handleSourcesPage renders the public sources/channels page (linked
// from the footer).
func (s *Server) handleSourcesPage(w http.ResponseWriter, r *http.Request) {
	lang := s.langFor(r)
	v := sourcesView{publicChrome: s.publicChromeFor(), Lang: lang}
	s.stampSession(&v.publicChrome, lang, r)
	if s.actions != nil {
		v.Channels = publicChannels(s.actions.Statuses(), lang)
	}
	if s.router != nil {
		v.Sources = publicSources(s.router.Statuses(), lang)
	}
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "sources_page", v)
}
