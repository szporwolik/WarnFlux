package web

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/aprs"
)

// aprsMessagesPageSize bounds one page of the admin APRS message history.
const aprsMessagesPageSize = 100

// aprsMessageView is one history row shown on the admin page.
type aprsMessageView struct {
	ID        int64
	Direction string
	From      string
	To        string
	Text      string
	Via       string
	At        time.Time
	// Bulletin marks broadcast frames (addressed to BLN0-BLN9, BLNA-Z):
	// visible in the history, never routed as alerts.
	Bulletin bool
}

// aprsMessagesView is the admin APRS message history page model.
type aprsMessagesView struct {
	Lang     string
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
	Role     string

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavLogs          bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
	NavCompose       bool
	NavEmcom         bool
	NavAccount       bool
	NavAudit         bool
	NavMessages      bool
	NavMeshcore      bool

	Messages []aprsMessageView
	Dir      string // all | rx | tx
	Page     int
	Pages    int
	From     int
	To       int
	Total    int

	// Calls lists the registered user callsigns for the send-form picker.
	Calls []string

	// Send-form feedback (query flashes).
	Error string
	Sent  bool
}

// handleAPRSMessagesPage renders the admin view of every received and sent
// APRS message, newest first, paginated, with an rx/tx filter. The history
// lives in the database and survives restarts.
func (s *Server) handleAPRSMessagesPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	v := aprsMessagesView{
		AppTitle:    s.cfg.Title,
		Name:        s.displayName(),
		Header1:     s.displayHeader1(),
		Header2:     s.cfg.Header2,
		Tagline:     s.cfg.Tagline,
		Version:     s.version,
		Commit:      s.commit,
		RepoURL:     repoURL,
		CSRF:        sess.csrf,
		Username:    sess.username,
		Role:        sess.role,
		NavMessages: true,
		Dir:         "all",
	}
	w.Header().Set("Cache-Control", "no-store")
	if errMsg := r.URL.Query().Get("err"); errMsg != "" {
		v.Error = errMsg
	}
	v.Sent = r.URL.Query().Get("sent") != ""
	if s.users != nil {
		v.Calls, _ = s.users.AllAPRSCallsigns()
	}

	if s.aprsMsgs == nil {
		s.renderL(w, r, "messages", v)
		return
	}
	s.fillAPRSMessages(r, &v)
	s.renderL(w, r, "messages", v)
}

// fillAPRSMessages loads the filtered, paginated message list into the
// view (shared by the page and the polled fragment).
func (s *Server) fillAPRSMessages(r *http.Request, v *aprsMessagesView) {
	switch d := r.URL.Query().Get("dir"); d {
	case "rx", "tx":
		v.Dir = d
	}
	dirFilter := ""
	if v.Dir != "all" {
		dirFilter = v.Dir
	}

	total, err := s.aprsMsgs.CountAPRSMessages(r.Context(), dirFilter)
	if err != nil {
		s.logger.Warn("web: aprs messages count failed", "error", err)
		return
	}
	v.Total = total
	pages := (total + aprsMessagesPageSize - 1) / aprsMessagesPageSize
	if pages < 1 {
		pages = 1
	}
	v.Pages = pages

	page := pageParam(r, "page")
	if page < 1 {
		page = 1
	}
	if page > pages {
		page = pages
	}
	v.Page = page
	if total > 0 {
		v.From = (page-1)*aprsMessagesPageSize + 1
		v.To = page * aprsMessagesPageSize
		if v.To > total {
			v.To = total
		}
	}

	stored, err := s.aprsMsgs.ListAPRSMessages(r.Context(), dirFilter, aprsMessagesPageSize, (page-1)*aprsMessagesPageSize)
	if err != nil {
		s.logger.Warn("web: aprs messages list failed", "error", err)
		return
	}
	v.Messages = make([]aprsMessageView, 0, len(stored))
	for _, m := range stored {
		v.Messages = append(v.Messages, aprsMessageView{
			ID:        m.ID,
			Direction: m.Direction,
			From:      m.From,
			To:        m.To,
			Text:      m.Text,
			Via:       m.Via,
			At:        m.At,
			Bulletin:  aprs.IsBulletin(m.To),
		})
	}
}

// handlePartialMessages serves the polled APRS message-list fragment so
// the admin sees new traffic as it happens.
func (s *Server) handlePartialMessages(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	v := aprsMessagesView{
		CSRF: sess.csrf,
		Dir:  "all",
	}
	if s.aprsMsgs != nil {
		s.fillAPRSMessages(r, &v)
	}
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "aprs_msgs", v)
}

// handleAPRSSend transmits one APRS message from the admin panel through
// the first ready transmitter. The hub validates the addressee, records
// the tx in the durable history and reports the outcome back via the
// page flash.
func (s *Server) handleAPRSSend(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || !csrfOK(r.PostFormValue("csrf"), sess.csrf) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	if s.aprs == nil {
		http.Redirect(w, r, "/messages?err="+url.QueryEscape("APRS hub not configured"), http.StatusSeeOther)
		return
	}
	to := strings.TrimSpace(r.PostFormValue("to"))
	text := strings.TrimSpace(r.PostFormValue("text"))
	if err := s.aprs.SendMessage(r.Context(), to, text); err != nil {
		http.Redirect(w, r, "/messages?err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	s.audit(sess.username, "aprs-send", to)
	http.Redirect(w, r, "/messages?sent=1", http.StatusSeeOther)
}

// handleAPRSBeacon forces an immediate position beacon through the radio
// backend (the manual "send beacon now" button above the page tabs).
func (s *Server) handleAPRSBeacon(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || !csrfOK(r.PostFormValue("csrf"), sess.csrf) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	if s.aprs == nil {
		http.Redirect(w, r, "/messages?err="+url.QueryEscape("APRS hub not configured"), http.StatusSeeOther)
		return
	}
	if err := s.aprs.SendBeacon(r.Context()); err != nil {
		http.Redirect(w, r, "/messages?err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	s.audit(sess.username, "aprs-beacon", "")
	http.Redirect(w, r, "/messages?sent=1", http.StatusSeeOther)
}
