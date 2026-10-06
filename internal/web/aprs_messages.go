package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/i18n"
)

// aprsMessagesPageSize bounds one page of the admin APRS message history.
const aprsMessagesPageSize = 100

// aprsMessageView is one history row shown on the admin page.
type aprsMessageView struct {
	ID        int64
	Direction string
	From      string
	// FromName is the directory username owning the sender's callsign on
	// rx rows (empty otherwise).
	FromName string
	To       string
	// ToName is the directory username owning the addressee's callsign
	// on tx rows (empty otherwise).
	ToName string
	Text   string
	MsgID  string
	Via    string
	At     time.Time
	// Status is the outbound delivery state ("delivered" after the
	// addressee's ack, "failed" after a rej); empty while unanswered.
	Status string
	// Bulletin marks broadcast frames (addressed to BLN0-BLN9, BLNA-Z):
	// visible in the history, never routed as alerts.
	Bulletin bool
}

// aprsMessagesData is the durable APRS message-history payload shared by
// the combined APRS section (/aprs, messages tab) and the polled
// fragment (/partials/messages).
type aprsMessagesData struct {
	// Lang is stamped by renderL so the {{tr}} calls in the shared
	// aprs_msgs template resolve in the user's UI language.
	Lang     string
	Messages []aprsMessageView
	Dir      string // all | rx | tx
	Page     int
	Pages    int
	From     int
	To       int
	Total    int
}

// handleAPRSMessagesPage redirects to the combined APRS section: the
// message history now lives on the messages tab of /aprs. Kept so old
// links and the send-form feedback URLs keep working.
func (s *Server) handleAPRSMessagesPage(w http.ResponseWriter, r *http.Request) {
	target := "/aprs?tab=msgs"
	if q := r.URL.RawQuery; q != "" {
		target += "&" + q
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

// fillAPRSMessages loads the filtered, paginated message list into the
// view (shared by the page and the polled fragment).
func (s *Server) fillAPRSMessages(r *http.Request, v *aprsMessagesData) {
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
	// Directory match: label senders (rx) and addressees (tx) whose
	// callsign is registered on a user, base-callsign insensitive.
	var owners map[string]string
	if s.users != nil {
		owners, _ = s.users.APRSCallsignOwners()
	}
	for _, m := range stored {
		view := aprsMessageView{
			ID:        m.ID,
			Direction: m.Direction,
			From:      m.From,
			To:        m.To,
			Text:      m.Text,
			MsgID:     m.MsgID,
			Via:       m.Via,
			At:        m.At,
			Status:    m.Status,
			Bulletin:  aprs.IsBulletin(m.To),
		}
		if m.Direction == "rx" {
			view.FromName = aprsOwnerFor(owners, m.From)
		} else {
			view.ToName = aprsOwnerFor(owners, m.To)
		}
		v.Messages = append(v.Messages, view)
	}
}

// aprsOwnerFor resolves the directory username for a callsign: an exact
// match on the registered callsign first, then a base-callsign match
// (SSID-insensitive), mirroring the routing sender allow-list.
func aprsOwnerFor(owners map[string]string, callsign string) string {
	c := strings.ToUpper(strings.TrimSpace(callsign))
	if c == "" {
		return ""
	}
	if u, ok := owners[c]; ok {
		return u
	}
	base := c
	if i := strings.IndexByte(base, '-'); i > 0 {
		base = base[:i]
	}
	for key, u := range owners {
		k := key
		if i := strings.IndexByte(k, '-'); i > 0 {
			k = k[:i]
		}
		if k == base {
			return u
		}
	}
	return ""
}

// handlePartialMessages serves the polled APRS message-list fragment so
// the admin sees new traffic as it happens.
func (s *Server) handlePartialMessages(w http.ResponseWriter, r *http.Request) {
	v := aprsMessagesData{Dir: "all"}
	if s.aprsMsgs != nil {
		s.fillAPRSMessages(r, &v)
	}
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "aprs_msgs", v)
}

// handleAPRSSend transmits one APRS message from the admin panel with an
// ack id and waits briefly for the addressee's ack. The outcome lands in
// the durable history status and is reported back via the page flash.
func (s *Server) handleAPRSSend(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	if s.aprs == nil {
		http.Redirect(w, r, "/aprs?tab=msgs&err="+url.QueryEscape(i18n.T(s.langFor(r), "messages.err.no_hub")), http.StatusSeeOther)
		return
	}
	to := strings.TrimSpace(r.PostFormValue("to"))
	text := strings.TrimSpace(r.PostFormValue("text"))
	lang := s.langFor(r)
	ackCtx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	acked, err := s.aprs.SendMessageWaitAck(ackCtx, to, text, 15*time.Second)
	cancel()
	if err != nil {
		key := "messages.send_failed"
		if errors.Is(err, aprs.ErrNoAck) {
			key = "messages.send_no_ack"
		}
		flash := fmt.Sprintf(i18n.T(lang, key), err)
		http.Redirect(w, r, "/aprs?tab=msgs&err="+url.QueryEscape(flash), http.StatusSeeOther)
		return
	}
	if !acked {
		flash := i18n.T(lang, "messages.send_no_ack")
		http.Redirect(w, r, "/aprs?tab=msgs&err="+url.QueryEscape(flash), http.StatusSeeOther)
		return
	}
	s.audit(sess.username, "aprs-send", to)
	http.Redirect(w, r, "/aprs?tab=msgs&sent=1", http.StatusSeeOther)
}

// handleAPRSBeacon forces an immediate position beacon through the radio
// backend (the manual "send beacon now" button above the page tabs).
func (s *Server) handleAPRSBeacon(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	if s.aprs == nil {
		http.Redirect(w, r, "/aprs?tab=msgs&err="+url.QueryEscape(i18n.T(s.langFor(r), "messages.err.no_hub")), http.StatusSeeOther)
		return
	}
	if err := s.aprs.SendBeacon(r.Context()); err != nil {
		flash := fmt.Sprintf(i18n.T(s.langFor(r), "messages.beacon_failed"), err)
		http.Redirect(w, r, "/aprs?tab=msgs&err="+url.QueryEscape(flash), http.StatusSeeOther)
		return
	}
	s.audit(sess.username, "aprs-beacon", "")
	http.Redirect(w, r, "/aprs?tab=msgs&beacon=1", http.StatusSeeOther)
}
