package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/gsm"
	"github.com/szporwolik/WarnFlux/internal/i18n"
)

// gsmMessagePageSize bounds one page of the admin SMS history.
const gsmMessagePageSize = 100

// gsmMessageView is one SMS history row.
type gsmMessageView struct {
	Direction string // rx | tx
	// From/To are the raw phone numbers (tx rows: self → number).
	From string
	To   string
	Text string
	At   time.Time
	// FromName/ToName are directory usernames whose registered phone
	// matches the number (empty when nobody registered it).
	FromName string
	ToName   string
}

// gsmPhoneView is one directory phone offered by the send-form picker.
type gsmPhoneView struct {
	Number   string
	Username string
}

// gsmView is the admin GSM page model.
type gsmView struct {
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
	NavWebsite       bool
	NavNotifications bool
	NavHealth        bool
	NavConfig        bool
	NavCompose       bool
	NavEmcom         bool
	NavHelp          bool
	NavAccount       bool
	NavAudit         bool
	NavAPRS          bool
	NavMessages      bool
	NavGSM           bool
	NavMeshtastic    bool
	NavMeshMap       bool

	Messages []gsmMessageView
	// Dir is the direction filter: all, rx or tx.
	Dir   string
	Page  int
	Pages int
	From  int
	To    int
	Total int

	// Phones feeds the send form's datalist.
	Phones []gsmPhoneView

	// HubEnabled/HubConnected/HubReady report the modem session state.
	HubEnabled   bool
	HubConnected bool
	HubReady     bool
	// OperatorName is the friendly network name ("Play"; "" until the
	// first status refresh). SignalBars is a 5-element bar indicator
	// (true = lit) and SignalDetail carries the tooltip text.
	OperatorName string
	SignalBars   []bool
	SignalDetail string

	// Send-form feedback (query flashes).
	Error string
	Sent  bool
}

// fillGSMPhones collects every directory phone number (with the owning
// username) for the send form's datalist.
func (s *Server) fillGSMPhones(ctx context.Context, v *gsmView) {
	if s.users == nil {
		return
	}
	const perPage = 100
	for page := 1; ; page++ {
		users, total, err := s.users.ListUsers(page, perPage)
		if err != nil || len(users) == 0 {
			return
		}
		for _, u := range users {
			number := strings.TrimSpace(u.Phone)
			if !gsm.ValidNumber(number) {
				continue
			}
			v.Phones = append(v.Phones, gsmPhoneView{Number: number, Username: u.Username})
		}
		if page*perPage >= total {
			return
		}
	}
}

// fillGSMMessages loads the paginated SMS history and resolves directory
// names for the numbers.
func (s *Server) fillGSMMessages(r *http.Request, v *gsmView) {
	if s.gsmMsgs == nil {
		return
	}
	switch d := r.URL.Query().Get("dir"); d {
	case "rx", "tx":
		v.Dir = d
	default:
		v.Dir = "all"
	}
	total, err := s.gsmMsgs.CountGSMMessages(r.Context(), v.Dir)
	if err != nil {
		s.logger.Warn("web: gsm messages count failed", "error", err)
		return
	}
	v.Total = total
	pages := (total + gsmMessagePageSize - 1) / gsmMessagePageSize
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
		v.From = (page-1)*gsmMessagePageSize + 1
		v.To = page * gsmMessagePageSize
		if v.To > total {
			v.To = total
		}
	}
	stored, err := s.gsmMsgs.ListGSMMessages(r.Context(), v.Dir, gsmMessagePageSize, (page-1)*gsmMessagePageSize)
	if err != nil {
		s.logger.Warn("web: gsm messages list failed", "error", err)
		return
	}
	v.Messages = make([]gsmMessageView, 0, len(stored))
	// One pass over the phones resolves display names in both columns.
	// Keyed canonically so "+48..." and "0048..." find the same user.
	names := make(map[string]string, len(v.Phones))
	for _, p := range v.Phones {
		k := gsm.CanonicalNumberKey(p.Number)
		if _, ok := names[k]; !ok {
			names[k] = p.Username
		}
	}
	for _, m := range stored {
		view := gsmMessageView{
			Direction: m.Direction,
			From:      m.From,
			To:        m.To,
			Text:      m.Text,
			At:        m.At,
			FromName:  names[gsm.CanonicalNumberKey(m.From)],
			ToName:    names[gsm.CanonicalNumberKey(m.To)],
		}
		v.Messages = append(v.Messages, view)
	}
	// Join legacy split parts BEFORE decoding: the raw parts still
	// carry their boundary spacing, which the decode would trim away.
	v.Messages = mergeLegacyParts(v.Messages)
	for i := range v.Messages {
		// Older rows may still carry a raw UCS-2 hex body — the decode
		// is idempotent, so re-applying it at display time cleans them
		// up too.
		v.Messages[i].Text = gsm.DecodeSMSBody(v.Messages[i].Text)
	}
}

// mergeLegacyParts joins the parts of long messages imported before the
// PDU pipeline existed: the old text-mode ingest stored each segment as
// its own row (parts in ascending id order). Consecutive rx rows from
// the same sender within the same minute are one message — the list is
// newest-first, so the parts join in reverse. New PDU-ingested messages
// arrive already assembled and are never merged.
func mergeLegacyParts(rows []gsmMessageView) []gsmMessageView {
	out := rows[:0]
	for i := 0; i < len(rows); {
		m := rows[i]
		j := i + 1
		if m.Direction == "rx" && m.From != "self" {
			minute := m.At.Truncate(time.Minute)
			for j < len(rows) && rows[j].Direction == "rx" &&
				rows[j].From == m.From &&
				rows[j].At.Truncate(time.Minute).Equal(minute) {
				j++
			}
		}
		if j-i > 1 {
			var sb strings.Builder
			for k := j - 1; k >= i; k-- {
				sb.WriteString(rows[k].Text)
			}
			m.Text = sb.String()
		}
		// Some senders pad message parts with zero-width characters —
		// strip them so the merged text has no invisible gaps.
		m.Text = strings.ReplaceAll(m.Text, "\u200B", "")
		m.Text = strings.ReplaceAll(m.Text, "\uFEFF", "")
		out = append(out, m)
		i = j
	}
	return out
}

// handleGSMPage renders the admin GSM page.
func (s *Server) handleGSMPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	v := gsmView{
		Lang:     s.langFor(r),
		AppTitle: s.cfg.Title,
		Name:     s.displayName(),
		Header1:  s.displayHeader1(),
		Header2:  s.DisplayHeader2(),
		Tagline:  s.DisplayTagline(),
		Version:  s.version,
		Commit:   s.commit,
		RepoURL:  repoURL,
		CSRF:     sess.csrf,
		Username: sess.username,
		Role:     sess.role,
		NavGSM:   true,
	}
	v.Sent = r.URL.Query().Get("sent") == "1"
	v.Error = r.URL.Query().Get("err")
	if s.gsm != nil {
		v.HubEnabled = s.gsm.Enabled()
		v.HubConnected = s.gsm.Connected()
		v.HubReady = s.gsm.Ready()
		v.OperatorName = gsm.OperatorName(s.gsm.Operator())
		if csq := s.gsm.SignalCSQ(); csq > 0 && csq < 99 {
			// Standard CSQ→dBm approximation (2*csq-113).
			v.SignalDetail = fmt.Sprintf("CSQ %d · %d dBm", csq, 2*csq-113)
			lvl := gsm.SignalLevel(csq)
			v.SignalBars = make([]bool, 5)
			for i := range v.SignalBars {
				v.SignalBars[i] = i < lvl
			}
		}
	}
	s.fillGSMPhones(r.Context(), &v)
	s.fillGSMMessages(r, &v)
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "gsm", v)
}

// handleGSMSend transmits one SMS from the admin panel. The tx row lands
// in the durable history after the modem accepted the message.
func (s *Server) handleGSMSend(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	lang := s.langFor(r)
	number := strings.TrimSpace(r.PostFormValue("number"))
	text := strings.TrimSpace(r.PostFormValue("text"))
	if !gsm.ValidNumber(number) {
		http.Redirect(w, r, "/gsm?err="+url.QueryEscape(i18n.T(lang, "gsm.bad_number")), http.StatusSeeOther)
		return
	}
	if text == "" {
		http.Redirect(w, r, "/gsm?err="+url.QueryEscape(i18n.T(lang, "gsm.empty_text")), http.StatusSeeOther)
		return
	}
	if s.gsm == nil {
		http.Redirect(w, r, "/gsm?err="+url.QueryEscape(i18n.T(lang, "gsm.no_modem")), http.StatusSeeOther)
		return
	}
	// The modem's accept reply can take tens of seconds on a busy
	// channel — beyond the server's global WriteTimeout. Extend THIS
	// response's write deadline like the traceroute handler does.
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Now().Add(90 * time.Second))
	}
	if err := s.gsm.Send(r.Context(), number, text); err != nil {
		var flash string
		if errors.Is(err, gsm.ErrTooLong) {
			flash = i18n.T(lang, "gsm.too_long")
		} else {
			flash = fmt.Sprintf(i18n.T(lang, "gsm.send_failed"), err)
		}
		http.Redirect(w, r, "/gsm?err="+url.QueryEscape(flash), http.StatusSeeOther)
		return
	}
	s.audit(sess.username, "gsm-send", number)
	http.Redirect(w, r, "/gsm?sent=1", http.StatusSeeOther)
}

// handlePartialGSM serves the polled SMS history fragment so the admin
// sees new traffic as it happens.
func (s *Server) handlePartialGSM(w http.ResponseWriter, r *http.Request) {
	v := gsmView{Dir: "all"}
	if s.gsmMsgs != nil {
		s.fillGSMMessages(r, &v)
	}
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "gsm_msgs", v)
}
