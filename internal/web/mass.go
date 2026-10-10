package web

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/szporwolik/WarnFlux/internal/actions/discord"
	"github.com/szporwolik/WarnFlux/internal/actions/smtp"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// massMessageMax bounds the one-shot notice (a short operator broadcast,
// not a hazard communication).
const massMessageMax = 50

// massChannel is one delivery channel offered on the Mass info form.
type massChannel struct {
	Kind  string
	Label string // i18n key
}

// massNetwork is one EMCOM network selectable as a recipient scope.
type massNetwork struct {
	Slug string
	Name string
}

// massView is the admin-only Mass info page model.
type massView struct {
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

	// Message echoes the submitted text on a validation error.
	Message string
	Msg     string
	Error   string

	Users    []storage.User
	Groups   []storage.Group
	Networks []massNetwork
	Channels []massChannel

	// Sel* carry the submitted selection back into the form after a
	// validation error, so a failed send does not lose the operator's
	// picks.
	SelGroups   map[int64]bool
	SelNetworks map[string]bool
	SelUsers    map[int64]bool
	SelChannels map[string]bool

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavCompose       bool
	NavEmcom         bool
	NavAccount       bool
	NavLogs          bool
	NavAudit         bool
	NavAPRS          bool
	NavMessages      bool
	NavGSM           bool
	NavMeshtastic    bool
	NavMeshMap       bool
	NavTraffic       bool
	NavWebsite       bool
	NavNotifications bool
	NavHealth        bool
	NavConfig        bool
	NavHelp          bool
	NavMass          bool
}

// buildMassView assembles the page model in a UI language.
func (s *Server) buildMassView(lang string) massView {
	v := massView{
		Lang:        lang,
		AppTitle:    s.cfg.Title,
		Name:        s.displayName(),
		Header1:     s.displayHeader1(),
		Header2:     s.DisplayHeader2(),
		Tagline:     s.DisplayTagline(),
		Version:     s.version,
		Commit:      s.commit,
		RepoURL:     repoURL,
		NavMass:     true,
		SelGroups:   map[int64]bool{},
		SelNetworks: map[string]bool{},
		SelUsers:    map[int64]bool{},
		SelChannels: map[string]bool{},
		Channels: []massChannel{
			{Kind: "aprs", Label: "mass.ch.aprs"},
			{Kind: "sms", Label: "mass.ch.sms"},
			{Kind: "email", Label: "mass.ch.email"},
			{Kind: "discord", Label: "mass.ch.discord"},
			{Kind: "meshtastic", Label: "mass.ch.meshtastic"},
		},
	}
	if s.users != nil {
		v.Users = s.loadAllUsers()
		v.Groups, _ = s.users.ListAllGroups()
		for _, n := range s.emcomNetworks() {
			v.Networks = append(v.Networks, massNetwork{Slug: n.Slug, Name: n.Name})
		}
	}
	return v
}

// loadAllUsers reads the whole directory (small by design) for the
// recipient picker and the dedup logic.
func (s *Server) loadAllUsers() []storage.User {
	var out []storage.User
	page, perPage := 1, 100
	for {
		rows, total, err := s.users.ListUsers(page, perPage)
		if err != nil {
			s.logger.Warn("mass: user list failed", "error", err)
			return out
		}
		out = append(out, rows...)
		if len(out) >= total || len(rows) < perPage {
			return out
		}
		page++
	}
}

// handleMassPage renders the admin-only Mass info form.
func (s *Server) handleMassPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := s.buildMassView(s.langFor(r))
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "mass", view)
}

// massRecipients resolves the unique recipients: directly selected users
// plus the members of the selected groups plus the members of the groups
// assigned to the selected EMCOM networks. Every user appears once.
func (s *Server) massRecipients(ctx context.Context, userIDs, groupIDs []int64, slugs []string) []storage.User {
	if s.users == nil {
		return nil
	}
	users := s.loadAllUsers()
	byID := make(map[int64]storage.User, len(users))
	for _, u := range users {
		byID[u.ID] = u
	}

	selected := make(map[int64]bool)
	for _, id := range userIDs {
		if _, ok := byID[id]; ok {
			selected[id] = true
		}
	}

	membership := make(map[int64]map[int64]bool)
	inGroup := func(uid, gid int64) bool {
		set, ok := membership[uid]
		if !ok {
			ids, err := s.users.GroupIDsForUser(uid)
			if err != nil {
				s.logger.Warn("mass: membership lookup failed", "user_id", uid, "error", err)
				return false
			}
			set = make(map[int64]bool, len(ids))
			for _, id := range ids {
				set[id] = true
			}
			membership[uid] = set
		}
		return set[gid]
	}

	networkGroups := make(map[int64]bool)
	if gs, ok := s.users.(storage.GroupStore); ok {
		for _, slug := range slugs {
			ids, err := gs.EmcomNetworkGroups(ctx, slug)
			if err != nil {
				s.logger.Warn("mass: network groups failed", "slug", slug, "error", err)
				continue
			}
			for _, id := range ids {
				networkGroups[id] = true
			}
		}
	}

	for _, u := range users {
		for _, gid := range groupIDs {
			if inGroup(u.ID, gid) {
				selected[u.ID] = true
				break
			}
		}
		if len(networkGroups) == 0 {
			continue
		}
		for gid := range networkGroups {
			if inGroup(u.ID, gid) {
				selected[u.ID] = true
				break
			}
		}
	}

	out := make([]storage.User, 0, len(selected))
	for id := range selected {
		out = append(out, byID[id])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// MassRecipientsForTest exposes the recipient resolution to cross-package
// tests (the dedup contract is the important part).
func (s *Server) MassRecipientsForTest(ctx context.Context, userIDs, groupIDs []int64, slugs []string) []storage.User {
	return s.massRecipients(ctx, userIDs, groupIDs, slugs)
}

// handleMassSend broadcasts one short notice to the selected users over
// the selected channels. Every user receives it once per channel,
// regardless of how many groups or networks covered them.
func (s *Server) handleMassSend(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	lang := s.langFor(r)
	message := strings.TrimSpace(r.PostFormValue("message"))
	if message == "" {
		s.renderMassError(w, r, http.StatusUnprocessableEntity, message, i18n.T(lang, "mass.err.empty"))
		return
	}
	if utf8.RuneCountInString(message) > massMessageMax {
		s.renderMassError(w, r, http.StatusUnprocessableEntity, message,
			fmt.Sprintf(i18n.T(lang, "mass.err.long"), massMessageMax))
		return
	}

	channels := make(map[string]bool)
	for _, v := range r.PostForm["channels"] {
		switch v {
		case "aprs", "sms", "email", "discord", "meshtastic":
			channels[v] = true
		}
	}
	if len(channels) == 0 {
		s.renderMassError(w, r, http.StatusUnprocessableEntity, message, i18n.T(lang, "mass.err.no_channels"))
		return
	}
	var userIDs, groupIDs []int64
	for _, v := range r.PostForm["users"] {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil && id > 0 {
			userIDs = append(userIDs, id)
		}
	}
	for _, v := range r.PostForm["groups"] {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil && id > 0 {
			groupIDs = append(groupIDs, id)
		}
	}
	var slugs []string
	for _, v := range r.PostForm["networks"] {
		if v = strings.TrimSpace(v); v != "" {
			slugs = append(slugs, v)
		}
	}
	if len(userIDs) == 0 && len(groupIDs) == 0 && len(slugs) == 0 {
		s.renderMassError(w, r, http.StatusUnprocessableEntity, message, i18n.T(lang, "mass.err.no_targets"))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	recipients := s.massRecipients(ctx, userIDs, groupIDs, slugs)

	var aprsN, smsN, emailN, discordN, meshN, failures int

	if channels["aprs"] && s.aprs != nil {
		for _, u := range recipients {
			for _, call := range u.APRSCallsigns {
				if err := s.aprs.SendMessage(ctx, call, message); err != nil {
					failures++
				} else {
					aprsN++
				}
			}
		}
	}
	if channels["sms"] && s.gsm != nil && s.gsm.Enabled() {
		for _, u := range recipients {
			if strings.TrimSpace(u.Phone) == "" {
				continue
			}
			if err := s.gsm.Send(ctx, u.Phone, message); err != nil {
				failures++
			} else {
				smsN++
			}
		}
	}
	if channels["meshtastic"] && s.meshtastic != nil && s.meshtastic.Enabled() {
		for _, u := range recipients {
			for _, id := range u.MeshtasticIDs {
				if err := s.meshtastic.SendContactMessage(ctx, id, message, sess.username); err != nil {
					failures++
				} else {
					meshN++
				}
			}
		}
	}

	if channels["email"] && s.actions != nil {
		subject := i18n.T(s.SystemLanguage(), "mass.mail_subject")
		for _, st := range s.actions.Statuses() {
			if st.Type != smtp.Type || !st.Enabled {
				continue
			}
			p, ok := s.actions.Plugin(st.ID)
			if !ok {
				continue
			}
			mailer, ok := p.(smtp.DirectMailer)
			if !ok {
				continue
			}
			for _, u := range recipients {
				if strings.TrimSpace(u.Email) == "" {
					continue
				}
				if err := mailer.SendPlain(ctx, []string{u.Email}, subject, message); err != nil {
					failures++
				} else {
					emailN++
				}
			}
			break
		}
	}
	if channels["discord"] && s.actions != nil {
		for _, st := range s.actions.Statuses() {
			if st.Type != discord.Type || !st.Enabled {
				continue
			}
			p, ok := s.actions.Plugin(st.ID)
			if !ok {
				continue
			}
			poster, ok := p.(discord.PlainPoster)
			if !ok {
				continue
			}
			if err := poster.PostText(ctx, message); err != nil {
				failures++
			} else {
				discordN++
			}
			break
		}
	}

	s.logger.Info("mass: notice broadcast",
		"users", len(recipients), "aprs", aprsN, "sms", smsN, "email", emailN,
		"discord", discordN, "meshtastic", meshN, "failures", failures)
	s.audit(sess.username, "mass-info", fmt.Sprintf("users=%d aprs=%d sms=%d email=%d discord=%d mesh=%d failures=%d",
		len(recipients), aprsN, smsN, emailN, discordN, meshN, failures))
	flash := fmt.Sprintf(i18n.T(lang, "mass.flash.sent"), len(recipients), aprsN, smsN, emailN, discordN, meshN)
	if failures > 0 {
		flash += " " + fmt.Sprintf(i18n.T(lang, "mass.flash.failures"), failures)
	}
	http.Redirect(w, r, "/mass?msg="+url.QueryEscape(flash), http.StatusSeeOther)
}

// renderMassError re-renders the form with an error banner and the
// submitted message preserved.
func (s *Server) renderMassError(w http.ResponseWriter, r *http.Request, status int, message, msg string) {
	sess := s.sessions.currentSession(r)
	view := s.buildMassView(s.langFor(r))
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	view.Message = message
	view.Error = msg
	view.SelGroups = intSet(r.PostForm["groups"])
	view.SelUsers = intSet(r.PostForm["users"])
	view.SelNetworks = stringSet(r.PostForm["networks"])
	view.SelChannels = stringSet(r.PostForm["channels"])
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	s.renderL(w, r, "mass", view)
}

// intSet collects the positive integer form values (group and user ids)
// into a lookup set for the form's checked state.
func intSet(values []string) map[int64]bool {
	set := make(map[int64]bool, len(values))
	for _, v := range values {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil && id > 0 {
			set[id] = true
		}
	}
	return set
}

// stringSet collects the non-empty form values (network slugs, channels)
// into a lookup set for the form's checked state.
func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, v := range values {
		if v = strings.TrimSpace(v); v != "" {
			set[v] = true
		}
	}
	return set
}
