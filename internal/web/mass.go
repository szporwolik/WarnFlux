package web

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// massMessageMax bounds the short one-shot notice (a short operator
// broadcast, not a hazard communication); massMessageLongMax bounds the
// long one, which only the surfaces that render markdown well can carry.
const (
	massMessageMax     = 50
	massMessageLongMax = 1000
)

// The two message kinds of the Mass info composer.
const (
	massKindShort = "short"
	massKindLong  = "long"
)

// massChannelsByKind lists the channels that can carry a message of a
// given kind: a short notice fits every surface, a long one goes out over
// e-mail and Discord only — the two channels that render it as composed.
var massChannelsByKind = map[string][]string{
	massKindShort: {"aprs", "sms", "email", "discord", "meshtastic"},
	massKindLong:  {"email", "discord"},
}

// massMessageLimit returns the character budget of a message kind.
func massMessageLimit(kind string) int {
	if kind == massKindLong {
		return massMessageLongMax
	}
	return massMessageMax
}

// massNormalizeKind maps a submitted kind onto a known one; an empty value
// means "short", so older forms and bookmarks keep working.
func massNormalizeKind(kind string) string {
	switch strings.TrimSpace(kind) {
	case massKindLong:
		return massKindLong
	default:
		return massKindShort
	}
}

// massChannelAllows reports whether a kind may be delivered over a channel.
func massChannelAllows(kind, channel string) bool {
	for _, k := range massChannelsByKind[kind] {
		if k == channel {
			return true
		}
	}
	return false
}

// massChannelKinds renders the kinds a channel accepts as a
// space-separated list for the form's data attribute, so the client side
// switch and the server validation can never disagree.
func massChannelKinds(channel string) string {
	var kinds []string
	for _, kind := range []string{massKindShort, massKindLong} {
		if massChannelAllows(kind, channel) {
			kinds = append(kinds, kind)
		}
	}
	return strings.Join(kinds, " ")
}

// massCharLimits renders the character budget per kind for the form, which
// drives the live counter and the field's maxlength.
func massCharLimits() string {
	var parts []string
	for _, kind := range []string{massKindShort, massKindLong} {
		parts = append(parts, fmt.Sprintf("%s:%d", kind, massMessageLimit(kind)))
	}
	return strings.Join(parts, " ")
}

// massChannel is one delivery channel offered on the Mass info form.
type massChannel struct {
	Kind  string
	Label string // i18n key
	// Kinds lists the message kinds this channel can carry ("short" always,
	// plus "long" for e-mail and Discord).
	Kinds string
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
	// Kind is the selected message kind ("short" or "long").
	Kind string
	// CharLimits is the character budget per kind, for the live counter.
	CharLimits string
	Msg        string
	Error      string

	// Progress is the live broadcast panel (nil when no job is shown).
	Progress *massProgressView

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

	// DiscordReady reports that an enabled Discord instance carries a bot
	// token, so the notice can be delivered as a direct message. Without
	// one the panel warns instead of silently doing nothing.
	DiscordReady bool

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
		Kind:        massKindShort,
		CharLimits:  massCharLimits(),
		SelGroups:   map[int64]bool{},
		SelNetworks: map[string]bool{},
		SelUsers:    map[int64]bool{},
		SelChannels: map[string]bool{},
		Channels: []massChannel{
			{Kind: "aprs", Label: "mass.ch.aprs", Kinds: massChannelKinds("aprs")},
			{Kind: "sms", Label: "mass.ch.sms", Kinds: massChannelKinds("sms")},
			{Kind: "email", Label: "mass.ch.email", Kinds: massChannelKinds("email")},
			{Kind: "discord", Label: "mass.ch.discord", Kinds: massChannelKinds("discord")},
			{Kind: "meshtastic", Label: "mass.ch.meshtastic", Kinds: massChannelKinds("meshtastic")},
		},
		// Discord is delivered as a direct message, which needs a bot
		// token: without one the notice cannot be sent at all, and the
		// panel says so instead of posting on the channel.
		DiscordReady: s.discordDMAvailable(),
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
	// A job id in the URL resumes the live progress panel (the redirect
	// after a submit, or a page reload while the broadcast runs).
	if job := s.massJobs.get(strings.TrimSpace(r.URL.Query().Get("job"))); job != nil {
		pv := job.progressView(view.Lang)
		view.Progress = &pv
	}
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

// handleMassSend broadcasts one notice to the selected users over the
// selected channels. Every user receives it once per channel, regardless
// of how many groups or networks covered them. A short notice is plain
// text and fits every channel; a long one is markdown and goes out over
// e-mail and Discord only.
func (s *Server) handleMassSend(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	lang := s.langFor(r)
	rawKind := strings.TrimSpace(r.PostFormValue("kind"))
	kind := massNormalizeKind(rawKind)
	if rawKind != "" && rawKind != kind {
		s.renderMassError(w, r, http.StatusUnprocessableEntity, massKindShort,
			strings.TrimSpace(r.PostFormValue("message")), i18n.T(lang, "mass.err.kind"))
		return
	}
	message := strings.TrimSpace(r.PostFormValue("message"))
	if message == "" {
		s.renderMassError(w, r, http.StatusUnprocessableEntity, kind, message, i18n.T(lang, "mass.err.empty"))
		return
	}
	if limit := massMessageLimit(kind); utf8.RuneCountInString(message) > limit {
		s.renderMassError(w, r, http.StatusUnprocessableEntity, kind, message,
			fmt.Sprintf(i18n.T(lang, "mass.err.long"), limit))
		return
	}

	channels := make(map[string]bool)
	wrongKind := ""
	for _, v := range r.PostForm["channels"] {
		switch v {
		case "aprs", "sms", "email", "discord", "meshtastic":
			if !massChannelAllows(kind, v) {
				wrongKind = v
				continue
			}
			channels[v] = true
		}
	}
	if wrongKind != "" {
		// Refuse instead of silently dropping a channel the operator
		// ticked: a notice that quietly skipped part of its audience is
		// worse than a rejected form.
		s.renderMassError(w, r, http.StatusUnprocessableEntity, kind, message,
			i18n.T(lang, "mass.err.channels_kind"))
		return
	}
	if len(channels) == 0 {
		s.renderMassError(w, r, http.StatusUnprocessableEntity, kind, message, i18n.T(lang, "mass.err.no_channels"))
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
		s.renderMassError(w, r, http.StatusUnprocessableEntity, kind, message, i18n.T(lang, "mass.err.no_targets"))
		return
	}

	// Run the broadcast in the background and hand the page a job id, so
	// the operator watches live progress instead of a blocked POST.
	kinds := make([]string, 0, len(channels))
	for _, ch := range massChannelOrder {
		if channels[ch] {
			kinds = append(kinds, ch)
		}
	}
	job := s.startMassJob(massSendRequest{
		kind:     kind,
		message:  message,
		channels: kinds,
		userIDs:  userIDs,
		groupIDs: groupIDs,
		slugs:    slugs,
		username: sess.username,
		lang:     lang,
		app: action.AppInfo{
			Version: s.version,
			Header1: s.displayHeader1(),
			Domain:  s.DisplayDomain(),
			RepoURL: repoURL,
		},
	})
	http.Redirect(w, r, "/mass?job="+url.QueryEscape(job.id), http.StatusSeeOther)
}

// renderMassError re-renders the form with an error banner and the
// submitted message preserved.
func (s *Server) renderMassError(w http.ResponseWriter, r *http.Request, status int, kind, message, msg string) {
	sess := s.sessions.currentSession(r)
	view := s.buildMassView(s.langFor(r))
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	view.Kind = massNormalizeKind(kind)
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
