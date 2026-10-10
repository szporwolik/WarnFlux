package web

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/notify"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// usersPerPage bounds the users table to a compact, paginated view.
const usersPerPage = 10

// usernamePattern is the accepted username shape: lowercase slug, like the
// plugin instance IDs elsewhere.
var usernamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// userRow is one users-table row for the template.
type userRow struct {
	ID            int64
	Username      string
	Phone         string
	Email         string
	Discord       string
	IsAdmin       bool
	Role          string
	Lang          string
	GroupNames    []string
	APRSCallsigns []string
	APRSJoin      string
	MeshtasticIDs []string
	UpdatedAt     time.Time
}

// userForm carries the add/edit form values (also used to re-render the
// form after a validation error).
type userForm struct {
	Username string
	Phone    string
	Email    string
	Discord  string
	Role     string
	Password string
	// APRSCallsigns is the free-text APRS callsign list (space or comma
	// separated, each with optional -SSID).
	APRSCallsigns string
	// MeshtasticIDs is the free-text Meshtastic node id list (space or comma
	// separated, 64-hex keys, optional 0x prefix).
	MeshtasticIDs string
	// GroupSet / ChannelSet carry the modal's checked boxes (group
	// membership and enabled delivery channels) for the edit prefill
	// and the validation-error echo.
	GroupSet   map[int64]bool
	ChannelSet map[string]bool
	// NotifLang is the user's notification language ("" = system
	// default). Named to avoid the render-time UI-language stamping of
	// every field literally called "Lang".
	NotifLang string
}

// usersView is the users-tab panel model of the merged /access page.
type usersView struct {
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

	// Tab carries the active access-page tab ("users" | "groups") so the
	// panel knows whether it is the visible one.
	Tab string

	Users  []userRow
	Groups []storage.Group
	// Channels is the delivery-channel list (same registry as the
	// self-service account page); admins override per-user opt-outs in
	// the row preferences popover.
	Channels []notify.ChannelDef
	// Languages lists the supported notification languages for the
	// user dialog picker.
	Languages []string
	Form      userForm
	EditID    int64
	// AdminEdit marks the dialog editing the configured admin row:
	// identity fields (username, role, password) are shown read-only and
	// only contact data plus delivery preferences are editable.
	AdminEdit bool
	// DialogOpen re-renders the add/edit dialog already open (an edit via
	// ?edit=, or a failed add/edit submit that echoes the form back).
	DialogOpen bool
	Error      string
	// Notice is a green, one-time banner (e.g. a freshly generated
	// password after an admin-side reset).
	Notice string

	Page, Pages, From, To, Total int
	HasPrev, HasNext             bool

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
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
	NavCompose       bool
	NavEmcom         bool
	NavHelp          bool
	NavAccount       bool
}

// handleUsersPage keeps the old /users URL working: the directory is the
// default tab of the merged /access page. ?edit=<id> opens the dialog.
func (s *Server) handleUsersPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := s.buildAccessView(r, sess, "users")
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "access", view)
}

// handleUserSave creates or updates a user from the top form. A hidden
// edit_id turns the request into an update.
func (s *Server) handleUserSave(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if csrfMismatch(r, sess) {
		s.redirectAfterCSRFMismatch(w, r, "/users")
		return
	}

	form := userForm{
		Username:      strings.TrimSpace(r.PostFormValue("username")),
		Phone:         strings.TrimSpace(r.PostFormValue("phone")),
		Email:         strings.TrimSpace(r.PostFormValue("email")),
		Discord:       strings.TrimSpace(r.PostFormValue("discord")),
		Role:          strings.ToLower(strings.TrimSpace(r.PostFormValue("role"))),
		Password:      r.PostFormValue("password"),
		APRSCallsigns: strings.TrimSpace(r.PostFormValue("aprs_callsigns")), MeshtasticIDs: strings.TrimSpace(r.PostFormValue("meshtastic_ids")), GroupSet: groupSetFromForm(r.PostForm["groups"]),
		ChannelSet: channelSetFromForm(r.PostForm["channels"]),
		NotifLang:  strings.ToLower(strings.TrimSpace(r.PostFormValue("lang"))),
	}
	// The modal always submits the preference boxes; clients that omit
	// the marker (older flows, the basic save tests) leave membership
	// and channel opt-outs untouched.
	savePrefs := r.PostFormValue("prefs") != ""
	editID := int64(0)
	if raw := strings.TrimSpace(r.PostFormValue("edit_id")); raw != "" {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			http.Error(w, "invalid user id", http.StatusBadRequest)
			return
		}
		editID = id
	}

	if msg := validateUserForm(form, editID == 0); msg != "" {
		s.renderUsersError(w, r, http.StatusUnprocessableEntity, form, dialogEditID(editID), msg)
		return
	}

	if editID == 0 {
		u, err := s.users.CreateUser(form.Username, form.Phone, form.Email, form.Discord, form.Role, form.Password)
		if err != nil {
			s.renderUsersError(w, r, userErrorStatus(err), form, dialogEditID(editID), userErrorMessage(err))
			return
		}
		s.audit(sess.username, "user-create", form.Username)
		if err := s.users.SetUserAPRS(u.ID, parseAPRSCallsigns(form.APRSCallsigns)); err != nil {
			s.renderUsersError(w, r, userErrorStatus(err), form, dialogEditID(editID), userErrorMessage(err))
			return
		}
		if err := s.users.SetUserMeshtasticIDs(u.ID, parseMeshtasticIDs(form.MeshtasticIDs)); err != nil {
			s.renderUsersError(w, r, userErrorStatus(err), form, dialogEditID(editID), userErrorMessage(err))
			return
		}
		if err := s.users.SetUserLanguage(u.ID, form.NotifLang); err != nil {
			s.renderUsersError(w, r, userErrorStatus(err), form, dialogEditID(editID), userErrorMessage(err))
			return
		}
		if savePrefs {
			if msg := s.applyUserPrefs(w, r, u.ID, form, editID, sess.username); msg != "" {
				return
			}
		}
	} else {
		// Identity-relevant edits (password, role, username) revoke every
		// live session of the user: a demoted or re-passworded account
		// must not keep its old sessions authorized.
		old, oldErr := s.users.GetUser(editID)
		u, err := s.users.UpdateUser(editID, form.Username, form.Phone, form.Email, form.Discord, form.Role, form.Password)
		if err != nil {
			s.renderUsersError(w, r, userErrorStatus(err), form, dialogEditID(editID), userErrorMessage(err))
			return
		}
		s.audit(sess.username, "user-update", form.Username)
		// Identity-relevant edits (password, role, username) revoke every
		// live session of the user: a demoted or re-passworded account
		// must not keep its old sessions authorized. The admin row's
		// identity is config-owned and never changes here.
		if !old.IsAdmin && (form.Password != "" || oldErr != nil || old.Role != form.Role || old.Username != form.Username) {
			if n := s.sessions.revokeUser(editID); n > 0 {
				s.audit(sess.username, "sessions-revoked", fmt.Sprintf("user %s sessions=%d", form.Username, n))
			}
		}
		if err := s.users.SetUserAPRS(u.ID, parseAPRSCallsigns(form.APRSCallsigns)); err != nil {
			s.renderUsersError(w, r, userErrorStatus(err), form, dialogEditID(editID), userErrorMessage(err))
			return
		}
		if err := s.users.SetUserMeshtasticIDs(u.ID, parseMeshtasticIDs(form.MeshtasticIDs)); err != nil {
			s.renderUsersError(w, r, userErrorStatus(err), form, dialogEditID(editID), userErrorMessage(err))
			return
		}
		if err := s.users.SetUserLanguage(u.ID, form.NotifLang); err != nil {
			s.renderUsersError(w, r, userErrorStatus(err), form, dialogEditID(editID), userErrorMessage(err))
			return
		}
		if savePrefs {
			if msg := s.applyUserPrefs(w, r, u.ID, form, editID, sess.username); msg != "" {
				return
			}
		}
	}
	http.Redirect(w, r, "/users", http.StatusSeeOther)
}

// applyUserPrefs persists the modal's group-membership and delivery-channel
// boxes for one user. On failure it re-renders the dialog with the error
// and returns a non-empty message.
func (s *Server) applyUserPrefs(w http.ResponseWriter, r *http.Request, userID int64, form userForm, editID int64, actor string) string {
	groupIDs := make([]int64, 0, len(form.GroupSet))
	for id := range form.GroupSet {
		groupIDs = append(groupIDs, id)
	}
	if err := s.users.SetUserGroups(userID, groupIDs); err != nil {
		s.logger.Error("web: set user groups failed", "user", userID, "error", err)
		s.renderUsersError(w, r, http.StatusInternalServerError, form, dialogEditID(editID), "could not update group membership")
		return "groups"
	}
	var optOuts []string
	for _, c := range notify.Channels {
		if !form.ChannelSet[c.Kind] {
			optOuts = append(optOuts, c.Kind)
		}
	}
	if err := s.users.SetUserChannelOptOuts(userID, optOuts); err != nil {
		s.logger.Error("web: set user channels failed", "user", userID, "error", err)
		s.renderUsersError(w, r, http.StatusInternalServerError, form, dialogEditID(editID), "could not update delivery channels")
		return "channels"
	}
	s.audit(actor, "user-prefs", fmt.Sprintf("%d groups=%d channels=%d", userID, len(groupIDs), len(form.ChannelSet)))
	return ""
}

// groupSetFromForm builds the checked-group set from the form values.
func groupSetFromForm(values []string) map[int64]bool {
	set := make(map[int64]bool, len(values))
	for _, raw := range values {
		if id, err := strconv.ParseInt(raw, 10, 64); err == nil && id > 0 {
			set[id] = true
		}
	}
	return set
}

// channelSetFromForm builds the enabled-channel set from the form values
// (checked = enabled, like the dedicated prefs endpoint).
func channelSetFromForm(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, v := range values {
		if notify.Known(v) {
			set[v] = true
		}
	}
	return set
}

// userGroupSet loads the current group membership of one user.
func (s *Server) userGroupSet(userID int64) map[int64]bool {
	ids, err := s.users.GroupIDsForUser(userID)
	if err != nil {
		s.logger.Error("web: user groups failed", "user", userID, "error", err)
		ids = nil
	}
	set := make(map[int64]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set
}

// userChannelSet loads the enabled delivery channels of one user.
func (s *Server) userChannelSet(userID int64) map[string]bool {
	opts, err := s.users.UserChannelOptOuts(userID)
	if err != nil {
		s.logger.Error("web: user channel opt-outs failed", "user", userID, "error", err)
		opts = nil
	}
	set := make(map[string]bool, len(notify.Channels))
	for _, c := range notify.Channels {
		set[c.Kind] = !opts[c.Kind]
	}
	return set
}

// dialogEditID maps a create attempt (editID 0) onto the dialog-open
// sentinel (-1), so a failed add re-renders the page with the add dialog
// open and the submitted values echoed back. Positive IDs pass through.
func dialogEditID(editID int64) int64 {
	if editID == 0 {
		return -1
	}
	return editID
}

// handleUserDelete removes a regular user. The admin row is protected.
func (s *Server) handleUserDelete(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if csrfMismatch(r, sess) {
		s.redirectAfterCSRFMismatch(w, r, "/users")
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	name := ""
	if u, err := s.users.GetUser(id); err == nil {
		name = u.Username
	}
	if err := s.users.DeleteUser(id); err != nil {
		s.renderUsersError(w, r, userErrorStatus(err), userForm{}, 0, userErrorMessage(err))
		return
	}
	s.audit(sess.username, "user-delete", name)
	if n := s.sessions.revokeUser(id); n > 0 {
		s.audit(sess.username, "sessions-revoked", fmt.Sprintf("user %s sessions=%d", name, n))
	}
	http.Redirect(w, r, "/users", http.StatusSeeOther)
}

// handleUserPrefs replaces one user's notification preferences: group
// membership (checked boxes) and delivery-channel opt-outs (checked means
// enabled). Empty selection clears all groups / disables every channel.
func (s *Server) handleUserPrefs(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if csrfMismatch(r, sess) {
		s.redirectAfterCSRFMismatch(w, r, "/users")
		return
	}
	userID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	var groupIDs []int64
	for _, raw := range r.PostForm["groups"] {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || id < 1 {
			http.Error(w, "invalid group id", http.StatusBadRequest)
			return
		}
		groupIDs = append(groupIDs, id)
	}
	checked := make(map[string]bool)
	for _, v := range r.PostForm["channels"] {
		if notify.Known(v) {
			checked[v] = true
		}
	}
	var optOuts []string
	for _, c := range notify.Channels {
		if !checked[c.Kind] {
			optOuts = append(optOuts, c.Kind)
		}
	}
	if err := s.users.SetUserGroups(userID, groupIDs); err != nil {
		s.logger.Error("web: set user groups failed", "user", userID, "error", err)
		http.Error(w, "could not update group membership", http.StatusInternalServerError)
		return
	}
	if err := s.users.SetUserChannelOptOuts(userID, optOuts); err != nil {
		s.logger.Error("web: set user channels failed", "user", userID, "error", err)
		http.Error(w, "could not update delivery channels", http.StatusInternalServerError)
		return
	}
	s.audit(sess.username, "user-prefs", fmt.Sprintf("%d groups=%d channels=%d", userID, len(groupIDs), len(checked)))
	page := r.URL.Query().Get("page")
	if page == "" {
		page = "1"
	}
	http.Redirect(w, r, "/users?page="+page, http.StatusSeeOther)
}

// handleUserResetPassword replaces one user's password with a freshly
// generated one and shows it once (the admin hands it over out of band).
// The configured admin account is read-only.
func (s *Server) handleUserResetPassword(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if csrfMismatch(r, sess) {
		s.redirectAfterCSRFMismatch(w, r, "/users")
		return
	}
	userID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	u, err := s.users.GetUser(userID)
	if err != nil {
		s.renderUsersError(w, r, userErrorStatus(err), userForm{}, 0, userErrorMessage(err))
		return
	}
	if u.IsAdmin {
		s.renderUsersError(w, r, http.StatusForbidden, userForm{}, 0, "admin pass is defined in the configuration file")
		return
	}
	password, err := newRandomPassword()
	if err != nil {
		s.logger.Error("web: reset password generation failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if err := s.users.SetUserPassword(userID, password); err != nil {
		s.logger.Error("web: reset password failed", "user", userID, "error", err)
		http.Error(w, "could not reset the password", http.StatusInternalServerError)
		return
	}
	s.audit(sess.username, "user-reset", strconv.FormatInt(userID, 10))
	if n := s.sessions.revokeUser(userID); n > 0 {
		s.audit(sess.username, "sessions-revoked", fmt.Sprintf("user %s sessions=%d", u.Username, n))
	}

	view := s.buildAccessView(r, sess, "users")
	view.Users.Notice = fmt.Sprintf(i18n.T(s.langFor(r), "users.notice.password"), u.Username, password)
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "access", view)
}

// buildUsersView assembles the page model from the store.
func (s *Server) buildUsersView(r *http.Request, form userForm, editID int64, errMsg string) usersView {
	page := pageParam(r, "page")
	users, total, err := s.users.ListUsers(page, usersPerPage)
	if err != nil {
		s.logger.Error("web: list users failed", "error", err)
		users, total = nil, 0
	}
	pages := (total + usersPerPage - 1) / usersPerPage
	if pages < 1 {
		pages = 1
	}
	if page > pages {
		page = pages
	}
	from := 0
	to := 0
	if total > 0 {
		from = (page-1)*usersPerPage + 1
		to = page * usersPerPage
		if to > total {
			to = total
		}
	}
	rows := make([]userRow, 0, len(users))
	groups, gerr := s.users.ListAllGroups()
	if gerr != nil {
		s.logger.Error("web: list all groups failed", "error", gerr)
		groups = nil
	}
	for _, u := range users {
		ids, err := s.users.GroupIDsForUser(u.ID)
		if err != nil {
			s.logger.Error("web: user groups failed", "user", u.ID, "error", err)
			ids = nil
		}
		set := make(map[int64]bool, len(ids))
		for _, id := range ids {
			set[id] = true
		}
		names := make([]string, 0, len(ids))
		for _, g := range groups {
			if set[g.ID] {
				names = append(names, g.Name)
			}
		}
		rows = append(rows, userRow{
			ID:            u.ID,
			Username:      u.Username,
			Phone:         u.Phone,
			Email:         u.Email,
			Discord:       u.Discord,
			IsAdmin:       u.IsAdmin,
			Role:          u.Role,
			Lang:          u.Lang,
			GroupNames:    names,
			APRSCallsigns: u.APRSCallsigns,
			APRSJoin:      strings.Join(u.APRSCallsigns, " "), MeshtasticIDs: u.MeshtasticIDs, UpdatedAt: u.UpdatedAt,
		})
	}
	return usersView{
		AppTitle:   s.cfg.Title,
		Name:       s.displayName(),
		Header1:    s.displayHeader1(),
		Header2:    s.DisplayHeader2(),
		Tagline:    s.DisplayTagline(),
		Version:    s.version,
		Commit:     s.commit,
		RepoURL:    repoURL,
		Users:      rows,
		Groups:     groups,
		Channels:   notify.Channels,
		Languages:  i18n.Codes(),
		Form:       form,
		EditID:     editID,
		DialogOpen: editID != 0,
		Error:      errMsg,
		Page:       page,
		Pages:      pages,
		From:       from,
		To:         to,
		Total:      total,
		HasPrev:    page > 1,
		HasNext:    page < pages,
		NavUsers:   true,
	}
}

// renderUsersError re-renders the merged access page with the users tab
// open, an error banner and the submitted form values preserved.
func (s *Server) renderUsersError(w http.ResponseWriter, r *http.Request, status int, form userForm, editID int64, msg string) {
	sess := s.sessions.currentSession(r)
	view := s.buildAccessView(r, sess, "users")
	view.Users = s.buildUsersView(r, form, editID, msg)
	view.Users.CSRF = sess.csrf
	view.Users.Username = sess.username
	view.Users.Role = sess.role
	view.Users.Tab = "users"
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	s.renderL(w, r, "access", view)
}

// validateUserForm returns a human-readable problem or "". requirePassword
// applies to new users only: editing a user leaves the password untouched
// when the field is empty.
func validateUserForm(f userForm, requirePassword bool) string {
	if !usernamePattern.MatchString(f.Username) {
		return "username must be 1-64 lowercase letters, digits, dots, dashes or underscores"
	}
	if f.Role != "" && f.Role != "member" && f.Role != "emcom" {
		return "role must be empty, member or emcom"
	}
	if f.NotifLang != "" && !i18n.Supported(f.NotifLang) {
		return "invalid language"
	}
	if requirePassword && f.Role != "" && f.Password == "" {
		return "a password is required for users with a role (they sign in with it)"
	}
	if f.Password != "" && (len(f.Password) < 8 || len(f.Password) > 72) {
		return "password must be 8-72 characters"
	}
	if len(f.Phone) > 32 || strings.ContainsAny(f.Phone, "\r\n") {
		return "phone must be at most 32 characters"
	}
	if len(f.Email) > 128 || strings.ContainsAny(f.Email, " \r\n") {
		return "email must be at most 128 characters and contain no spaces"
	}
	if f.Email != "" && !strings.Contains(f.Email, "@") {
		return "email must contain '@'"
	}
	if len(f.Discord) > 128 || strings.ContainsAny(f.Discord, "\r\n") {
		return "discord must be at most 128 characters"
	}
	if callsigns, msg := validateAPRSCallsigns(f.APRSCallsigns); msg != "" {
		_ = callsigns
		return msg
	}
	if keys, msg := validateMeshtasticIDs(f.MeshtasticIDs); msg != "" {
		_ = keys
		return msg
	}
	return ""
}

// maxAPRSCallsignsPerUser bounds the registered callsign list per user.
const maxAPRSCallsignsPerUser = 8

// parseAPRSCallsigns normalizes the free-text list (space/comma
// separated) into uppercase de-duplicated callsigns.
func parseAPRSCallsigns(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ' ' || r == '\t' || r == ',' || r == ';' || r == '\n'
	})
	seen := make(map[string]bool, len(fields))
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		c := aprs.NormalizeCallsign(f)
		if c != "" && !seen[c] {
			seen[c] = true
			out = append(out, c)
		}
	}
	return out
}

// validateAPRSCallsigns returns a user-facing problem when the raw list
// contains too many or malformed callsigns.
func validateAPRSCallsigns(raw string) ([]string, string) {
	callsigns := parseAPRSCallsigns(raw)
	if len(callsigns) > maxAPRSCallsignsPerUser {
		return callsigns, fmt.Sprintf("at most %d APRS callsigns per user", maxAPRSCallsignsPerUser)
	}
	for _, c := range callsigns {
		if !aprs.ValidCallsign(c) {
			return callsigns, fmt.Sprintf("%q is not a valid APRS callsign (e.g. SP9XXX or SP9XXX-16)", c)
		}
	}
	return callsigns, ""
}

// maxMeshtasticIDsPerUser bounds the registered Meshtastic node id list per
// user.
const maxMeshtasticIDsPerUser = 4

// parseMeshtasticIDs normalizes the free-text id list (space/comma
// separated) into lowercase de-duplicated 8-hex node ids (optional ! or
// 0x prefix stripped).
func parseMeshtasticIDs(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ' ' || r == '\t' || r == ',' || r == ';' || r == '\n'
	})
	seen := make(map[string]bool, len(fields))
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		k := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(f), "0x"))
		k = strings.TrimPrefix(k, "!")
		if k != "" && !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

// validateMeshtasticIDs returns a user-facing problem when the raw list
// contains too many or malformed Meshtastic node ids (8 lowercase hex
// chars, the canonical Meshtastic node id without the '!' prefix).
func validateMeshtasticIDs(raw string) ([]string, string) {
	keys := parseMeshtasticIDs(raw)
	if len(keys) > maxMeshtasticIDsPerUser {
		return keys, fmt.Sprintf("at most %d Meshtastic node ids per user", maxMeshtasticIDsPerUser)
	}
	for _, k := range keys {
		if !isHex8(k) {
			return keys, fmt.Sprintf("%q is not a valid Meshtastic node id (8 hex characters)", k)
		}
	}
	return keys, ""
}

// isHex8 reports whether s is exactly 8 lowercase hex characters (the
// canonical Meshtastic node id form used across the UI and the wire
// protocol).
func isHex8(s string) bool {
	if len(s) != 8 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// userErrorStatus maps storage errors to HTTP statuses.
func userErrorStatus(err error) int {
	switch {
	case errors.Is(err, storage.ErrUserProtected):
		return http.StatusForbidden
	case errors.Is(err, storage.ErrUserNotFound):
		return http.StatusNotFound
	case errors.Is(err, storage.ErrUsernameTaken):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

// userErrorMessage maps storage errors to user-facing text.
func userErrorMessage(err error) string {
	switch {
	case errors.Is(err, storage.ErrUserProtected):
		return "the admin user is read-only and cannot be changed"
	case errors.Is(err, storage.ErrUserNotFound):
		return "user not found"
	case errors.Is(err, storage.ErrUsernameTaken):
		return "username already exists"
	default:
		return "user operation failed"
	}
}
