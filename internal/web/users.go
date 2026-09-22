package web

import (
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// usersPerPage bounds the users table to a compact, paginated view.
const usersPerPage = 10

// usernamePattern is the accepted username shape: lowercase slug, like the
// plugin instance IDs elsewhere.
var usernamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// userRow is one users-table row for the template.
type userRow struct {
	ID         int64
	Username   string
	Phone      string
	Email      string
	Discord    string
	IsAdmin    bool
	GroupNames []string
	GroupSet   map[int64]bool
	UpdatedAt  time.Time
}

// userForm carries the add/edit form values (also used to re-render the
// form after a validation error).
type userForm struct {
	Username string
	Phone    string
	Email    string
	Discord  string
}

// usersView is the full /users page model.
type usersView struct {
	AppTitle string
	Name     string
	Header1  string
	Header2  string
	Version  string
	Commit   string
	RepoURL  string
	CSRF     string
	Username string

	Users  []userRow
	Groups []storage.Group
	Form   userForm
	EditID int64
	Error  string

	Page, Pages, From, To, Total int
	HasPrev, HasNext             bool

	NavDashboard bool
	NavUsers     bool
	NavGroups    bool
	NavTest      bool
}

// handleUsersPage renders the user administration page. ?edit=<id>
// prefills the top form for editing that user.
func (s *Server) handleUsersPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := s.buildUsersView(r, userForm{}, 0, "")
	view.CSRF = sess.csrf
	view.Username = sess.username

	if raw := r.URL.Query().Get("edit"); raw != "" {
		if id, err := strconv.ParseInt(raw, 10, 64); err == nil && id > 0 {
			if u, err := s.users.GetUser(id); err == nil {
				view.EditID = u.ID
				view.Form = userForm{Username: u.Username, Phone: u.Phone, Email: u.Email, Discord: u.Discord}
			}
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "users", view)
}

// handleUserSave creates or updates a user from the top form. A hidden
// edit_id turns the request into an update.
func (s *Server) handleUserSave(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || r.PostFormValue("csrf") == "" || r.PostFormValue("csrf") != sess.csrf {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}

	form := userForm{
		Username: strings.TrimSpace(r.PostFormValue("username")),
		Phone:    strings.TrimSpace(r.PostFormValue("phone")),
		Email:    strings.TrimSpace(r.PostFormValue("email")),
		Discord:  strings.TrimSpace(r.PostFormValue("discord")),
	}
	editID := int64(0)
	if raw := strings.TrimSpace(r.PostFormValue("edit_id")); raw != "" {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			http.Error(w, "invalid user id", http.StatusBadRequest)
			return
		}
		editID = id
	}

	if msg := validateUserForm(form); msg != "" {
		s.renderUsersError(w, r, http.StatusUnprocessableEntity, form, editID, msg)
		return
	}

	if editID == 0 {
		if _, err := s.users.CreateUser(form.Username, form.Phone, form.Email, form.Discord); err != nil {
			s.renderUsersError(w, r, userErrorStatus(err), form, editID, userErrorMessage(err))
			return
		}
	} else {
		if _, err := s.users.UpdateUser(editID, form.Username, form.Phone, form.Email, form.Discord); err != nil {
			s.renderUsersError(w, r, userErrorStatus(err), form, editID, userErrorMessage(err))
			return
		}
	}
	http.Redirect(w, r, "/users", http.StatusSeeOther)
}

// handleUserDelete removes a regular user. The admin row is protected.
func (s *Server) handleUserDelete(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || r.PostFormValue("csrf") == "" || r.PostFormValue("csrf") != sess.csrf {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	if err := s.users.DeleteUser(id); err != nil {
		s.renderUsersError(w, r, userErrorStatus(err), userForm{}, 0, userErrorMessage(err))
		return
	}
	http.Redirect(w, r, "/users", http.StatusSeeOther)
}

// handleUserGroups replaces one user's group membership from the checkboxes
// on the users page. Empty selection clears all groups.
func (s *Server) handleUserGroups(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || r.PostFormValue("csrf") == "" || r.PostFormValue("csrf") != sess.csrf {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
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
	if err := s.users.SetUserGroups(userID, groupIDs); err != nil {
		s.logger.Error("web: set user groups failed", "user", userID, "error", err)
		http.Error(w, "could not update group membership", http.StatusInternalServerError)
		return
	}
	// Return to the same users page.
	page := r.URL.Query().Get("page")
	if page == "" {
		page = "1"
	}
	http.Redirect(w, r, "/users?page="+page, http.StatusSeeOther)
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
			ID:         u.ID,
			Username:   u.Username,
			Phone:      u.Phone,
			Email:      u.Email,
			Discord:    u.Discord,
			IsAdmin:    u.IsAdmin,
			GroupNames: names,
			GroupSet:   set,
			UpdatedAt:  u.UpdatedAt,
		})
	}
	return usersView{
		AppTitle: s.cfg.Title,
		Name:     s.displayName(),
		Header1:  s.displayHeader1(),
		Header2:  s.cfg.Header2,
		Version:  s.version,
		Commit:   s.commit,
		RepoURL:  repoURL,
		Users:    rows,
		Groups:   groups,
		Form:     form,
		EditID:   editID,
		Error:    errMsg,
		Page:     page,
		Pages:    pages,
		From:     from,
		To:       to,
		Total:    total,
		HasPrev:  page > 1,
		HasNext:  page < pages,
		NavUsers: true,
	}
}

// renderUsersError re-renders the page with an error banner, preserving
// the submitted form values.
func (s *Server) renderUsersError(w http.ResponseWriter, r *http.Request, status int, form userForm, editID int64, msg string) {
	sess := s.sessions.currentSession(r)
	view := s.buildUsersView(r, form, editID, msg)
	view.CSRF = sess.csrf
	view.Username = sess.username
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	s.render(w, "users", view)
}

// validateUserForm returns a human-readable problem or "".
func validateUserForm(f userForm) string {
	if !usernamePattern.MatchString(f.Username) {
		return "username must be 1-64 lowercase letters, digits, dots, dashes or underscores"
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
	return ""
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
