package web

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// groupsPerPage bounds the groups table to a compact, paginated view.
const groupsPerPage = 10

// groupRow is one groups-table row for the template.
type groupRow struct {
	ID          int64
	Name        string
	Members     int64
	MinSeverity string
	Actions     []string
	Outputs     []string
	UpdatedAt   time.Time
}

// channelOption is one assignable action or output instance shown as a
// checkbox in the routing form.
type channelOption struct {
	ID   string
	Type string
}

// severityChoice is one routing threshold option.
type severityChoice struct {
	Value string
	Label string
}

// severityChoices lists the canonical thresholds from permissive to
// strict, as offered by the routing form.
var severityChoices = []severityChoice{
	{Value: "unknown", Label: "everything"},
	{Value: "minor", Label: "minor or higher"},
	{Value: "moderate", Label: "moderate or higher"},
	{Value: "severe", Label: "severe or higher"},
	{Value: "extreme", Label: "extreme only"},
}

// groupForm carries the add/edit form values.
type groupForm struct {
	Name string
}

// groupsView is the full /groups page model.
type groupsView struct {
	AppTitle string
	Name     string
	Header1  string
	Header2  string
	Version  string
	Commit   string
	RepoURL  string
	CSRF     string
	Username string

	Groups []groupRow
	Form   groupForm
	EditID int64
	Error  string

	// Routing options offered by the per-group routing form.
	Severities []severityChoice
	Actions    []channelOption
	Outputs    []channelOption

	Page, Pages, From, To, Total int
	HasPrev, HasNext             bool

	NavDashboard bool
	NavUsers     bool
	NavGroups    bool
}

// handleGroupsPage renders the group administration page. ?edit=<id>
// prefills the top form for editing that group.
func (s *Server) handleGroupsPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := s.buildGroupsView(r, groupForm{}, 0, "")
	view.CSRF = sess.csrf
	view.Username = sess.username

	if raw := r.URL.Query().Get("edit"); raw != "" {
		if id, err := strconv.ParseInt(raw, 10, 64); err == nil && id > 0 {
			if g, err := s.users.GetGroup(id); err == nil {
				view.EditID = g.ID
				view.Form = groupForm{Name: g.Name}
			}
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "groups", view)
}

// handleGroupSave creates or updates a group from the top form. A hidden
// edit_id turns the request into an update.
func (s *Server) handleGroupSave(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || r.PostFormValue("csrf") == "" || r.PostFormValue("csrf") != sess.csrf {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	form := groupForm{Name: strings.TrimSpace(r.PostFormValue("name"))}
	editID := int64(0)
	if raw := strings.TrimSpace(r.PostFormValue("edit_id")); raw != "" {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			http.Error(w, "invalid group id", http.StatusBadRequest)
			return
		}
		editID = id
	}

	if msg := validateGroupForm(form); msg != "" {
		s.renderGroupsError(w, r, http.StatusUnprocessableEntity, form, editID, msg)
		return
	}

	if editID == 0 {
		if _, err := s.users.CreateGroup(form.Name); err != nil {
			s.renderGroupsError(w, r, groupErrorStatus(err), form, editID, groupErrorMessage(err))
			return
		}
	} else {
		if _, err := s.users.UpdateGroup(editID, form.Name); err != nil {
			s.renderGroupsError(w, r, groupErrorStatus(err), form, editID, groupErrorMessage(err))
			return
		}
	}
	http.Redirect(w, r, "/groups", http.StatusSeeOther)
}

// handleGroupDelete removes a group. Membership rows cascade away; users
// are never touched.
func (s *Server) handleGroupDelete(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || r.PostFormValue("csrf") == "" || r.PostFormValue("csrf") != sess.csrf {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid group id", http.StatusBadRequest)
		return
	}
	if err := s.users.DeleteGroup(id); err != nil {
		s.renderGroupsError(w, r, groupErrorStatus(err), groupForm{}, 0, groupErrorMessage(err))
		return
	}
	http.Redirect(w, r, "/groups", http.StatusSeeOther)
}

// handleGroupRouting saves one group's notification routing: severity
// threshold plus assigned action/output instance IDs. Only IDs that exist
// in the current configuration are accepted, so stale form values can
// never land in the database.
func (s *Server) handleGroupRouting(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || r.PostFormValue("csrf") == "" || r.PostFormValue("csrf") != sess.csrf {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid group id", http.StatusBadRequest)
		return
	}

	minSeverity := strings.ToLower(strings.TrimSpace(r.PostFormValue("min_severity")))
	if minSeverity == "" {
		minSeverity = "unknown"
	}
	if !storage.ValidSeverity(minSeverity) {
		s.renderGroupsError(w, r, http.StatusUnprocessableEntity, groupForm{}, 0, "invalid minimum severity")
		return
	}

	actions, okActions := s.filterChannelIDs(r.PostForm["actions"], s.availableActions())
	outputs, okOutputs := s.filterChannelIDs(r.PostForm["outputs"], s.availableOutputs())
	if !okActions || !okOutputs {
		s.renderGroupsError(w, r, http.StatusUnprocessableEntity, groupForm{}, 0, "unknown action or output id in routing form")
		return
	}

	if err := s.users.SetGroupRouting(id, minSeverity, actions, outputs); err != nil {
		s.renderGroupsError(w, r, groupErrorStatus(err), groupForm{}, 0, groupErrorMessage(err))
		return
	}
	http.Redirect(w, r, "/groups", http.StatusSeeOther)
}

// filterChannelIDs keeps only posted IDs that exist in the allowed set and
// reports whether any posted ID was unknown.
func (s *Server) filterChannelIDs(posted []string, allowed []channelOption) ([]string, bool) {
	known := make(map[string]bool, len(allowed))
	for _, o := range allowed {
		known[o.ID] = true
	}
	seen := make(map[string]bool, len(posted))
	out := make([]string, 0, len(posted))
	ok := true
	for _, id := range posted {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if !known[id] {
			ok = false
			continue
		}
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out, ok
}

// availableActions lists the enabled configured action instances.
func (s *Server) availableActions() []channelOption {
	var out []channelOption
	for _, st := range s.actions.Statuses() {
		if !st.Enabled {
			continue
		}
		out = append(out, channelOption{ID: st.ID, Type: st.Type})
	}
	return out
}

// availableOutputs lists the enabled configured output plugin instances.
func (s *Server) availableOutputs() []channelOption {
	var out []channelOption
	for _, st := range s.router.Statuses() {
		if st.Kind != plugin.KindOutput || st.State == plugin.StateDisabled {
			continue
		}
		out = append(out, channelOption{ID: st.ID, Type: st.Type})
	}
	return out
}

// validateGroupForm returns a user-facing message for invalid input.
func validateGroupForm(form groupForm) string {
	if form.Name == "" {
		return "group name must not be empty"
	}
	if len(form.Name) > 64 {
		return "group name is too long (maximum 64 characters)"
	}
	return ""
}

// groupErrorStatus maps storage errors to HTTP status codes.
func groupErrorStatus(err error) int {
	switch {
	case errors.Is(err, storage.ErrGroupNameTaken):
		return http.StatusConflict
	case errors.Is(err, storage.ErrGroupNotFound):
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}

// groupErrorMessage maps storage errors to user-facing messages.
func groupErrorMessage(err error) string {
	switch {
	case errors.Is(err, storage.ErrGroupNameTaken):
		return "a group with this name already exists"
	case errors.Is(err, storage.ErrGroupNotFound):
		return "group not found"
	default:
		return "internal error"
	}
}

// buildGroupsView assembles the page model from the store.
func (s *Server) buildGroupsView(r *http.Request, form groupForm, editID int64, errMsg string) groupsView {
	page := pageParam(r, "page")
	groups, total, err := s.users.ListGroups(page, groupsPerPage)
	if err != nil {
		s.logger.Error("web: list groups failed", "error", err)
		groups, total = nil, 0
	}
	pages := (total + groupsPerPage - 1) / groupsPerPage
	if pages < 1 {
		pages = 1
	}
	if page > pages {
		page = pages
	}
	from := 0
	to := 0
	if total > 0 {
		from = (page-1)*groupsPerPage + 1
		to = page * groupsPerPage
		if to > total {
			to = total
		}
	}
	rows := make([]groupRow, 0, len(groups))
	for _, g := range groups {
		row := groupRow{
			ID:          g.ID,
			Name:        g.Name,
			Members:     g.Members,
			MinSeverity: g.MinSeverity,
			UpdatedAt:   g.UpdatedAt,
		}
		if routing, err := s.users.GroupRouting(g.ID); err == nil {
			row.MinSeverity = routing.MinSeverity
			row.Actions = routing.Actions
			row.Outputs = routing.Outputs
		} else {
			s.logger.Warn("web: group routing unavailable", "group", g.ID, "error", err)
		}
		rows = append(rows, row)
	}
	return groupsView{
		AppTitle:   s.cfg.Title,
		Name:       s.displayName(),
		Header1:    s.displayHeader1(),
		Header2:    s.cfg.Header2,
		Version:    s.version,
		Commit:     s.commit,
		RepoURL:    repoURL,
		Groups:     rows,
		Form:       form,
		EditID:     editID,
		Error:      errMsg,
		Severities: severityChoices,
		Actions:    s.availableActions(),
		Outputs:    s.availableOutputs(),
		Page:       page,
		Pages:      pages,
		From:       from,
		To:         to,
		Total:      total,
		HasPrev:    page > 1,
		HasNext:    page < pages,
		NavGroups:  true,
	}
}

// renderGroupsError re-renders the page with an error banner, preserving
// the submitted form values.
func (s *Server) renderGroupsError(w http.ResponseWriter, r *http.Request, status int, form groupForm, editID int64, msg string) {
	sess := s.sessions.currentSession(r)
	view := s.buildGroupsView(r, form, editID, msg)
	view.CSRF = sess.csrf
	view.Username = sess.username
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	s.render(w, "groups", view)
}
