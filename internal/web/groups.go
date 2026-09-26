package web

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/severity"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// groupsPerPage bounds the groups table to a compact, paginated view.
const groupsPerPage = 10

// groupRow is one groups-table row for the template. Assignments holds the
// saved routing cells (rendered as chips).
type groupRow struct {
	ID          int64
	Name        string
	Members     int64
	Assignments []storage.ChannelAssignment
	UpdatedAt   time.Time
}

// channelOption is one assignable action instance offered in the routing
// matrix.
type channelOption struct {
	ID   string
	Type string
}

// sourceOption is one input-plugin (hazard event source) row of the
// routing matrix. An empty Value is the "any source" fallback row.
type sourceOption struct {
	Value string
	Label string
}

// baseRoutingSources lists the built-in hazard event source slugs offered
// as matrix rows. "" is the any-source fallback. Sources of events
// received through the MQTT receiver are the upstream producers' slugs,
// so they match "any" (or their own row when it exists here). Labels are
// translated per UI language in routingSourcesFor.
var baseRoutingSources = []sourceOption{
	{Value: "", Label: "groups.routing.any"},
	{Value: "imgw-meteo", Label: "IMGW meteo"},
	{Value: "imgw-hydro", Label: "IMGW hydro"},
	{Value: "rso", Label: "RSO"},
	{Value: "aprs", Label: "groups.src.aprs"},
	{Value: "giosaq", Label: "groups.src.giosaq"},
	{Value: "compose", Label: "groups.src.compose"},
	// EMCOM readiness-level changes flow through the dispatch ingress
	// like every other source, so groups can route them explicitly.
	{Value: "emcom", Label: "groups.src.emcom"},
}

// routingSourcesFor returns the full matrix row set for a UI language:
// the built-in sources plus one row per configured public ingest
// endpoint (its id is the event source stamped on builder-mode alerts).
func (s *Server) routingSourcesFor(lang string) []sourceOption {
	ids := make([]string, 0, len(s.ingest))
	for id := range s.ingest {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]sourceOption, 0, len(baseRoutingSources)+len(ids))
	for _, src := range baseRoutingSources {
		out = append(out, sourceOption{Value: src.Value, Label: i18n.T(lang, src.Label)})
	}
	for _, id := range ids {
		out = append(out, sourceOption{Value: id, Label: trfSafe(i18n.T(lang, "groups.src.ingest"), id)})
	}
	return out
}

func trfSafe(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}

// routingSources returns the routing sources in English (legacy callers).
func (s *Server) routingSources() []sourceOption {
	return s.routingSourcesFor(i18n.LangEN)
}

// matrixCell is one severity select of the popover grid.
type matrixCell struct {
	Source string // "" = any source
	Action string // action instance ID
	Sev    string // current severity, "" = cell off
}

// matrixSourceRow is one source row of the popover grid.
type matrixSourceRow struct {
	Label string
	Cells []matrixCell
}

// cellKey identifies one popover cell.
func cellKey(source, actionID string) string {
	return source + "|" + actionID
}

// severityChoice is one routing threshold option.
type severityChoice struct {
	Value string
	Label string
}

// severityChoicesFor lists the canonical thresholds from permissive to
// strict in a UI language, as offered by the routing form.
func severityChoicesFor(lang string) []severityChoice {
	return []severityChoice{
		{Value: "unknown", Label: i18n.T(lang, "groups.routing.sev_unknown")},
		{Value: "minor", Label: i18n.T(lang, "groups.routing.sev_minor")},
		{Value: "moderate", Label: i18n.T(lang, "groups.routing.sev_moderate")},
		{Value: "severe", Label: i18n.T(lang, "groups.routing.sev_severe")},
		{Value: "extreme", Label: i18n.T(lang, "groups.routing.sev_extreme")},
	}
}

// groupForm carries the add/edit form values.
type groupForm struct {
	Name string
}

// groupsView is the full /groups page model.
type groupsView struct {
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

	Groups []groupRow
	Form   groupForm
	EditID int64
	Error  string

	// Routing options offered by the per-group routing form.
	Severities []severityChoice
	Actions    []channelOption

	// RoutingName + RoutingID + RoutingMatrix turn the page into the
	// dedicated routing editor of one group (GET /groups/{id}/routing).
	RoutingName   string
	RoutingID     int64
	RoutingMatrix []matrixSourceRow

	Page, Pages, From, To, Total int
	HasPrev, HasNext             bool

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavLogs          bool
	NavAudit         bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
	NavCompose       bool
	NavEmcom         bool
	NavAccount       bool
}

// handleGroupsPage renders the group administration page. ?edit=<id>
// prefills the top form for editing that group.
func (s *Server) handleGroupsPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := s.buildGroupsView(r, groupForm{}, 0, "")
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	if raw := r.URL.Query().Get("edit"); raw != "" {
		if id, err := strconv.ParseInt(raw, 10, 64); err == nil && id > 0 {
			if g, err := s.users.GetGroup(id); err == nil {
				view.EditID = g.ID
				view.Form = groupForm{Name: g.Name}
			}
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "groups", view)
}

// handleGroupSave creates or updates a group from the top form. A hidden
// edit_id turns the request into an update.
func (s *Server) handleGroupSave(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || !csrfOK(r.PostFormValue("csrf"), sess.csrf) {
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

	if msg := validateGroupForm(s.langFor(r), form); msg != "" {
		s.renderGroupsError(w, r, http.StatusUnprocessableEntity, form, editID, msg)
		return
	}

	if editID == 0 {
		if _, err := s.users.CreateGroup(form.Name); err != nil {
			s.renderGroupsError(w, r, groupErrorStatus(err), form, editID, groupErrorMessage(s.langFor(r), err))
			return
		}
		s.audit(sess.username, "group-create", form.Name)
	} else {
		if _, err := s.users.UpdateGroup(editID, form.Name); err != nil {
			s.renderGroupsError(w, r, groupErrorStatus(err), form, editID, groupErrorMessage(s.langFor(r), err))
			return
		}
		s.audit(sess.username, "group-update", form.Name)
	}
	http.Redirect(w, r, "/groups", http.StatusSeeOther)
}

// handleGroupDelete removes a group. Membership rows cascade away; users
// are never touched.
func (s *Server) handleGroupDelete(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || !csrfOK(r.PostFormValue("csrf"), sess.csrf) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid group id", http.StatusBadRequest)
		return
	}
	if err := s.users.DeleteGroup(id); err != nil {
		s.renderGroupsError(w, r, groupErrorStatus(err), groupForm{}, 0, groupErrorMessage(s.langFor(r), err))
		return
	}
	s.audit(sess.username, "group-delete", strconv.FormatInt(id, 10))
	http.Redirect(w, r, "/groups", http.StatusSeeOther)
}

// handleGroupRoutingPage renders the dedicated routing editor of one
// group: the full source × action matrix with one severity select per
// cell (the groups table links here instead of the old in-row popover).
func (s *Server) handleGroupRoutingPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		http.NotFound(w, r)
		return
	}
	group, err := s.users.GetGroup(id)
	if err != nil {
		if errors.Is(err, storage.ErrGroupNotFound) {
			http.NotFound(w, r)
			return
		}
		s.logger.Error("web: get group failed", "group", id, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	routing, err := s.users.GroupRouting(id)
	if err != nil {
		s.logger.Warn("web: group routing unavailable", "group", id, "error", err)
		routing.Actions = nil
	}
	view := s.buildGroupsView(r, groupForm{}, 0, "")
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	view.RoutingName = group.Name
	view.RoutingID = group.ID
	view.RoutingMatrix = buildMatrix(routing.Actions, s.availableActions(), s.routingSourcesFor(s.langFor(r)))
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "group_routing", view)
}

// handleGroupRouting saves one group's notification routing matrix: every
// posted cell carries its own source and minimum severity. Only source
// slugs and action IDs that exist in the current configuration are
// accepted, so stale form values can never land in the database.
func (s *Server) handleGroupRouting(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || !csrfOK(r.PostFormValue("csrf"), sess.csrf) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid group id", http.StatusBadRequest)
		return
	}

	actions, err := parseMatrixCells(r, s.routingSources(), s.availableActions())
	if err != nil {
		s.renderGroupsError(w, r, http.StatusUnprocessableEntity, groupForm{}, 0, err.Error())
		return
	}

	if err := s.users.SetGroupRouting(id, actions); err != nil {
		s.renderGroupsError(w, r, groupErrorStatus(err), groupForm{}, 0, groupErrorMessage(s.langFor(r), err))
		return
	}
	s.audit(sess.username, "group-routing", strconv.FormatInt(id, 10)+" cells="+strconv.Itoa(len(actions)))
	http.Redirect(w, r, "/groups", http.StatusSeeOther)
}

// parseMatrixCells reads the posted routing grid. Form field names are
// "cell:<source>|<action>" and values are canonical severities; an empty
// value means the cell is off and is skipped. Unknown sources, unknown
// action IDs or invalid severities are rejected.
func parseMatrixCells(r *http.Request, sources []sourceOption, allowed []channelOption) ([]storage.ChannelAssignment, error) {
	knownSrc := make(map[string]bool, len(sources))
	for _, o := range sources {
		knownSrc[o.Value] = true
	}
	knownAct := make(map[string]bool, len(allowed))
	for _, o := range allowed {
		knownAct[o.ID] = true
	}
	out := make([]storage.ChannelAssignment, 0, len(r.PostForm))
	for key, values := range r.PostForm {
		if !strings.HasPrefix(key, "cell:") {
			continue
		}
		src, id, ok := strings.Cut(strings.TrimPrefix(key, "cell:"), "|")
		if !ok {
			return nil, fmt.Errorf("malformed routing cell %q in routing form", key)
		}
		if !knownSrc[src] {
			return nil, fmt.Errorf("unknown source %q in routing form", src)
		}
		if !knownAct[id] {
			return nil, fmt.Errorf("unknown action or output id %q in routing form", id)
		}
		sev := strings.ToLower(strings.TrimSpace(values[len(values)-1]))
		if sev == "" {
			continue // cell left on "—": not assigned
		}
		if !severity.Valid(sev) {
			return nil, fmt.Errorf("invalid severity for cell %q", key)
		}
		out = append(out, storage.ChannelAssignment{Source: src, ID: id, MinSeverity: sev})
	}
	return out, nil
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

// buildMatrix lays out the popover grid: one row per known source, one
// severity select per action, prefilled with the saved severity ("" when
// the cell is unassigned).
func buildMatrix(assignments []storage.ChannelAssignment, actions []channelOption, sources []sourceOption) []matrixSourceRow {
	saved := make(map[string]string, len(assignments))
	for _, a := range assignments {
		saved[cellKey(a.Source, a.ID)] = a.MinSeverity
	}
	rows := make([]matrixSourceRow, 0, len(sources))
	for _, src := range sources {
		row := matrixSourceRow{Label: src.Label, Cells: make([]matrixCell, 0, len(actions))}
		for _, act := range actions {
			row.Cells = append(row.Cells, matrixCell{
				Source: src.Value,
				Action: act.ID,
				Sev:    saved[cellKey(src.Value, act.ID)],
			})
		}
		rows = append(rows, row)
	}
	return rows
}

// validateGroupForm returns a user-facing message for invalid input.
func validateGroupForm(lang string, form groupForm) string {
	if form.Name == "" {
		return i18n.T(lang, "groups.err.empty")
	}
	if len(form.Name) > 64 {
		return i18n.T(lang, "groups.err.long")
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
func groupErrorMessage(lang string, err error) string {
	switch {
	case errors.Is(err, storage.ErrGroupNameTaken):
		return i18n.T(lang, "groups.err.taken")
	case errors.Is(err, storage.ErrGroupNotFound):
		return i18n.T(lang, "groups.err.not_found")
	default:
		return i18n.T(lang, "groups.err.internal")
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
			ID:        g.ID,
			Name:      g.Name,
			Members:   g.Members,
			UpdatedAt: g.UpdatedAt,
		}
		if routing, err := s.users.GroupRouting(g.ID); err == nil {
			row.Assignments = routing.Actions
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
		Tagline:    s.cfg.Tagline,
		Version:    s.version,
		Commit:     s.commit,
		RepoURL:    repoURL,
		Groups:     rows,
		Form:       form,
		EditID:     editID,
		Error:      errMsg,
		Severities: severityChoicesFor(s.langFor(r)),
		Actions:    s.availableActions(),
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
	view.Role = sess.role
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	s.renderL(w, r, "groups", view)
}
