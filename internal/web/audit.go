package web

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// auditView is the full /audit page model.
type auditView struct {
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
	NavTest          bool
	NavLogs          bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
	NavCompose       bool
	NavAudit         bool
	MaxEntries       int
}

// handleAuditPage renders the self-refreshing user-action audit viewer.
func (s *Server) handleAuditPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	v := auditView{
		AppTitle:   s.cfg.Title,
		Name:       s.displayName(),
		Header1:    s.displayHeader1(),
		Header2:    s.cfg.Header2,
		Tagline:    s.cfg.Tagline,
		Version:    s.version,
		Commit:     s.commit,
		RepoURL:    repoURL,
		CSRF:       sess.csrf,
		Username:   sess.username,
		Role:       sess.role,
		NavAudit:   true,
		MaxEntries: s.auditLog.Max(),
	}
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "audit", v)
}

// handlePartialAudit serves the incremental audit feed:
// {"entries":[...]}. The cursor is ?after=<seq>; the initial request uses
// 0 (or omits the parameter) and receives the whole retained buffer.
func (s *Server) handlePartialAudit(w http.ResponseWriter, r *http.Request) {
	var after int64
	if raw := strings.TrimSpace(r.URL.Query().Get("after")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			http.Error(w, "invalid after cursor", http.StatusBadRequest)
			return
		}
		after = n
	}
	entries := s.auditLog.Snapshot(after)
	if entries == nil {
		entries = []auditEntry{}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
}

// audit records one user action in the bounded audit buffer (no-op when
// the buffer is disabled in tests).
func (s *Server) audit(user, action, detail string) {
	if s.auditLog == nil {
		return
	}
	s.auditLog.Add(user, action, detail)
}
