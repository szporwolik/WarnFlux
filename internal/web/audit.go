package web

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// auditPageSize bounds one poll of the audit feed.
const auditPageSize = 500

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
	NavLogs          bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
	NavCompose       bool
	NavAccount       bool
	NavAudit         bool
	MaxEntries       int
}

// handleAuditPage renders the self-refreshing user-action audit viewer.
func (s *Server) handleAuditPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	max := s.auditLog.Max()
	if _, ok := s.users.(storage.AuditStore); ok {
		max = storage.AuditRetentionEntries
	}
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
		MaxEntries: max,
	}
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "audit", v)
}

// handlePartialAudit serves the incremental audit feed:
// {"entries":[...]}. The cursor is ?after=<seq>; the initial request uses
// 0 (or omits the parameter) and receives the whole retained buffer. With
// a database-backed store the feed reads straight from the persisted log.
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

	type entryJSON = auditEntry
	var entries []entryJSON
	if st, ok := s.users.(storage.AuditStore); ok {
		dbEntries, err := st.ListAudit(after, auditPageSize)
		if err != nil {
			s.logger.Warn("web: list audit entries failed", "error", err)
			http.Error(w, "could not read the audit log", http.StatusInternalServerError)
			return
		}
		entries = make([]entryJSON, 0, len(dbEntries))
		for _, e := range dbEntries {
			entries = append(entries, entryJSON{
				Seq: e.Seq, At: e.At, User: e.User, Action: e.Action, Detail: e.Detail,
			})
		}
	} else {
		entries = s.auditLog.Snapshot(after)
		if entries == nil {
			entries = []entryJSON{}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
}

// audit records one user action: the bounded in-memory buffer always gets
// it, and a database-backed user store persists it (pruned to the
// retention bound by the store).
func (s *Server) audit(user, action, detail string) {
	if s.auditLog != nil {
		s.auditLog.Add(user, action, detail)
	}
	if st, ok := s.users.(storage.AuditStore); ok {
		if err := st.RecordAudit(user, action, detail, time.Now()); err != nil {
			s.logger.Warn("web: audit record failed", "error", err)
		}
	}
}
