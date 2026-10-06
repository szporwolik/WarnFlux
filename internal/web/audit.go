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

// handleAuditPage keeps the old /audit URL working: the audit trail is
// the second tab of the merged /logs page now.
func (s *Server) handleAuditPage(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/logs?tab=audit", http.StatusSeeOther)
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
