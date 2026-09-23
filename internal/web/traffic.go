package web

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
)

// trafficView is the full /traffic page model.
type trafficView struct {
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

	NavHealth  bool
	NavCompose bool
	MaxEntries int
}

// handleTrafficPage renders the self-refreshing MQTT traffic viewer.
func (s *Server) handleTrafficPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := s.baseTrafficView()
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "traffic", view)
}

func (s *Server) baseTrafficView() trafficView {
	v := trafficView{
		AppTitle:   s.cfg.Title,
		Name:       s.displayName(),
		Header1:    s.displayHeader1(),
		Header2:    s.cfg.Header2,
		Tagline:    s.cfg.Tagline,
		Version:    s.version,
		Commit:     s.commit,
		RepoURL:    repoURL,
		NavTraffic: true,
	}
	if s.traffic != nil {
		v.MaxEntries = s.traffic.Max()
	}
	return v
}

// handlePartialTraffic serves the incremental traffic feed:
// {"entries":[...]}. The cursor is ?after=<seq>; the initial request uses
// 0 (or omits the parameter) and receives the whole retained buffer.
func (s *Server) handlePartialTraffic(w http.ResponseWriter, r *http.Request) {
	var after int64
	if raw := strings.TrimSpace(r.URL.Query().Get("after")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			http.Error(w, "invalid after cursor", http.StatusBadRequest)
			return
		}
		after = n
	}
	entries := s.traffic.Snapshot(after)
	if entries == nil {
		entries = []mqttreceiver.TrafficEntry{}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
}
