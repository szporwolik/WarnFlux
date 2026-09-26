package web

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
)

// receiverChoice is one option of the MQTT browser receiver select.
type receiverChoice struct {
	ID        string
	Label     string
	Connected bool
}

// trafficView is the full /traffic page model.
type trafficView struct {
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

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavLogs          bool
	NavAudit         bool
	NavTraffic       bool
	NavNotifications bool

	NavHealth  bool
	NavCompose bool
	NavEmcom   bool
	NavAccount bool
	MaxEntries int
	Receivers  []receiverChoice
}

// handleTrafficPage renders the self-refreshing MQTT traffic viewer.
func (s *Server) handleTrafficPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := s.baseTrafficView()
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "traffic", view)
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
	for _, st := range s.receivers.Statuses() {
		label := st.ID
		if st.Broker != "" {
			label += " · " + st.Broker
		}
		v.Receivers = append(v.Receivers, receiverChoice{
			ID:        st.ID,
			Label:     label,
			Connected: st.Connected,
		})
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

// handleMQTTBrowse runs a temporary MQTT subscription on one receiver and
// returns the collected messages (retained state included).
// GET /api/mqtt/browse?receiver=&topic=&window=
func (s *Server) handleMQTTBrowse(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	q := r.URL.Query()
	topic := strings.TrimSpace(q.Get("topic"))
	if topic == "" {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}
	window := mqttreceiver.BrowseDefaultWindow
	if raw := strings.TrimSpace(q.Get("window")); raw != "" {
		secs, err := strconv.ParseFloat(raw, 64)
		if err != nil || secs <= 0 || secs > mqttreceiver.BrowseMaxWindow.Seconds() {
			http.Error(w, "invalid window", http.StatusBadRequest)
			return
		}
		window = time.Duration(secs * float64(time.Second))
	}

	ctx, cancel := context.WithTimeout(r.Context(), window+3*time.Second)
	defer cancel()
	entries, err := s.receivers.Browse(ctx, q.Get("receiver"), topic, window, mqttreceiver.BrowseMaxEntries)
	s.audit(sess.username, "mqtt-browse", topic)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error(), "entries": []mqttreceiver.BrowseEntry{}})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
}
