package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// DefaultLogLines bounds the in-memory log viewer buffer.
const DefaultLogLines = 1000

// logLine is one buffered log line delivered to the viewer.
type logLine struct {
	Seq   int64  `json:"seq"`
	Level string `json:"level"`
	Text  string `json:"text"`
}

// LogBuffer is a bounded ring buffer of the application's own log output.
// It is plugged into the slog handler writer, so everything the process
// logs (plugins, receivers, actions, paho adapter) lands here without any
// file access. The web UI polls snapshots of it.
type LogBuffer struct {
	mu      sync.Mutex
	max     int
	nextSeq int64
	partial []byte
	lines   []logLine
}

// NewLogBuffer builds a buffer retaining at most max lines (at least 1).
func NewLogBuffer(max int) *LogBuffer {
	if max < 1 {
		max = DefaultLogLines
	}
	return &LogBuffer{max: max}
}

var logLevelRE = regexp.MustCompile(`\blevel=(DEBUG|INFO|WARN|ERROR)\b`)

// Write implements io.Writer: it appends bytes, splitting on newlines and
// keeping an unterminated tail for the next write. It always accepts the
// full write (a log buffer must never fail the logger).
func (b *LogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.partial = append(b.partial, p...)
	for {
		if i := bytes.IndexByte(b.partial, '\n'); i >= 0 {
			line := strings.TrimSuffix(string(b.partial[:i]), "\r")
			b.appendLine(line)
			b.partial = b.partial[i+1:]
			continue
		}
		break
	}
	return len(p), nil
}

func (b *LogBuffer) appendLine(text string) {
	b.nextSeq++
	level := ""
	if m := logLevelRE.FindStringSubmatch(text); m != nil {
		level = strings.ToLower(m[1])
	}
	b.lines = append(b.lines, logLine{Seq: b.nextSeq, Level: level, Text: text})
	if len(b.lines) > b.max {
		b.lines = b.lines[len(b.lines)-b.max:]
	}
}

// Snapshot returns every buffered line with Seq > after (0 = everything).
// The caller uses the highest Seq as the next "after" cursor.
func (b *LogBuffer) Snapshot(after int64) []logLine {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]logLine, 0, len(b.lines))
	for _, l := range b.lines {
		if l.Seq > after {
			out = append(out, l)
		}
	}
	return out
}

// logsView is the full /logs page model: one page, four tabs — the app
// log tail, the user-action audit trail, the notification history and
// the MQTT traffic viewer.
type logsView struct {
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

	// Tab selects the active panel: "applog" (default), "audit", "notif"
	// or "traffic".
	Tab string
	// FocusKey highlights one notification trail (?key=… deep links).
	FocusKey string
	// Trails is the server-rendered notification history.
	Trails []trailView
	// MaxEntries bounds the audit retention hint.
	MaxEntries int
	// Traffic carries the MQTT traffic panel model.
	Traffic trafficView

	NavDashboard  bool
	NavUsers      bool
	NavGroups     bool
	NavLogs       bool
	NavAPRS       bool
	NavMessages   bool
	NavGSM        bool
	NavMeshtastic bool
	NavMeshMap    bool
	NavTraffic    bool
	NavWebsite    bool
	NavConfig     bool
	NavCompose    bool
	NavEmcom      bool
	NavAccount    bool
}

// handleLogsPage renders the merged logs page with its four tabs.
func (s *Server) handleLogsPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	lang := s.langFor(r)
	tab := r.URL.Query().Get("tab")
	switch tab {
	case "audit", "notif", "traffic":
	default:
		tab = "applog"
	}
	focus := r.URL.Query().Get("key")

	max := s.auditLog.Max()
	if _, ok := s.users.(storage.AuditStore); ok {
		max = storage.AuditRetentionEntries
	}

	view := s.baseLogsView()
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	view.Tab = tab
	view.FocusKey = focus
	view.MaxEntries = max
	view.Trails = wrapTrails(lang, focus, s.recentTrails(notificationsPerPage), s.recentDeliveries(deliveriesPerPage))
	view.Traffic = s.baseTrafficView()
	view.Traffic.Tab = tab
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "logs", view)
}

func (s *Server) baseLogsView() logsView {
	return logsView{
		AppTitle: s.cfg.Title,
		Name:     s.displayName(),
		Header1:  s.displayHeader1(),
		Header2:  s.DisplayHeader2(),
		Tagline:  s.DisplayTagline(),
		Version:  s.version,
		Commit:   s.commit,
		RepoURL:  repoURL,
		NavLogs:  true,
		// The logs surface lives under the Config menu entry; the sidebar
		// highlights Config while a log tab is open.
		NavConfig: true,
	}
}

// handlePartialLogs serves the incremental log feed: {"lines":[...]}. The
// cursor is ?after=<seq>; the initial request uses 0 (or omits the
// parameter) and receives the whole retained buffer.
func (s *Server) handlePartialLogs(w http.ResponseWriter, r *http.Request) {
	var after int64
	if raw := strings.TrimSpace(r.URL.Query().Get("after")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			http.Error(w, "invalid after cursor", http.StatusBadRequest)
			return
		}
		after = n
	}
	var lines []logLine
	if s.logs != nil {
		lines = s.logs.Snapshot(after)
	} else {
		lines = []logLine{}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{"lines": lines})
}
