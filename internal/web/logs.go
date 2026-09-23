package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
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

// logsView is the full /logs page model.
type logsView struct {
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

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavTest          bool
	NavLogs          bool
	NavTraffic       bool
	NavNotifications bool
}

// handleLogsPage renders the self-refreshing log viewer.
func (s *Server) handleLogsPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := s.baseLogsView()
	view.CSRF = sess.csrf
	view.Username = sess.username
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "logs", view)
}

func (s *Server) baseLogsView() logsView {
	return logsView{
		AppTitle: s.cfg.Title,
		Name:     s.displayName(),
		Header1:  s.displayHeader1(),
		Header2:  s.cfg.Header2,
		Tagline:  s.cfg.Tagline,
		Version:  s.version,
		Commit:   s.commit,
		RepoURL:  repoURL,
		NavLogs:  true,
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
