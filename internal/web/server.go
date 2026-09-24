// Package web serves the authenticated admin UI, partial dashboard
// fragments, static assets and health/readiness endpoints.
//
// The UI is fully server-rendered (html/template) with a tiny embedded
// JavaScript poller refreshing the dashboard sections every 5 seconds.
// Everything — templates, CSS, JS — is embedded via go:embed; the UI works
// on a LAN without Internet access.
package web

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/appinfo"
	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/metrics"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/trail"
)

//go:embed templates/*.html
var templatesFS embed.FS

//go:embed static
var staticFS embed.FS

// RouterStatuses is the minimal Router plugin status surface the web UI
// needs (the real plugin.Manager satisfies it).
type RouterStatuses interface {
	Statuses() []plugin.PluginStatus
}

// Server is the HTTP layer of the merged application.
type Server struct {
	cfg       config.Web
	st        *state.State
	receivers *mqttreceiver.Manager
	pub       composePublisher
	router    RouterStatuses
	actions   *action.Manager
	aprs      *aprs.Hub
	ingress   *dispatch.Ingress
	users     storage.DirectoryStore
	logger    *slog.Logger
	sessions  *sessionStore
	logs      *LogBuffer
	traffic   *mqttreceiver.TrafficBuffer
	auditLog  *AuditBuffer
	trails    *trail.Recorder
	metrics   *metrics.Registry

	// ingest maps each configured public ingest endpoint id to its
	// API-key-protected handler (may be empty).
	ingest map[string]http.Handler

	version string
	commit  string

	startedAt time.Time
	ready     atomic.Bool

	tmpl     *template.Template
	mux      *http.ServeMux
	httpSrv  *http.Server
	listener net.Listener
}

// maxPasswordFileBytes bounds the admin password file read.
const maxPasswordFileBytes = 64 * 1024

// repoURL is linked from the bottom bar of both the login page and the
// dashboard; the shared constant lives in internal/appinfo.
const repoURL = appinfo.RepoURL

// New builds the web server (no listener created yet). ingest maps public
// ingest endpoint ids to their handlers; empty ids are ignored. logs is
// the optional in-memory log ring buffer served by the /logs viewer;
// traffic is the optional MQTT traffic ring buffer served by /traffic;
// trails is the optional notification audit recorder served by
// /notifications; metricsReg is the optional Prometheus registry served
// by /metrics.
func New(cfg config.Web, st *state.State, receivers *mqttreceiver.Manager,
	pub composePublisher, router RouterStatuses, actions *action.Manager,
	aprsHub *aprs.Hub, ingress *dispatch.Ingress, logger *slog.Logger, version, commit string,
	users storage.DirectoryStore, ingest map[string]http.Handler,
	logs *LogBuffer, traffic *mqttreceiver.TrafficBuffer,
	trails *trail.Recorder, metricsReg *metrics.Registry) (*Server, error) {

	// Read the admin password file at construction: a missing secret is a
	// startup error, never a runtime surprise. Secrets are never logged.
	if cfg.Auth.PasswordFile != "" {
		if info, err := os.Stat(cfg.Auth.PasswordFile); err != nil {
			return nil, fmt.Errorf("web: stat auth.password_file: %w", err)
		} else if info.Size() > maxPasswordFileBytes {
			return nil, fmt.Errorf("web: auth.password_file is %d bytes, maximum %d", info.Size(), maxPasswordFileBytes)
		}
		data, err := os.ReadFile(cfg.Auth.PasswordFile)
		if err != nil {
			return nil, fmt.Errorf("web: read auth.password_file: %w", err)
		}
		cfg.Auth.Password = strings.TrimRight(string(data), "\r\n")
	}

	tmpl, err := template.New("root").Funcs(templateFuncs()).ParseFS(templatesFS, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("web: parse templates: %w", err)
	}

	s := &Server{
		cfg:       cfg,
		st:        st,
		receivers: receivers,
		pub:       pub,
		router:    router,
		actions:   actions,
		aprs:      aprsHub,
		ingress:   ingress,
		users:     users,
		logger:    logger,
		sessions:  newSessionStore(cfg.Auth.SecureCookie),
		logs:      logs,
		traffic:   traffic,
		auditLog:  NewAuditBuffer(DefaultAuditEntries),
		trails:    trails,
		metrics:   metricsReg,
		version:   version,
		commit:    commit,
		startedAt: time.Now(),
		tmpl:      tmpl,
		mux:       http.NewServeMux(),
		ingest:    ingest,
	}

	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		return nil, fmt.Errorf("web: static assets: %w", err)
	}
	s.routes(http.FileServerFS(static))
	s.httpSrv = &http.Server{
		Handler:           s.mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s, nil
}

// noCacheStatic forces browsers to revalidate every embedded asset on
// every request. The embedded files carry no ETag/Last-Modified, so
// plain caching can serve stale app.js/style.css indefinitely; this
// guarantees a rebuild is picked up on the next page load.
func noCacheStatic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) routes(static http.Handler) {
	s.mux.Handle("GET /static/", http.StripPrefix("/static/", noCacheStatic(static)))
	s.mux.HandleFunc("GET /login", s.handleLoginPage)
	s.mux.HandleFunc("POST /login", s.handleLoginSubmit)
	s.mux.HandleFunc("POST /logout", s.handleLogout)
	// Public landing page: header1/header2 + the current active hazards.
	// The login form lives behind the top-right icon button (/login).
	s.mux.HandleFunc("GET /{$}", s.handleHome)
	s.mux.HandleFunc("GET /partials/home", s.handlePartialHome)
	s.mux.HandleFunc("GET /api/aprs/stations", s.handleAPRSStations)
	s.mux.HandleFunc("GET /api/weather", s.handleWeather)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.Handle("GET /logs", s.requireAdmin(s.handleLogsPage))
	s.mux.Handle("GET /partials/logs", s.requireAdminPartial(s.handlePartialLogs))
	s.mux.Handle("GET /audit", s.requireAdmin(s.handleAuditPage))
	s.mux.Handle("GET /partials/audit", s.requireAdminPartial(s.handlePartialAudit))
	s.mux.Handle("GET /traffic", s.requireAdmin(s.handleTrafficPage))
	s.mux.Handle("GET /partials/traffic", s.requireAdminPartial(s.handlePartialTraffic))
	s.mux.Handle("GET /api/mqtt/browse", s.requireAdmin(s.handleMQTTBrowse))
	s.mux.Handle("GET /notifications", s.requireAdmin(s.handleNotificationsPage))
	s.mux.Handle("GET /partials/notifications", s.requireAdminPartial(s.handlePartialNotifications))
	s.mux.Handle("GET /health", s.requireAdmin(s.handleHealthPage))
	s.mux.Handle("GET /partials/health", s.requireAdminPartial(s.handlePartialHealth))
	// Metrics: unauthenticated on purpose (Prometheus cannot log in);
	// only counters are exposed.
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	// Public ingest endpoints: authenticated per instance with the
	// configured API key, never with a UI session.
	if len(s.ingest) > 0 {
		s.mux.HandleFunc("POST /api/v1/ingest/{id}", s.handleIngest)
	}
	s.mux.Handle("GET /dashboard", s.requireAdmin(s.handleDashboard))
	s.mux.Handle("GET /test", s.requireAdmin(s.handleTestPage))
	s.mux.Handle("POST /test", s.requireAdmin(s.handleTestEmit))
	// Compose: any authenticated role may issue/update/expire
	// communications; it is the emcom operator's only surface.
	s.mux.Handle("GET /compose", s.requirePage(s.handleComposePage))
	s.mux.Handle("POST /compose", s.requirePage(s.handleComposeSave))
	s.mux.Handle("POST /compose/expire", s.requirePage(s.handleComposeExpire))
	s.mux.Handle("GET /users", s.requireAdmin(s.handleUsersPage))
	s.mux.Handle("POST /users", s.requireAdmin(s.handleUserSave))
	s.mux.Handle("POST /users/{id}/delete", s.requireAdmin(s.handleUserDelete))
	s.mux.Handle("POST /users/{id}/groups", s.requireAdmin(s.handleUserGroups))
	s.mux.Handle("GET /groups", s.requireAdmin(s.handleGroupsPage))
	s.mux.Handle("POST /groups", s.requireAdmin(s.handleGroupSave))
	s.mux.Handle("POST /groups/{id}/delete", s.requireAdmin(s.handleGroupDelete))
	s.mux.Handle("GET /groups/{id}/routing", s.requireAdmin(s.handleGroupRoutingPage))
	s.mux.Handle("POST /groups/{id}/routing", s.requireAdmin(s.handleGroupRouting))
	s.mux.Handle("GET /partials/status", s.requireAdminPartial(s.handlePartialStatus))
	s.mux.Handle("GET /partials/mqtt", s.requireAdminPartial(s.handlePartialMQTT))
	s.mux.Handle("GET /partials/weather", s.requireAdminPartial(s.handlePartialWeather))
	s.mux.Handle("GET /partials/warnings", s.requireAdminPartial(s.handlePartialWarnings))
	s.mux.Handle("GET /partials/plugins", s.requireAdminPartial(s.handlePartialPlugins))
	s.mux.Handle("GET /partials/actions", s.requireAdminPartial(s.handlePartialActions))
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
}

// MarkReady flips readiness (database opened, HTTP initialized).
func (s *Server) MarkReady() { s.ready.Store(true) }

// Handler returns the root HTTP handler (used by tests).
func (s *Server) Handler() http.Handler { return s.mux }

// Bind creates the listening socket. It fails startup when the address is
// already in use — the application must not discover that only after
// everything else is running.
func (s *Server) Bind() error {
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("web: listen on %s: %w", s.cfg.Listen, err)
	}
	s.listener = ln
	return nil
}

// Serve starts serving on the bound listener. It returns a channel
// receiving the serve error.
func (s *Server) Serve(errCh chan<- error) {
	s.logger.Info("http: listening", "addr", s.cfg.Listen)
	errCh <- s.httpSrv.Serve(s.listener)
}

// Shutdown gracefully stops the HTTP server within a bounded context.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.listener == nil {
		return nil
	}
	return s.httpSrv.Shutdown(ctx)
}

// requirePage protects browser routes: unauthenticated requests are
// redirected to the login page.
func (s *Server) requirePage(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.sessions.currentSession(r) == nil {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		next(w, r)
	})
}

// requireAdmin protects admin-tier routes: unauthenticated requests go to
// the login page, non-admin sessions (emcom) are sent to their own
// landing page (/compose) instead.
func (s *Server) requireAdmin(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess := s.sessions.currentSession(r)
		if sess == nil {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		if sess.role != "admin" {
			http.Redirect(w, r, "/compose", http.StatusSeeOther)
			return
		}
		next(w, r)
	})
}

// requirePartial protects fragment routes: unauthenticated requests get 401
// so the embedded poller can redirect to the login page.
func (s *Server) requirePartial(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.sessions.currentSession(r) == nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	})
}

// requireAdminPartial is the fragment-route variant of requireAdmin:
// unauthenticated and non-admin sessions both get 401 so the embedded
// poller redirects the browser instead of receiving foreign HTML.
func (s *Server) requireAdminPartial(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess := s.sessions.currentSession(r)
		if sess == nil || sess.role != "admin" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	})
}

// handleIngest dispatches a public ingest request to the configured
// endpoint instance (API key auth happens inside the instance handler).
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	h, ok := s.ingest[r.PathValue("id")]
	if !ok {
		http.NotFound(w, r)
		return
	}
	h.ServeHTTP(w, r)
}

func (s *Server) render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.ExecuteTemplate(w, name, data); err != nil {
		s.logger.Error("web: template render failed", "template", name, "error", err)
	}
}

// templateFuncs provides the small set of formatting helpers used by the UI.
func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"timeFull": func(t time.Time) string {
			if t.IsZero() {
				return "—"
			}
			return t.Local().Format("2006-01-02 15:04:05")
		},
		"derefTime": func(t *time.Time) time.Time {
			if t == nil {
				return time.Time{}
			}
			return *t
		},
		"initials": func(s string) string {
			s = strings.TrimSpace(s)
			if s == "" {
				return "?"
			}
			return strings.ToUpper(string([]rune(s)[0]))
		},
		"timeShort": func(t time.Time) string {
			if t.IsZero() {
				return "—"
			}
			return t.Local().Format("15:04:05")
		},
		// Trail steps carry RFC3339 timestamps as strings (JSON shape);
		// these two helpers render them for the notifications page.
		"timeHMS": func(s string) string {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return s
			}
			return t.Local().Format("15:04:05")
		},
		"timeFullStr": func(s string) string {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return s
			}
			return t.Local().Format("2006-01-02 15:04:05")
		},
		"dur": func(d time.Duration) string {
			if d < 0 {
				d = 0
			}
			day := 24 * time.Hour
			switch {
			case d < time.Second:
				return fmt.Sprintf("%dms", d.Milliseconds())
			case d < time.Minute:
				return fmt.Sprintf("%ds", int(d.Seconds()))
			case d < time.Hour:
				return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
			case d < day:
				return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
			default:
				return fmt.Sprintf("%dd%dh", int(d.Hours())/24, int(d.Hours())%24)
			}
		},
		"age": func(t time.Time) string {
			if t.IsZero() {
				return "—"
			}
			d := time.Since(t)
			if d < 0 {
				d = 0
			}
			switch {
			case d < time.Second:
				return "just now"
			case d < time.Minute:
				return fmt.Sprintf("%ds ago", int(d.Seconds()))
			case d < time.Hour:
				return fmt.Sprintf("%dm ago", int(d.Minutes()))
			case d < 24*time.Hour:
				return fmt.Sprintf("%dh ago", int(d.Hours()))
			default:
				return fmt.Sprintf("%dd ago", int(d.Hours()/24))
			}
		},
		"sevClass": func(severity string) string {
			switch strings.ToLower(severity) {
			case "extreme", "severe", "moderate", "minor", "unknown":
				return strings.ToLower(severity)
			default:
				return "unknown"
			}
		},
		"contains": func(list []string, s string) bool {
			for _, v := range list {
				if v == s {
					return true
				}
			}
			return false
		},
		"pluginClass": func(st plugin.PluginState) string {
			switch st {
			case plugin.StateRunning:
				return "healthy"
			case plugin.StateDegraded, plugin.StateSuspended:
				return "degraded"
			default:
				return "disabled"
			}
		},
		"actionClass": func(st action.InstanceState) string {
			switch st {
			case action.StateHealthy:
				return "healthy"
			case action.StateDegraded:
				return "degraded"
			default:
				return "disabled"
			}
		},
		"heartbeatClass": func(freshness string) string {
			switch freshness {
			case "fresh":
				return "ok"
			case "stale":
				return "warn"
			default:
				return "muted"
			}
		},
		"float1": func(v *float64) string {
			if v == nil {
				return ""
			}
			return fmt.Sprintf("%.1f", *v)
		},
		"float2": func(v *float64) string {
			if v == nil {
				return ""
			}
			return fmt.Sprintf("%.2f", *v)
		},
		"int0": func(v *float64) string {
			if v == nil {
				return ""
			}
			return fmt.Sprintf("%.0f", *v)
		},
		"add": func(a, b int) int { return a + b },
	}
}
