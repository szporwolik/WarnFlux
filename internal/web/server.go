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
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/plugin"
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
	router    RouterStatuses
	actions   *action.Manager
	ingress   *dispatch.Ingress
	logger    *slog.Logger
	sessions  *sessionStore

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

// New builds the web server (no listener created yet).
func New(cfg config.Web, st *state.State, receivers *mqttreceiver.Manager,
	router RouterStatuses, actions *action.Manager, ingress *dispatch.Ingress,
	logger *slog.Logger, version, commit string) (*Server, error) {

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
		router:    router,
		actions:   actions,
		ingress:   ingress,
		logger:    logger,
		sessions:  newSessionStore(cfg.Auth.SecureCookie),
		version:   version,
		commit:    commit,
		startedAt: time.Now(),
		tmpl:      tmpl,
		mux:       http.NewServeMux(),
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

func (s *Server) routes(static http.Handler) {
	s.mux.Handle("GET /static/", http.StripPrefix("/static/", static))
	s.mux.HandleFunc("GET /login", s.handleLoginPage)
	s.mux.HandleFunc("POST /login", s.handleLoginSubmit)
	s.mux.HandleFunc("POST /logout", s.handleLogout)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.Handle("GET /dashboard", s.requirePage(s.handleDashboard))
	s.mux.Handle("GET /partials/status", s.requirePartial(s.handlePartialStatus))
	s.mux.Handle("GET /partials/mqtt", s.requirePartial(s.handlePartialMQTT))
	s.mux.Handle("GET /partials/weather", s.requirePartial(s.handlePartialWeather))
	s.mux.Handle("GET /partials/warnings", s.requirePartial(s.handlePartialWarnings))
	s.mux.Handle("GET /partials/plugins", s.requirePartial(s.handlePartialPlugins))
	s.mux.Handle("GET /partials/actions", s.requirePartial(s.handlePartialActions))
	s.mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
	})
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
		"timeShort": func(t time.Time) string {
			if t.IsZero() {
				return "—"
			}
			return t.Local().Format("15:04:05")
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
		"int0": func(v *float64) string {
			if v == nil {
				return ""
			}
			return fmt.Sprintf("%.0f", *v)
		},
	}
}
