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
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/appinfo"
	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/gsm"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/metrics"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/severity"
	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/sysinfo"
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
	cfg        config.Web
	st         *state.State
	receivers  *mqttreceiver.Manager
	pub        composePublisher
	router     RouterStatuses
	actions    *action.Manager
	aprs       *aprs.Hub
	meshtastic *meshtastic.Hub
	gsm        *gsm.Hub
	ingress    *dispatch.Ingress
	users      storage.DirectoryStore
	// events backs the public archive (180-day history of communications).
	// nil in minimal constructions (the archive tab then shows an empty
	// state).
	events storage.EventStore
	// aprsMsgs backs the admin APRS message history page; nil in minimal
	// constructions (the page then shows an empty state).
	aprsMsgs storage.APRSMessageStore
	// meshtasticMsgs backs the admin Meshtastic message history page; nil in
	// minimal constructions (the page then shows an empty state).
	meshtasticMsgs storage.MeshtasticMessageStore
	// gsmMsgs backs the admin GSM/SMS history page; nil in minimal
	// constructions (the page then shows an empty state).
	gsmMsgs storage.GSMMessageStore
	// visits persists the per-day visitor analytics when the store
	// supports it (nil otherwise — the counting then stays off).
	visits       storage.VisitorStore
	logger       *slog.Logger
	sessions     *sessionStore
	loginLimiter *loginLimiter
	logs         *LogBuffer
	traffic      *mqttreceiver.TrafficBuffer
	auditLog     *AuditBuffer
	trails       *trail.Recorder
	metrics      *metrics.Registry

	// sys samples host CPU/memory utilization for the System card;
	// nil in minimal constructions (the view degrades to n/a).
	sys *sysinfo.Sampler

	// minFreeBytes is the low-disk alarm threshold: below it the health
	// DB row turns red and the retention policy shortens (0 = disabled).
	minFreeBytes int64

	// offline is the runtime offline-mode switch (startup value from the
	// config; an admin toggles it on the Config page). offlineOn is the
	// hook that propagates a toggle to the plugin and action managers.
	offline   atomic.Bool
	offlineOn func(on bool)
	// forceTiles forces every map to the local tile tree even while the
	// station is online (the Config page switch).
	forceTiles atomic.Bool
	// tilesMaxZoom caches the deepest zoom level available in the local
	// tile tree (-1 = not computed yet); offline maps use it as the
	// tile layer's maxNativeZoom so zooming past the tree stretches the
	// deepest tiles instead of showing gaps.
	tilesMaxZoom atomic.Int64
	// systemLang is the runtime system notification language (startup
	// value from web.system_language; the admin Config page switches it
	// at runtime). "" = the i18n default. Broadcast channels (APRS,
	// EMCOM group posts) use it.
	systemLang atomic.Value

	// configPath is the YAML file the admin Config page writes its
	// switches back to (empty = persistence disabled, e.g. tests).
	configPath string

	// Runtime branding/content fields: initialized from the config at
	// startup, editable on the Config page and persisted to the YAML
	// file. All view builders read them through the Display* getters,
	// so a panel edit takes effect immediately.
	header1    atomic.Value // string
	header2    atomic.Value // string
	tagline    atomic.Value // string
	about      atomic.Value // string
	disclaimer atomic.Value // string
	domain     atomic.Value // string

	// resetMailer delivers password-reset emails. nil = email delivery
	// unavailable (the self-service flow degrades gracefully).
	resetMailer func(to, subject, text string) error

	// ingest maps each configured public ingest endpoint id to its
	// API-key-protected handler (may be empty).
	ingest map[string]http.Handler

	// trustedProxies is the parsed set of reverse proxies whose
	// X-Forwarded-For header may identify the client (empty = direct
	// exposure, proxy headers ignored).
	trustedProxies []*net.IPNet

	version string
	commit  string

	// tz is the display timezone for every formatted timestamp (nil =
	// the process-local zone). SetTimezone overrides it before serving.
	tz *time.Location

	startedAt time.Time
	ready     atomic.Bool

	tmpl     *template.Template
	mux      *http.ServeMux
	httpSrv  *http.Server
	listener net.Listener
}

// SetConfigFile enables persistence of the admin Config page switches:
// every toggle is additionally written back to this YAML file so the
// choice survives restarts. Empty disables persistence.
func (s *Server) SetConfigFile(path string) { s.configPath = path }

// SetTimezone overrides the display timezone for every formatted
// timestamp (nil = the process-local zone).
func (s *Server) SetTimezone(loc *time.Location) { s.tz = loc }

// displayLoc returns the display timezone (the process-local zone when
// none was configured).
func (s *Server) displayLoc() *time.Location {
	if s.tz != nil {
		return s.tz
	}
	return time.Local
}

// DisplayHeader1 returns the runtime primary header line.
func (s *Server) DisplayHeader1() string { return s.header1.Load().(string) }

// DisplayHeader2 returns the runtime subtitle.
func (s *Server) DisplayHeader2() string { return s.header2.Load().(string) }

// DisplayTagline returns the runtime footer motto.
func (s *Server) DisplayTagline() string { return s.tagline.Load().(string) }

// DisplayAbout returns the runtime about text (markdown).
func (s *Server) DisplayAbout() string { return s.about.Load().(string) }

// DisplayDisclaimer returns the runtime public-page disclaimer.
func (s *Server) DisplayDisclaimer() string { return s.disclaimer.Load().(string) }

// DisplayDomain returns the runtime public domain used for deep links.
func (s *Server) DisplayDomain() string { return s.domain.Load().(string) }

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
	aprsHub *aprs.Hub, meshtasticHub *meshtastic.Hub, gsmHub *gsm.Hub, ingress *dispatch.Ingress, logger *slog.Logger, version, commit string,
	users storage.DirectoryStore, events storage.EventStore, aprsMsgs storage.APRSMessageStore,
	meshtasticMsgs storage.MeshtasticMessageStore, gsmMsgs storage.GSMMessageStore, ingest map[string]http.Handler,
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

	// Trusted reverse proxies: proxy headers are honored ONLY from these
	// peers (IP or CIDR). An empty list means direct exposure.
	trusted, err := parseTrustedProxies(cfg.Auth.TrustedProxies)
	if err != nil {
		return nil, fmt.Errorf("web: auth.trusted_proxies: %w", err)
	}

	s := &Server{
		cfg:            cfg,
		st:             st,
		receivers:      receivers,
		pub:            pub,
		router:         router,
		actions:        actions,
		aprs:           aprsHub,
		meshtastic:     meshtasticHub,
		gsm:            gsmHub,
		ingress:        ingress,
		users:          users,
		events:         events,
		aprsMsgs:       aprsMsgs,
		meshtasticMsgs: meshtasticMsgs,
		gsmMsgs:        gsmMsgs,
		logger:         logger,
		sessions:       newSessionStore(cfg.Auth.SecureCookie),
		loginLimiter:   newLoginLimiter(),
		trustedProxies: trusted,
		logs:           logs,
		traffic:        traffic,
		auditLog:       NewAuditBuffer(DefaultAuditEntries),
		trails:         trails,
		metrics:        metricsReg,
		sys:            sysinfo.New(),
		version:        version,
		commit:         commit,
		startedAt:      time.Now(),
		mux:            http.NewServeMux(),
		ingest:         ingest,
	}
	// Parsed after construction: the template funcs close over the
	// server's display timezone (set later via SetTimezone, nil = the
	// process-local zone).
	tmpl, err := template.New("root").Funcs(s.templateFuncs()).ParseFS(templatesFS, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("web: parse templates: %w", err)
	}
	s.tmpl = tmpl
	s.tilesMaxZoom.Store(-1)
	// Visitor analytics ride the same SQLite store as the events: the
	// optional interface keeps minimal test constructions untouched.
	if v, ok := events.(storage.VisitorStore); ok {
		s.visits = v
	}

	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		return nil, fmt.Errorf("web: static assets: %w", err)
	}
	s.offline.Store(cfg.OfflineMode)
	s.forceTiles.Store(cfg.ForceLocalTiles)
	s.systemLang.Store(cfg.SystemLanguage)
	s.header1.Store(cfg.Header1)
	s.header2.Store(cfg.Header2)
	s.tagline.Store(cfg.Tagline)
	s.about.Store(cfg.About)
	s.disclaimer.Store(cfg.Disclaimer)
	s.domain.Store(cfg.Domain)
	s.routes(http.FileServerFS(static))
	s.httpSrv = &http.Server{
		Handler:           securityHeaders(s.mux),
		ReadHeaderTimeout: 10 * time.Second,
		// Slowloris / connection-exhaustion bounds: the UI has no long
		// polls, so modest read/write/idle limits are safe.
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	return s, nil
}

// noCacheStatic forces browsers to revalidate every embedded asset on
// every request. The embedded files carry no ETag/Last-Modified, so
// plain caching can serve stale app.js/style.css indefinitely; the
// etagStatic wrapper below adds content hashes so the revalidation is a
// cheap 304 instead of a re-download.
func noCacheStatic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		next.ServeHTTP(w, r)
	})
}

// etagStatic stamps every embedded asset with a strong ETag (a sha-256
// prefix of its content, hashed once per file) and answers conditional
// requests with 304. app.js, style.css, the fonts and the bundled
// Leaflet add up to a few hundred KB per page load — revalidating them
// instead of re-downloading on every navigation keeps the UI snappy on
// a low-power station.
func etagStatic(next http.Handler) http.Handler {
	var hashes sync.Map // file path -> etag string
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		etag, ok := hashes.Load(path)
		if !ok {
			f, err := staticFS.Open("static/" + path)
			if err != nil {
				next.ServeHTTP(w, r) // 404 through the regular handler
				return
			}
			h := sha256.New()
			if _, err := io.Copy(h, f); err != nil {
				f.Close()
				next.ServeHTTP(w, r)
				return
			}
			f.Close()
			etag = `"` + hex.EncodeToString(h.Sum(nil)[:16]) + `"`
			hashes.Store(path, etag)
		}
		w.Header().Set("ETag", etag.(string))
		if inm := r.Header.Get("If-None-Match"); inm != "" && inm == etag.(string) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) routes(static http.Handler) {
	s.mux.Handle("GET /static/", http.StripPrefix("/static/", etagStatic(noCacheStatic(static))))
	s.mux.HandleFunc("GET /login", s.handleLoginPage)
	s.mux.HandleFunc("POST /login", s.handleLoginSubmit)
	s.mux.HandleFunc("POST /logout", s.handleLogout)
	// Self-service password recovery (token link delivered by email).
	s.mux.HandleFunc("GET /forgot", s.handleForgotPage)
	s.mux.HandleFunc("POST /forgot", s.handleForgotSubmit)
	s.mux.HandleFunc("GET /reset", s.handleResetPage)
	s.mux.HandleFunc("POST /reset", s.handleResetSubmit)
	// Public landing page: header1/header2 + the current active hazards.
	// The login form lives behind the top-right icon button (/login).
	s.mux.HandleFunc("GET /{$}", s.handleHome)
	// Deep link: /message/<event-key> opens the public home page with the
	// detail popup for that specific hazard — the link form used in
	// email and Discord notifications.
	s.mux.HandleFunc("GET /message/{key}", s.handleHome)
	// Deep link data: one hazard by key, including ended (expired /
	// cancelled) ones — the /message popup asks here when the hazard is
	// no longer on the active list, so a mail link always shows WHAT the
	// message was about (with the ended status on top).
	s.mux.HandleFunc("GET /api/hazard/{key}", s.handleHazardByKey)
	s.mux.HandleFunc("GET /partials/home", s.handlePartialHome)
	s.mux.HandleFunc("GET /archive", s.handleArchive)
	// Public sources/channels page (linked from the footer).
	s.mux.HandleFunc("GET /sources", s.handleSourcesPage)
	// UI language switch: stores the choice in a cookie and returns.
	s.mux.HandleFunc("GET /lang/{code}", s.handleLanguage)
	s.mux.HandleFunc("GET /api/aprs/stations", s.handleAPRSStations)
	s.mux.HandleFunc("GET /api/meshtastic/stations", s.handleMeshtasticStations)
	s.mux.HandleFunc("GET /api/events", s.handleEventsMap)
	s.mux.HandleFunc("GET /api/weather", s.handleWeather)
	s.mux.HandleFunc("GET /api/aircraft", s.handleAircraft)
	s.mux.HandleFunc("GET /api/airquality", s.handleAirQuality)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.Handle("GET /logs", s.requireAdmin(s.handleLogsPage))
	s.mux.Handle("GET /partials/logs", s.requireAdminPartial(s.handlePartialLogs))
	s.mux.Handle("GET /audit", s.requireAdmin(s.handleAuditPage))
	s.mux.Handle("GET /partials/audit", s.requireAdminPartial(s.handlePartialAudit))
	s.mux.Handle("GET /traffic", s.requireAdmin(s.handleTrafficPage))
	s.mux.Handle("GET /partials/traffic", s.requireAdminPartial(s.handlePartialTraffic))
	s.mux.Handle("GET /website", s.requireAdmin(s.handleWebsitePage))
	s.mux.Handle("GET /api/stats/visits", s.requireAdmin(s.handleVisitorStats))
	s.mux.Handle("GET /messages", s.requireAdmin(s.handleAPRSMessagesPage))
	s.mux.Handle("POST /messages/send", s.requireAdmin(s.handleAPRSSend))
	s.mux.Handle("POST /messages/beacon", s.requireAdmin(s.handleAPRSBeacon))
	s.mux.Handle("GET /aprs", s.requireAdmin(s.handleAPRSPage))
	s.mux.Handle("GET /gsm", s.requireAdmin(s.handleGSMPage))
	s.mux.Handle("GET /partials/gsm", s.requireAdminPartial(s.handlePartialGSM))
	s.mux.Handle("POST /gsm/send", s.requireAdmin(s.handleGSMSend))
	s.mux.Handle("GET /meshtastic", s.requireAdmin(s.handleMeshtasticPage))
	s.mux.Handle("GET /meshmap", s.requireAdmin(s.handleMeshMapPage))
	s.mux.Handle("GET /partials/meshtastic", s.requireAdminPartial(s.handlePartialMeshtastic))
	s.mux.Handle("GET /partials/messages", s.requireAdminPartial(s.handlePartialMessages))
	s.mux.Handle("POST /meshtastic/send", s.requireAdmin(s.handleMeshtasticSend))
	s.mux.Handle("POST /api/meshtastic/traceroute", s.requireAdmin(s.handleMeshtasticTraceroute))
	s.mux.Handle("GET /api/mqtt/browse", s.requireAdmin(s.handleMQTTBrowse))
	s.mux.Handle("GET /notifications", s.requireAdmin(s.handleNotificationsPage))
	s.mux.Handle("GET /partials/notifications", s.requireAdminPartial(s.handlePartialNotifications))
	// /health merged into the dashboard: the old URL now redirects there.
	s.mux.HandleFunc("GET /health", s.handleHealthLegacy)
	s.mux.Handle("GET /partials/health", s.requirePagePartial(s.handlePartialHealth))
	// Metrics: unauthenticated on purpose (Prometheus cannot log in);
	// only counters are exposed.
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	// Public ingest endpoints: authenticated per instance with the
	// configured API key, never with a UI session.
	if len(s.ingest) > 0 {
		s.mux.HandleFunc("POST /api/v1/ingest/{id}", s.handleIngest)
	}
	s.mux.Handle("GET /dashboard", s.requirePage(s.handleDashboard))
	// Compose: admin and emcom sessions issue/update/expire
	// communications; it is the emcom operator's main surface.
	s.mux.Handle("GET /compose", s.requireCompose(s.handleComposePage))
	s.mux.Handle("POST /compose", s.requireCompose(s.handleComposeSave))
	s.mux.Handle("POST /compose/expire", s.requireCompose(s.handleComposeExpire))
	// EMCOM networks: admin and emcom sessions use the panel and move
	// the readiness level. Adding and deleting networks is an
	// admin-tier structural change and lives on the Config page.
	s.mux.Handle("GET /emcom", s.requireCompose(s.handleEmcomPage))
	s.mux.Handle("POST /emcom/{slug}/level", s.requireCompose(s.handleEmcomSetLevel))
	s.mux.Handle("POST /config/emcom/add", s.requireAdmin(s.handleEmcomAdd))
	s.mux.Handle("POST /config/emcom/{slug}/delete", s.requireAdmin(s.handleEmcomDelete))
	// Self-service account page: member and emcom sessions edit their
	// own contact data and password.
	s.mux.Handle("GET /account", s.requirePage(s.handleAccountPage))
	s.mux.Handle("POST /account", s.requirePage(s.handleAccountSave))
	s.mux.Handle("GET /access", s.requireAdmin(s.handleAccessPage))
	s.mux.Handle("GET /users", s.requireAdmin(s.handleUsersPage))
	s.mux.Handle("POST /users", s.requireAdmin(s.handleUserSave))
	s.mux.Handle("POST /users/{id}/delete", s.requireAdmin(s.handleUserDelete))
	s.mux.Handle("POST /users/{id}/prefs", s.requireAdmin(s.handleUserPrefs))
	s.mux.Handle("POST /users/{id}/reset", s.requireAdmin(s.handleUserResetPassword))
	// Config: the offline-mode switch, the local map tile tree and the
	// MQTT publish mask.
	s.mux.Handle("GET /config", s.requireAdmin(s.handleConfigPage))
	s.mux.Handle("POST /config/offline", s.requireAdmin(s.handleConfigOffline))
	s.mux.Handle("POST /config/tiles", s.requireAdmin(s.handleConfigTiles))
	s.mux.Handle("POST /config/mesh", s.requireAdmin(s.handleConfigMesh))
	s.mux.Handle("POST /config/lang", s.requireAdmin(s.handleConfigLang))
	s.mux.Handle("POST /config/content", s.requireAdmin(s.handleConfigContent))
	s.mux.Handle("POST /config/mqtt", s.requireAdmin(s.handleConfigMqtt))
	// Local map tiles ({z}/{x}/{y}.jpg under web.tiles_dir) for offline
	// mode. Registered unconditionally; empty tiles_dir yields 404s.
	s.mux.HandleFunc("GET /tiles/{z}/{x}/{y}", s.handleTile)
	s.mux.HandleFunc("GET /api/tiles/maxzoom", s.handleTilesMaxZoom)
	s.mux.Handle("GET /groups", s.requireAdmin(s.handleGroupsPage))
	s.mux.Handle("POST /groups", s.requireAdmin(s.handleGroupSave))
	s.mux.Handle("POST /groups/{id}/delete", s.requireAdmin(s.handleGroupDelete))
	s.mux.Handle("GET /groups/{id}/routing", s.requireAdmin(s.handleGroupRoutingPage))
	s.mux.Handle("POST /groups/{id}/routing", s.requireAdmin(s.handleGroupRouting))
	s.mux.Handle("GET /partials/status", s.requirePagePartial(s.handlePartialStatus))
	s.mux.Handle("GET /partials/mqtt", s.requirePagePartial(s.handlePartialMQTT))
	s.mux.Handle("GET /partials/weather", s.requirePagePartial(s.handlePartialWeather))
	s.mux.Handle("GET /partials/warnings", s.requirePagePartial(s.handlePartialWarnings))
	s.mux.Handle("GET /partials/plugins", s.requirePagePartial(s.handlePartialPlugins))
	s.mux.Handle("GET /partials/actions", s.requirePagePartial(s.handlePartialActions))
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
}

// MarkReady flips readiness (database opened, HTTP initialized).
func (s *Server) MarkReady() { s.ready.Store(true) }

// SetStorageAlarm installs the low-disk alarm threshold in bytes (0
// disables the alarm): below it the health DB row turns red, /metrics
// reports the free space, and the dispatch inbox retention shortens.
// Must be called before serving traffic.
func (s *Server) SetStorageAlarm(minFreeBytes int64) {
	s.minFreeBytes = minFreeBytes
}

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

// requirePagePartial is the fragment-route variant of requirePage:
// unauthenticated requests get 401 so the embedded poller redirects the
// browser instead of receiving foreign HTML.
func (s *Server) requirePagePartial(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.sessions.currentSession(r) == nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	})
}

// requireCompose protects the compose routes: any authenticated admin or
// emcom session; members are sent back to the dashboard.
func (s *Server) requireCompose(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess := s.sessions.currentSession(r)
		if sess == nil {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		if sess.role == "member" {
			http.Redirect(w, r, landingForRole(sess.role), http.StatusSeeOther)
			return
		}
		next(w, r)
	})
}

// requireAdmin protects admin-tier routes: unauthenticated requests go to
// the login page, non-admin sessions (member/emcom) are sent to their own
// landing page instead.
func (s *Server) requireAdmin(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess := s.sessions.currentSession(r)
		if sess == nil {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		if sess.role != "admin" {
			http.Redirect(w, r, landingForRole(sess.role), http.StatusSeeOther)
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

// langFor resolves the UI language for a request (cookie, then the
// browser's Accept-Language, then English).
func (s *Server) langFor(r *http.Request) string {
	return i18n.FromRequest(r)
}

// SystemLanguage returns the runtime system notification language
// ("" = the i18n default). Broadcast channels — APRS messages and
// EMCOM group-channel Meshtastic posts — are sent in it.
func (s *Server) SystemLanguage() string {
	code, _ := s.systemLang.Load().(string)
	return code
}

// SetSystemLanguage switches the runtime system notification language
// (validated by the Config page handler). Unsupported codes are ignored.
func (s *Server) SetSystemLanguage(code string) {
	if code == "" || i18n.Supported(code) {
		s.systemLang.Store(code)
	}
}

// handleLanguage stores the chosen UI language in a long-lived cookie and
// sends the visitor back where they came from (same-origin only).
func (s *Server) handleLanguage(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")
	if !i18n.Supported(code) {
		http.NotFound(w, r)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: "wf_lang", Value: code, Path: "/", MaxAge: 365 * 24 * 3600,
		HttpOnly: true, SameSite: http.SameSiteLaxMode,
		Secure: s.sessions.secure,
	})
	// Return address: the ?next= path wins (the app's Referrer-Policy is
	// no-referrer, so the Referer is usually empty), then the Referer,
	// then the public page.
	target := r.URL.Query().Get("next")
	if !strings.HasPrefix(target, "/") || strings.HasPrefix(target, "//") {
		target = ""
	}
	if target == "" {
		target = r.Referer()
	}
	if u, err := url.Parse(target); target == "" || err != nil || u.Scheme != "" && u.Host != r.Host {
		target = "/"
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func (s *Server) render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.ExecuteTemplate(w, name, data); err != nil {
		s.logger.Error("web: template render failed", "template", name, "error", err)
	}
}

// langFieldCache memoizes the Lang field index per view type so the
// per-request language injection stays cheap.
var langFieldCache sync.Map // reflect.Type -> int (index, -1 = none)

// stampLang returns a copy of the view with the UI language stamped onto
// every struct (and nested struct field) carrying a Lang field. Pointers
// are stamped in place; value structs are copied. Maps and other kinds
// (login) are returned unchanged.
func stampLang(data any, lang string) any {
	if data == nil {
		return data
	}
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Pointer {
		if v.Kind() != reflect.Struct {
			return data
		}
		pv := reflect.New(v.Type())
		pv.Elem().Set(v)
		setLangValue(pv, lang)
		return pv.Elem().Interface()
	}
	setLangValue(v, lang)
	return data
}

func setLangValue(v reflect.Value, lang string) {
	for v.IsValid() && v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct || !v.CanSet() {
		return
	}
	t := v.Type()
	cached, ok := langFieldCache.Load(t)
	var idx int
	if ok {
		idx, _ = cached.(int)
	} else {
		sf, found := t.FieldByName("Lang")
		idx = -1
		if found && sf.Type.Kind() == reflect.String {
			idx = sf.Index[0]
		}
		langFieldCache.Store(t, idx)
	}
	if idx >= 0 {
		v.Field(idx).SetString(lang)
	}
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if f.Kind() != reflect.Struct {
			continue
		}
		if f.CanSet() {
			setLangValue(f, lang)
		} else if f.CanAddr() && f.Addr().CanInterface() {
			setLangValue(f.Addr(), lang)
		}
	}
}

// renderL renders a page template with the request's UI language stamped
// onto the view (so the {{tr}} calls resolve correctly).
func (s *Server) renderL(w http.ResponseWriter, r *http.Request, name string, data any) {
	s.countVisit(w, r)
	s.render(w, name, stampLang(data, s.langFor(r)))
}

// templateFuncs provides the small set of formatting helpers used by the UI.
func (s *Server) templateFuncs() template.FuncMap {
	loc := s.displayLoc()
	return template.FuncMap{
		"timeFull": func(t time.Time) string {
			if t.IsZero() {
				return "—"
			}
			return t.In(loc).Format("2006-01-02 15:04:05")
		},
		"derefTime": func(t *time.Time) time.Time {
			if t == nil {
				return time.Time{}
			}
			return *t
		},
		// tr/trf resolve a translation key in the page's language.
		"tr": func(lang, key string) string {
			return i18n.T(lang, key)
		},
		// trf resolves a translation key with format args in the page language.
		"trf": func(lang, key string, args ...any) string {
			return fmt.Sprintf(i18n.T(lang, key), args...)
		},
		// trh is tr for keys whose values contain trusted HTML markup.
		"trh": func(lang, key string) template.HTML {
			return template.HTML(i18n.T(lang, key))
		},
		"initials": func(s string) string {
			s = strings.TrimSpace(s)
			if s == "" {
				return "?"
			}
			return strings.ToUpper(string([]rune(s)[0]))
		},
		// roleLabel renders a session role for the user menu.
		"roleLabel": func(role string) string {
			return i18n.T(i18n.LangEN, "role."+role)
		},
		// roleLabelL renders a role label in the given UI language.
		"roleLabelL": func(lang, role string) string {
			return i18n.T(lang, "role."+role)
		},
		// shortCommit trims full hashes for display (links keep the
		// full hash; cache-busting query strings must too).
		"shortCommit": func(s string) string {
			if len(s) > 7 {
				return s[:7]
			}
			return s
		},
		// jsonLevels serializes the EMCOM readiness levels for the
		// panel's slider script (the official names and descriptions).
		"jsonLevels": func(levels []emcomLevelView) template.JS {
			type lv struct {
				Level       int    `json:"level"`
				Name        string `json:"name"`
				Description string `json:"description"`
				Class       string `json:"class"`
			}
			out := make([]lv, 0, len(levels))
			for _, l := range levels {
				out = append(out, lv{Level: l.Level, Name: l.Name, Description: l.Description, Class: l.Class})
			}
			b, err := json.Marshal(out)
			if err != nil {
				return template.JS("[]")
			}
			return template.JS(b)
		},
		"timeShort": func(t time.Time) string {
			if t.IsZero() {
				return "—"
			}
			return t.In(loc).Format("15:04:05")
		},
		// Trail steps carry RFC3339 timestamps as strings (JSON shape);
		// these two helpers render them for the notifications page.
		"timeHMS": func(s string) string {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return s
			}
			return t.In(loc).Format("15:04:05")
		},
		"timeFullStr": func(s string) string {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return s
			}
			return t.In(loc).Format("2006-01-02 15:04:05")
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
		// ageL renders a human age ("3m ago") in the page language.
		"ageL": func(lang string, t time.Time) string {
			if t.IsZero() {
				return "—"
			}
			d := time.Since(t)
			if d < 0 {
				d = 0
			}
			switch {
			case d < time.Second:
				return i18n.T(lang, "time.just_now")
			case d < time.Minute:
				return fmt.Sprintf(i18n.T(lang, "time.secs_ago"), int(d.Seconds()))
			case d < time.Hour:
				return fmt.Sprintf(i18n.T(lang, "time.mins_ago"), int(d.Minutes()))
			case d < 24*time.Hour:
				return fmt.Sprintf(i18n.T(lang, "time.hours_ago"), int(d.Hours()))
			default:
				return fmt.Sprintf(i18n.T(lang, "time.days_ago"), int(d.Hours()/24))
			}
		},
		// stateL renders a plugin/action state word in the page language.
		// The state arrives as a defined string type, so accept any.
		"stateL": func(lang string, st any) string {
			return i18n.T(lang, "dash."+fmt.Sprint(st))
		},
		"sevClass": func(severity string) string {
			switch strings.ToLower(severity) {
			case "extreme", "severe", "moderate", "minor", "unknown":
				return strings.ToLower(severity)
			default:
				return "unknown"
			}
		},
		// sevLabel renders a canonical severity in the page language;
		// non-canonical strings pass through untouched.
		"sevLabel": func(lang, sev string) string {
			if _, ok := severity.Rank(sev); ok {
				return i18n.T(lang, "sev."+sev)
			}
			return sev
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

// securityHeaders applies defense-in-depth response headers to every web
// response. A strict Content-Security-Policy is deliberately NOT set: the
// map stack (Leaflet, MapLibre GL blob workers, tile CDNs) requires
// inline styles and blob workers, and a policy without a full map-stack
// audit would break rendering.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		next.ServeHTTP(w, r)
	})
}
