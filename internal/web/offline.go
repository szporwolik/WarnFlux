package web

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/mqttpolicy"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// configView is the admin-only /config page model: the offline-mode
// switch, the local tile tree status and the list of sources that the
// switch suspends.
type configView struct {
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
	NavMessages      bool
	NavMeshcore      bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
	NavCompose       bool
	NavEmcom         bool
	NavAccount       bool
	NavConfig        bool

	// Offline is the current state of the switch.
	Offline bool
	// TilesDir is the configured local tile tree directory.
	TilesDir string
	// TilesOK reports whether TilesDir exists and is a directory.
	TilesOK bool
	// InternetSources lists the enabled internet-backed sources the
	// switch suspends (id · type).
	InternetSources []configSourceView
	// MqttRows is the long checkbox list of the publish-mask
	// categories (what WarnFlux may publish to the MQTT broker).
	MqttRows []configMqttRow
	// Msg is the flash message after a toggle.
	Msg string
}

// configMqttRow is one checkbox row of the publish mask.
type configMqttRow struct {
	Key     string
	Checked bool
}

// configSourceView is one row of the internet-backed source list.
type configSourceView struct {
	ID   string
	Type string
}

// OfflineMode reports the current offline-mode switch state.
func (s *Server) OfflineMode() bool { return s.offline.Load() }

// SetOfflineHandler installs the propagation hook invoked after every
// successful offline-mode toggle (the application suspends/resumes the
// internet-backed sources and actions there). Must be called before
// serving traffic.
func (s *Server) SetOfflineHandler(fn func(on bool)) {
	s.offlineOn = fn
}

// SetOffline flips the offline-mode switch. It is idempotent; the
// propagation hook runs only on an actual change.
func (s *Server) SetOffline(on bool) {
	if s.offline.Swap(on) == on {
		return
	}
	if s.offlineOn != nil {
		s.offlineOn(on)
	}
}

// tilesAvailable reports whether the local tile tree is usable.
func (s *Server) tilesAvailable() bool {
	if s.cfg.TilesDir == "" {
		return false
	}
	info, err := os.Stat(s.cfg.TilesDir)
	return err == nil && info.IsDir()
}

// internetSources lists the enabled internet-backed sources (the ones the
// offline switch suspends).
func (s *Server) internetSources() []configSourceView {
	var out []configSourceView
	for _, st := range s.router.Statuses() {
		if st.Kind != plugin.KindSource || !st.Internet || st.State == plugin.StateDisabled {
			continue
		}
		out = append(out, configSourceView{ID: st.ID, Type: st.Type})
	}
	return out
}

// mqttRows snapshots the publish mask as checkbox rows (UI order).
func (s *Server) mqttRows() []configMqttRow {
	rows := make([]configMqttRow, 0, 8)
	for _, c := range mqttpolicy.List() {
		rows = append(rows, configMqttRow{
			Key:     mqttpolicy.Key(c),
			Checked: mqttpolicy.Allowed(c),
		})
	}
	return rows
}

// handleConfigPage renders the admin-only configuration page.
func (s *Server) handleConfigPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	v := configView{
		AppTitle:        s.cfg.Title,
		Name:            s.displayName(),
		Header1:         s.displayHeader1(),
		Header2:         s.cfg.Header2,
		Tagline:         s.cfg.Tagline,
		Version:         s.version,
		Commit:          s.commit,
		RepoURL:         repoURL,
		CSRF:            sess.csrf,
		Username:        sess.username,
		Role:            sess.role,
		NavConfig:       true,
		Offline:         s.OfflineMode(),
		TilesDir:        s.cfg.TilesDir,
		TilesOK:         s.tilesAvailable(),
		InternetSources: s.internetSources(),
		MqttRows:        s.mqttRows(),
	}
	switch msg := r.URL.Query().Get("msg"); msg {
	case "on", "off":
		v.Msg = i18n.T(s.langFor(r), "config.offline."+msg)
	case "mqtt":
		v.Msg = i18n.T(s.langFor(r), "config.mqtt.saved")
	}
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "configpage", v)
}

// handleConfigMqtt replaces the runtime publish mask. Checked categories
// publish, unchecked ones are skipped by every publisher immediately —
// the admin decides what reaches the broker to keep its traffic and CPU
// down.
func (s *Server) handleConfigMqtt(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		if err == nil {
			return
		}
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	checked := make(map[string]bool, 8)
	for _, k := range r.PostForm["cat"] {
		checked[strings.TrimSpace(k)] = true
	}
	var mask uint32
	for _, c := range mqttpolicy.List() {
		if checked[mqttpolicy.Key(c)] {
			mask |= uint32(c)
		}
	}
	mqttpolicy.Set(mask)
	s.audit(sess.username, "config-mqtt",
		"publish mask: "+strings.Join(mqttpolicy.EnabledKeys(mask), ","))
	s.logger.Info("mqtt publish mask updated by admin",
		"user", sess.username, "categories", mqttpolicy.EnabledKeys(mask))
	http.Redirect(w, r, "/config?msg=mqtt", http.StatusSeeOther)
}

// handleConfigOffline flips the offline-mode switch. Admin-only; the
// action lands in the audit log.
func (s *Server) handleConfigOffline(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		if err == nil {
			return
		}
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	switch strings.TrimSpace(r.PostFormValue("offline")) {
	case "on", "off":
	default:
		http.Error(w, "invalid offline value", http.StatusBadRequest)
		return
	}
	on := strings.TrimSpace(r.PostFormValue("offline")) == "on"
	if on == s.OfflineMode() {
		http.Redirect(w, r, "/config", http.StatusSeeOther)
		return
	}
	s.SetOffline(on)
	if on {
		s.audit(sess.username, "config-offline", "offline mode enabled")
		s.logger.Info("offline mode enabled by admin", "user", sess.username)
	} else {
		s.audit(sess.username, "config-offline", "offline mode disabled")
		s.logger.Info("offline mode disabled by admin", "user", sess.username)
	}
	if on {
		http.Redirect(w, r, "/config?msg=on", http.StatusSeeOther)
	} else {
		http.Redirect(w, r, "/config?msg=off", http.StatusSeeOther)
	}
}

// handleTile serves one local map tile ({z}/{x}/{y}.jpg) from the
// operator-provided tree under web.tiles_dir. Coordinates are validated
// as non-negative integers, so no path traversal is possible. Tiles are
// immutable: browsers may cache them aggressively.
func (s *Server) handleTile(w http.ResponseWriter, r *http.Request) {
	if s.cfg.TilesDir == "" {
		http.NotFound(w, r)
		return
	}
	z, errZ := strconv.Atoi(r.PathValue("z"))
	x, errX := strconv.Atoi(r.PathValue("x"))
	// The frontend requests {z}/{x}/{y}.jpg and the mux wildcard captures
	// the whole segment including the extension, so strip it before parsing.
	yStr := strings.TrimSuffix(r.PathValue("y"), ".jpg")
	y, errY := strconv.Atoi(yStr)
	if errZ != nil || errX != nil || errY != nil || z < 0 || z > 25 || x < 0 || y < 0 {
		http.NotFound(w, r)
		return
	}
	rel := filepath.Join(strconv.Itoa(z), strconv.Itoa(x), strconv.Itoa(y)+".jpg")
	f, err := os.Open(filepath.Join(s.cfg.TilesDir, rel))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	if _, err := io.Copy(w, f); err != nil {
		s.logger.Warn("web: serving tile failed", "path", rel, "error", err)
	}
}
