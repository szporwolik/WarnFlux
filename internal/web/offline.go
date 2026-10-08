package web

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/mqttpolicy"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// domainPattern accepts a host (or host:port) without scheme and path.
var domainPattern = regexp.MustCompile(`^[a-zA-Z0-9.-]+(:[0-9]{1,5})?$`)

// contentFieldCap bounds every editable content field on the Config page.
const contentFieldCap = 4000

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
	NavAPRS          bool
	NavMessages      bool
	NavGSM           bool
	NavMeshtastic    bool
	NavMeshMap       bool
	NavTraffic       bool
	NavWebsite       bool
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
	// EmcomNetworks lists the managed EMCOM networks (admin-level
	// add/remove moved here from the operator panel).
	EmcomNetworks []emcomNetworkView
	// MeshChannel is the runtime switch for hazard broadcasts on the
	// Meshtastic group channel.
	MeshChannel bool
	// MeshDM is the runtime switch for hazard direct messages to users'
	// registered node IDs.
	MeshDM bool
	// MeshStations is the runtime switch for APRS station announcements
	// on the Meshtastic group channel.
	MeshStations bool
	// ForceLocalTiles forces every map onto the local tile tree even
	// while the station is online.
	ForceLocalTiles bool
	// SystemLanguage is the runtime broadcast notification language
	// ("" = i18n default).
	SystemLanguage string
	// Languages lists the supported notification languages for the
	// picker.
	Languages []string
	// Msg is the flash message after a toggle.
	Msg string
	// Error carries the banner after a rejected EMCOM management
	// action (re-rendered on the config page).
	Error string
	// Editable branding/content (the "appearance" section):
	// populated from the runtime getters, saved back to the YAML file.
	About      string
	Disclaimer string
	Domain     string
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

// buildConfigView assembles the shared admin configuration page model
// (everything except the flash message).
func (s *Server) buildConfigView(sess *session, lang string) configView {
	return configView{
		AppTitle:        s.cfg.Title,
		Name:            s.displayName(),
		Header1:         s.displayHeader1(),
		Header2:         s.DisplayHeader2(),
		Tagline:         s.DisplayTagline(),
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
		EmcomNetworks:   s.emcomNetworkViews(lang),
		MeshChannel:     meshtastic.ChannelAlerts(),
		MeshDM:          meshtastic.DMAlerts(),
		MeshStations:    meshtastic.StationAlerts(),
		ForceLocalTiles: s.forceTiles.Load(),
		SystemLanguage:  s.SystemLanguage(),
		Languages:       i18n.Codes(),
		About:           s.DisplayAbout(),
		Disclaimer:      s.DisplayDisclaimer(),
		Domain:          s.DisplayDomain(),
	}
}

// handleConfigPage renders the admin-only configuration page.
func (s *Server) handleConfigPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	lang := s.langFor(r)
	v := s.buildConfigView(sess, lang)
	switch msg := r.URL.Query().Get("msg"); msg {
	case "on", "off":
		v.Msg = i18n.T(lang, "config.offline."+msg)
	case "mqtt":
		v.Msg = i18n.T(lang, "config.mqtt.saved")
	case "emcom-added":
		v.Msg = i18n.T(lang, "emcom.flash.added")
	case "emcom-deleted":
		v.Msg = i18n.T(lang, "emcom.flash.deleted")
	case "mesh":
		v.Msg = i18n.T(lang, "config.mesh.saved")
	case "tiles":
		v.Msg = i18n.T(lang, "config.tiles.saved")
	case "lang":
		v.Msg = i18n.T(lang, "config.lang.saved")
	case "content":
		v.Msg = i18n.T(lang, "config.content.saved")
	}
	if r.URL.Query().Get("perr") == "1" {
		v.Error = i18n.T(lang, "config.persist_failed")
	}
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "configpage", v)
}

// persistConfigYAML applies one panel change to the YAML configuration
// file. The runtime state has already been flipped; this only makes the
// change durable.
func (s *Server) persistConfigYAML(change string, set func() error) bool {
	if s.configPath == "" {
		return true
	}
	if err := set(); err != nil {
		s.logger.Warn("config persist failed", "change", change, "error", err)
		return false
	}
	s.logger.Info("config persisted", "change", change, "path", s.configPath)
	return true
}

// renderConfigEmcomError re-renders the config page with an error banner
// after a rejected EMCOM add/delete action (the management surface).
func (s *Server) renderConfigEmcomError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	sess := s.sessions.currentSession(r)
	v := s.buildConfigView(sess, s.langFor(r))
	v.Error = msg
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	s.renderL(w, r, "configpage", v)
}

// renderConfigError re-renders the config page with an error banner after
// a rejected panel action.
func (s *Server) renderConfigError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	s.renderConfigEmcomError(w, r, status, msg)
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
	perr := ""
	if !s.persistConfigYAML("mqtt_publish", func() error {
		edits := make([]config.ScalarEdit, 0, 10)
		for _, c := range mqttpolicy.List() {
			edits = append(edits, config.ScalarEdit{Path: "mqtt_publish." + mqttpolicy.Key(c), Value: config.BoolScalar(mask&uint32(c) != 0)})
		}
		return config.UpdateFile(s.configPath, edits)
	}) {
		perr = "&perr=1"
	}
	s.audit(sess.username, "config-mqtt",
		"publish mask: "+strings.Join(mqttpolicy.EnabledKeys(mask), ","))
	s.logger.Info("mqtt publish mask updated by admin",
		"user", sess.username, "categories", mqttpolicy.EnabledKeys(mask))
	http.Redirect(w, r, "/config?msg=mqtt"+perr, http.StatusSeeOther)
}

// handleConfigTiles flips the local-tile forcing switch: ON means every
// map serves the operator-provided tile tree even while the station is
// online (no internet tile provider is ever contacted). Admin-only.
func (s *Server) handleConfigTiles(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		if err == nil {
			return
		}
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	switch strings.TrimSpace(r.PostFormValue("tiles")) {
	case "on":
		s.forceTiles.Store(true)
	case "off":
		s.forceTiles.Store(false)
	default:
		http.Error(w, "invalid value", http.StatusBadRequest)
		return
	}
	perr := ""
	if !s.persistConfigYAML("force_local_tiles", func() error {
		return config.UpdateFile(s.configPath, []config.ScalarEdit{
			{Path: "web.force_local_tiles", Value: config.BoolScalar(s.forceTiles.Load())},
		})
	}) {
		perr = "&perr=1"
	}
	s.audit(sess.username, "config-tiles", r.PostFormValue("tiles"))
	s.logger.Info("local tile forcing toggled by admin",
		"user", sess.username, "state", r.PostFormValue("tiles"))
	http.Redirect(w, r, "/config?msg=tiles"+perr, http.StatusSeeOther)
}

// handleConfigMesh flips the three Meshtastic announcement switches: the
// group-channel broadcast, the direct messages to registered node IDs and
// the APRS station range announcements are muted independently. Admin-only.
func (s *Server) handleConfigMesh(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		if err == nil {
			return
		}
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	channel := strings.TrimSpace(r.PostFormValue("channel"))
	dm := strings.TrimSpace(r.PostFormValue("dm"))
	stations := strings.TrimSpace(r.PostFormValue("stations"))
	if channel == "" && dm == "" && stations == "" {
		http.Error(w, "missing switch value", http.StatusBadRequest)
		return
	}
	apply := func(v string, fn func(bool)) bool {
		switch v {
		case "on":
			fn(true)
		case "off":
			fn(false)
		case "":
			return true // this switch was not part of the submit
		default:
			return false
		}
		return true
	}
	if !apply(channel, meshtastic.SetChannelAlerts) ||
		!apply(dm, meshtastic.SetDMAlerts) ||
		!apply(stations, meshtastic.SetStationAlerts) {
		http.Error(w, "invalid value", http.StatusBadRequest)
		return
	}
	perr := ""
	if !s.persistConfigYAML("meshtastic alerts", func() error {
		var edits []config.ScalarEdit
		if channel != "" {
			edits = append(edits, config.ScalarEdit{Path: "meshtastic.channel_alerts", Value: config.BoolScalar(meshtastic.ChannelAlerts())})
		}
		if dm != "" {
			edits = append(edits, config.ScalarEdit{Path: "meshtastic.dm_alerts", Value: config.BoolScalar(meshtastic.DMAlerts())})
		}
		if stations != "" {
			edits = append(edits, config.ScalarEdit{Path: "meshtastic.station_alerts", Value: config.BoolScalar(meshtastic.StationAlerts())})
		}
		return config.UpdateFile(s.configPath, edits)
	}) {
		perr = "&perr=1"
	}
	s.audit(sess.username, "config-mesh",
		fmt.Sprintf("channel=%v dm=%v stations=%v", meshtastic.ChannelAlerts(), meshtastic.DMAlerts(), meshtastic.StationAlerts()))
	s.logger.Info("meshtastic announcements toggled by admin",
		"user", sess.username,
		"channel", meshtastic.ChannelAlerts(), "dm", meshtastic.DMAlerts(),
		"stations", meshtastic.StationAlerts())
	http.Redirect(w, r, "/config?msg=mesh"+perr, http.StatusSeeOther)
}

// handleConfigLang switches the system notification language: broadcast
// channels (APRS messages, EMCOM group-channel Meshtastic posts) are
// sent in it. Admin-only.
func (s *Server) handleConfigLang(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		if err == nil {
			return
		}
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	code := strings.ToLower(strings.TrimSpace(r.PostFormValue("lang")))
	if code != "" && !i18n.Supported(code) {
		http.Error(w, "invalid value", http.StatusBadRequest)
		return
	}
	s.SetSystemLanguage(code)
	perr := ""
	if !s.persistConfigYAML("system_language", func() error {
		return config.UpdateFile(s.configPath, []config.ScalarEdit{
			{Path: "web.system_language", Value: config.StringScalar(code)},
		})
	}) {
		perr = "&perr=1"
	}
	s.audit(sess.username, "config-lang", code)
	s.logger.Info("system notification language changed by admin",
		"user", sess.username, "lang", code)
	http.Redirect(w, r, "/config?msg=lang"+perr, http.StatusSeeOther)
}

// handleConfigContent saves the editable branding/content fields of the
// admin Config page: they apply to every page immediately and are
// written back to the YAML file so they survive restarts. Admin-only.
func (s *Server) handleConfigContent(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		if err == nil {
			return
		}
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	trim := func(k string) string { return strings.TrimSpace(r.PostFormValue(k)) }
	fields := map[string]string{
		"header1":    trim("header1"),
		"header2":    trim("header2"),
		"tagline":    trim("tagline"),
		"about":      trim("about"),
		"disclaimer": trim("disclaimer"),
		"domain":     strings.TrimSuffix(trim("domain"), "/"),
	}
	for k, v := range fields {
		if len(v) > contentFieldCap {
			s.renderConfigError(w, r, http.StatusUnprocessableEntity, i18n.T(s.langFor(r), "config.content.toolong"))
			return
		}
		if k == "domain" && v != "" && !domainPattern.MatchString(v) {
			s.renderConfigError(w, r, http.StatusUnprocessableEntity, i18n.T(s.langFor(r), "config.content.baddomain"))
			return
		}
	}
	s.header1.Store(fields["header1"])
	s.header2.Store(fields["header2"])
	s.tagline.Store(fields["tagline"])
	s.about.Store(fields["about"])
	s.disclaimer.Store(fields["disclaimer"])
	s.domain.Store(fields["domain"])

	perr := ""
	if !s.persistConfigYAML("web content", func() error {
		paths := []struct{ field, path string }{
			{"header1", "web.header1"}, {"header2", "web.header2"}, {"tagline", "web.tagline"},
			{"about", "web.about"}, {"disclaimer", "web.disclaimer"}, {"domain", "web.domain"},
		}
		edits := make([]config.ScalarEdit, 0, len(paths))
		for _, p := range paths {
			edits = append(edits, config.ScalarEdit{Path: p.path, Value: config.StringScalar(fields[p.field])})
		}
		return config.UpdateFile(s.configPath, edits)
	}) {
		perr = "&perr=1"
	}
	s.audit(sess.username, "config-content",
		fmt.Sprintf("header1=%q header2=%q tagline=%q domain=%q", fields["header1"], fields["header2"], fields["tagline"], fields["domain"]))
	s.logger.Info("web branding content updated by admin", "user", sess.username, "domain", fields["domain"])
	http.Redirect(w, r, "/config?msg=content"+perr, http.StatusSeeOther)
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
	perr := ""
	if !s.persistConfigYAML("offline_mode", func() error {
		return config.UpdateFile(s.configPath, []config.ScalarEdit{
			{Path: "web.offline_mode", Value: config.BoolScalar(on)},
		})
	}) {
		perr = "&perr=1"
	}
	if on {
		s.audit(sess.username, "config-offline", "offline mode enabled")
		s.logger.Info("offline mode enabled by admin", "user", sess.username)
	} else {
		s.audit(sess.username, "config-offline", "offline mode disabled")
		s.logger.Info("offline mode disabled by admin", "user", sess.username)
	}
	if on {
		http.Redirect(w, r, "/config?msg=on"+perr, http.StatusSeeOther)
	} else {
		http.Redirect(w, r, "/config?msg=off"+perr, http.StatusSeeOther)
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

// localTilesMaxZoom returns the deepest zoom level (0..25) that holds at
// least one tile in the configured tree, scanning each level once and
// caching the result. The tree is static in production, so the cache is
// never invalidated; a concurrent first call may scan twice — harmless.
func (s *Server) localTilesMaxZoom() int {
	if v := s.tilesMaxZoom.Load(); v >= 0 {
		return int(v)
	}
	if s.cfg.TilesDir == "" {
		s.tilesMaxZoom.Store(0)
		return 0
	}
	max := 0
	for z := 0; z <= 25; z++ {
		zdir := filepath.Join(s.cfg.TilesDir, strconv.Itoa(z))
		xs, err := os.ReadDir(zdir)
		if err != nil {
			continue
		}
		found := false
		for _, x := range xs {
			if !x.IsDir() {
				continue
			}
			files, err := os.ReadDir(filepath.Join(zdir, x.Name()))
			if err == nil && len(files) > 0 {
				found = true
				break
			}
		}
		if found {
			max = z
		}
	}
	s.tilesMaxZoom.Store(int64(max))
	return max
}

// handleTilesMaxZoom reports the deepest zoom level available in the
// local tile tree (0 when there are no tiles). The offline maps use it
// as the tile layer's maxNativeZoom: beyond it Leaflet stretches the
// deepest available tiles instead of requesting missing ones.
func (s *Server) handleTilesMaxZoom(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	fmt.Fprintf(w, `{"maxzoom":%d}`, s.localTilesMaxZoom())
}
