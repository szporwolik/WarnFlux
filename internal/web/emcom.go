package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/severity"
)

// EMCOM operational readiness networks.
//
// The canonical state of every network is a RETAINED MQTT document on
// <prefix>/info/emcom/emcom/<slug>/emcom — it survives broker and process
// restarts and is mirrored into the dispatch state like every other
// informational document. The panel edits it, the public header reads it.
//
// Level changes above monitoring (level 1-3) are published as hazard
// documents (severe) and flow through the canonical dispatch ingress so
// the per-group routing matrix fires exactly like for any other source.

const (
	emcomSource = "emcom"
	emcomKind   = "emcom"

	// maxEmcomNetworks bounds the managed networks (the public header
	// renders two or three; the panel handles a few more).
	maxEmcomNetworks = 12
	maxEmcomName     = 64
)

// emcomSlugRe validates generated network slugs (the info-topic key).
var emcomSlugRe = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// emcomLevel is one operational readiness level with its official Polish
// definition and the hazard severity used when the network is raised to it.
type emcomLevel struct {
	Level       int
	Name        string
	Description string
	Severity    string
}

// emcomLevels are the four readiness levels of the club's crisis
// communications plan. Anything above monitoring dispatches as a severe
// communication so the message goes out immediately.
var emcomLevels = []emcomLevel{
	{
		Level: 0, Name: "Monitoring", Severity: severity.Minor,
		Description: "Ongoing monitoring of the agreed frequencies and public channels (e.g. PMR, CB) without activating an organized communications network.",
	},
	{
		Level: 1, Name: "Increased readiness", Severity: severity.Severe,
		Description: "Operators ready to act: radio equipment prepared and a duty station maintained on the agreed primary frequency.",
	},
	{
		Level: 2, Name: "Local activation", Severity: severity.Severe,
		Description: "An organized radio network is activated in the affected area, including the net control station (SKS), field operators and relay stations. Activating level 2 or 3 means the network works as a directed net.",
	},
	{
		Level: 3, Name: "Full activation", Severity: severity.Severe,
		Description: "The full organizational structure is activated — base station, field operators and relay stations — with continuous operation: around-the-clock work in shifts.",
	},
}

// emcomLevelAt returns the definition for a level (0-3).
func emcomLevelAt(n int) (emcomLevel, bool) {
	for _, l := range emcomLevels {
		if l.Level == n {
			return l, true
		}
	}
	return emcomLevel{}, false
}

// emcomLevelClass maps a level onto the CSS color class (l0..l3).
func emcomLevelClass(level int) string {
	switch level {
	case 1:
		return "l1"
	case 2:
		return "l2"
	case 3:
		return "l3"
	default:
		return "l0"
	}
}

// emcomWire is the retained MQTT document on the info topic.
type emcomWire struct {
	SchemaVersion int    `json:"schema_version"`
	Network       string `json:"network"`
	Slug          string `json:"slug"`
	Level         int    `json:"level"`
	LevelName     string `json:"level_name"`
	UpdatedBy     string `json:"updated_by"`
	UpdatedAt     string `json:"updated_at"`
}

// emcomNetwork is one network's current state as read from the mirrored
// MQTT state.
type emcomNetwork struct {
	Slug      string
	Name      string
	Level     int
	LevelName string
	UpdatedBy string
	UpdatedAt time.Time
}

// emcomSlugify builds a topic-safe slug from a network display name
// (Polish diacritics transliterated, everything else folded to dashes).
func emcomSlugify(name string) string {
	var b strings.Builder
	lastDash := false
	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		switch r {
		case 'ą':
			r = 'a'
		case 'ć':
			r = 'c'
		case 'ę':
			r = 'e'
		case 'ł':
			r = 'l'
		case 'ń':
			r = 'n'
		case 'ó':
			r = 'o'
		case 'ś':
			r = 's'
		case 'ź', 'ż':
			r = 'z'
		}
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if b.Len() > 0 && !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	s := strings.Trim(b.String(), "-")
	if len(s) > 63 {
		s = strings.Trim(s[:63], "-")
	}
	return s
}

// emcomEventKey is the stable hazard event key of one network
// ("emcom:<slug>") — level changes update the same document, so the
// routing ledger deduplicates repeated activations correctly.
func emcomEventKey(slug string) string {
	return emcomSource + ":" + slug
}

// emcomHazard builds the severe communication published when a network is
// raised above monitoring.
func emcomHazard(net emcomNetwork, now time.Time) state.Hazard {
	lvl, _ := emcomLevelAt(net.Level)
	return state.Hazard{
		EventKey:   emcomEventKey(net.Slug),
		Source:     emcomSource,
		SourceID:   "ops",
		Event:      "EMCOM",
		Severity:   lvl.Severity,
		Urgency:    "immediate",
		Certainty:  "observed",
		Headline:   fmt.Sprintf("%s: level %d – %s", net.Name, net.Level, lvl.Name),
		Status:     "active",
		ReceivedAt: now,
		UpdatedAt:  now,
	}
}

// emcomTransition builds the canonical ingress event for one level change
// (new / updated / expired), mirroring the compose module.
func emcomTransition(h state.Hazard, typ dispatch.TransitionType) dispatch.Event {
	now := time.Now()
	return dispatch.Event{
		Kind:       dispatch.EventHazardTransition,
		ReceivedAt: now,
		Origin:     dispatch.Origin{Type: "web", ReceiverID: emcomSource},
		Hazard: &dispatch.HazardTransition{
			Type:      typ,
			Key:       h.EventKey,
			Source:    emcomSource,
			Timestamp: now,
			Hazard: dispatch.Hazard{
				EventKey:    h.EventKey,
				Source:      h.Source,
				SourceID:    h.SourceID,
				Event:       h.Event,
				Severity:    h.Severity,
				Urgency:     h.Urgency,
				Certainty:   h.Certainty,
				Headline:    h.Headline,
				Areas:       h.Areas,
				Latitude:    h.Latitude,
				Longitude:   h.Longitude,
				EffectiveAt: h.EffectiveAt,
				ExpiresAt:   h.ExpiresAt,
				ReceivedAt:  h.ReceivedAt,
				UpdatedAt:   h.UpdatedAt,
			},
		},
	}
}

// emcomNetworks reads the networks from the mirrored MQTT state, newest
// document per slug, sorted by display name.
func (s *Server) emcomNetworks() []emcomNetwork {
	best := make(map[string]emcomNetwork, 4)
	times := make(map[string]time.Time, 4)
	for _, e := range s.st.Snapshot().Info {
		if e.Kind != emcomKind || len(e.Payload) == 0 {
			continue
		}
		var w emcomWire
		if err := json.Unmarshal(e.Payload, &w); err != nil {
			continue
		}
		if w.Slug == "" || w.Network == "" || w.Slug != e.Key {
			continue
		}
		if prev, ok := times[w.Slug]; ok && !e.ReceivedAt.After(prev) {
			continue
		}
		times[w.Slug] = e.ReceivedAt
		levelName := w.LevelName
		if levelName == "" {
			if lvl, ok := emcomLevelAt(w.Level); ok {
				levelName = lvl.Name
			}
		}
		best[w.Slug] = emcomNetwork{
			Slug:      w.Slug,
			Name:      w.Network,
			Level:     w.Level,
			LevelName: levelName,
			UpdatedBy: w.UpdatedBy,
			UpdatedAt: e.ReceivedAt,
		}
	}
	out := make([]emcomNetwork, 0, len(best))
	for _, net := range best {
		out = append(out, net)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// emcomNetworkBySlug returns one network from the mirrored state.
func (s *Server) emcomNetworkBySlug(slug string) (emcomNetwork, bool) {
	for _, net := range s.emcomNetworks() {
		if net.Slug == slug {
			return net, true
		}
	}
	return emcomNetwork{}, false
}

// emcomHazardInMirror reports whether the network currently has an active
// hazard document in the mirror.
func (s *Server) emcomHazardInMirror(slug string) bool {
	for _, h := range s.st.Snapshot().Hazards {
		if h.Source == emcomSource && h.EventKey == emcomEventKey(slug) {
			return true
		}
	}
	return false
}

// publishEmcomState publishes (or deletes, with an empty net) the retained
// info document of one network.
func (s *Server) publishEmcomState(net emcomNetwork, by string) error {
	suffix := "info/" + emcomSource + "/" + emcomSource + "/" + net.Slug + "/" + emcomKind
	if net.Name == "" {
		return s.pub.PublishRaw(suffix, true, nil)
	}
	lvl, _ := emcomLevelAt(net.Level)
	wire := emcomWire{
		SchemaVersion: 1,
		Network:       net.Name,
		Slug:          net.Slug,
		Level:         net.Level,
		LevelName:     lvl.Name,
		UpdatedBy:     by,
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return err
	}
	return s.pub.PublishRaw(suffix, true, payload)
}

// emcomView is the /emcom page model.
type emcomView struct {
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

	Msg      string
	Error    string
	Networks []emcomNetworkView
	Levels   []emcomLevelView

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavCompose       bool
	NavEmcom         bool
	NavAccount       bool
	NavLogs          bool
	NavAudit         bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
}

// emcomNetworkView is one managed network for the panel.
type emcomNetworkView struct {
	Slug       string
	Name       string
	Level      int
	LevelName  string
	LevelClass string
	UpdatedBy  string
	UpdatedAt  time.Time
	Active     bool
}

// emcomLevelView is one readiness level for the panel legend.
type emcomLevelView struct {
	Level       int
	Name        string
	Description string
	Class       string
}

// emcomFlash maps the post-action redirect marker to a confirmation.
var emcomFlash = map[string]string{
	"added":   "Network added — it starts at level 0 (Monitoring).",
	"level":   "Network readiness level updated and broadcast.",
	"deleted": "Network deleted.",
}

// handleEmcomPage renders the EMCOM networks panel.
func (s *Server) handleEmcomPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := s.buildEmcomView()
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	view.Msg = emcomFlash[r.URL.Query().Get("msg")]
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "emcom", view)
}

// buildEmcomView assembles the page model from the mirrored state.
func (s *Server) buildEmcomView() emcomView {
	view := emcomView{
		AppTitle: s.cfg.Title,
		Name:     s.displayName(),
		Header1:  s.displayHeader1(),
		Header2:  s.cfg.Header2,
		Tagline:  s.cfg.Tagline,
		Version:  s.version,
		Commit:   s.commit,
		RepoURL:  repoURL,
		NavEmcom: true,
	}
	for _, l := range emcomLevels {
		view.Levels = append(view.Levels, emcomLevelView{
			Level: l.Level, Name: l.Name, Description: l.Description,
			Class: emcomLevelClass(l.Level),
		})
	}
	for _, net := range s.emcomNetworks() {
		view.Networks = append(view.Networks, emcomNetworkView{
			Slug:       net.Slug,
			Name:       net.Name,
			Level:      net.Level,
			LevelName:  net.LevelName,
			LevelClass: emcomLevelClass(net.Level),
			UpdatedBy:  net.UpdatedBy,
			UpdatedAt:  net.UpdatedAt,
			Active:     s.emcomHazardInMirror(net.Slug),
		})
	}
	return view
}

// renderEmcomError re-renders the page with an error banner.
func (s *Server) renderEmcomError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	sess := s.sessions.currentSession(r)
	view := s.buildEmcomView()
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	view.Error = msg
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	s.render(w, "emcom", view)
}

// handleEmcomAdd creates a new network at level 0 (monitoring). The
// retained info document is the durable record — no hazard is raised for
// a network that is only monitoring.
func (s *Server) handleEmcomAdd(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || !csrfOK(r.PostFormValue("csrf"), sess.csrf) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	name := strings.TrimSpace(r.PostFormValue("name"))
	if name == "" {
		s.renderEmcomError(w, r, http.StatusUnprocessableEntity, "Enter a network name (e.g. SP9MOA EMCOM).")
		return
	}
	if len([]rune(name)) > maxEmcomName {
		s.renderEmcomError(w, r, http.StatusUnprocessableEntity,
			fmt.Sprintf("Network name is too long (maximum %d characters).", maxEmcomName))
		return
	}
	slug := emcomSlugify(name)
	if !emcomSlugRe.MatchString(slug) {
		s.renderEmcomError(w, r, http.StatusUnprocessableEntity,
			"Network name must contain letters or digits.")
		return
	}
	if len(s.emcomNetworks()) >= maxEmcomNetworks {
		s.renderEmcomError(w, r, http.StatusUnprocessableEntity,
			fmt.Sprintf("Too many networks (maximum %d).", maxEmcomNetworks))
		return
	}
	if _, ok := s.emcomNetworkBySlug(slug); ok {
		s.renderEmcomError(w, r, http.StatusConflict, "A network with this name already exists.")
		return
	}
	if s.pub == nil {
		s.renderEmcomError(w, r, http.StatusServiceUnavailable,
			"Publishing is unavailable: no broker connection.")
		return
	}
	net := emcomNetwork{Slug: slug, Name: name, Level: 0}
	if err := s.publishEmcomState(net, sess.username); err != nil {
		s.logger.Warn("emcom: publish failed", "slug", slug, "error", err)
		s.renderEmcomError(w, r, http.StatusServiceUnavailable, "Publishing to the broker failed.")
		return
	}
	s.logger.Info("emcom: network added", "slug", slug, "name", name, "by", sess.username)
	s.audit(sess.username, "emcom-add", slug)
	http.Redirect(w, r, "/emcom?msg=added", http.StatusSeeOther)
}

// handleEmcomSetLevel moves one network to a readiness level. Levels 1-3
// publish a severe hazard document and flow through the routing matrix;
// level 0 retires the hazard document (monitoring continues silently).
func (s *Server) handleEmcomSetLevel(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || !csrfOK(r.PostFormValue("csrf"), sess.csrf) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	slug := r.PathValue("slug")
	net, ok := s.emcomNetworkBySlug(slug)
	if !ok {
		http.Error(w, "unknown network", http.StatusNotFound)
		return
	}
	level, err := strconv.Atoi(strings.TrimSpace(r.PostFormValue("level")))
	if err != nil || level < 0 || level > 3 {
		s.renderEmcomError(w, r, http.StatusUnprocessableEntity, "Level must be a number 0-3.")
		return
	}
	if s.pub == nil {
		s.renderEmcomError(w, r, http.StatusServiceUnavailable,
			"Publishing is unavailable: no broker connection.")
		return
	}

	net.Level = level
	net.UpdatedBy = sess.username
	if err := s.publishEmcomState(net, sess.username); err != nil {
		s.logger.Warn("emcom: state publish failed", "slug", slug, "error", err)
		s.renderEmcomError(w, r, http.StatusServiceUnavailable, "Publishing to the broker failed.")
		return
	}

	now := time.Now()
	if level >= 1 {
		h := emcomHazard(net, now)
		if err := s.pub.PublishActive(emcomSource, h); err != nil {
			s.logger.Warn("emcom: hazard publish failed", "slug", slug, "error", err)
			s.renderEmcomError(w, r, http.StatusServiceUnavailable, "Publishing the communication failed.")
			return
		}
		typ := dispatch.TransitionNew
		if s.emcomHazardInMirror(slug) {
			typ = dispatch.TransitionUpdated
		}
		if !s.ingress.Enqueue(emcomTransition(h, typ)) {
			s.logger.Warn("emcom: dispatch queue full, transition dropped", "slug", slug)
		}
	} else if s.emcomHazardInMirror(slug) {
		// Back to monitoring: retire the hazard document; the expiry
		// transition never starts the notification machine (like compose).
		if err := s.pub.ExpireActive(emcomSource, emcomEventKey(slug)); err != nil {
			s.logger.Warn("emcom: hazard retire failed", "slug", slug, "error", err)
		}
		if !s.ingress.Enqueue(emcomTransition(emcomHazard(net, now), dispatch.TransitionExpired)) {
			s.logger.Warn("emcom: dispatch queue full, expiry transition dropped", "slug", slug)
		}
	}
	s.logger.Info("emcom: level changed", "slug", slug, "level", level, "by", sess.username)
	s.audit(sess.username, "emcom-level", fmt.Sprintf("%s=%d", slug, level))
	http.Redirect(w, r, "/emcom?msg=level", http.StatusSeeOther)
}

// handleEmcomDelete removes one network: the retained info document is
// deleted and a still-active hazard document is retired.
func (s *Server) handleEmcomDelete(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || !csrfOK(r.PostFormValue("csrf"), sess.csrf) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	slug := r.PathValue("slug")
	net, ok := s.emcomNetworkBySlug(slug)
	if !ok {
		http.Error(w, "unknown network", http.StatusNotFound)
		return
	}
	if s.pub == nil {
		s.renderEmcomError(w, r, http.StatusServiceUnavailable,
			"Publishing is unavailable: no broker connection.")
		return
	}
	if err := s.publishEmcomState(emcomNetwork{Slug: slug}, sess.username); err != nil {
		s.logger.Warn("emcom: state delete failed", "slug", slug, "error", err)
		s.renderEmcomError(w, r, http.StatusServiceUnavailable, "Deleting from the broker failed.")
		return
	}
	if s.emcomHazardInMirror(slug) {
		if err := s.pub.ExpireActive(emcomSource, emcomEventKey(slug)); err != nil {
			s.logger.Warn("emcom: hazard retire failed", "slug", slug, "error", err)
		}
		if !s.ingress.Enqueue(emcomTransition(emcomHazard(net, time.Now()), dispatch.TransitionExpired)) {
			s.logger.Warn("emcom: dispatch queue full, expiry transition dropped", "slug", slug)
		}
	}
	s.logger.Info("emcom: network deleted", "slug", slug, "name", net.Name, "by", sess.username)
	s.audit(sess.username, "emcom-delete", slug)
	http.Redirect(w, r, "/emcom?msg=deleted", http.StatusSeeOther)
}
