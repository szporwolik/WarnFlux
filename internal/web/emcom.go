package web

import (
	"context"
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
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/severity"
	"github.com/szporwolik/WarnFlux/internal/storage"
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

// emcomStore is the optional local persistence surface for EMCOM
// networks (satisfied by *sqlite.Store): the panel works without a
// broker and the retained MQTT document is only an asynchronous sync
// copy for other instances. SaveEmcomNetwork returns the lifecycle
// version the store allocated for the transition (a database-owned
// monotonic counter, independent of the wall clock) so the handler can
// stamp it onto the dispatched transition.
type emcomStore interface {
	SaveEmcomNetwork(ctx context.Context, net storage.EmcomNetwork) (int64, error)
	// TombstoneEmcomNetwork atomically retires one network: tombstone +
	// lifecycle + the expiry transition's durable inbox row commit in
	// one transaction, so a crash or a refused live handoff can never
	// leave an active lifecycle behind the deleted network.
	TombstoneEmcomNetwork(ctx context.Context, slug string, ev dispatch.Event) (int64, int64, error)
	DeleteEmcomNetwork(ctx context.Context, slug string) error
	EmcomNetworks(ctx context.Context) ([]storage.EmcomNetwork, error)
	// InstanceID returns the persistent publisher UUID stamped onto
	// EMCOM transitions, so the lifecycle ledger can identify and
	// version them (a later level drop blocks a queued activation).
	InstanceID(ctx context.Context) (string, error)
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
// (new / updated / expired), mirroring the compose module. The transition
// carries the instance's publisher identity and the version the store
// allocated for the network state (a database-owned monotonic counter,
// independent of the wall clock) as its ChangeID, so the lifecycle
// ledger can identify and version it — a level drop back to monitoring
// then blocks a previously queued activation through the delivery gate.
func emcomTransition(h state.Hazard, typ dispatch.TransitionType, publisher string, version int64) dispatch.Event {
	now := time.Now()
	return dispatch.Event{
		Kind:       dispatch.EventHazardTransition,
		ReceivedAt: now,
		Origin:     dispatch.Origin{Type: "web", ReceiverID: emcomSource},
		Hazard: &dispatch.HazardTransition{
			Type:      typ,
			Key:       h.EventKey,
			Source:    emcomSource,
			ChangeID:  version,
			Publisher: publisher,
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

// emcomPublisher returns the persistent instance UUID stamped onto EMCOM
// transitions (empty for mirror-only installations without a local store:
// there are no durable delivery jobs to gate there).
func (s *Server) emcomPublisher() string {
	st, ok := s.users.(emcomStore)
	if !ok {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	id, err := st.InstanceID(ctx)
	if err != nil {
		s.logger.Warn("emcom: publisher id lookup failed", "error", err)
		return ""
	}
	return id
}

// emcomNetworks reads the networks from the LOCAL database record
// (authoritative, broker-independent); mirror-only installations fall
// back to the mirrored MQTT state (newest document per slug, sorted by
// display name).
func (s *Server) emcomNetworks() []emcomNetwork {
	if st, ok := s.users.(emcomStore); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		rows, err := st.EmcomNetworks(ctx)
		cancel()
		if err != nil {
			s.logger.Warn("emcom: local network list failed", "error", err)
		} else {
			out := make([]emcomNetwork, 0, len(rows))
			for _, n := range rows {
				if n.Level < 0 {
					continue // tombstones are invisible to the panel
				}
				lvl, _ := emcomLevelAt(n.Level)
				out = append(out, emcomNetwork{
					Slug:      n.Slug,
					Name:      n.Name,
					Level:     n.Level,
					LevelName: lvl.Name,
					UpdatedBy: n.UpdatedBy,
					UpdatedAt: n.UpdatedAt,
				})
			}
			return out
		}
	}
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

// emcomHazardActive reports whether the network currently has a raised
// hazard: the local record (level ≥ 1) is authoritative; mirror-only
// installations ask the mirror.
func (s *Server) emcomHazardActive(slug string) bool {
	if st, ok := s.users.(emcomStore); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		rows, err := st.EmcomNetworks(ctx)
		cancel()
		if err != nil {
			s.logger.Warn("emcom: local network list failed", "error", err)
		} else {
			for _, n := range rows {
				if n.Level < 0 {
					continue
				}
				if n.Slug == slug {
					return n.Level >= 1
				}
			}
			return false
		}
	}
	return s.emcomHazardInMirror(slug)
}

// saveEmcomNetwork persists one network to the local record and returns
// the lifecycle version the store allocated for the transition (nil when
// no store is attached: mirror-only installations keep the broker
// document as their record).
func (s *Server) saveEmcomNetwork(net emcomNetwork) (int64, error) {
	st, ok := s.users.(emcomStore)
	if !ok {
		return 0, nil
	}
	return st.SaveEmcomNetwork(context.Background(), storage.EmcomNetwork{
		Slug:      net.Slug,
		Name:      net.Name,
		Level:     net.Level,
		UpdatedBy: net.UpdatedBy,
		UpdatedAt: net.UpdatedAt,
	})
}

// publishEmcomStateAsync publishes the retained state document in the
// background: broker sync is best-effort and must never block the panel
// (the resync hook republishes the current state on receiver reconnect).
func (s *Server) publishEmcomStateAsync(net emcomNetwork, by string) {
	if s.pub == nil {
		return
	}
	go func() {
		if err := s.publishEmcomState(net, by); err != nil {
			s.logger.Warn("emcom: broker state sync failed", "slug", net.Slug, "error", err)
		}
	}()
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

// SyncBrokerState re-publishes the current local-first state (panel
// communications and EMCOM networks/hazards) to the broker. It is
// invoked on every receiver (re)connect so broker outages never lose
// panel state permanently. Best-effort: failures are logged.
func (s *Server) SyncBrokerState() {
	if s.pub == nil {
		return
	}
	now := time.Now()

	// Panel communications: the local record is authoritative. Active
	// rows re-publish their retained document; expired rows tombstone a
	// stale broker copy.
	if st, ok := s.users.(composeStore); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		rows, err := st.ComposeHazards(ctx)
		cancel()
		if err != nil {
			s.logger.Warn("compose: state resync read failed", "error", err)
		} else {
			for _, row := range rows {
				if row.Status == "expired" {
					if err := s.pub.ExpireActive(composeSource, row.EventKey); err != nil {
						s.logger.Warn("compose: tombstone resync failed", "event_key", row.EventKey, "error", err)
					}
					continue
				}
				var h state.Hazard
				if err := json.Unmarshal(row.State, &h); err != nil {
					continue
				}
				if err := s.pub.PublishActive(composeSource, h); err != nil {
					s.logger.Warn("compose: state resync failed", "event_key", row.EventKey, "error", err)
				}
			}
		}
	} else {
		for _, h := range s.st.Snapshot().Hazards {
			if h.Source != composeSource {
				continue
			}
			if err := s.pub.PublishActive(composeSource, h); err != nil {
				s.logger.Warn("compose: state resync failed", "event_key", h.EventKey, "error", err)
			}
		}
	}

	// EMCOM networks: live rows publish their state (and the hazard
	// document when raised); tombstones remove stale broker copies.
	if st, ok := s.users.(emcomStore); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		rows, err := st.EmcomNetworks(ctx)
		cancel()
		if err != nil {
			s.logger.Warn("emcom: state resync read failed", "error", err)
		} else {
			for _, n := range rows {
				if n.Level < 0 {
					if err := s.publishEmcomState(emcomNetwork{Slug: n.Slug}, ""); err != nil {
						s.logger.Warn("emcom: tombstone resync failed", "slug", n.Slug, "error", err)
					}
					continue
				}
				lvl, _ := emcomLevelAt(n.Level)
				net := emcomNetwork{
					Slug:      n.Slug,
					Name:      n.Name,
					Level:     n.Level,
					LevelName: lvl.Name,
					UpdatedBy: n.UpdatedBy,
					UpdatedAt: n.UpdatedAt,
				}
				if err := s.publishEmcomState(net, n.UpdatedBy); err != nil {
					s.logger.Warn("emcom: state resync failed", "slug", n.Slug, "error", err)
					continue
				}
				if n.Level >= 1 {
					if err := s.pub.PublishActive(emcomSource, emcomHazard(net, now)); err != nil {
						s.logger.Warn("emcom: hazard resync failed", "slug", n.Slug, "error", err)
					}
				}
			}
		}
	}
}

// emcomView is the /emcom page model.
type emcomView struct {
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
	NavMessages      bool
	NavMeshtastic    bool
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
	NavConfig        bool
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

// emcomFlashKey maps the post-action redirect marker to the banner
// message i18n key.
func emcomFlashKey(marker string) string {
	switch marker {
	case "added":
		return "emcom.flash.added"
	case "level":
		return "emcom.flash.level"
	case "deleted":
		return "emcom.flash.deleted"
	}
	return ""
}

// handleEmcomPage renders the EMCOM networks panel.
func (s *Server) handleEmcomPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := s.buildEmcomView(s.langFor(r))
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	view.Msg = i18n.T(s.langFor(r), emcomFlashKey(r.URL.Query().Get("msg")))
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "emcom", view)
}

// buildEmcomView assembles the page model from the mirrored state in a UI
// language. Level names/descriptions are localized for display; the wire
// payload keeps the English canonical names.
func (s *Server) buildEmcomView(lang string) emcomView {
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
			Level:       l.Level,
			Name:        i18n.T(lang, fmt.Sprintf("emcom.levels.%d", l.Level)),
			Description: i18n.T(lang, fmt.Sprintf("emcom.desc.%d", l.Level)),
			Class:       emcomLevelClass(l.Level),
		})
	}
	for _, net := range s.emcomNetworks() {
		view.Networks = append(view.Networks, emcomNetworkView{
			Slug:       net.Slug,
			Name:       net.Name,
			Level:      net.Level,
			LevelName:  i18n.T(lang, fmt.Sprintf("emcom.levels.%d", net.Level)),
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
	view := s.buildEmcomView(s.langFor(r))
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	view.Error = msg
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	s.renderL(w, r, "emcom", view)
}

// handleEmcomAdd creates a new network at level 0 (monitoring). The
// retained info document is the durable record — no hazard is raised for
// a network that is only monitoring.
func (s *Server) handleEmcomAdd(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	name := strings.TrimSpace(r.PostFormValue("name"))
	if name == "" {
		s.renderEmcomError(w, r, http.StatusUnprocessableEntity, i18n.T(s.langFor(r), "emcom.err.name_required"))
		return
	}
	if len([]rune(name)) > maxEmcomName {
		s.renderEmcomError(w, r, http.StatusUnprocessableEntity,
			fmt.Sprintf(i18n.T(s.langFor(r), "emcom.err.name_long"), maxEmcomName))
		return
	}
	slug := emcomSlugify(name)
	if !emcomSlugRe.MatchString(slug) {
		s.renderEmcomError(w, r, http.StatusUnprocessableEntity,
			i18n.T(s.langFor(r), "emcom.err.name_chars"))
		return
	}
	if len(s.emcomNetworks()) >= maxEmcomNetworks {
		s.renderEmcomError(w, r, http.StatusUnprocessableEntity,
			fmt.Sprintf(i18n.T(s.langFor(r), "emcom.err.too_many"), maxEmcomNetworks))
		return
	}
	if _, ok := s.emcomNetworkBySlug(slug); ok {
		s.renderEmcomError(w, r, http.StatusConflict, i18n.T(s.langFor(r), "emcom.err.exists"))
		return
	}
	net := emcomNetwork{Slug: slug, Name: name, Level: 0, UpdatedBy: sess.username, UpdatedAt: time.Now()}
	if _, ok := s.users.(emcomStore); ok {
		// LOCAL-FIRST: the local database row is the durable record; the
		// retained MQTT document is only an asynchronous sync copy.
		if _, err := s.saveEmcomNetwork(net); err != nil {
			s.logger.Warn("emcom: local save failed", "slug", slug, "error", err)
			s.renderEmcomError(w, r, http.StatusServiceUnavailable, i18n.T(s.langFor(r), "emcom.err.save"))
			return
		}
		s.publishEmcomStateAsync(net, sess.username)
	} else {
		// Mirror-only installations keep the broker document as the
		// record; publishing is required there.
		if s.pub == nil {
			s.renderEmcomError(w, r, http.StatusServiceUnavailable,
				i18n.T(s.langFor(r), "emcom.err.no_broker"))
			return
		}
		if err := s.publishEmcomState(net, sess.username); err != nil {
			s.logger.Warn("emcom: publish failed", "slug", slug, "error", err)
			s.renderEmcomError(w, r, http.StatusServiceUnavailable, i18n.T(s.langFor(r), "emcom.err.publish"))
			return
		}
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
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	slug := r.PathValue("slug")
	net, ok := s.emcomNetworkBySlug(slug)
	if !ok {
		http.Error(w, i18n.T(s.langFor(r), "emcom.err.unknown"), http.StatusNotFound)
		return
	}
	level, err := strconv.Atoi(strings.TrimSpace(r.PostFormValue("level")))
	if err != nil || level < 0 || level > 3 {
		s.renderEmcomError(w, r, http.StatusUnprocessableEntity, i18n.T(s.langFor(r), "emcom.err.level_range"))
		return
	}

	// The PREVIOUS level decides new vs updated vs expiry — captured
	// before the local record changes.
	wasActive := net.Level >= 1

	net.Level = level
	net.UpdatedBy = sess.username
	net.UpdatedAt = time.Now()
	// The lifecycle version comes from the store's clock-independent
	// counter when a store is attached; mirror-only installations fall
	// back to the wall clock (no durable jobs to gate there).
	version := net.UpdatedAt.UnixMilli()
	_, hasStore := s.users.(emcomStore)
	if hasStore {
		// LOCAL-FIRST: the local database row is the durable record; the
		// retained MQTT document is only an asynchronous sync copy.
		v, err := s.saveEmcomNetwork(net)
		if err != nil {
			s.logger.Warn("emcom: local save failed", "slug", slug, "error", err)
			s.renderEmcomError(w, r, http.StatusServiceUnavailable, i18n.T(s.langFor(r), "emcom.err.save"))
			return
		}
		version = v
	} else {
		// Mirror-only installations keep the broker document as the
		// record; publishing is required and synchronous there.
		if s.pub == nil {
			s.renderEmcomError(w, r, http.StatusServiceUnavailable,
				i18n.T(s.langFor(r), "emcom.err.no_broker"))
			return
		}
		if err := s.publishEmcomState(net, sess.username); err != nil {
			s.logger.Warn("emcom: state publish failed", "slug", slug, "error", err)
			s.renderEmcomError(w, r, http.StatusServiceUnavailable, i18n.T(s.langFor(r), "emcom.err.publish"))
			return
		}
	}

	publisher := s.emcomPublisher()
	if level >= 1 {
		// The hazard carries the SAVED network timestamp: the transition
		// ChangeID (updated_at_ms) then equals the lifecycle version the
		// store committed with the network state.
		h := emcomHazard(net, net.UpdatedAt)
		typ := dispatch.TransitionNew
		if wasActive {
			typ = dispatch.TransitionUpdated
		}
		// The transition is durable and routed locally BEFORE any broker
		// I/O — the panel never depends on the broker round-trip.
		switch s.ingress.Enqueue(emcomTransition(h, typ, publisher, version)) {
		case dispatch.Rejected:
			s.logger.Warn("emcom: local dispatch rejected the transition", "slug", slug)
			s.renderEmcomError(w, r, http.StatusServiceUnavailable, i18n.T(s.langFor(r), "emcom.err.dispatch"))
			return
		case dispatch.AcceptedEmergency:
			s.logger.Warn("emcom: transition accepted WITHOUT durable storage (emergency mode; lost on restart)", "slug", slug)
		}
		if hasStore {
			// Broker sync: the state document and the retained hazard,
			// best-effort (the resync hook republishes on reconnect).
			s.publishEmcomStateAsync(net, sess.username)
			if s.pub != nil {
				go func() {
					if err := s.pub.PublishActive(emcomSource, h); err != nil {
						s.logger.Warn("emcom: broker hazard sync failed", "slug", slug, "error", err)
					}
				}()
			}
		} else if err := s.pub.PublishActive(emcomSource, h); err != nil {
			s.logger.Warn("emcom: hazard publish failed", "slug", slug, "error", err)
			s.renderEmcomError(w, r, http.StatusServiceUnavailable, i18n.T(s.langFor(r), "emcom.err.communication"))
			return
		}
	} else if wasActive {
		// Back to monitoring: retire the hazard document; the expiry
		// transition never starts the notification machine (like compose).
		if s.ingress.Enqueue(emcomTransition(emcomHazard(net, net.UpdatedAt), dispatch.TransitionExpired, publisher, version)) == dispatch.Rejected {
			s.logger.Warn("emcom: local dispatch rejected the expiry transition", "slug", slug)
		}
		if hasStore {
			s.publishEmcomStateAsync(net, sess.username)
			if s.pub != nil {
				go func() {
					if err := s.pub.ExpireActive(emcomSource, emcomEventKey(slug)); err != nil {
						s.logger.Warn("emcom: broker hazard retire failed", "slug", slug, "error", err)
					}
				}()
			}
		} else if err := s.pub.ExpireActive(emcomSource, emcomEventKey(slug)); err != nil {
			s.logger.Warn("emcom: hazard retire failed", "slug", slug, "error", err)
		}
	} else if hasStore {
		s.publishEmcomStateAsync(net, sess.username)
	}
	s.logger.Info("emcom: level changed", "slug", slug, "level", level, "by", sess.username)
	s.audit(sess.username, "emcom-level", fmt.Sprintf("%s=%d", slug, level))
	http.Redirect(w, r, "/emcom?msg=level", http.StatusSeeOther)
}

// handleEmcomDelete removes one network: the retained info document is
// deleted and a still-active hazard document is retired.
func (s *Server) handleEmcomDelete(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	slug := r.PathValue("slug")
	net, ok := s.emcomNetworkBySlug(slug)
	if !ok {
		http.Error(w, i18n.T(s.langFor(r), "emcom.err.unknown"), http.StatusNotFound)
		return
	}
	wasActive := net.Level >= 1
	// The expiry transition is built ONCE: the store stamps its ChangeID
	// with the allocated counter version inside the tombstone
	// transaction, and the live handoff reuses the very same event (plus
	// its committed inbox id), so the durable row and the live copy are
	// identical.
	version := time.Now().UnixMilli()
	var expiryEv dispatch.Event
	if wasActive {
		expiryEv = emcomTransition(emcomHazard(net, time.Now()), dispatch.TransitionExpired, s.emcomPublisher(), 0)
	}
	_, hasStore := s.users.(emcomStore)
	if hasStore {
		// LOCAL-FIRST + ATOMIC (P1): the tombstone, the lifecycle record
		// and the expiry transition's durable inbox row commit in ONE
		// transaction — a crash between the deletion and the dispatch,
		// or a refused live handoff, can never lose the cancellation.
		st, _ := s.users.(emcomStore)
		id, v, err := st.TombstoneEmcomNetwork(r.Context(), slug, expiryEv)
		if err != nil {
			s.logger.Warn("emcom: local delete failed", "slug", slug, "error", err)
			s.renderEmcomError(w, r, http.StatusServiceUnavailable, i18n.T(s.langFor(r), "emcom.err.deleting"))
			return
		}
		version = v
		if wasActive {
			expiryEv.InboxID = id
		}
	} else {
		// Mirror-only installations keep the broker document as the
		// record; the tombstone publish is required there.
		if s.pub == nil {
			s.renderEmcomError(w, r, http.StatusServiceUnavailable,
				i18n.T(s.langFor(r), "emcom.err.no_broker"))
			return
		}
		if err := s.publishEmcomState(emcomNetwork{Slug: slug}, sess.username); err != nil {
			s.logger.Warn("emcom: state delete failed", "slug", slug, "error", err)
			s.renderEmcomError(w, r, http.StatusServiceUnavailable, i18n.T(s.langFor(r), "emcom.err.deleting"))
			return
		}
	}
	if wasActive {
		// The expiry transition is handed to the live worker after the
		// commit; a refused handoff only defers it — the durable inbox
		// row delivers it through recovery. The ChangeID is the version
		// allocated (store counter) or the wall-clock fallback
		// (mirror-only).
		expiryEv.Hazard.ChangeID = version
		switch s.ingress.Enqueue(expiryEv) {
		case dispatch.Rejected:
			if hasStore {
				s.logger.Warn("emcom: live handoff refused; the expiry stays in the durable inbox", "slug", slug)
			} else {
				s.logger.Warn("emcom: local dispatch rejected the expiry transition", "slug", slug)
			}
		case dispatch.AcceptedEmergency:
			s.logger.Warn("emcom: transition accepted WITHOUT durable storage (emergency mode; lost on restart)", "slug", slug)
		}
	}
	if hasStore {
		// Broker sync: tombstone the state document and retire the
		// hazard, best-effort (the resync hook republishes on reconnect).
		s.publishEmcomStateAsync(emcomNetwork{Slug: slug}, sess.username)
		if wasActive && s.pub != nil {
			go func() {
				if err := s.pub.ExpireActive(emcomSource, emcomEventKey(slug)); err != nil {
					s.logger.Warn("emcom: broker hazard retire failed", "slug", slug, "error", err)
				}
			}()
		}
	} else if wasActive {
		if err := s.pub.ExpireActive(emcomSource, emcomEventKey(slug)); err != nil {
			s.logger.Warn("emcom: hazard retire failed", "slug", slug, "error", err)
		}
	}
	s.logger.Info("emcom: network deleted", "slug", slug, "name", net.Name, "by", sess.username)
	s.audit(sess.username, "emcom-delete", slug)
	http.Redirect(w, r, "/emcom?msg=deleted", http.StatusSeeOther)
}
