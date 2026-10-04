package web

import (
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	mesh "github.com/szporwolik/WarnFlux/internal/meshtastic"

	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// meshtasticMessagesPageSize bounds one page of the admin mesh message history.
const meshtasticMessagesPageSize = 100

// meshtasticMessageView is one history row shown on the admin page.
type meshtasticMessageView struct {
	Direction string
	// Sender is the sender's 8-hex node id for rx rows (tx rows empty).
	Sender string
	// SenderName is the directory username owning the sender's node id
	// when one exists (empty otherwise, so the raw id stays visible).
	SenderName string
	Channel    string
	Hops       int
	// Operator is the admin username behind a tx row (rx rows empty).
	Operator string
	Text     string
	// Status is the tx delivery state ("", sent, delivered, failed).
	Status string
	At     time.Time
	// ID is the sender's node id when the row can prefill the send form
	// (direct messages), empty otherwise.
	ID string
}

// meshtasticNodeView is one heard neighbour shown on the admin page.
type meshtasticNodeView struct {
	ID   string
	Name string
	// Short is the node's short name from the device directory.
	Short string
	// Sends lists the observed packet kinds (telemetry/position/text).
	Sends []string
	// Owner is the directory username whose registered node id matches
	// the node's id (empty when nobody registered it).
	Owner      string
	Lat        float64
	Lon        float64
	DistKM     float64
	BearingDeg float64
	Cardinal   string
	LastSeen   string
}

// meshtasticContactView is one directory user's Meshtastic node offered by
// the send-form picker.
type meshtasticContactView struct {
	Username string
	ID       string // 8-hex node id
}

// meshtasticChannelOption is one device channel offered by the send form.
type meshtasticChannelOption struct {
	Idx   int
	Label string
}

// meshtasticView is the admin Meshtastic page model.
type meshtasticView struct {
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
	NavTraffic       bool
	NavNotifications bool
	NavHealth        bool
	NavConfig        bool
	NavCompose       bool
	NavEmcom         bool
	NavAccount       bool
	NavAudit         bool
	NavMessages      bool
	NavMeshtastic    bool

	Tab string // messages | nodes

	// messages tab
	Messages []meshtasticMessageView
	Dir      string
	Page     int
	Pages    int
	From     int
	To       int
	Total    int

	// Send-form feedback (query flashes).
	Error string
	Sent  bool

	// nodes tab
	Snap     mesh.Snapshot
	Nodes    []meshtasticNodeView
	Contacts []meshtasticContactView
	// Channels are the device channel options for the send form.
	Channels []meshtasticChannelOption
}

// handleMeshtasticPage renders the admin Meshtastic page.
func (s *Server) handleMeshtasticPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	v := meshtasticView{
		AppTitle:      s.cfg.Title,
		Name:          s.displayName(),
		Header1:       s.displayHeader1(),
		Header2:       s.cfg.Header2,
		Tagline:       s.cfg.Tagline,
		Version:       s.version,
		Commit:        s.commit,
		RepoURL:       repoURL,
		CSRF:          sess.csrf,
		Username:      sess.username,
		Role:          sess.role,
		NavMeshtastic: true,
		Tab:           "messages",
		Dir:           "all",
	}
	if tab := r.URL.Query().Get("tab"); tab == "nodes" {
		v.Tab = "nodes"
	}
	v.Sent = r.URL.Query().Get("sent") == "1"
	v.Error = r.URL.Query().Get("err")
	w.Header().Set("Cache-Control", "no-store")

	// Both panels render at once; the tab strip toggles them client-side
	// (Tab only preselects the visible one, e.g. /meshtastic?tab=nodes).
	s.fillMeshtasticNodes(&v)
	s.fillMeshtasticMessages(r, &v)
	s.renderL(w, r, "meshtastic", v)
}

func (s *Server) fillMeshtasticMessages(r *http.Request, v *meshtasticView) {
	if s.meshtasticMsgs == nil {
		return
	}
	switch d := r.URL.Query().Get("dir"); d {
	case "rx", "tx", "ch0":
		v.Dir = d
	}
	// The primary channel (0) owns the dedicated "ch0" tab; every other
	// view (all, rx, tx) hides it. The label comes from the hub's
	// channel table ("ch0" unless the device names the primary channel).
	primary := "ch0"
	if s.meshtastic != nil {
		primary = s.meshtastic.ChannelLabel(0)
	}
	filter := storage.MeshtasticMessageFilter{Channel: primary, Exclude: true}
	switch v.Dir {
	case "ch0":
		filter = storage.MeshtasticMessageFilter{Channel: primary}
	case "rx", "tx":
		filter.Direction = v.Dir
	}
	total, err := s.meshtasticMsgs.CountMeshtasticMessages(r.Context(), filter)
	if err != nil {
		s.logger.Warn("web: mesh messages count failed", "error", err)
		return
	}
	v.Total = total
	pages := (total + meshtasticMessagesPageSize - 1) / meshtasticMessagesPageSize
	if pages < 1 {
		pages = 1
	}
	v.Pages = pages
	page := pageParam(r, "page")
	if page < 1 {
		page = 1
	}
	if page > pages {
		page = pages
	}
	v.Page = page
	if total > 0 {
		v.From = (page-1)*meshtasticMessagesPageSize + 1
		v.To = page * meshtasticMessagesPageSize
		if v.To > total {
			v.To = total
		}
	}
	stored, err := s.meshtasticMsgs.ListMeshtasticMessages(r.Context(), filter, meshtasticMessagesPageSize, (page-1)*meshtasticMessagesPageSize)
	if err != nil {
		s.logger.Warn("web: mesh messages list failed", "error", err)
		return
	}
	v.Messages = make([]meshtasticMessageView, 0, len(stored))
	// Resolve directory usernames for direct-message senders so the
	// history shows names instead of raw node ids.
	var owners map[string]string
	if s.users != nil {
		owners, _ = s.users.MeshtasticOwners()
	}
	for _, m := range stored {
		// The device uses 0xFF as the "no path info" sentinel; treat it
		// as unknown so the history never claims 255 hops.
		hops := m.Hops
		if hops >= 255 {
			hops = 0
		}
		view := meshtasticMessageView{
			Direction: m.Direction,
			Sender:    m.Sender,
			Channel:   s.meshtasticChannelName(m.Channel),
			Hops:      hops,
			Operator:  m.Operator,
			Text:      m.Text,
			Status:    m.Status,
			At:        m.At,
		}
		if id := meshtasticNormalizeID(m.Sender); id != "" {
			view.ID = id
			view.SenderName = meshtasticOwnerFor(owners, id)
		}
		v.Messages = append(v.Messages, view)
	}
}

// meshtasticChannelName resolves a stored channel label for display. New rows
// already carry the friendly name from the hub ("Public", "#sp9moa");
// legacy rows store "ch<N>" and are resolved through the hub's
// configured channel_names map.
func (s *Server) meshtasticChannelName(stored string) string {
	if stored == "" || stored[0] != 'c' || s.meshtastic == nil {
		return stored
	}
	var idx int
	if _, err := fmt.Sscanf(stored, "ch%d", &idx); err != nil {
		return stored
	}
	return s.meshtastic.ChannelLabel(idx)
}

func (s *Server) fillMeshtasticNodes(v *meshtasticView) {
	// Contact picker comes from the directory (independent of the hub).
	var owners map[string]string
	if s.users != nil {
		owners, _ = s.users.MeshtasticOwners()
	}
	v.Contacts = make([]meshtasticContactView, 0, len(owners))
	for id, username := range owners {
		if id = meshtasticNormalizeID(id); id == "" {
			continue
		}
		v.Contacts = append(v.Contacts, meshtasticContactView{Username: username, ID: id})
	}
	sort.Slice(v.Contacts, func(i, j int) bool {
		if v.Contacts[i].Username != v.Contacts[j].Username {
			return v.Contacts[i].Username < v.Contacts[j].Username
		}
		return v.Contacts[i].ID < v.Contacts[j].ID
	})

	if s.meshtastic == nil {
		return
	}
	v.Snap = s.meshtastic.Snapshot()
	// The send form offers the device channel table (the node is the
	// source of truth); the default PRIMARY channel (0) is receive-only
	// by policy and is never offered.
	v.Channels = make([]meshtasticChannelOption, 0, len(v.Snap.Channels))
	for i, name := range v.Snap.Channels {
		if i == 0 {
			continue
		}
		v.Channels = append(v.Channels, meshtasticChannelOption{Idx: i, Label: name})
	}
	if len(v.Channels) == 0 {
		v.Channels = append(v.Channels, meshtasticChannelOption{Idx: 1})
	}
	// Directory match: any user who registered a node's id labels it (as
	// the name when the node carries none, and next to the name
	// otherwise). The public home page never sees this.
	v.Nodes = make([]meshtasticNodeView, 0, len(v.Snap.Nodes))
	for _, n := range v.Snap.Nodes {
		owner := meshtasticOwnerFor(owners, n.ID)
		name := n.Name
		if name == "" {
			name = owner
		}
		v.Nodes = append(v.Nodes, meshtasticNodeView{
			ID:         n.ID,
			Name:       name,
			Short:      n.Short,
			Sends:      n.Sends,
			Owner:      owner,
			Lat:        n.Lat,
			Lon:        n.Lon,
			DistKM:     n.DistKM,
			BearingDeg: n.BearingDeg,
			Cardinal:   cardinalDirection(n.BearingDeg),
			LastSeen:   n.LastSeen.Format("15:04:05"),
		})
	}
}

// meshtasticNormalizeID canonicalizes a Meshtastic node id: lowercase,
// 8 hex chars, no '!' / 0x prefix. Returns "" for anything else.
func meshtasticNormalizeID(s string) string {
	s = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(s), "0x"))
	s = strings.TrimPrefix(s, "!")
	if len(s) != 8 {
		return ""
	}
	if _, err := hex.DecodeString(s); err != nil {
		return ""
	}
	return s
}

// meshtasticOwnerFor resolves the directory username for a node id.
func meshtasticOwnerFor(owners map[string]string, id string) string {
	id = meshtasticNormalizeID(id)
	if id == "" {
		return ""
	}
	if u, ok := owners[id]; ok {
		return u
	}
	for key, u := range owners {
		if meshtasticNormalizeID(key) == id {
			return u
		}
	}
	return ""
}

// cardinalDirection maps a bearing in degrees to the 8-wind compass point.
func cardinalDirection(bearing float64) string {
	dirs := []string{"N", "NE", "E", "SE", "S", "SW", "W", "NW"}
	return dirs[int(math.Mod(bearing+22.5, 360)/45)]
}

// handlePartialMeshtastic serves the polled fragments: the nodes tab
// (default) or the messages list (?tab=messages).
func (s *Server) handlePartialMeshtastic(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	v := meshtasticView{
		AppTitle: s.cfg.Title,
		Header1:  s.displayHeader1(),
		CSRF:     sess.csrf,
	}
	if r.URL.Query().Get("tab") == "messages" {
		v.Dir = "all"
		s.fillMeshtasticMessages(r, &v)
		w.Header().Set("Cache-Control", "no-store")
		s.renderL(w, r, "mesh_msgs", v)
		return
	}
	s.fillMeshtasticNodes(&v)
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "mesh_nodes", v)
}

// handleMeshtasticSend sends one broadcast (channel) or direct message
// (to a node id) from the admin panel.
func (s *Server) handleMeshtasticSend(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	if s.meshtastic == nil {
		http.Error(w, i18n.T(s.langFor(r), "meshtastic.hub_not_configured"), http.StatusBadGateway)
		return
	}
	text := strings.TrimSpace(r.PostFormValue("text"))
	if text == "" {
		http.Redirect(w, r, "/meshtastic?err="+url.QueryEscape(i18n.T(s.langFor(r), "meshtastic.text_required")), http.StatusSeeOther)
		return
	}
	var err error
	if r.PostFormValue("target") == "contact" {
		id := meshtasticNormalizeID(r.PostFormValue("contact"))
		if id == "" {
			http.Redirect(w, r, "/meshtastic?err="+url.QueryEscape(i18n.T(s.langFor(r), "meshtastic.bad_node_id")), http.StatusSeeOther)
			return
		}
		err = s.meshtastic.SendContactMessage(r.Context(), id, text, sess.username)
	} else {
		idx := 1
		if raw := strings.TrimSpace(r.PostFormValue("channel_idx")); raw != "" {
			parsed, parseErr := strconv.Atoi(raw)
			if parseErr != nil || parsed < 1 || parsed > 7 {
				http.Redirect(w, r, "/meshtastic?err="+url.QueryEscape(i18n.T(s.langFor(r), "meshtastic.bad_channel")), http.StatusSeeOther)
				return
			}
			idx = parsed
		}
		err = s.meshtastic.SendChannelText(r.Context(), idx, text, sess.username)
	}
	if err != nil {
		s.logger.Warn("web: meshtastic send failed", "target", r.PostFormValue("target"), "error", err)
		flash := fmt.Sprintf(i18n.T(s.langFor(r), "meshtastic.send_failed"), err)
		http.Redirect(w, r, "/meshtastic?err="+url.QueryEscape(flash), http.StatusSeeOther)
		return
	}
	s.audit(sess.username, "meshtastic-send", text)
	http.Redirect(w, r, "/meshtastic?sent=1", http.StatusSeeOther)
}
