package web

import (
	"context"
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
	// Recipient is the 8-hex target of a tx direct message (empty for
	// broadcasts and rx rows); RecipientName is its display name.
	Recipient     string
	RecipientName string
	Text          string
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
	// Hops is the radio path length of the last packet (0 = direct).
	Hops int
	// Owner is the directory username whose registered node id matches
	// the node's id (empty when nobody registered it).
	Owner      string
	Lat        float64
	Lon        float64
	DistKM     float64
	BearingDeg float64
	Cardinal   string
	LastSeen   string
	// Stale marks nodes unheard for over the trace threshold: their
	// Trasa button is disabled and the row carries the inactivity age.
	Stale bool
	// Inactive is the human age ("3 godz. temu") shown next to the
	// last-seen time and in the disabled button's tooltip.
	Inactive string
	// LastSeenUnix is the last-heard instant in Unix milliseconds
	// (0 = never): the browser re-checks staleness so a button can
	// disable itself the moment the threshold passes without waiting
	// for the next fragment poll.
	LastSeenUnix int64
}

// meshtasticContactView is one directory user's Meshtastic node offered by
// the send-form picker.
type meshtasticContactView struct {
	Username string
	ID       string // 8-hex node id
}

// meshtasticPeerView is one conversation partner in the DM tab's filter
// dropdown: the raw node id and its display label (directory username,
// heard node name, or the bare id).
type meshtasticPeerView struct {
	ID    string
	Label string
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
	NavWebsite       bool
	NavNotifications bool
	NavHealth        bool
	NavConfig        bool
	NavCompose       bool
	NavEmcom         bool
	NavHelp          bool
	NavAccount       bool
	NavAudit         bool
	NavAPRS          bool
	NavMessages      bool
	NavGSM           bool
	NavMeshtastic    bool
	NavMeshMap       bool

	Tab string // map | msgs (the heard-node directory lives on the map tab)

	// map tab
	MeshEnabled bool
	SelfLat     float64
	SelfLon     float64
	SelfName    string
	CenterLat   float64
	CenterLon   float64
	OfflineMode bool
	ForceTiles  bool

	// messages tab
	Messages []meshtasticMessageView
	Dir      string
	// Peer is the selected conversation partner (8-hex node id; empty =
	// no filter). Peers feeds the DM tab's filter dropdown.
	Peer  string
	Peers []meshtasticPeerView
	// EmcomDir / EmcomTab carry the configured emcom-channel tab (empty
	// when the channel is not configured): the dir value ("ch1") and the
	// display label ("ch1 · SP9MOA" once the device names the channel).
	EmcomDir string
	EmcomTab string
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
		Header2:       s.DisplayHeader2(),
		Tagline:       s.DisplayTagline(),
		Version:       s.version,
		Commit:        s.commit,
		RepoURL:       repoURL,
		CSRF:          sess.csrf,
		Username:      sess.username,
		Role:          sess.role,
		NavMeshtastic: true,
		Tab:           "map",
		Dir:           "all",
		OfflineMode:   s.OfflineMode(),
		ForceTiles:    s.forceTiles.Load(),
	}
	if tab := r.URL.Query().Get("tab"); tab == "msgs" {
		v.Tab = "msgs"
	}
	v.Sent = r.URL.Query().Get("sent") == "1"
	v.Error = r.URL.Query().Get("err")
	w.Header().Set("Cache-Control", "no-store")

	// Both panels render at once; the tab strip toggles them client-side
	// (Tab only preselects the visible one, e.g. /meshtastic?tab=nodes).
	s.fillMeshtasticNodes(r, &v)
	s.fillMeshtasticMessages(r, &v)

	// Map tab data: our own node position when the device reported one;
	// without it the initial view falls back to the operational area
	// (the APRS territory center).
	if s.meshtastic != nil && s.meshtastic.Enabled() {
		v.MeshEnabled = true
		snap := s.meshtastic.Snapshot()
		v.SelfLat = snap.Self.Lat
		v.SelfLon = snap.Self.Lon
		v.SelfName = snap.Self.LongName
		v.CenterLat = snap.Self.Lat
		v.CenterLon = snap.Self.Lon
	}
	if v.CenterLat == 0 && v.CenterLon == 0 && s.aprs != nil && s.aprs.Enabled() {
		v.CenterLat = s.aprs.AreaLat()
		v.CenterLon = s.aprs.AreaLon()
	}
	s.renderL(w, r, "meshtastic", v)
}

func (s *Server) fillMeshtasticMessages(r *http.Request, v *meshtasticView) {
	if s.meshtasticMsgs == nil {
		return
	}
	// The configured emcom channel (meshtastic.emcom_channel in the
	// YAML) or the read-only watch channel (meshtastic.watch_channel)
	// owns a dedicated tab next to ch0; 0 = not configured.
	emcomIdx := 0
	if s.meshtastic != nil {
		emcomIdx = s.meshtastic.TabChannel()
	}
	emcomDir := ""
	if emcomIdx > 0 {
		emcomDir = "ch" + strconv.Itoa(emcomIdx)
	}
	switch d := r.URL.Query().Get("dir"); d {
	case "rx", "tx", "ch0":
		v.Dir = d
	default:
		if d == emcomDir && emcomDir != "" {
			v.Dir = d
		}
	}
	// Channel labels come from the hub's channel table ("ch<N>" until
	// the device names them). The primary channel (0) owns the "ch0"
	// tab; every other view (all, rx, tx) hides it.
	primary := "ch0"
	emcomLabel := ""
	if s.meshtastic != nil {
		primary = s.meshtastic.ChannelLabel(0)
		if emcomIdx > 0 {
			emcomLabel = s.meshtastic.ChannelLabel(emcomIdx)
		}
	}
	if emcomDir != "" {
		v.EmcomDir = emcomDir
		v.EmcomTab = emcomDir
		if emcomLabel != "" && emcomLabel != emcomDir {
			v.EmcomTab = emcomDir + " · " + emcomLabel
		}
	}
	// The peer dropdown filter (?peer=<8hex>): one conversation with a
	// specific Meshtastic user.
	if id := meshtasticNormalizeID(r.URL.Query().Get("peer")); id != "" {
		v.Peer = id
	}
	s.fillMeshtasticPeers(r.Context(), v)

	// The DM tab (default) shows direct messages only; the legacy rx/tx
	// dirs keep their direction filter on top. ch0 and the configured
	// emcom channel have their own tabs.
	filter := storage.MeshtasticMessageFilter{Channel: "dm"}
	switch v.Dir {
	case "ch0":
		filter = storage.MeshtasticMessageFilter{Channel: primary}
	case "rx", "tx":
		filter.Direction = v.Dir
	default:
		if v.Dir == emcomDir && emcomDir != "" {
			label := emcomLabel
			if label == "" {
				label = emcomDir
			}
			filter = storage.MeshtasticMessageFilter{Channel: label}
		}
	}
	filter.Peer = v.Peer
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
	// Resolve display names for senders so the history shows names
	// instead of raw node ids: the directory username wins, then the
	// heard node name from the device, then the bare id stays visible.
	var owners map[string]string
	if s.users != nil {
		owners, _ = s.users.MeshtasticOwners()
	}
	nodeNames := make(map[string]string, len(stored))
	if s.meshtastic != nil {
		for _, n := range s.meshtastic.Snapshot().Nodes {
			nodeNames[strings.ToLower(n.ID)] = n.Name
		}
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
			if view.SenderName == "" {
				view.SenderName = nodeNames[id]
			}
		}
		if rid := meshtasticNormalizeID(m.Recipient); rid != "" {
			view.Recipient = rid
			view.RecipientName = meshtasticOwnerFor(owners, rid)
			if view.RecipientName == "" {
				view.RecipientName = nodeNames[rid]
			}
		}
		v.Messages = append(v.Messages, view)
	}
}

// fillMeshtasticPeers builds the DM tab's conversation dropdown from the
// stored direct messages: directory usernames for registered node ids,
// heard node names for the rest, and the bare id as the last resort.
func (s *Server) fillMeshtasticPeers(ctx context.Context, v *meshtasticView) {
	if s.meshtasticMsgs == nil {
		return
	}
	ids, err := s.meshtasticMsgs.MeshtasticPeers(ctx)
	if err != nil {
		s.logger.Warn("web: mesh peers failed", "error", err)
		return
	}
	var owners map[string]string
	if s.users != nil {
		owners, _ = s.users.MeshtasticOwners()
	}
	names := make(map[string]string, len(ids))
	if s.meshtastic != nil {
		for _, n := range s.meshtastic.Snapshot().Nodes {
			names[strings.ToLower(n.ID)] = n.Name
		}
	}
	v.Peers = make([]meshtasticPeerView, 0, len(ids))
	for _, id := range ids {
		label := ""
		if u, ok := owners[id]; ok {
			label = u
		} else if name := names[id]; name != "" {
			label = name
		}
		if label == "" {
			label = "!" + id
		}
		v.Peers = append(v.Peers, meshtasticPeerView{ID: id, Label: label})
	}
	sort.Slice(v.Peers, func(i, j int) bool {
		return strings.ToLower(v.Peers[i].Label) < strings.ToLower(v.Peers[j].Label)
	})
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

func (s *Server) fillMeshtasticNodes(r *http.Request, v *meshtasticView) {
	lang := s.langFor(r)
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
	// The send form offers ONLY the device channel table (the node is
	// the source of truth): the default PRIMARY channel (0) is
	// receive-only by policy and never offered, and there is no
	// fallback — when the device has not reported its channels yet, the
	// list is simply empty.
	v.Channels = make([]meshtasticChannelOption, 0, len(v.Snap.Channels))
	for i, name := range v.Snap.Channels {
		if i == 0 {
			continue
		}
		v.Channels = append(v.Channels, meshtasticChannelOption{Idx: i, Label: name})
	}
	// Directory match: any user who registered a node's id labels it (as
	// the name when the node carries none, and next to the name
	// otherwise). The public home page never sees this.
	v.Nodes = make([]meshtasticNodeView, 0, len(v.Snap.Nodes))
	// Newest first: the directory reads like a live feed.
	ordered := append([]mesh.Node(nil), v.Snap.Nodes...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].LastSeen.After(ordered[j].LastSeen)
	})
	for _, n := range ordered {
		owner := meshtasticOwnerFor(owners, n.ID)
		name := n.Name
		if name == "" {
			name = owner
		}
		age := time.Since(n.LastSeen)
		stale := !n.LastSeen.IsZero() && age > meshtasticTraceStaleAfter
		seenUnix := int64(0)
		if !n.LastSeen.IsZero() {
			seenUnix = n.LastSeen.UnixMilli()
		}
		v.Nodes = append(v.Nodes, meshtasticNodeView{
			ID:    n.ID,
			Name:  name,
			Short: n.Short,
			Sends: n.Sends, Hops: n.Hops, Owner: owner,
			Lat:          n.Lat,
			Lon:          n.Lon,
			DistKM:       n.DistKM,
			BearingDeg:   n.BearingDeg,
			Cardinal:     cardinalDirection(n.BearingDeg),
			LastSeen:     n.LastSeen.Format("15:04:05"),
			Stale:        stale,
			Inactive:     meshInactiveText(lang, age),
			LastSeenUnix: seenUnix,
		})
	}
}

// meshtasticTraceStaleAfter is the inactivity threshold beyond which a
// node is marked stale and its traceroute button is disabled.
const meshtasticTraceStaleAfter = 30 * time.Minute

// meshInactiveText renders how long a node stayed unheard, as a full
// sentence ("nieaktywny od 4 dni") for badges, tooltips and the
// traceroute stale gate.
func meshInactiveText(lang string, d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return i18n.T(lang, "time.just_now")
	case d < time.Hour:
		return fmt.Sprintf(i18n.T(lang, "meshtastic.trace_inactive_m"), int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf(i18n.T(lang, "meshtastic.trace_inactive_h"), int(d.Hours()))
	default:
		return fmt.Sprintf(i18n.T(lang, "meshtastic.trace_inactive_d"), int(d.Hours()/24))
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
	s.fillMeshtasticNodes(r, &v)
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
		http.Redirect(w, r, "/meshtastic?tab=msgs&err="+url.QueryEscape(i18n.T(s.langFor(r), "meshtastic.text_required")), http.StatusSeeOther)
		return
	}
	var err error
	if r.PostFormValue("target") == "contact" {
		// The contact field accepts an 8-hex id, a node name or a short
		// name (the directory resolves the latter two) — typing a name
		// must not bounce with a validation error.
		id := meshtasticNormalizeID(r.PostFormValue("contact"))
		if id == "" && s.meshtastic != nil {
			id = s.meshtastic.ResolveNode(r.PostFormValue("contact"))
		}
		if id == "" {
			http.Redirect(w, r, "/meshtastic?tab=msgs&err="+url.QueryEscape(fmt.Sprintf(
				i18n.T(s.langFor(r), "meshtastic.contact_not_found"), r.PostFormValue("contact"))), http.StatusSeeOther)
			return
		}
		err = s.meshtastic.SendContactMessage(r.Context(), id, text, sess.username)
	} else {
		idx := 1
		if raw := strings.TrimSpace(r.PostFormValue("channel_idx")); raw != "" {
			parsed, parseErr := strconv.Atoi(raw)
			if parseErr != nil || parsed < 1 || parsed > 7 {
				http.Redirect(w, r, "/meshtastic?tab=msgs&err="+url.QueryEscape(i18n.T(s.langFor(r), "meshtastic.bad_channel")), http.StatusSeeOther)
				return
			}
			idx = parsed
		}
		err = s.meshtastic.SendChannelText(r.Context(), idx, text, sess.username)
	}
	if err != nil {
		s.logger.Warn("web: meshtastic send failed", "target", r.PostFormValue("target"), "error", err)
		flash := fmt.Sprintf(i18n.T(s.langFor(r), "meshtastic.send_failed"), err)
		http.Redirect(w, r, "/meshtastic?tab=msgs&err="+url.QueryEscape(flash), http.StatusSeeOther)
		return
	}
	s.audit(sess.username, "meshtastic-send", text)
	http.Redirect(w, r, "/meshtastic?tab=msgs&sent=1", http.StatusSeeOther)
}
