package web

import (
	"encoding/hex"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	mesh "github.com/szporwolik/WarnFlux/internal/meshcore"
)

// meshMessagesPageSize bounds one page of the admin mesh message history.
const meshMessagesPageSize = 100

// meshMessageView is one history row shown on the admin page.
type meshMessageView struct {
	Direction string
	Sender    string
	Channel   string
	Text      string
	At        time.Time
	// Key is the 12-hex pubkey prefix when the row can prefill the send
	// form (direct messages), empty otherwise.
	Key string
}

// meshNodeView is one heard neighbour shown on the admin page.
type meshNodeView struct {
	PubKey     string
	Short      string
	Name       string
	Type       string
	Hops       int
	Lat        float64
	Lon        float64
	DistKM     float64
	BearingDeg float64
	Cardinal   string
	LastSeen   string
}

// meshContactView is one directory user's MeshCore key offered by the
// send-form picker.
type meshContactView struct {
	Username string
	Prefix   string // first 12 hex chars of the public key
}

// meshView is the admin MeshCore page model.
type meshView struct {
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
	NavCompose       bool
	NavEmcom         bool
	NavAccount       bool
	NavAudit         bool
	NavMessages      bool
	NavMeshcore      bool

	Tab string // messages | nodes

	// messages tab
	Messages []meshMessageView
	Dir      string
	Page     int
	Pages    int
	From     int
	To       int
	Total    int

	// nodes tab
	Snap     mesh.Snapshot
	Nodes    []meshNodeView
	Contacts []meshContactView
}

// handleMeshcorePage renders the admin MeshCore page.
func (s *Server) handleMeshcorePage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	v := meshView{
		AppTitle:    s.cfg.Title,
		Name:        s.displayName(),
		Header1:     s.displayHeader1(),
		Header2:     s.cfg.Header2,
		Tagline:     s.cfg.Tagline,
		Version:     s.version,
		Commit:      s.commit,
		RepoURL:     repoURL,
		CSRF:        sess.csrf,
		Username:    sess.username,
		Role:        sess.role,
		NavMeshcore: true,
		Tab:         "messages",
		Dir:         "all",
	}
	if tab := r.URL.Query().Get("tab"); tab == "nodes" {
		v.Tab = "nodes"
	}
	w.Header().Set("Cache-Control", "no-store")

	// Both panels render at once; the tab strip toggles them client-side
	// (Tab only preselects the visible one, e.g. /meshcore?tab=nodes).
	s.fillMeshNodes(&v)
	s.fillMeshMessages(r, &v)
	s.renderL(w, r, "meshcore", v)
}

func (s *Server) fillMeshMessages(r *http.Request, v *meshView) {
	if s.meshMsgs == nil {
		return
	}
	switch d := r.URL.Query().Get("dir"); d {
	case "rx", "tx":
		v.Dir = d
	}
	dirFilter := ""
	if v.Dir != "all" {
		dirFilter = v.Dir
	}
	total, err := s.meshMsgs.CountMeshMessages(r.Context(), dirFilter)
	if err != nil {
		s.logger.Warn("web: mesh messages count failed", "error", err)
		return
	}
	v.Total = total
	pages := (total + meshMessagesPageSize - 1) / meshMessagesPageSize
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
		v.From = (page-1)*meshMessagesPageSize + 1
		v.To = page * meshMessagesPageSize
		if v.To > total {
			v.To = total
		}
	}
	stored, err := s.meshMsgs.ListMeshMessages(r.Context(), dirFilter, meshMessagesPageSize, (page-1)*meshMessagesPageSize)
	if err != nil {
		s.logger.Warn("web: mesh messages list failed", "error", err)
		return
	}
	v.Messages = make([]meshMessageView, 0, len(stored))
	for _, m := range stored {
		view := meshMessageView{
			Direction: m.Direction,
			Sender:    m.Sender,
			Channel:   m.Channel,
			Text:      m.Text,
			At:        m.At,
		}
		if b, hexErr := hex.DecodeString(m.Sender); hexErr == nil && len(b) == 6 {
			view.Key = strings.ToLower(m.Sender)
		}
		v.Messages = append(v.Messages, view)
	}
}

func (s *Server) fillMeshNodes(v *meshView) {
	// Contact picker comes from the directory (independent of the hub).
	var owners map[string]string
	if s.users != nil {
		owners, _ = s.users.MeshKeyOwners()
	}
	v.Contacts = make([]meshContactView, 0, len(owners))
	for key, username := range owners {
		prefix := key
		if len(prefix) > 12 {
			prefix = prefix[:12]
		}
		v.Contacts = append(v.Contacts, meshContactView{Username: username, Prefix: prefix})
	}
	sort.Slice(v.Contacts, func(i, j int) bool {
		if v.Contacts[i].Username != v.Contacts[j].Username {
			return v.Contacts[i].Username < v.Contacts[j].Username
		}
		return v.Contacts[i].Prefix < v.Contacts[j].Prefix
	})

	if s.mesh == nil {
		return
	}
	v.Snap = s.mesh.Snapshot()
	// Resolve names for nodes whose adverts carry none: any directory
	// user who registered that public key labels the node.
	v.Nodes = make([]meshNodeView, 0, len(v.Snap.Nodes))
	for _, n := range v.Snap.Nodes {
		short := n.PubKey
		if len(short) > 12 {
			short = short[:12]
		}
		typ := "node"
		switch n.Type {
		case 1:
			typ = "chat"
		case 2:
			typ = "repeater"
		case 3:
			typ = "room"
		}
		name := n.Name
		if name == "" {
			name = meshOwnerFor(owners, n.PubKey)
		}
		v.Nodes = append(v.Nodes, meshNodeView{
			PubKey:     n.PubKey,
			Short:      short,
			Name:       name,
			Type:       typ,
			Hops:       n.Hops,
			Lat:        n.Lat,
			Lon:        n.Lon,
			DistKM:     n.DistKM,
			BearingDeg: n.BearingDeg,
			Cardinal:   cardinalDirection(n.BearingDeg),
			LastSeen:   n.LastSeen.Format("15:04:05"),
		})
	}
}

// meshOwnerFor resolves the directory username for a heard node: the
// registered key may be the full 64-hex public key or its 12-hex short
// prefix, so an exact match wins, then a prefix match either way.
func meshOwnerFor(owners map[string]string, pubKey string) string {
	if u, ok := owners[pubKey]; ok {
		return u
	}
	for key, u := range owners {
		if strings.HasPrefix(key, pubKey) || strings.HasPrefix(pubKey, key) {
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

// handlePartialMeshcore serves the nodes-tab fragment (polled by the page).
func (s *Server) handlePartialMeshcore(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	v := meshView{
		AppTitle: s.cfg.Title,
		Header1:  s.displayHeader1(),
		CSRF:     sess.csrf,
	}
	s.fillMeshNodes(&v)
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "mesh_nodes", v)
}

// handleMeshcoreAdvert triggers a manual flood advert on the node.
func (s *Server) handleMeshcoreAdvert(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || !csrfOK(r.PostFormValue("csrf"), sess.csrf) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	if s.mesh == nil {
		http.Error(w, "meshcore hub not configured", http.StatusBadGateway)
		return
	}
	kind := mesh.AdvertFlood
	if r.PostFormValue("kind") == "zerohop" {
		kind = mesh.AdvertZeroHop
	}
	if err := s.mesh.SendAdvert(kind); err != nil {
		s.logger.Warn("web: meshcore advert failed", "error", err)
		http.Error(w, "advert failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	s.audit(sess.username, "meshcore-advert", "manual advert")
	http.Redirect(w, r, "/meshcore?tab=nodes", http.StatusSeeOther)
}

// handleMeshcoreSend sends one message on the channel (group) or to a
// contact prefix (user) from the admin panel.
func (s *Server) handleMeshcoreSend(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || sess == nil || !csrfOK(r.PostFormValue("csrf"), sess.csrf) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	if s.mesh == nil {
		http.Error(w, "meshcore hub not configured", http.StatusBadGateway)
		return
	}
	text := strings.TrimSpace(r.PostFormValue("text"))
	if text == "" {
		http.Error(w, "message text is required", http.StatusBadRequest)
		return
	}
	var err error
	if r.PostFormValue("target") == "contact" {
		prefix := strings.TrimPrefix(strings.TrimSpace(r.PostFormValue("contact")), "0x")
		if _, hexErr := hex.DecodeString(prefix); hexErr != nil || len(prefix) != 12 {
			http.Error(w, "contact prefix must be 12 hex characters", http.StatusBadRequest)
			return
		}
		// Prefer the full key from the directory: the hub can then add the
		// contact on the device when it is missing there.
		addr := prefix
		if s.users != nil {
			if owners, ownersErr := s.users.MeshKeyOwners(); ownersErr == nil {
				for key := range owners {
					if strings.HasPrefix(key, prefix) {
						addr = key
						break
					}
				}
			}
		}
		err = s.mesh.SendContactMessage(addr, text)
	} else {
		err = s.mesh.SendChannelMessage(text)
	}
	if err != nil {
		s.logger.Warn("web: meshcore send failed", "target", r.PostFormValue("target"), "error", err)
		http.Error(w, "send failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	s.audit(sess.username, "meshcore-send", text)
	http.Redirect(w, r, "/meshcore", http.StatusSeeOther)
}
