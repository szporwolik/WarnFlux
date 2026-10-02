package web

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/szporwolik/WarnFlux/internal/meshtastic"
)

// meshNodeMapView is the public JSON shape of one heard Meshtastic node
// with a position, served to the home map.
type meshNodeMapView struct {
	ID         string   `json:"id"`
	Name       string   `json:"name"`
	ShortName  string   `json:"short_name"`
	Sends      []string `json:"sends"`
	Latitude   float64  `json:"latitude"`
	Longitude  float64  `json:"longitude"`
	LastSeen   string   `json:"last_seen"`
	DistanceKM float64  `json:"distance_km"`
}

// meshNodeNoPosView is one heard node without a position: the home page
// shows these as a badge list below the located-node cards.
type meshNodeNoPosView struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	ShortName string   `json:"short_name"`
	Sends     []string `json:"sends"`
	LastSeen  string   `json:"last_seen"`
}

// handleMeshtasticStations serves the public node list for the home map:
// located nodes inside the operational ring (the APRS area center and
// radius) plus, separately, the heard nodes that carry no position —
// the home page renders those as a badge list below the node cards.
func (s *Server) handleMeshtasticStations(w http.ResponseWriter, r *http.Request) {
	if s.meshtastic == nil || !s.meshtastic.Enabled() {
		http.Error(w, "meshtastic disabled", http.StatusNotFound)
		return
	}
	var owners map[string]string
	if s.users != nil {
		owners, _ = s.users.MeshtasticOwners()
	}
	ringLat, ringLon, ringR := 0.0, 0.0, 0.0
	if s.aprs != nil && s.aprs.Enabled() {
		ringLat, ringLon, ringR = s.aprs.AreaLat(), s.aprs.AreaLon(), s.aprs.AreaRadius()
	}
	snap := s.meshtastic.Snapshot()
	nodes := make([]meshNodeMapView, 0, 8)
	nopos := make([]meshNodeNoPosView, 0, 8)
	for _, n := range snap.Nodes {
		// The public map shows the live network: nodes unheard for
		// longer than the node TTL stay only in the admin directory.
		if snap.NodeTTL > 0 && time.Since(n.LastSeen) > snap.NodeTTL {
			continue
		}
		name := n.Name
		if name == "" {
			name = meshtasticOwnerFor(owners, n.ID)
		}
		seen := n.LastSeen.UTC().Format(time.RFC3339)
		if n.Lat == 0 && n.Lon == 0 {
			nopos = append(nopos, meshNodeNoPosView{
				ID: n.ID, Name: name, ShortName: n.Short, Sends: n.Sends, LastSeen: seen,
			})
			continue
		}
		d := meshtastic.DistanceKM(ringLat, ringLon, n.Lat, n.Lon)
		if ringR > 0 && d > ringR {
			// Outside the operation ring: the map is limited to the area
			// we serve, so the node is deliberately not shown.
			continue
		}
		nodes = append(nodes, meshNodeMapView{
			ID: n.ID, Name: name, ShortName: n.Short, Sends: n.Sends,
			Latitude: n.Lat, Longitude: n.Lon,
			LastSeen: seen, DistanceKM: d,
		})
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].DistanceKM != nodes[j].DistanceKM {
			return nodes[i].DistanceKM < nodes[j].DistanceKM
		}
		return nodes[i].Name < nodes[j].Name
	})
	sort.Slice(nopos, func(i, j int) bool {
		if nopos[i].Name != nopos[j].Name {
			return nopos[i].Name < nopos[j].Name
		}
		return nopos[i].ID < nopos[j].ID
	})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(map[string]any{"nodes": nodes, "nopos": nopos}); err != nil {
		s.logger.Warn("web: encode meshtastic stations failed", "error", err)
	}
}
