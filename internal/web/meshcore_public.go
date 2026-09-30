package web

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/szporwolik/WarnFlux/internal/meshcore"
)

// meshNodeMapView is the public JSON shape of one heard MeshCore node
// with a position, served to the home map.
type meshNodeMapView struct {
	Key        string  `json:"key"`
	Name       string  `json:"name"`
	Type       string  `json:"type"`
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
	Hops       int     `json:"hops"`
	LastSeen   string  `json:"last_seen"`
	DistanceKM float64 `json:"distance_km"`
}

// meshNodeNoPosView is one heard node without a position: the home page
// shows these as a badge list below the located-node cards.
type meshNodeNoPosView struct {
	Key      string `json:"key"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Hops     int    `json:"hops"`
	LastSeen string `json:"last_seen"`
}

// meshNodeTypeName maps the device type byte onto the friendly label.
func meshNodeTypeName(t byte) string {
	switch t {
	case 1:
		return "chat"
	case 2:
		return "repeater"
	case 3:
		return "room"
	}
	return "node"
}

// handleMeshcoreStations serves the public node list for the home map:
// located nodes inside the operational ring (the APRS area center and
// radius) plus, separately, the heard nodes that carry no position —
// the home page renders those as a badge list below the node cards.
func (s *Server) handleMeshcoreStations(w http.ResponseWriter, r *http.Request) {
	if s.mesh == nil || !s.mesh.Enabled() {
		http.Error(w, "meshcore disabled", http.StatusNotFound)
		return
	}
	var owners map[string]string
	if s.users != nil {
		owners, _ = s.users.MeshKeyOwners()
	}
	ringLat, ringLon, ringR := 0.0, 0.0, 0.0
	if s.aprs != nil && s.aprs.Enabled() {
		ringLat, ringLon, ringR = s.aprs.AreaLat(), s.aprs.AreaLon(), s.aprs.AreaRadius()
	}
	nodes := make([]meshNodeMapView, 0, 8)
	nopos := make([]meshNodeNoPosView, 0, 8)
	for _, n := range s.mesh.Snapshot().Nodes {
		typ := meshNodeTypeName(n.Type)
		name := n.Name
		if name == "" {
			name = owners[n.PubKey]
		}
		seen := n.LastSeen.UTC().Format(time.RFC3339)
		if n.Lat == 0 && n.Lon == 0 {
			nopos = append(nopos, meshNodeNoPosView{
				Key: n.PubKey, Name: name, Type: typ, Hops: n.Hops, LastSeen: seen,
			})
			continue
		}
		d := meshcore.DistanceKM(ringLat, ringLon, n.Lat, n.Lon)
		if ringR > 0 && d > ringR {
			// Outside the operation ring: the map is limited to the area
			// we serve, so the node is deliberately not shown.
			continue
		}
		nodes = append(nodes, meshNodeMapView{
			Key: n.PubKey, Name: name, Type: typ,
			Latitude: n.Lat, Longitude: n.Lon,
			Hops: n.Hops, LastSeen: seen, DistanceKM: d,
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
		return nopos[i].Key < nopos[j].Key
	})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(map[string]any{"nodes": nodes, "nopos": nopos}); err != nil {
		s.logger.Warn("web: encode meshcore stations failed", "error", err)
	}
}
