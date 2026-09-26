package web

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"
)

// aqStationView is the public JSON shape of one air-quality station
// snapshot served to the home map (/api/airquality).
type aqStationView struct {
	StationCode    string            `json:"station_code"`
	StationName    string            `json:"station_name"`
	Latitude       float64           `json:"latitude"`
	Longitude      float64           `json:"longitude"`
	IndexLevelID   *int              `json:"index_level_id,omitempty"`
	IndexLevelName string            `json:"index_level_name"`
	GeneratedAt    string            `json:"generated_at"`
	Pollutants     []aqPollutantView `json:"pollutants,omitempty"`
}

// aqPollutantView is one pollutant's index level within a station
// snapshot.
type aqPollutantView struct {
	Code      string `json:"code"`
	LevelID   *int   `json:"level_id,omitempty"`
	LevelName string `json:"level_name"`
}

// handleAirQuality serves the public air-quality station layer of the
// home map: the latest air_quality informational snapshot per station
// from the retained MQTT state (newest received wins).
func (s *Server) handleAirQuality(w http.ResponseWriter, r *http.Request) {
	type rec struct {
		view aqStationView
		at   time.Time
	}
	best := make(map[string]rec, 16)
	for _, e := range s.st.Snapshot().Info {
		if e.Kind != "air_quality" || len(e.Payload) == 0 {
			continue
		}
		var v aqStationView
		if err := json.Unmarshal(e.Payload, &v); err != nil || v.StationCode == "" {
			continue
		}
		if cur, ok := best[v.StationCode]; !ok || e.ReceivedAt.After(cur.at) {
			best[v.StationCode] = rec{view: v, at: e.ReceivedAt}
		}
	}
	stations := make([]aqStationView, 0, len(best))
	for _, r := range best {
		stations = append(stations, r.view)
	}
	sort.Slice(stations, func(i, j int) bool {
		return stations[i].StationName < stations[j].StationName
	})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{"stations": stations})
}
