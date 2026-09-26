package web

import (
	"encoding/json"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/severity"
)

// publicHazardView is one active hazard as shown on the public home page:
// severity, headline, source, event, areas and times — no admin surface.
type publicHazardView struct {
	Severity    string
	Headline    string
	Event       string
	Source      string
	Areas       string
	EffectiveAt *time.Time
	ExpiresAt   *time.Time
	UpdatedAt   time.Time
}

// homeView is the PUBLIC home page model. The login form is deliberately
// not part of it: the sign-in lives behind the top-right icon button.
type homeView struct {
	AppTitle string
	Header1  string
	Header2  string
	Tagline  string
	// About is operator-authored content (config file): rendered with
	// line breaks preserved and a deliberately small HTML surface so
	// links work.
	About        template.HTML
	Disclaimer   string
	Version      string
	Commit       string
	RepoURL      string
	LoggedIn     bool
	Username     string
	Landing      string
	LandingLabel string

	ActiveCount int
	Hazards     []publicHazardView

	// MainCount counts the important (moderate and above) hazards shown
	// in the always-rendered Important section.
	MainCount int

	// MinorCount and MinorHazards carry the low-priority tail (minor and
	// unknown severity) shown in a collapsed section on the home page.
	MinorCount   int
	MinorHazards []publicHazardView

	// AprsEnabled turns the second home tab into the APRS neighbourhood
	// map: centered on our locator, range circle, radar overlay and the
	// stations held in the MQTT state.
	AprsEnabled   bool
	AprsCenterLat float64
	AprsCenterLon float64
	AprsOwnLat    float64
	AprsOwnLon    float64
	AprsRadiusKM  float64
	AprsCallsign  string

	// Channels carries the friendly public view of the configured
	// delivery channels (one row per medium, no technical detail).
	Channels []publicChannelView
}

// publicChannelView is one delivery medium shown to the public on the
// home page: icon, name and a one-line plain-language description.
type publicChannelView struct {
	Icon        string
	Name        string
	Description string
}

// mapEventView is the public JSON shape of one geo-located active hazard
// served to the home map (/api/events).
type mapEventView struct {
	EventKey    string     `json:"event_key"`
	Source      string     `json:"source"`
	Severity    string     `json:"severity"`
	Headline    string     `json:"headline"`
	Event       string     `json:"event"`
	Description string     `json:"description,omitempty"`
	Latitude    float64    `json:"latitude"`
	Longitude   float64    `json:"longitude"`
	EffectiveAt *time.Time `json:"effective_at,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// handleEventsMap serves the public JSON of active hazards that carry
// coordinates, so the home map can draw them as icons.
func (s *Server) handleEventsMap(w http.ResponseWriter, r *http.Request) {
	events := make([]mapEventView, 0, 16)
	for _, h := range s.st.Snapshot().Hazards {
		if h.Latitude == nil || h.Longitude == nil {
			continue
		}
		events = append(events, mapEventView{
			EventKey:    h.EventKey,
			Source:      h.Source,
			Severity:    h.Severity,
			Headline:    h.Headline,
			Event:       h.Event,
			Description: h.Description,
			Latitude:    *h.Latitude,
			Longitude:   *h.Longitude, EffectiveAt: h.EffectiveAt, ExpiresAt: h.ExpiresAt,
			UpdatedAt: h.UpdatedAt,
		})
	}
	sort.Slice(events, func(i, j int) bool {
		ri, _ := severity.Rank(events[i].Severity)
		rj, _ := severity.Rank(events[j].Severity)
		if ri != rj {
			return ri > rj
		}
		return events[i].UpdatedAt.After(events[j].UpdatedAt)
	})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{"events": events})
}

// handleHome renders the public landing page: header1/header2 plus the
// current active hazards. No session is required; a logged-in operator
// sees a Dashboard entry in the header instead of the sign-in icon.
func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	v := s.buildHomeView()
	if sess := s.sessions.currentSession(r); sess != nil {
		v.LoggedIn = true
		v.Username = sess.username
		v.Landing = "/dashboard"
		v.LandingLabel = "Dashboard"
	}
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "home", v)
}

// handlePartialHome serves the public auto-refresh fragment of the active
// hazard list (the home page polls it).
func (s *Server) handlePartialHome(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "home_alerts_section", s.buildHomeView())
}

// buildHomeView assembles the public view from the mirrored MQTT state,
// most severe first, then newest.
func (s *Server) buildHomeView() homeView {
	v := homeView{
		AppTitle:   s.cfg.Title,
		Header1:    s.displayHeader1(),
		Header2:    s.cfg.Header2,
		Tagline:    s.cfg.Tagline,
		About:      template.HTML(s.cfg.About),
		Disclaimer: s.cfg.Disclaimer,
		Version:    s.version,
		Commit:     s.commit,
		RepoURL:    repoURL,
	}

	snap := s.st.Snapshot()
	v.ActiveCount = len(snap.Hazards)
	v.Hazards = make([]publicHazardView, 0, len(snap.Hazards))
	v.MinorHazards = make([]publicHazardView, 0)
	minorRank, _ := severity.Rank(severity.Minor)
	for _, h := range snap.Hazards {
		view := publicHazardView{
			Severity:    h.Severity,
			Headline:    h.Headline,
			Event:       h.Event,
			Source:      h.Source,
			Areas:       strings.Join(h.Areas, ", "),
			EffectiveAt: h.EffectiveAt,
			ExpiresAt:   h.ExpiresAt,
			UpdatedAt:   h.UpdatedAt,
		}
		// Moderate and above stay up front; minor/unknown drop into the
		// collapsed low-priority section so routine road-info noise does
		// not push real communications down the page.
		if r, _ := severity.Rank(h.Severity); r > minorRank {
			v.Hazards = append(v.Hazards, view)
		} else {
			v.MinorHazards = append(v.MinorHazards, view)
		}
	}
	v.MinorCount = len(v.MinorHazards)
	v.MainCount = len(v.Hazards)
	sortHazards(v.Hazards)
	sortHazards(v.MinorHazards)

	if s.aprs != nil && s.aprs.Enabled() {
		v.AprsEnabled = true
		// The map centers and the range circle follow the OPERATIONAL
		// AREA (config territory center/radius); the station locator dot
		// stays at the antenna position.
		v.AprsCenterLat = s.aprs.AreaLat()
		v.AprsCenterLon = s.aprs.AreaLon()
		v.AprsOwnLat = s.aprs.OwnLat()
		v.AprsOwnLon = s.aprs.OwnLon()
		v.AprsRadiusKM = s.aprs.AreaRadius()
		v.AprsCallsign = s.aprs.Callsign()
	}
	if s.actions != nil {
		v.Channels = publicChannels(s.actions.Statuses())
	}
	return v
}

// sortHazards orders a hazard slice most severe first; within one
// severity, newest first.
func sortHazards(hazards []publicHazardView) {
	sort.Slice(hazards, func(i, j int) bool {
		ri, _ := severity.Rank(hazards[i].Severity)
		rj, _ := severity.Rank(hazards[j].Severity)
		if ri != rj {
			return ri > rj
		}
		return hazards[i].UpdatedAt.After(hazards[j].UpdatedAt)
	})
}

// handleAPRSStations serves the public station list for the home-page map:
// the merged retained MQTT state, newest last-heard documents excluded when
// the APRS hub is disabled (the map tab is not rendered then either).
// Weather stations (symbol '_') are excluded: their readings ride on the
// weather layer of the same map instead of cluttering the station list.
func (s *Server) handleAPRSStations(w http.ResponseWriter, r *http.Request) {
	if s.aprs == nil || !s.aprs.Enabled() {
		http.Error(w, "aprs disabled", http.StatusNotFound)
		return
	}
	stations := s.aprs.Stations()
	out := stations[:0:0]
	for _, doc := range stations {
		if doc.Symbol == "_" {
			continue
		}
		out = append(out, doc)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		s.logger.Warn("web: encode aprs stations failed", "error", err)
	}
}

// ---- public weather reports (combined map layer) -----------------------

// weatherReportView is one current weather report for the public home
// map: internet providers (retained info topics) and APRS weather
// stations heard in range.
type weatherReportView struct {
	Provider         string   `json:"provider"`
	Name             string   `json:"name"`
	Latitude         float64  `json:"latitude"`
	Longitude        float64  `json:"longitude"`
	Via              string   `json:"via"` // "internet" or "aprs"
	Condition        string   `json:"condition"`
	TemperatureC     *float64 `json:"temperature_c,omitempty"`
	HumidityPct      *float64 `json:"humidity_pct,omitempty"`
	WindSpeedKmh     *float64 `json:"wind_speed_kmh,omitempty"`
	WindDirectionDeg *float64 `json:"wind_direction_deg,omitempty"`
	WindGustsKmh     *float64 `json:"wind_gusts_kmh,omitempty"`
	PressureHpa      *float64 `json:"pressure_hpa,omitempty"`
	RadiationUSvh    *float64 `json:"radiation_usv_h,omitempty"`
	RadiationCPM     *float64 `json:"radiation_cpm,omitempty"`
	GeneratedAt      string   `json:"generated_at,omitempty"`
}

// weatherForecastDayView is one day of a multi-day forecast.
type weatherForecastDayView struct {
	Date               string   `json:"date"`
	Condition          string   `json:"condition"`
	TemperatureMaxC    *float64 `json:"temperature_max_c,omitempty"`
	TemperatureMinC    *float64 `json:"temperature_min_c,omitempty"`
	PrecipitationSumMm *float64 `json:"precipitation_sum_mm,omitempty"`
	WindSpeedMaxKmh    *float64 `json:"wind_speed_max_kmh,omitempty"`
}

// weatherForecastView is one multi-day forecast (one per provider and
// location, the retained MQTT documents).
type weatherForecastView struct {
	Provider string                   `json:"provider"`
	Name     string                   `json:"name"`
	Daily    []weatherForecastDayView `json:"daily"`
}

type weatherAPIView struct {
	Reports   []weatherReportView   `json:"reports"`
	Forecasts []weatherForecastView `json:"forecasts"`
}

// handleWeather serves the public weather-layer data of the combined map:
// current reports from every internet provider and every APRS weather
// station in range, plus the multi-day forecasts held in the retained
// MQTT info topics.
func (s *Server) handleWeather(w http.ResponseWriter, r *http.Request) {
	view := weatherAPIView{
		Reports:   []weatherReportView{},
		Forecasts: []weatherForecastView{},
	}

	// APRS weather stations first: the hub merges their reports into the
	// station state (with the merged position, so positionless reports
	// still map correctly). Their names take precedence over the echo of
	// our own info topics, which the receiver ingests back off the broker.
	seenReports := make(map[string]bool)
	if s.aprs != nil && s.aprs.Enabled() {
		for _, doc := range s.aprs.Stations() {
			if doc.Weather == nil {
				continue
			}
			lat, lon := 0.0, 0.0
			if doc.Position != nil {
				lat, lon = doc.Position.Latitude, doc.Position.Longitude
			}
			seenReports["aprs:"+doc.Callsign] = true
			view.Reports = append(view.Reports, weatherReportView{
				Provider:         "APRS",
				Name:             doc.Callsign,
				Latitude:         lat,
				Longitude:        lon,
				Via:              "aprs",
				Condition:        aprsWeatherCondition(doc.Weather),
				TemperatureC:     doc.Weather.TemperatureC,
				HumidityPct:      doc.Weather.HumidityPct,
				WindSpeedKmh:     doc.Weather.WindSpeedKmh,
				WindDirectionDeg: doc.Weather.WindDirectionDeg,
				WindGustsKmh:     doc.Weather.WindGustsKmh,
				PressureHpa:      doc.Weather.PressureHpa,
				RadiationUSvh:    doc.Weather.RadiationUSvh,
				RadiationCPM:     doc.Weather.RadiationCPM,
				GeneratedAt:      doc.Weather.GeneratedAt,
			})
		}
	}

	// Internet providers: the dashboard state mirrors the retained
	// <prefix>/info/<source>/<producer>/<key>/weather topics.
	seenForecasts := make(map[string]bool)
	for _, e := range s.st.Snapshot().Weather {
		ww := e.Weather
		if ww == nil {
			continue
		}
		name := ww.LocationName
		if name == "" {
			name = ww.LocationID
		}
		// Skip the echo of our own APRS info topics: the hub station
		// entry above is the authoritative one (position + via=aprs).
		if seenReports["aprs:"+name] {
			continue
		}
		repKey := e.ProducerID + ":" + ww.LocationID
		if !seenReports[repKey] {
			seenReports[repKey] = true
			view.Reports = append(view.Reports, weatherReportView{
				Provider:         ww.ProviderName,
				Name:             name,
				Latitude:         ww.Latitude,
				Longitude:        ww.Longitude,
				Via:              "internet",
				Condition:        ww.Condition,
				TemperatureC:     ww.TemperatureC,
				HumidityPct:      ww.HumidityPct,
				WindSpeedKmh:     ww.WindSpeedKmh,
				WindDirectionDeg: ww.WindDirectionDeg,
				WindGustsKmh:     ww.WindGustsKmh,
				PressureHpa:      ww.PressureMSLHpa,
				RadiationUSvh:    ww.RadiationUSvh,
				RadiationCPM:     ww.RadiationCPM,
				GeneratedAt:      ww.GeneratedAt.UTC().Format(time.RFC3339),
			})
		}
		if len(ww.Daily) > 0 && !seenForecasts[repKey] {
			seenForecasts[repKey] = true
			days := make([]weatherForecastDayView, 0, len(ww.Daily))
			for _, d := range ww.Daily {
				days = append(days, weatherForecastDayView{
					Date:               d.Date,
					Condition:          d.Condition,
					TemperatureMaxC:    d.TemperatureMaxC,
					TemperatureMinC:    d.TemperatureMinC,
					PrecipitationSumMm: d.PrecipitationSumMm,
					WindSpeedMaxKmh:    d.WindSpeedMaxKmh,
				})
			}
			view.Forecasts = append(view.Forecasts, weatherForecastView{Provider: ww.ProviderName, Name: name, Daily: days})
		}
	}

	sort.Slice(view.Reports, func(i, j int) bool { return view.Reports[i].Name < view.Reports[j].Name })
	sort.Slice(view.Forecasts, func(i, j int) bool { return view.Forecasts[i].Name < view.Forecasts[j].Name })

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(view); err != nil {
		s.logger.Warn("web: encode weather failed", "error", err)
	}
}

// aprsWeatherCondition maps an APRS weather observation to the canonical
// condition enum (rain/snow when the station reports precipitation).
func aprsWeatherCondition(w *aprs.WeatherReport) string {
	switch {
	case w == nil:
		return "unknown"
	case w.Snow24hCm != nil && *w.Snow24hCm > 0:
		return "snow"
	case w.Rain1hMm != nil && *w.Rain1hMm > 0, w.Rain24hMm != nil && *w.Rain24hMm > 0:
		return "rain"
	default:
		return "unknown"
	}
}

// aircraftTrailView is one recorded position of one aircraft.
type aircraftTrailView struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	At        int64   `json:"at"` // unix seconds
}

// aircraftView is one aircraft in the area of interest.
type aircraftView struct {
	Icao24         string              `json:"icao24"`
	Callsign       string              `json:"callsign,omitempty"`
	Category       string              `json:"category,omitempty"`
	Latitude       float64             `json:"latitude"`
	Longitude      float64             `json:"longitude"`
	AltitudeM      *float64            `json:"altitude_m,omitempty"`
	OnGround       bool                `json:"on_ground"`
	SpeedKmh       *float64            `json:"speed_kmh,omitempty"`
	TrackDeg       *float64            `json:"track_deg,omitempty"`
	VerticalRateMS *float64            `json:"vertical_rate_m_s,omitempty"`
	SeenAt         int64               `json:"seen_at"`
	Trail          []aircraftTrailView `json:"trail,omitempty"`
}

// handleAircraft serves the public aircraft layer: the newest retained
// <prefix>/info/adsb/<producer>/area/aircraft snapshots, merged across
// producers with the newest observation per aircraft winning.
func (s *Server) handleAircraft(w http.ResponseWriter, r *http.Request) {
	type wireSnap struct {
		Type          string         `json:"type"`
		SchemaVersion int            `json:"schema_version"`
		GeneratedAt   time.Time      `json:"generated_at"`
		Provider      string         `json:"provider"`
		Aircraft      []aircraftView `json:"aircraft"`
	}

	snaps := make([]wireSnap, 0, 2)
	for _, e := range s.st.Snapshot().Info {
		if e.Kind != "aircraft" || len(e.Payload) == 0 {
			continue
		}
		var ws wireSnap
		if err := json.Unmarshal(e.Payload, &ws); err != nil {
			s.logger.Warn("web: aircraft info payload invalid", "topic", e.Topic, "error", err)
			continue
		}
		if ws.Type != "aircraft" {
			continue
		}
		snaps = append(snaps, ws)
	}
	// Newest snapshot first.
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].GeneratedAt.After(snaps[j].GeneratedAt) })

	view := struct {
		GeneratedAt string         `json:"generated_at,omitempty"`
		Aircraft    []aircraftView `json:"aircraft"`
	}{Aircraft: []aircraftView{}}

	byHex := make(map[string]aircraftView)
	for _, snap := range snaps {
		for _, a := range snap.Aircraft {
			if _, exists := byHex[a.Icao24]; !exists {
				byHex[a.Icao24] = a
			}
		}
		if view.GeneratedAt == "" {
			view.GeneratedAt = snap.GeneratedAt.UTC().Format(time.RFC3339)
		}
	}
	for _, a := range byHex {
		view.Aircraft = append(view.Aircraft, a)
	}
	sort.Slice(view.Aircraft, func(i, j int) bool {
		a, b := view.Aircraft[i], view.Aircraft[j]
		if a.Callsign != b.Callsign {
			return a.Callsign < b.Callsign
		}
		return a.Icao24 < b.Icao24
	})

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(view); err != nil {
		s.logger.Warn("web: encode aircraft failed", "error", err)
	}
}
