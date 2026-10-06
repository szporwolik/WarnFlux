package web

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/severity"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// publicHazardView is one active hazard as shown on the public home page:
// severity, headline, source, event, areas and times — no admin surface.
// Description/Instruction/Status/Urgency/Certainty ride along for the
// client-side detail popup opened by clicking a card.
type publicHazardView struct {
	EventKey    string
	Severity    string
	Headline    string
	Event       string
	Source      string
	Areas       string
	Description string
	Instruction string
	Status      string
	Urgency     string
	Certainty   string
	EffectiveAt *time.Time
	ExpiresAt   *time.Time
	UpdatedAt   time.Time
}

// homeSeverityGroup is one severity bucket on the public home page: the
// canonical level, its hazards and whether the group renders expanded.
type homeSeverityGroup struct {
	Severity string
	Count    int
	Hazards  []publicHazardView
	Open     bool
}

// homeView is the PUBLIC home page model. The login form is deliberately
// not part of it: the sign-in lives behind the top-right icon button.
type homeView struct {
	Lang     string
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

	// Groups is one entry per non-empty severity level — extreme,
	// severe, moderate, minor (most severe first). Moderate and above
	// render expanded; only minor starts collapsed.
	Groups []homeSeverityGroup

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

	// Sources carries the friendly public view of the enabled data
	// sources (one row per feed, no technical detail).
	Sources []publicChannelView

	// HazardsJSON carries the full detail payload of the active hazards
	// (both sections) embedded in the alerts fragment so a card click
	// can open the detail popup without another round trip. It is a
	// template.JS: json.Marshal HTML-escapes <, > and &, so the payload
	// is safe to emit verbatim inside the <script> element.
	HazardsJSON template.JS

	// EmcomNetworks carries the current readiness level of every EMCOM
	// network (retained MQTT state) for the colored header chips.
	EmcomNetworks []emcomChipView

	// EmcomRaised reports whether ANY EMCOM network is above the
	// default monitoring level: the communications heading then shows
	// the readiness info icon opening the shared levels legend.
	EmcomRaised bool

	// EmcomLevels carries the shared operational-readiness legend (the
	// same data the EMCOM panel shows) for the popup on the public
	// page.
	EmcomLevels []emcomLevelView

	// OfflineMode turns the header banner on and tells the map to use
	// the station's local tile tree instead of internet providers.
	OfflineMode bool
}

// emcomChipView is one EMCOM network's readiness status as shown in the
// public header.
type emcomChipView struct {
	Network    string
	Level      int
	LevelName  string
	LevelClass string
	UpdatedAt  time.Time
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

// homeHazardJSON is the client-side payload of one home hazard card,
// embedded as JSON in the alerts section for the detail popup. json.Marshal
// escapes <, > and &, so the payload is safe inside a <script> element.
type homeHazardJSON struct {
	EventKey    string   `json:"event_key"`
	Severity    string   `json:"severity"`
	Headline    string   `json:"headline"`
	Event       string   `json:"event"`
	Source      string   `json:"source"`
	Areas       string   `json:"areas,omitempty"`
	Description string   `json:"description,omitempty"`
	Instruction string   `json:"instruction,omitempty"`
	Status      string   `json:"status,omitempty"`
	Urgency     string   `json:"urgency,omitempty"`
	Certainty   string   `json:"certainty,omitempty"`
	EffectiveAt string   `json:"effective_at,omitempty"`
	ExpiresAt   string   `json:"expires_at,omitempty"`
	UpdatedAt   string   `json:"updated_at,omitempty"`
	Latitude    *float64 `json:"latitude,omitempty"`
	Longitude   *float64 `json:"longitude,omitempty"`
}

// handleEventsMap serves the public JSON of active hazards that carry
// coordinates, so the home map can draw them as icons.
func (s *Server) handleEventsMap(w http.ResponseWriter, r *http.Request) {
	events := make([]mapEventView, 0, 16)
	for _, h := range s.activeHazards() {
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
	lang := s.langFor(r)
	v := s.buildHomeView(lang)
	if sess := s.sessions.currentSession(r); sess != nil {
		v.LoggedIn = true
		v.Username = sess.username
		v.Landing = "/dashboard"
		v.LandingLabel = i18n.T(lang, "nav.dashboard")
	}
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "home", v)
}

// handlePartialHome serves the public auto-refresh fragment of the active
// hazard list (the home page polls it every 5 s). It renders ONLY the
// hazard fields — the channels/sources/EMCOM-chip reads of the full page
// are skipped on this hottest endpoint. The EMCOM levels legend rides
// along so the readiness info icon survives the poll.
func (s *Server) handlePartialHome(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "home_alerts_section", s.buildHomeAlertsView(s.langFor(r)))
}

// buildHomeAlertsView assembles just the alerts-fragment fields
// (hazards, counts, the client-side JSON payload and the EMCOM legend).
func (s *Server) buildHomeAlertsView(lang string) homeView {
	v := homeView{}
	nets := s.emcomNetworks()
	s.fillHomeHazards(&v, nets)
	s.fillHomeEmcomState(&v, lang, nets)
	return v
}

// buildHomeView assembles the public view, most severe first, then
// newest. Hazards come from activeHazards: the LOCAL database first, the
// MQTT mirror only fills documents the local record does not own. lang
// picks the page language for the channel/source names and descriptions.
func (s *Server) buildHomeView(lang string) homeView {
	v := homeView{
		AppTitle:    s.cfg.Title,
		Header1:     s.displayHeader1(),
		Header2:     s.cfg.Header2,
		Tagline:     s.cfg.Tagline,
		About:       template.HTML(s.cfg.About),
		Disclaimer:  s.cfg.Disclaimer,
		Version:     s.version,
		Commit:      s.commit,
		RepoURL:     repoURL,
		OfflineMode: s.OfflineMode(),
	}

	// One EMCOM read serves both the hazard list and the header chips.
	nets := s.emcomNetworks()
	s.fillHomeHazards(&v, nets)

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
	s.fillHomeEmcomState(&v, lang, nets)
	return v
}

// fillHomeEmcomState fills the EMCOM-related fields of a home view: the
// raised-network header chips, the raised flag (drives the readiness
// info icon) and the shared readiness-level legend. The FULL page and
// the 5-second partial share this helper, so the icon and its popup
// survive the poll.
func (s *Server) fillHomeEmcomState(v *homeView, lang string, nets []emcomNetwork) {
	for _, l := range emcomLevels {
		v.EmcomLevels = append(v.EmcomLevels, emcomLevelView{
			Level:       l.Level,
			Name:        i18n.T(lang, fmt.Sprintf("emcom.levels.%d", l.Level)),
			Description: i18n.T(lang, fmt.Sprintf("emcom.desc.%d", l.Level)),
			Class:       emcomLevelClass(l.Level),
		})
	}
	for _, net := range nets {
		// Monitoring (level 0) is the default, calm state of every
		// network — the public header only announces networks that are
		// actually raised.
		if net.Level <= 0 {
			continue
		}
		v.EmcomNetworks = append(v.EmcomNetworks, emcomChipView{
			Network:    net.Name,
			Level:      net.Level,
			LevelName:  net.LevelName,
			LevelClass: emcomLevelClass(net.Level),
			UpdatedAt:  net.UpdatedAt,
		})
	}
	v.EmcomRaised = len(v.EmcomNetworks) > 0
}

// displayAreas renders the areas of a PUBLIC hazard card: type:slug
// tokens pass through, teryt: codes are dropped — a wall of hundreds of
// TERYT codes says nothing to a resident and reads like noise (the
// powiat:/gmina: entries already carry the geography).
func displayAreas(tokens []string) []string {
	out := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if strings.HasPrefix(t, "teryt:") {
			continue
		}
		out = append(out, t)
	}
	return out
}

// fillHomeHazards fills the hazard-related fields of a home view: the
// severity-split groups (one per level), counts and the client-side JSON
// payload. nets carries the already-read EMCOM network list (nil = read
// it here).
func (s *Server) fillHomeHazards(v *homeView, nets []emcomNetwork) {
	hazards := s.collectHazards(nets)
	v.ActiveCount = len(hazards)

	// Groups follow the canonical scale 1:1, most severe first; the
	// minor group also absorbs unknown severities.
	rankExtreme, _ := severity.Rank(severity.Extreme)
	rankSevere, _ := severity.Rank(severity.Severe)
	rankModerate, _ := severity.Rank(severity.Moderate)
	groupOrder := []string{severity.Extreme, severity.Severe, severity.Moderate, severity.Minor}
	buckets := make(map[string]*homeSeverityGroup, len(groupOrder))
	for _, sev := range groupOrder {
		buckets[sev] = &homeSeverityGroup{Severity: sev}
	}

	hazardsJSON := make([]homeHazardJSON, 0, len(hazards))
	for _, h := range hazards {
		view := publicHazardView{
			EventKey:    h.EventKey,
			Severity:    h.Severity,
			Headline:    h.Headline,
			Event:       h.Event,
			Source:      h.Source,
			Areas:       strings.Join(displayAreas(h.Areas), ", "),
			Description: h.Description,
			Instruction: h.Instruction,
			Status:      h.Status,
			Urgency:     h.Urgency,
			Certainty:   h.Certainty,
			EffectiveAt: h.EffectiveAt,
			ExpiresAt:   h.ExpiresAt,
			UpdatedAt:   h.UpdatedAt,
		}
		r, _ := severity.Rank(h.Severity)
		key := severity.Minor
		switch {
		case r >= rankExtreme:
			key = severity.Extreme
		case r >= rankSevere:
			key = severity.Severe
		case r >= rankModerate:
			key = severity.Moderate
		}
		buckets[key].Hazards = append(buckets[key].Hazards, view)
		hazardsJSON = append(hazardsJSON, hazardJSONFromState(h, view.Areas))
	}
	if b, err := json.Marshal(hazardsJSON); err == nil {
		v.HazardsJSON = template.JS(b)
	} else {
		v.HazardsJSON = template.JS("[]")
	}

	// Only non-empty groups render; moderate and above start expanded.
	v.Groups = make([]homeSeverityGroup, 0, len(groupOrder))
	for _, sev := range groupOrder {
		g := buckets[sev]
		if len(g.Hazards) == 0 {
			continue
		}
		sortHazards(g.Hazards)
		g.Count = len(g.Hazards)
		r, _ := severity.Rank(sev)
		g.Open = r >= rankModerate
		v.Groups = append(v.Groups, *g)
	}
}

// hazardTime renders an optional time as RFC3339 (empty for nil) for the
// client-side JSON payloads.
func hazardTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(time.RFC3339)
}

// hazardJSONFromState builds the client-side detail payload from one
// state hazard; areas carries the pre-joined display form.
func hazardJSONFromState(h state.Hazard, areas string) homeHazardJSON {
	return homeHazardJSON{
		EventKey:    h.EventKey,
		Severity:    h.Severity,
		Headline:    h.Headline,
		Event:       h.Event,
		Source:      h.Source,
		Areas:       areas,
		Description: h.Description,
		Instruction: h.Instruction,
		Status:      h.Status,
		Urgency:     h.Urgency,
		Certainty:   h.Certainty,
		EffectiveAt: hazardTime(h.EffectiveAt),
		ExpiresAt:   hazardTime(h.ExpiresAt),
		UpdatedAt:   h.UpdatedAt.Format(time.RFC3339),
		Latitude:    h.Latitude,
		Longitude:   h.Longitude,
	}
}

// hazardJSONFromEvent builds the same payload from one persisted
// HazardEvent (the current-state table keeps ended events for the
// retention window, so mail deep links can still render them).
func hazardJSONFromEvent(ev core.HazardEvent) homeHazardJSON {
	return homeHazardJSON{
		EventKey:    ev.Key(),
		Severity:    ev.Severity,
		Headline:    ev.Headline,
		Event:       ev.Event,
		Source:      ev.Source,
		Areas:       strings.Join(displayAreas(ev.Areas), ", "),
		Description: ev.Description,
		Instruction: ev.Instruction,
		Status:      string(ev.Status),
		Urgency:     ev.Urgency,
		Certainty:   ev.Certainty,
		EffectiveAt: hazardTime(ev.EffectiveAt),
		ExpiresAt:   hazardTime(ev.ExpiresAt),
		UpdatedAt:   ev.UpdatedAt.Format(time.RFC3339),
		Latitude:    ev.Latitude,
		Longitude:   ev.Longitude,
	}
}

// handleHazardByKey serves one hazard by event key, including ended ones
// (expired/cancelled): notification deep links must show what the message
// was about even after the hazard left the active list. Lookup order:
// the active views, the local compose record (expired tombstones
// included), then the current-state table.
func (s *Server) handleHazardByKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing event key", http.StatusBadRequest)
		return
	}
	for _, h := range s.activeHazards() {
		if h.EventKey == key {
			writeHazardJSON(w, hazardJSONFromState(h, strings.Join(displayAreas(h.Areas), ", ")))
			return
		}
	}
	if h, ok := s.composeHazard(key); ok {
		writeHazardJSON(w, hazardJSONFromState(h, strings.Join(displayAreas(h.Areas), ", ")))
		return
	}
	if s.events != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		ev, err := s.events.Get(ctx, key)
		cancel()
		if err == nil && ev != nil {
			writeHazardJSON(w, hazardJSONFromEvent(ev.Event))
			return
		}
	}
	http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
}

// writeHazardJSON emits one hazard detail payload.
func writeHazardJSON(w http.ResponseWriter, hz homeHazardJSON) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(hz)
}

// activeHazards returns the current active communications for the public
// views. The LOCAL database is authoritative — active events from the
// current-state table, active panel communications and raised EMCOM
// networks: SQLite alone must suffice to serve them after a restart
// without a broker. The MQTT mirror only fills keys the local record does
// not own (other instances' documents; mirror-only installations behave
// exactly as before).
func (s *Server) activeHazards() []state.Hazard {
	return s.collectHazards(nil)
}

// collectHazards assembles the merged hazard list. nets carries the
// already-read EMCOM network list (nil = read it here).
func (s *Server) collectHazards(nets []emcomNetwork) []state.Hazard {
	var out []state.Hazard
	seen := make(map[string]bool)
	add := func(h state.Hazard) {
		if h.EventKey == "" || seen[h.EventKey] {
			return
		}
		seen[h.EventKey] = true
		out = append(out, h)
	}

	if s.events != nil {
		if lister, ok := s.events.(storage.ActiveEventLister); ok {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			after := ""
			for {
				page, err := lister.ListActiveEvents(ctx, after, 256)
				if err != nil {
					s.logger.Warn("home: active events read failed", "error", err)
					break
				}
				for _, ev := range page {
					add(hazardFromCore(ev))
				}
				if len(page) < 256 {
					break
				}
				after = page[len(page)-1].Key()
			}
			cancel()
		}
	}

	if cs, ok := s.users.(composeStore); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		rows, err := cs.ComposeHazards(ctx)
		cancel()
		if err != nil {
			s.logger.Warn("home: compose record read failed", "error", err)
		} else {
			for _, row := range rows {
				if row.Status != "active" {
					continue
				}
				var h state.Hazard
				if err := json.Unmarshal(row.State, &h); err != nil {
					continue
				}
				add(h)
			}
		}
	}

	if nets == nil {
		nets = s.emcomNetworks()
	}
	now := time.Now()
	for _, net := range nets {
		if net.Level < 1 {
			continue
		}
		add(emcomHazard(net, now))
	}

	for _, h := range s.st.Snapshot().Hazards {
		add(h)
	}
	return out
}

// hazardFromCore projects one current-state event onto the public hazard
// shape (the same shape the MQTT mirror carries).
func hazardFromCore(ev core.HazardEvent) state.Hazard {
	return state.Hazard{
		EventKey:    ev.Key(),
		Source:      ev.Source,
		SourceID:    ev.SourceID,
		Category:    ev.Category,
		Event:       ev.Event,
		Severity:    ev.Severity,
		Urgency:     ev.Urgency,
		Certainty:   ev.Certainty,
		Headline:    ev.Headline,
		Description: ev.Description,
		Instruction: ev.Instruction,
		Areas:       ev.Areas,
		Status:      string(ev.Status),
		Latitude:    ev.Latitude,
		Longitude:   ev.Longitude,
		EffectiveAt: ev.EffectiveAt,
		ExpiresAt:   ev.ExpiresAt,
		ReceivedAt:  ev.ReceivedAt,
		UpdatedAt:   ev.UpdatedAt,
	}
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
	// The public home map skips weather stations (symbol '_') because
	// the weather layer draws them; the admin APRS map asks for all=1
	// to see every heard station.
	all := r.URL.Query().Get("all") == "1"
	for _, doc := range stations {
		if doc.Symbol == "_" && !all {
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
	Provider  string  `json:"provider"`
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Via       string  `json:"via"` // "internet" or "aprs"
	// Origin classifies APRS reports: "rf" (heard by our radio),
	// "internet" (APRS-IS only) or "" (unknown).
	Origin string `json:"origin,omitempty"`
	// ReceivedVia lists which of our backends actually delivered the
	// station's packets: "aprs-radio" and/or "aprs-inet". APRS reports
	// only; the map shows it as APRS-RF / APRS-IS chips.
	ReceivedVia      []string `json:"received_via,omitempty"`
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
	Provider    string                   `json:"provider"`
	Name        string                   `json:"name"`
	GeneratedAt string                   `json:"generated_at,omitempty"`
	Daily       []weatherForecastDayView `json:"daily"`
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
		// The local weather cache is the authoritative source: it also
		// covers infrastructure weather stations that never enter the
		// station directory — without it their readings resurface as an
		// unclassified internet echo.
		for _, w := range s.aprs.WeatherSnapshot(time.Now()) {
			if w.Callsign == "" {
				continue
			}
			seenReports["aprs:"+w.Callsign] = true
			view.Reports = append(view.Reports, weatherReportView{
				Provider:         "APRS",
				Name:             w.Callsign,
				Latitude:         w.Latitude,
				Longitude:        w.Longitude,
				Via:              "aprs",
				Origin:           w.Origin,
				ReceivedVia:      w.ReceivedVia,
				Condition:        aprsWeatherCondition(&w),
				TemperatureC:     w.TemperatureC,
				HumidityPct:      w.HumidityPct,
				WindSpeedKmh:     w.WindSpeedKmh,
				WindDirectionDeg: w.WindDirectionDeg,
				WindGustsKmh:     w.WindGustsKmh,
				PressureHpa:      w.PressureHpa,
				RadiationUSvh:    w.RadiationUSvh,
				RadiationCPM:     w.RadiationCPM,
				GeneratedAt:      w.GeneratedAt,
			})
		}
		for _, doc := range s.aprs.Stations() {
			if doc.Weather == nil || seenReports["aprs:"+doc.Callsign] {
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
				Origin:           doc.Origin,
				ReceivedVia:      doc.ReceivedVia,
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
			view.Forecasts = append(view.Forecasts, weatherForecastView{
				Provider:    ww.ProviderName,
				Name:        name,
				GeneratedAt: ww.GeneratedAt.UTC().Format(time.RFC3339),
				Daily:       days,
			})
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
