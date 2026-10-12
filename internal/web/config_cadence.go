package web

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/schedule"
	"gopkg.in/yaml.v3"
)

// Cadence field bounds. Both periodic lines are announcements, so the
// floor keeps a mis-typed value from hammering the channels; the ceiling
// is a month.
const (
	cadenceMinInterval = time.Minute
	cadenceMaxInterval = 720 * time.Hour
)

// Range field bounds: what the loader accepts, so the panel can never
// write a value the station refuses to start with.
const (
	aprsRadiusMin     = 1.0
	aprsRadiusMax     = 1000.0
	aprsRadiusDefault = 60.0
	aprsRadiusUnit    = "km"
	aprsStationTTLDef = 30 * time.Minute
	rangeTTLMin       = time.Minute
	rangeTTLMax       = 24 * time.Hour
	meshNodeTTLMin    = time.Minute
	meshNodeTTLMax    = 24 * time.Hour
	pollFieldPrefix   = "poll"
)

// cadenceView is the "periodic lines" section: the presence beacon (a
// daily anchor or a spacing) and the hazard status spacing. The same
// values drive the Meshtastic channel and the Discord channel, so an
// operator moves the morning line once.
type cadenceView struct {
	Anchor   string
	Interval string
	Hazards  string
	// Discord reports whether a Discord instance is announcing: the
	// section then says the change reaches both surfaces.
	Discord bool
}

// scalarFieldView is one editable scalar: the dotted YAML path, the
// current value and the accepted range (both ends shown as a hint).
type scalarFieldView struct {
	// Index is the position of the row inside its form (the handler reads
	// path_<i> / value_<i>, so a disabled row can never shift the others).
	Index int
	// Path is the dotted YAML path, list entries included
	// ("sources[2].config.poll_interval").
	Path string
	// Label is the i18n key shown above the input.
	Label string
	// Title names the instance the row belongs to ("" in the global rows).
	Title string
	// Value is what the file holds ("" = the key is absent, the field is
	// then left alone on save); Placeholder shows the default behind it.
	Value       string
	Placeholder string
	// Min / Max are the accepted ends, rendered as the field hint.
	Min string
	Max string
	// Hint is an optional extra i18n key shown under the field (a rule the
	// range alone cannot express, such as "-1 means never").
	Hint string
}

// SetPollBounds installs the per-source poll_interval ranges the plugins
// declared. Without it the poll fields are read-only, so the page can
// never write a value a plugin would reject at startup.
func (s *Server) SetPollBounds(fn func(sourceType string) (min, max time.Duration, ok bool)) {
	s.pollBounds = fn
}

// SetNoticesCadence installs the live Discord cadence: main fans the call
// out to every notice runner (the Meshtastic hub is reached directly).
func (s *Server) SetNoticesCadence(fn func(hazards, uptime time.Duration, uptimeAt string)) {
	s.noticesCadence = fn
}

// cadenceFor renders the live cadence: the Meshtastic hub is the source
// of truth (both surfaces are kept in step), with the file behind it for
// a station that runs no hub at all.
func (s *Server) cadenceFor() cadenceView {
	v := cadenceView{}
	if s.meshtastic != nil {
		c := s.meshtastic.ReportCadence()
		v.Anchor = c.BeaconAt
		v.Interval = formatCadence(c.BeaconInterval)
		v.Hazards = formatCadence(c.HazardsInterval)
	} else if s.configPath != "" {
		v.Anchor = s.scalarOr("meshtastic.emcom_at", "")
		v.Interval = formatStoredCadence(s.scalarOr("meshtastic.emcom_interval", ""))
		v.Hazards = formatStoredCadence(s.scalarOr("meshtastic.emcom_hazards_interval", ""))
	}
	if s.configPath != "" {
		if acts, err := config.Instances(s.configPath, "actions"); err == nil {
			for _, a := range acts {
				if a.Type == "discord" && !s.noticeLinesOff(a) {
					v.Discord = true
					break
				}
			}
		}
	}
	return v
}

// pollFieldsFor renders the "polling and range" section: the two APRS
// range values, the Meshtastic node freshness and every source instance
// that declares a poll interval. All of them are read at startup, so the
// section says a restart applies them.
func (s *Server) pollFieldsFor() []scalarFieldView {
	if s.configPath == "" {
		return nil
	}
	var rows []scalarFieldView
	add := func(path, label, fallback, min, max string) {
		rows = append(rows, scalarFieldView{
			Index: len(rows), Path: path, Label: label,
			Value:       s.scalarOr(path, ""),
			Placeholder: fallback,
			Min:         min, Max: max,
		})
	}
	add("aprs.radius_km", "config.range.aprs_radius", formatRadius(aprsRadiusDefault),
		formatRadius(aprsRadiusMin), formatRadius(aprsRadiusMax))
	add("aprs.station_ttl", "config.range.aprs_ttl", formatCadence(aprsStationTTLDef),
		formatCadence(rangeTTLMin), formatCadence(rangeTTLMax))
	add("meshtastic.node_ttl", "config.range.mesh_ttl", formatCadence(meshtastic.DefaultNodeTTL),
		formatCadence(meshNodeTTLMin), formatCadence(meshNodeTTLMax))

	insts, err := config.Instances(s.configPath, "sources")
	if err != nil {
		return rows
	}
	for _, in := range insts {
		if !in.HasPoll {
			continue
		}
		title := in.ID
		if title == "" {
			title = fmt.Sprintf("#%d", in.Index)
		}
		if in.Type != "" {
			title = fmt.Sprintf("%s (%s)", title, in.Type)
		}
		path := in.ConfigPath("sources") + ".poll_interval"
		rows = append(rows, scalarFieldView{
			Index: len(rows), Path: path, Label: "config.range.poll_interval",
			Title: title, Value: formatCadence(in.PollInterval),
			Placeholder: formatCadence(in.PollInterval),
		})
		if in.Type != "" && s.pollBounds != nil {
			if min, max, ok := s.pollBounds(in.Type); ok {
				rows[len(rows)-1].Min = formatCadence(min)
				rows[len(rows)-1].Max = formatCadence(max)
			}
		}
	}
	return rows
}

// formatStoredCadence tidies a duration as the file spells it
// ("6h0m0s" becomes "6h"); unparseable text is left alone.
func formatStoredCadence(raw string) string {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return raw
	}
	return formatCadence(d)
}

// scalarOr reads a scalar from the configuration file, falling back to a
// shown default when the key is absent.
func (s *Server) scalarOr(path, fallback string) string {
	if v, ok := config.ScalarAt(s.configPath, path); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}

// handleConfigCadence applies the periodic-line cadence: it validates
// first, then moves both live surfaces (Meshtastic hub and every Discord
// notice runner) and only then makes the change durable, so a rejected
// value leaves the station exactly as it was.
func (s *Server) handleConfigCadence(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		if err == nil {
			return
		}
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	lang := s.langFor(r)
	anchor := strings.TrimSpace(r.PostFormValue("anchor"))
	interval, err := parseCadenceField(r.PostFormValue("interval"), true)
	if err != nil {
		s.renderConfigError(w, r, http.StatusBadRequest, i18nFieldError(lang, "config.cadence.interval", err))
		return
	}
	hazards, err := parseCadenceField(r.PostFormValue("hazards"), true)
	if err != nil {
		s.renderConfigError(w, r, http.StatusBadRequest, i18nFieldError(lang, "config.cadence.hazards", err))
		return
	}
	if anchor != "" && !schedule.Valid(anchor) {
		s.renderConfigError(w, r, http.StatusBadRequest,
			i18nFieldError(lang, "config.cadence.anchor", errClockFormat))
		return
	}
	if anchor != "" && interval > 0 {
		s.renderConfigError(w, r, http.StatusBadRequest,
			i18nFieldError(lang, "config.cadence.interval", errAnchorOrInterval))
		return
	}

	perr := ""
	if !s.persistConfigYAML("periodic cadence", func() error {
		s.applyCadence(anchor, interval, hazards)
		return s.updateConfigFile(s.cadenceEdits(anchor, interval, hazards))
	}) {
		perr = "&perr=1"
	}
	s.audit(sess.username, "config-cadence",
		fmt.Sprintf("anchor=%q interval=%s hazards=%s", anchor, interval, hazards))
	s.logger.Info("periodic line cadence updated by admin",
		"user", sess.username, "anchor", anchor, "interval", interval, "hazards", hazards)
	http.Redirect(w, r, "/config?msg=cadence"+perr, http.StatusSeeOther)
}

// errClockFormat / errAnchorOrInterval are the two cadence refusals; they
// are constants so the tests can pin the wording of the banner.
var (
	errClockFormat      = fmt.Errorf("use HH:MM, for example 06:00")
	errAnchorOrInterval = fmt.Errorf("set the daily hour or the interval, not both")
)

// i18nFieldError prefixes a rejected field so the banner names what the
// operator has to fix.
func i18nFieldError(lang, label string, err error) string {
	return fmt.Sprintf("%s: %v", i18nFieldName(lang, label), err)
}

// i18nFieldName returns the shortened field label for the error banner
// (the range hints are dropped from the flash, not from the field).
func i18nFieldName(lang, label string) string {
	name := i18n.T(lang, label)
	if i := strings.IndexByte(name, ':'); i > 0 {
		name = name[:i]
	}
	return strings.TrimSpace(name)
}

// applyCadence moves the live surfaces before the file is written.
func (s *Server) applyCadence(anchor string, interval, hazards time.Duration) {
	if s.meshtastic != nil {
		s.meshtastic.SetCadence(meshtastic.Cadence{
			BeaconAt:        anchor,
			BeaconInterval:  interval,
			HazardsInterval: hazards,
		})
	}
	if s.noticesCadence != nil {
		s.noticesCadence(hazards, interval, anchor)
	}
}

// cadenceEdits builds the file changes: the Meshtastic section plus every
// Discord action instance, which is where the other channel reads its
// cadence.
func (s *Server) cadenceEdits(anchor string, interval, hazards time.Duration) []config.ScalarEdit {
	edits := s.changedEdit("meshtastic.emcom_at", anchor, config.StringScalar(anchor))
	edits = append(edits,
		s.changedEdit("meshtastic.emcom_interval", formatCadence(interval), config.DurationScalar(interval))...)
	edits = append(edits,
		s.changedEdit("meshtastic.emcom_hazards_interval", formatCadence(hazards), config.DurationScalar(hazards))...)
	acts, err := config.Instances(s.configPath, "actions")
	if err != nil {
		s.logger.Warn("config: action instances unreadable", "error", err)
		return edits
	}
	for _, a := range acts {
		if a.Type != "discord" || s.noticeLinesOff(a) {
			continue
		}
		base := a.ConfigPath("actions")
		edits = append(edits, s.changedEdit(base+".uptime_at", anchor, config.StringScalar(anchor))...)
		edits = append(edits,
			s.changedEdit(base+".uptime_interval", formatCadence(interval), config.DurationScalar(interval))...)
		edits = append(edits,
			s.changedEdit(base+".hazards_interval", formatCadence(hazards), config.DurationScalar(hazards))...)
	}
	return edits
}

// changedEdit returns the edit only when the file does not already hold
// the submitted value (compared semantically), so a save without a real
// change leaves the file, and the audit trail, untouched.
func (s *Server) changedEdit(path, shown string, value *yaml.Node) []config.ScalarEdit {
	if cur, ok := s.scalarAt(path); ok && sameValue(cur, shown) {
		return nil
	}
	return []config.ScalarEdit{{Path: path, Value: value}}
}

// sameValue reports whether the file already holds what was submitted.
// The comparison is semantic where it can be (a duration written "2h" and
// stored "2h0m0s" is the same value), so saving a form without touching a
// field never rewrites the line or adds an audit entry.
func sameValue(current, submitted string) bool {
	current, submitted = strings.TrimSpace(current), strings.TrimSpace(submitted)
	if current == submitted {
		return true
	}
	if a, errA := time.ParseDuration(current); errA == nil {
		if b, errB := time.ParseDuration(submitted); errB == nil {
			return a == b
		}
	}
	if a, errA := strconv.ParseFloat(current, 64); errA == nil {
		if b, errB := strconv.ParseFloat(submitted, 64); errB == nil {
			return a == b
		}
	}
	return false
}

// noticeLinesOff reports whether a Discord instance silenced both
// periodic lines on purpose. Such an instance is left alone: a
// development station sharing the production webhook must not start
// announcing because an operator moved the morning line. The rules follow
// the loader: a missing key falls back to the shipped default (the
// channel announces), only an explicit zero silences a line.
func (s *Server) noticeLinesOff(a config.Instance) bool {
	base := a.ConfigPath("actions")
	if off, set := s.lineOff(base + ".hazards_interval"); !set || !off {
		return false
	}
	// A non-empty anchor runs the presence line whatever the interval says.
	if at, set := s.scalarAt(base + ".uptime_at"); set && strings.TrimSpace(at) != "" {
		return false
	}
	off, set := s.lineOff(base + ".uptime_interval")
	return set && off
}

// lineOff reads a duration key: off reports an explicit zero (or less),
// set whether the key exists at all.
func (s *Server) lineOff(path string) (off, set bool) {
	raw, ok := s.scalarAt(path)
	if !ok {
		return false, false
	}
	d, err := time.ParseDuration(strings.TrimSpace(raw))
	return err == nil && d <= 0, true
}

// scalarAt reads a scalar from the configuration file ("" plus false when
// the key or the file is missing).
func (s *Server) scalarAt(path string) (string, bool) {
	if s.configPath == "" {
		return "", false
	}
	return config.ScalarAt(s.configPath, path)
}

// handleConfigPolling applies the polling-and-range section: every row is
// validated against the range the consumer accepts, so the station can
// always start with what the panel wrote. The values are read at startup
// only, hence the restart hint on the section.
func (s *Server) handleConfigPolling(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		if err == nil {
			return
		}
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	var edits []config.ScalarEdit
	for i := 0; ; i++ {
		path := strings.TrimSpace(r.PostFormValue(fmt.Sprintf("path_%d", i)))
		if path == "" {
			break
		}
		value := strings.TrimSpace(r.PostFormValue(fmt.Sprintf("value_%d", i)))
		if value == "" {
			continue // an empty row means "leave it alone"
		}
		if cur, ok := s.scalarAt(path); ok && sameValue(cur, value) {
			continue // already what the field shows: nothing to write
		}
		edit, err := s.pollEdit(path, value)
		if err != nil {
			s.renderConfigError(w, r, http.StatusBadRequest, fmt.Sprintf("%s: %v", path, err))
			return
		}
		edits = append(edits, edit)
	}
	if len(edits) == 0 {
		http.Redirect(w, r, "/config?msg=polling-none", http.StatusSeeOther)
		return
	}
	perr := ""
	if !s.persistConfigYAML("polling and range", func() error {
		return s.updateConfigFile(edits)
	}) {
		perr = "&perr=1"
	}
	paths := make([]string, 0, len(edits))
	for _, e := range edits {
		paths = append(paths, e.Path+"="+e.Value.Value)
	}
	s.audit(sess.username, "config-polling", strings.Join(paths, " "))
	s.logger.Info("polling and range updated by admin", "user", sess.username, "changes", strings.Join(paths, " "))
	http.Redirect(w, r, "/config?msg=polling"+perr, http.StatusSeeOther)
}

// pollEdit validates one submitted row against the range its consumer
// accepts and returns the file change.
func (s *Server) pollEdit(path, value string) (config.ScalarEdit, error) {
	switch path {
	case "aprs.radius_km":
		km, err := strconv.ParseFloat(value, 64)
		if err != nil || km < aprsRadiusMin || km > aprsRadiusMax {
			return config.ScalarEdit{}, fmt.Errorf("expected %s..%s %s", formatRadius(aprsRadiusMin), formatRadius(aprsRadiusMax), aprsRadiusUnit)
		}
		return config.ScalarEdit{Path: path, Value: config.FloatScalar(km)}, nil
	case "aprs.station_ttl", "meshtastic.node_ttl":
		d, err := parseCadenceField(value, false)
		if err != nil {
			return config.ScalarEdit{}, err
		}
		if d < rangeTTLMin || d > rangeTTLMax {
			return config.ScalarEdit{}, fmt.Errorf("expected %s..%s", formatCadence(rangeTTLMin), formatCadence(rangeTTLMax))
		}
		return config.ScalarEdit{Path: path, Value: config.DurationScalar(d)}, nil
	}
	insts, err := config.Instances(s.configPath, "sources")
	if err != nil {
		return config.ScalarEdit{}, fmt.Errorf("cannot read the configuration file")
	}
	for _, in := range insts {
		if in.ConfigPath("sources")+".poll_interval" != path {
			continue
		}
		d, err := parseCadenceField(value, false)
		if err != nil {
			return config.ScalarEdit{}, err
		}
		if s.pollBounds != nil && in.Type != "" {
			if min, max, ok := s.pollBounds(in.Type); ok && (d < min || d > max) {
				return config.ScalarEdit{}, fmt.Errorf("%s accepts %s..%s", in.Type, formatCadence(min), formatCadence(max))
			}
		}
		return config.ScalarEdit{Path: path, Value: config.DurationScalar(d)}, nil
	}
	return config.ScalarEdit{}, fmt.Errorf("unknown setting")
}

// parseCadenceField reads a duration field. Empty means "off"; allowZero
// distinguishes the periodic lines (0 = the line is off) from the range
// values, where 0 would silently fall back to a default.
func parseCadenceField(raw string, allowZero bool) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("use a duration like 6h or 30m")
	}
	if d < 0 {
		return 0, fmt.Errorf("must not be negative")
	}
	if d == 0 {
		if allowZero {
			return 0, nil
		}
		return 0, fmt.Errorf("must be greater than zero")
	}
	if d < cadenceMinInterval || d > cadenceMaxInterval {
		return 0, fmt.Errorf("expected %s..%s", cadenceMinInterval, cadenceMaxInterval)
	}
	return d, nil
}

// formatRadius renders a radius the way the configuration file spells it:
// a whole number of kilometres stays whole, a fraction keeps its digits.
func formatRadius(km float64) string {
	return strconv.FormatFloat(km, 'f', -1, 64)
}

// formatCadence renders a duration for an input: the round units read as
// an operator writes them ("6h", "30m"), the rest fall back to Go's
// format. Zero renders empty, which is how the fields mean "off".
func formatCadence(d time.Duration) string {
	switch {
	case d <= 0:
		return ""
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", int64(d/time.Hour))
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", int64(d/time.Minute))
	default:
		return d.String()
	}
}
