package web

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/config"
)

// Retention windows the panel offers, with the bounds the loader itself
// enforces: the page must never write a value the station refuses to
// start with.
const (
	// The journal and event windows keep the station's own history: a
	// month is the shipped default, a year the ceiling.
	retentionMin = time.Minute
	retentionMax = 8760 * time.Hour // a year
	// The inbox window is the pending-dispatch safety net.
	inboxRetentionMax = 720 * time.Hour
	// Queue and log limits.
	queueSizeMin   = 1
	queueSizeMax   = 100_000
	minFreeMBMax   = 10_000_000
	logSizeMBMin   = 1
	logSizeMBMax   = 10_000
	logBackupsMin  = 0
	logBackupsMax  = 100
	minFreeMBMin   = 0
	neverRetention = -1 * time.Second
)

// RetentionLimits is one submission of the "retention and limits"
// section: the windows the prune loops honour and the low-disk threshold.
// The other rows of the section are read at startup only, so they never
// reach this struct.
type RetentionLimits struct {
	// Change is how long acknowledged journal records are kept.
	Change time.Duration
	// Event is how long cancelled/expired events are kept.
	Event time.Duration
	// Notification is the action-fire dedup window; negative disables
	// pruning entirely.
	Notification time.Duration
	// Inbox is the pending-dispatch window (0 disables pruning).
	Inbox time.Duration
	// MinFreeMB is the low-disk alarm threshold in megabytes (0 disables).
	MinFreeMB int64
}

// SetRetentionApplier installs the live retention sink: main fans it out
// to the plugin manager, the two prune loops and the panel's own low-disk
// alarm.
func (s *Server) SetRetentionApplier(fn func(RetentionLimits)) {
	s.retentionApply = fn
}

// retentionFieldsFor renders the section: the live windows first, then the
// limits that are read at startup. Values come from the file, the
// placeholder from the shipped default, so an empty field never reads as
// "zero" and saving an untouched form changes nothing.
func (s *Server) retentionFieldsFor() []scalarFieldView {
	if s.configPath == "" {
		return nil
	}
	def := config.Default()
	out := []scalarFieldView{}
	add := func(path, label, fallback, min, max, hint string) {
		out = append(out, scalarFieldView{
			Index:       len(out),
			Path:        path,
			Label:       label,
			Value:       s.scalarOr(path, ""),
			Placeholder: fallback,
			Min:         min,
			Max:         max,
			Hint:        hint,
		})
	}
	add("app.change_retention", "config.limits.change", formatCadence(def.App.ChangeRetention),
		formatCadence(retentionMin), formatCadence(retentionMax), "")
	add("app.event_retention", "config.limits.event", formatCadence(def.App.EventRetention),
		formatCadence(retentionMin), formatCadence(retentionMax), "")
	add("app.notification_retention", "config.limits.notification", formatCadence(def.App.NotificationRetention),
		formatCadence(retentionMin), formatCadence(retentionMax), "config.limits.never")
	add("dispatch.inbox_retention", "config.limits.inbox", formatCadence(def.Dispatch.InboxRetention),
		"0", formatCadence(inboxRetentionMax), "")
	add("storage.min_free_mb", "config.limits.min_free", strconv.FormatInt(def.Storage.MinFreeMB, 10),
		strconv.Itoa(minFreeMBMin), strconv.Itoa(minFreeMBMax), "config.limits.restart_free")
	add("dispatch.queue_size", "config.limits.queue", strconv.Itoa(def.Dispatch.QueueSize),
		strconv.Itoa(queueSizeMin), strconv.Itoa(queueSizeMax), "config.limits.restart")
	add("app.log_max_size_mb", "config.limits.log_size", strconv.Itoa(def.App.LogMaxSizeMB),
		strconv.Itoa(logSizeMBMin), strconv.Itoa(logSizeMBMax), "config.limits.restart")
	add("app.log_max_backups", "config.limits.log_backups", strconv.Itoa(def.App.LogMaxBackups),
		strconv.Itoa(logBackupsMin), strconv.Itoa(logBackupsMax), "config.limits.restart")
	return out
}

// handleConfigRetention applies the section: the live windows move the
// running station (journal cleanup, both prune loops, the low-disk alarm)
// and every row is written to the file, so a restart keeps the choice.
func (s *Server) handleConfigRetention(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		if err == nil {
			return
		}
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	var edits []config.ScalarEdit
	submitted := map[string]string{}
	for i := 0; ; i++ {
		path := strings.TrimSpace(r.PostFormValue(fmt.Sprintf("path_%d", i)))
		if path == "" {
			break
		}
		value := strings.TrimSpace(r.PostFormValue(fmt.Sprintf("value_%d", i)))
		if value == "" {
			continue // an empty field keeps what the station uses now
		}
		submitted[path] = value
		edit, err := s.retentionEdit(path, value)
		if err != nil {
			s.renderConfigError(w, r, http.StatusBadRequest,
				fmt.Sprintf("%s: %v", retentionFieldName(s.langFor(r), path), err))
			return
		}
		edits = append(edits, edit...)
	}

	live := s.retentionLimits(submitted)

	perr := ""
	if !s.persistConfigYAML("retention and limits", func() error {
		s.applyRetention(live)
		if len(edits) == 0 {
			return nil
		}
		return s.updateConfigFile(edits)
	}) {
		perr = "&perr=1"
	}
	s.audit(sess.username, "config-retention",
		fmt.Sprintf("change=%s event=%s notification=%s inbox=%s min_free_mb=%d",
			live.Change, live.Event, live.Notification, live.Inbox, live.MinFreeMB))
	s.logger.Info("retention and limits updated by admin",
		"user", sess.username,
		"change", live.Change, "event", live.Event,
		"notification", live.Notification, "inbox", live.Inbox,
		"min_free_mb", live.MinFreeMB, "rows", len(edits))
	http.Redirect(w, r, "/config?msg=retention"+perr, http.StatusSeeOther)
}

// retentionLimits folds the submission and the file into the settings the
// station ends up using: a field left empty keeps the value in force (the
// file's own, or the shipped default when the key is absent). Returning
// the effective values keeps a form save from silently switching a
// cleanup off.
func (s *Server) retentionLimits(submitted map[string]string) RetentionLimits {
	def := config.Default()
	value := func(path string, fallback string) string {
		if v, ok := submitted[path]; ok {
			return v
		}
		return s.scalarOr(path, fallback)
	}
	dur := func(path string, fallback time.Duration) time.Duration {
		raw := strings.TrimSpace(value(path, formatCadence(fallback)))
		if d, err := time.ParseDuration(raw); err == nil {
			return d
		}
		return fallback
	}
	mb, _ := strconv.ParseInt(strings.TrimSpace(value("storage.min_free_mb", strconv.FormatInt(def.Storage.MinFreeMB, 10))), 10, 64)
	notification := dur("app.notification_retention", def.App.NotificationRetention)
	if d, ok := parseNever(value("app.notification_retention", "")); ok {
		notification = d
	}
	return RetentionLimits{
		Change:       dur("app.change_retention", def.App.ChangeRetention),
		Event:        dur("app.event_retention", def.App.EventRetention),
		Notification: notification,
		Inbox:        dur("dispatch.inbox_retention", def.Dispatch.InboxRetention),
		MinFreeMB:    mb,
	}
}

// applyRetention hands the effective settings to the running station.
func (s *Server) applyRetention(l RetentionLimits) {
	if s.retentionApply != nil {
		s.retentionApply(l)
	}
	s.SetStorageAlarm(l.MinFreeMB * 1024 * 1024)
}

// retentionEdit validates one row and returns the file change (nil when
// the file already holds that value).
func (s *Server) retentionEdit(path, value string) ([]config.ScalarEdit, error) {
	switch path {
	case "app.change_retention", "app.event_retention":
		d, err := parseDurationField(value)
		if err != nil {
			return nil, err
		}
		if d < retentionMin || d > retentionMax {
			return nil, fmt.Errorf("expected %s..%s", formatCadence(retentionMin), formatCadence(retentionMax))
		}
		return s.changedEdit(path, formatCadence(d), config.DurationScalar(d)), nil
	case "app.notification_retention":
		if d, ok := parseNever(value); ok {
			return s.changedEdit(path, formatCadence(d), config.DurationScalar(d)), nil
		}
		d, err := parseDurationField(value)
		if err != nil {
			return nil, err
		}
		if d < retentionMin || d > retentionMax {
			return nil, fmt.Errorf("expected %s..%s, or -1 for never", formatCadence(retentionMin), formatCadence(retentionMax))
		}
		return s.changedEdit(path, formatCadence(d), config.DurationScalar(d)), nil
	case "dispatch.inbox_retention":
		d, err := parseCadenceField(value, true)
		if err != nil {
			return nil, err
		}
		if d > 0 && (d < retentionMin || d > inboxRetentionMax) {
			return nil, fmt.Errorf("expected 0 or %s..%s", formatCadence(retentionMin), formatCadence(inboxRetentionMax))
		}
		return s.changedEdit(path, formatCadence(d), config.DurationScalar(d)), nil
	case "storage.min_free_mb":
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < minFreeMBMin || n > minFreeMBMax {
			return nil, fmt.Errorf("expected %d..%d MB", minFreeMBMin, minFreeMBMax)
		}
		return s.changedEdit(path, strconv.FormatInt(n, 10), config.IntScalar(int(n))), nil
	case "dispatch.queue_size":
		n, err := strconv.Atoi(value)
		if err != nil || n < queueSizeMin || n > queueSizeMax {
			return nil, fmt.Errorf("expected %d..%d", queueSizeMin, queueSizeMax)
		}
		return s.changedEdit(path, strconv.Itoa(n), config.IntScalar(n)), nil
	case "app.log_max_size_mb":
		n, err := strconv.Atoi(value)
		if err != nil || n < logSizeMBMin || n > logSizeMBMax {
			return nil, fmt.Errorf("expected %d..%d MB", logSizeMBMin, logSizeMBMax)
		}
		return s.changedEdit(path, strconv.Itoa(n), config.IntScalar(n)), nil
	case "app.log_max_backups":
		n, err := strconv.Atoi(value)
		if err != nil || n < logBackupsMin || n > logBackupsMax {
			return nil, fmt.Errorf("expected %d..%d", logBackupsMin, logBackupsMax)
		}
		return s.changedEdit(path, strconv.Itoa(n), config.IntScalar(n)), nil
	}
	return nil, fmt.Errorf("unknown setting")
}

// parseNever reads the "-1" spelling of "never prune": the loader treats
// any negative notification window as "prune nothing", so the page offers
// one obvious number instead of a negative duration.
func parseNever(raw string) (time.Duration, bool) {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n >= 0 {
		return 0, false
	}
	return neverRetention, true
}

// retentionFieldName names the refused field in the error banner.
func retentionFieldName(lang, path string) string {
	for _, f := range retentionFieldLabels {
		if f.path == path {
			return i18nFieldName(lang, f.label)
		}
	}
	return path
}

// retentionFieldLabels maps a path to its label key, so the banner can
// name the field the way the page shows it.
var retentionFieldLabels = []struct{ path, label string }{
	{"app.change_retention", "config.limits.change"},
	{"app.event_retention", "config.limits.event"},
	{"app.notification_retention", "config.limits.notification"},
	{"dispatch.inbox_retention", "config.limits.inbox"},
	{"storage.min_free_mb", "config.limits.min_free"},
	{"dispatch.queue_size", "config.limits.queue"},
	{"app.log_max_size_mb", "config.limits.log_size"},
	{"app.log_max_backups", "config.limits.log_backups"},
}

// parseDurationField reads a plain duration field (empty is an error: an
// empty row is skipped before validation).
func parseDurationField(raw string) (time.Duration, error) {
	d, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("use a duration like 24h or 720h")
	}
	return d, nil
}
