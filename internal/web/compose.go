package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/severity"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// composeSource is the fixed source id stamped on every communication
// issued through the compose module. The "issued communications" list
// filters the mirrored active state by this source. It is generic and
// installation-agnostic on purpose.
const composeSource = "compose"

// composeEventKeyRe validates module-generated event keys
// ("compose:<id>"). Keys from the form are always module-generated;
// this is a defensive check at the HTTP boundary.
var composeEventKeyRe = regexp.MustCompile(`^compose:[a-z0-9.-]{1,64}$`)

// Compose form bounds (generous, like the wire).
const (
	maxComposeHeadline = 200
	maxComposeEvent    = 120
	maxComposeText     = 4000
	maxComposeAreasLen = 500
	maxComposeAreas    = 32
)

// composePublisher issues and removes active-hazard documents on the
// WarnFlux broker and publishes arbitrary retained raw documents (the
// EMCOM networks use the raw path for their state).
// *mqttreceiver.Manager implements it.
type composePublisher interface {
	PublishActive(source string, h state.Hazard) error
	ExpireActive(source string, eventKey string) error
	PublishRaw(suffix string, retained bool, payload []byte) error
}

// composeStore is the optional local persistence surface for panel
// communications (satisfied by *sqlite.Store): the transition is routed
// from the local record and the retained MQTT document is only an
// asynchronous sync copy.
//
// Writing goes through SaveComposeHazardAndEnqueue: the record, its
// lifecycle state and the dispatch intake row commit in ONE transaction,
// and only its returned row id is handed to the live queue. Saving first
// and dispatching later is the whole point — an enqueued transition is a
// promise that the record exists.
type composeStore interface {
	SaveComposeHazardAndEnqueue(ctx context.Context, h storage.ComposeHazard, ev dispatch.Event) (int64, dispatch.Event, error)
	// ExpireComposeHazardIfCurrent expires one communication only while it
	// is still the version the caller read; applied=false means a newer
	// save won and nothing was written.
	ExpireComposeHazardIfCurrent(ctx context.Context, h storage.ComposeHazard, ev dispatch.Event, expectedVersion int64) (int64, dispatch.Event, bool, error)
	DeleteComposeHazard(ctx context.Context, eventKey string) error
	ComposeHazards(ctx context.Context) ([]storage.ComposeHazard, error)
}

// composeStatusesFor are the allowed document states for the form.
func composeStatusesFor(lang string) []option {
	return []option{
		{Value: "active", Label: i18n.T(lang, "compose.status.active")},
		{Value: "expired", Label: i18n.T(lang, "compose.status.expired")},
	}
}

// composeForm carries the submitted (or prefilled) communication values.
type composeForm struct {
	EventKey    string
	Event       string
	Severity    string
	Urgency     string
	Certainty   string
	Status      string
	Headline    string
	Description string
	Instruction string
	Areas       string
	Latitude    string
	Longitude   string
	EffectiveAt string
	ExpiresAt   string
	ReceivedAt  string
}

// composeItem is one module-issued communication in the list.
type composeItem struct {
	EventKey    string
	MsgID       string
	Event       string
	Severity    string
	Urgency     string
	Certainty   string
	Status      string
	Headline    string
	Areas       string
	EffectiveAt *time.Time
	ExpiresAt   *time.Time
	UpdatedAt   time.Time
}

// option is one select-option entry for the compose form (severity,
// urgency, certainty, status).
type option struct {
	Value string
	Label string
}

// Shared select options for the compose form, localized per UI language.
func optionList(lang string, values []string) []option {
	out := make([]option, 0, len(values))
	for _, v := range values {
		out = append(out, option{Value: v, Label: i18n.T(lang, "compose.opt."+v)})
	}
	return out
}

var (
	testSeverityValues  = []string{"unknown", "minor", "moderate", "severe", "extreme"}
	testUrgencyValues   = []string{"", "unknown", "immediate", "expected", "future", "past"}
	testCertaintyValues = []string{"", "unknown", "observed", "likely", "possible", "unlikely"}
)

// oneOf reports whether v is one of the option values.
func oneOf(v string, opts []option) bool {
	for _, o := range opts {
		if o.Value == v {
			return true
		}
	}
	return false
}

// composeView is the /compose page model.
type composeView struct {
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

	Source string
	Form   composeForm
	Items  []composeItem
	// External lists the ACTIVE communications that came from other
	// sources (weather, sensors, ingest feeds...): the operator can
	// retire a nuisance one right here.
	External []externalHazardItem
	Msg      string
	Error    string

	// ShowForm opens the collapsible compose panel on the initial
	// render: editing an existing communication or re-rendering after
	// a validation error. The default view is the issued list with the
	// panel closed.
	ShowForm bool

	Severities  []option
	Urgencies   []option
	Certainties []option
	Statuses    []option

	// Latitude/Longitude are the optional event coordinates for the
	// compose map picker; 0 when the APRS hub is disabled (no picker).
	AprsLat float64
	AprsLon float64

	// OfflineMode switches the picker map to the local tile tree;
	// ForceTiles does the same even while online (the Config switch).
	OfflineMode bool
	ForceTiles  bool

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavCompose       bool
	NavEmcom         bool
	NavHelp          bool
	NavMass          bool
	NavAccount       bool
	NavLogs          bool
	NavAudit         bool
	NavAPRS          bool
	NavMessages      bool
	NavGSM           bool
	NavMeshtastic    bool
	NavMeshMap       bool
	NavTraffic       bool
	NavWebsite       bool
	NavNotifications bool
	NavHealth        bool
	NavConfig        bool
}

// externalHazardItem is one active communication from another source,
// shown below the issued list with a retire action.
type externalHazardItem struct {
	EventKey  string
	MsgID     string
	Source    string
	Event     string
	Severity  string
	Urgency   string
	Certainty string
	Headline  string
	Areas     string
	UpdatedAt time.Time
}

// externalHazards projects the active non-compose communications onto
// the panel list (same merged view the public page uses).
func (s *Server) externalHazards() []externalHazardItem {
	var out []externalHazardItem
	for _, h := range s.activeHazards() {
		if h.Source == composeSource {
			continue
		}
		out = append(out, externalHazardItem{
			EventKey:  h.EventKey,
			MsgID:     core.MessageID(h.EventKey),
			Source:    h.Source,
			Event:     h.Event,
			Severity:  h.Severity,
			Urgency:   h.Urgency,
			Certainty: h.Certainty,
			Headline:  h.Headline,
			Areas:     strings.Join(h.Areas, ", "),
			UpdatedAt: h.UpdatedAt,
		})
	}
	return out
}

// composeFlashKey maps the post-action redirect marker to the banner
// message i18n key.
func composeFlashKey(marker string) string {
	switch marker {
	case "published":
		return "compose.flash.published"
	case "updated":
		return "compose.flash.updated"
	case "expired":
		return "compose.flash.expired"
	case "external-expired":
		return "compose.flash.external_expired"
	}
	return ""
}

// handleComposePage renders the compose form and the issued list.
func (s *Server) handleComposePage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	form := composeForm{Severity: "moderate", Status: "active"}

	if key := r.URL.Query().Get("edit"); key != "" {
		h, ok := s.composeHazard(key)
		if !ok {
			form = composeForm{Severity: "moderate", Status: "active"}
		} else {
			form = composeFormFromHazard(h, s.displayLoc())
		}
	}

	view := s.buildComposeView(s.langFor(r), form)
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	view.Msg = i18n.T(s.langFor(r), composeFlashKey(r.URL.Query().Get("msg")))
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "compose", view)
}

// handleComposeSave validates the form and publishes the communication
// as a retained active-hazard document. Existing event keys update the
// same topic; status "expired" removes it (same protocol effect as the
// explicit expire action).
func (s *Server) handleComposeSave(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}

	form := composeForm{
		EventKey:    strings.TrimSpace(r.PostFormValue("event_key")),
		Event:       strings.TrimSpace(r.PostFormValue("event")),
		Severity:    strings.ToLower(strings.TrimSpace(r.PostFormValue("severity"))),
		Urgency:     strings.ToLower(strings.TrimSpace(r.PostFormValue("urgency"))),
		Certainty:   strings.ToLower(strings.TrimSpace(r.PostFormValue("certainty"))),
		Status:      strings.ToLower(strings.TrimSpace(r.PostFormValue("status"))),
		Headline:    strings.TrimSpace(r.PostFormValue("headline")),
		Description: strings.TrimSpace(r.PostFormValue("description")),
		Instruction: strings.TrimSpace(r.PostFormValue("instruction")),
		Areas:       strings.TrimSpace(r.PostFormValue("areas")),
		Latitude:    strings.TrimSpace(r.PostFormValue("latitude")),
		Longitude:   strings.TrimSpace(r.PostFormValue("longitude")),
		EffectiveAt: strings.TrimSpace(r.PostFormValue("effective_at")),
		ExpiresAt:   strings.TrimSpace(r.PostFormValue("expires_at")),
		ReceivedAt:  strings.TrimSpace(r.PostFormValue("received_at")),
	}
	// The time fields are shown as ISO stamps with a 24-hour clock. A
	// value that arrives in another accepted shape is rewritten on the way
	// in, so the re-rendered form always shows the same format.
	form.EffectiveAt = canonicalComposeStamp(form.EffectiveAt)
	form.ExpiresAt = canonicalComposeStamp(form.ExpiresAt)
	if form.Status == "" {
		form.Status = "active"
	}

	if msg := validateComposeForm(s.langFor(r), form); msg != "" {
		s.renderComposeError(w, r, http.StatusUnprocessableEntity, form, msg)
		return
	}

	h := composeHazardFromForm(form, time.Now())

	store, hasStore := s.users.(composeStore)
	if !hasStore {
		// Mirror-only installations keep the broker document as the
		// record: publishing is required and synchronous (the old
		// contract; tests and minimal constructions rely on it).
		if s.pub == nil {
			s.logger.Warn("compose: publish skipped, no broker publisher configured")
			s.renderComposeError(w, r, http.StatusServiceUnavailable, form,
				i18n.T(s.langFor(r), "compose.err.no_publisher"))
			return
		}
		if err := s.pub.PublishActive(composeSource, h); err != nil {
			s.logger.Warn("compose: publish failed", "event_key", h.EventKey, "error", err)
			s.renderComposeError(w, r, http.StatusServiceUnavailable, form,
				"publish failed: "+err.Error())
			return
		}
	}

	typ := dispatch.TransitionNew
	if form.EventKey != "" {
		typ = dispatch.TransitionUpdated
	}
	if form.Status == "expired" {
		typ = dispatch.TransitionExpired
	}

	transition := composeTransition(h, typ)
	// savedVersion is the version of the record this save wrote: the
	// expiry timer carries it so an edit that lands later supersedes the
	// timer instead of being expired by it.
	var savedVersion int64
	if hasStore {
		// LOCAL-FIRST + ATOMIC (P1): the state, the lifecycle and the
		// dispatch intake row commit in ONE transaction and only then
		// does the transition enter the live queue. The old order
		// (enqueue first, record later, save error only logged) answered
		// "published" for a message whose record never landed: the panel
		// view could not be rebuilt after a restart and the message
		// could not be traced or retired. A failed save now rejects the
		// operation and nothing is dispatched.
		stateJSON, err := json.Marshal(h)
		if err != nil {
			s.logger.Warn("compose: local record marshal failed", "event_key", h.EventKey, "error", err)
			s.renderComposeError(w, r, http.StatusServiceUnavailable, form,
				i18n.T(s.langFor(r), "compose.err.save"))
			return
		}
		// The store allocates the version of THIS save inside the same
		// transaction and returns the stamped transition: the version is
		// what lets the delivery gate hold the text being replaced back
		// once a correction supersedes it (reported P1).
		id, stamped, err := store.SaveComposeHazardAndEnqueue(context.Background(), storage.ComposeHazard{
			// Expired communications stay as authoritative tombstones so
			// a stale broker copy can never revive them.
			EventKey: h.EventKey, State: stateJSON, Status: form.Status, UpdatedAt: time.Now(),
		}, transition)
		if err != nil {
			s.logger.Warn("compose: local record save failed", "event_key", h.EventKey, "error", err)
			s.renderComposeError(w, r, http.StatusServiceUnavailable, form,
				i18n.T(s.langFor(r), "compose.err.save"))
			return
		}
		// The row is already durable, so the live offer never duplicates
		// it and a full queue defers delivery to intake recovery instead
		// of losing the message.
		transition = stamped
		transition.InboxID = id
		if stamped.Hazard != nil {
			savedVersion = stamped.Hazard.ChangeID
		}
	}

	switch s.ingress.Enqueue(transition) {
	case dispatch.Rejected:
		// The record is durable; only the live hop did not happen (the
		// intake is stopped, i.e. the station is shutting down). Intake
		// recovery delivers the row after the restart, so the operator
		// still gets an explicit answer instead of a silent success.
		s.logger.Warn("compose: local dispatch rejected the transition", "event_key", h.EventKey, "inbox_id", transition.InboxID)
		s.renderComposeError(w, r, http.StatusServiceUnavailable, form,
			i18n.T(s.langFor(r), "compose.err.dispatch"))
		return
	case dispatch.AcceptedEmergency:
		s.logger.Warn("compose: transition accepted WITHOUT durable storage (emergency mode; lost on restart)", "event_key", h.EventKey)
	}

	if hasStore {
		// Broker sync is asynchronous and best-effort: a down broker
		// never blocks local delivery (the resync hook republishes the
		// current state on every receiver reconnect).
		if s.pub != nil {
			go func() {
				if err := s.pub.PublishActive(composeSource, h); err != nil {
					s.logger.Warn("compose: broker sync failed (local dispatch already durable)",
						"event_key", h.EventKey, "error", err)
				}
			}()
		}
	}
	s.logger.Info("compose: communication dispatched",
		"event_key", h.EventKey, "severity", h.Severity, "status", h.Status)

	// Active communications expire on their own at expires_at (the
	// retained document is removed and the expiry flows through the
	// canonical ingress like the manual expire action).
	if form.Status == "active" {
		s.scheduleComposeExpiry(h, savedVersion)
	}

	flash := "published"
	if form.EventKey != "" {
		flash = "updated"
	}
	if form.Status == "expired" {
		flash = "expired"
	}
	s.audit(sess.username, "compose-"+flash, h.EventKey)
	http.Redirect(w, r, "/compose?msg="+flash, http.StatusSeeOther)
}

// handleComposeExpire removes one module-issued communication by
// publishing an empty retained payload on its topic.
func (s *Server) handleComposeExpire(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	key := strings.TrimSpace(r.PostFormValue("event_key"))
	h, ok := s.composeHazard(key)
	if !ok {
		http.Error(w, "unknown communication", http.StatusNotFound)
		return
	}

	store, hasStore := s.users.(composeStore)
	transition := composeTransition(h, dispatch.TransitionExpired)
	if hasStore {
		// LOCAL-FIRST + ATOMIC (P1): the expired record, its lifecycle
		// and the expiry intake row commit in ONE transaction, and the
		// transition reaches the live queue only after that commit — a
		// failed write rejects the operation (the form sees the failure
		// instead of a silent success) and a crash can never leave an
		// expired record with an active lifecycle that still allows the
		// retired message through the delivery queue.
		id, stamped, err := store.SaveComposeHazardAndEnqueue(context.Background(), storage.ComposeHazard{
			EventKey: key, State: composeStateJSON(h), Status: "expired", UpdatedAt: time.Now(),
		}, transition)
		if err != nil {
			s.logger.Warn("compose: local expiry record failed", "event_key", key, "error", err)
			http.Error(w, "local record save failed", http.StatusServiceUnavailable)
			return
		}
		transition = stamped
		transition.InboxID = id
	} else {
		// Mirror-only: the broker document is the record; the tombstone
		// publish is required and synchronous (the old contract).
		if s.pub == nil {
			s.logger.Warn("compose: expire skipped, no broker publisher configured", "event_key", key)
			http.Error(w, "publishing is unavailable: no broker publisher is configured", http.StatusServiceUnavailable)
			return
		}
		if err := s.pub.ExpireActive(composeSource, key); err != nil {
			s.logger.Warn("compose: expire failed", "event_key", key, "error", err)
			http.Error(w, "expire failed: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
	}

	// LOCAL-FIRST: the expiry transition is routed locally after the
	// acceptance above, before any broker I/O.
	if s.ingress.Enqueue(transition) == dispatch.Rejected {
		s.logger.Warn("compose: local dispatch rejected the expiry transition", "event_key", key, "inbox_id", transition.InboxID)
		if hasStore {
			http.Error(w, "dispatch intake rejected the expiry", http.StatusServiceUnavailable)
			return
		}
	}
	if hasStore {
		// Broker sync is asynchronous and best-effort (the resync hook
		// republishes the current state on receiver reconnect).
		if s.pub != nil {
			go func() {
				if err := s.pub.ExpireActive(composeSource, key); err != nil {
					s.logger.Warn("compose: broker expiry sync failed", "event_key", key, "error", err)
				}
			}()
		}
	}
	s.logger.Info("compose: communication expired", "event_key", key)
	s.audit(sess.username, "compose-expire", key)
	http.Redirect(w, r, "/compose?msg=expired", http.StatusSeeOther)
}

// handleExternalExpire retires one ACTIVE communication that came from
// another source (the operator kills a nuisance alert): the local store
// records the cancellation (quietly — cancellations never notify), the
// canonical transition flows through the local ingress and the retained
// broker document is removed so every instance drops it. A source that
// re-reports the hazard later revives it — that is a NEW report.
func (s *Server) handleExternalExpire(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	key := strings.TrimSpace(r.PostFormValue("event_key"))
	if key == "" {
		http.Error(w, "missing event key", http.StatusBadRequest)
		return
	}
	var h state.Hazard
	found := false
	for _, x := range s.activeHazards() {
		if x.Source != composeSource && x.EventKey == key {
			h = x
			found = true
			break
		}
	}
	if !found {
		http.Error(w, "unknown active communication", http.StatusNotFound)
		return
	}

	// LOCAL-FIRST: the store records the cancellation and the canonical
	// transition rides through the ingress like any other change.
	if s.events != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		st, err := s.events.Get(ctx, key)
		if err == nil && st != nil {
			ev := st.Event
			ev.Status = core.StatusCancelled
			_, change, err := s.events.Ingest(ctx, ev, core.Fingerprint(ev))
			cancel()
			if err != nil {
				s.logger.Warn("compose: external expire ingest failed", "event_key", key, "error", err)
				http.Error(w, "local record save failed", http.StatusServiceUnavailable)
				return
			}
			if change != nil {
				evt := dispatch.EventForJournalChange(change.ChangeType, change.ID, change.Publisher, change.Event, "panel", time.Now())
				evt.InboxID = change.InboxID
				if s.ingress.Enqueue(evt) == dispatch.Rejected {
					s.logger.Warn("compose: external expire dispatch rejected", "event_key", key)
				}
			}
		} else {
			cancel()
			// Mirror-only copy (another instance's document): the broker
			// tombstone below is the only record.
		}
	}

	// Broker tombstone: removes the retained document for every instance
	// (best-effort; the resync path reconciles on reconnect).
	if s.pub != nil {
		go func() {
			if err := s.pub.ExpireActive(h.Source, key); err != nil {
				s.logger.Warn("compose: external expire broker sync failed", "event_key", key, "error", err)
			}
		}()
	}
	s.logger.Info("compose: external communication expired", "event_key", key, "source", h.Source)
	s.audit(sess.username, "external-expire", key+" source="+h.Source)
	http.Redirect(w, r, "/compose?msg=external-expired", http.StatusSeeOther)
}

// scheduleComposeExpiry auto-expires a communication at its expires_at:
// the local record and the expiry transition are handled first, the
// retained broker document removal follows asynchronously. The timer
// lives only in this process; after a restart, the expiry transitions
// are recovered from the durable inbox and the local record keeps the
// tombstone.
func (s *Server) scheduleComposeExpiry(h state.Hazard, version int64) {
	if h.Status != "active" || h.ExpiresAt == nil || !h.ExpiresAt.After(time.Now()) {
		return
	}
	expires := *h.ExpiresAt
	key := h.EventKey
	time.AfterFunc(time.Until(expires), func() {
		cur, ok := s.composeHazard(key)
		if !ok || cur.Status != "active" {
			return // already removed (manual expire / empty retained payload)
		}
		if cur.ExpiresAt == nil || !cur.ExpiresAt.Equal(expires) {
			return // re-published with a different expiry; that timer owns it
		}
		if store, hasStore := s.users.(composeStore); hasStore {
			// The expired record, its lifecycle and the expiry intake row
			// commit in ONE transaction, guarded by the version this timer
			// was scheduled for: an edit that landed in the meantime (a
			// longer validity, a correction) allocated a newer version and
			// must not be expired by this one. A failed save must not
			// dispatch the expiry either — the stale message stays active
			// and the manual expire remains available.
			transition := composeTransition(cur, dispatch.TransitionExpired)
			id, stamped, applied, err := store.ExpireComposeHazardIfCurrent(context.Background(), storage.ComposeHazard{
				EventKey: key, State: composeStateJSON(cur), Status: "expired", UpdatedAt: time.Now(),
			}, transition, version)
			if err != nil {
				s.logger.Warn("compose: auto-expiry local record failed; keeping the expiry transition unsent", "event_key", key, "error", err)
				return
			}
			if !applied {
				s.logger.Info("compose: auto-expiry skipped, the communication changed or carries no version", "event_key", key)
				return
			}
			transition = stamped
			transition.InboxID = id
			if s.ingress.Enqueue(transition) == dispatch.Rejected {
				s.logger.Warn("compose: auto-expiry transition rejected by the stopped intake (recovery will deliver it)", "event_key", key, "inbox_id", id)
				return
			}
		} else if s.ingress.Enqueue(composeTransition(cur, dispatch.TransitionExpired)) == dispatch.Rejected {
			s.logger.Warn("compose: dispatch queue full, auto-expiry transition rejected", "event_key", key)
			return
		}
		s.logger.Info("compose: communication auto-expired", "event_key", key)
		if s.pub != nil {
			_, hasStore := s.users.(composeStore)
			if !hasStore {
				// Mirror-only: the broker document is the record.
				if err := s.pub.ExpireActive(composeSource, key); err != nil {
					s.logger.Warn("compose: auto-expire failed", "event_key", key, "error", err)
				}
			} else {
				go func() {
					if err := s.pub.ExpireActive(composeSource, key); err != nil {
						s.logger.Warn("compose: auto-expiry broker sync failed", "event_key", key, "error", err)
					}
				}()
			}
		}
	})
}

// AutoExpireCompose reconciles module-issued communications whose
// expires_at has passed. The per-communication AfterFunc timers die
// with the process, so a restart (or a missed timer) must never leave
// an active communication alive past its validity — the maintenance
// loop runs this periodically and the expiry is durable: local record
// first, then the dispatch transition, then the broker sync. Returns
// the number of communications expired.
func (s *Server) AutoExpireCompose(ctx context.Context) (int, error) {
	cs, ok := s.users.(composeStore)
	if !ok {
		return 0, nil // mirror-only: the mirror prunes expired entries itself
	}
	rows, err := cs.ComposeHazards(ctx)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	expired := 0
	for _, row := range rows {
		if row.Status != "active" {
			continue
		}
		var h state.Hazard
		if err := json.Unmarshal(row.State, &h); err != nil {
			s.logger.Warn("compose: local record corrupt during auto-expiry", "event_key", row.EventKey, "error", err)
			continue
		}
		if h.ExpiresAt == nil || h.ExpiresAt.After(now) {
			continue
		}
		// Record + intake row in ONE transaction, CONDITIONAL on the
		// version read above: the pass lists the communications and then
		// expires them, so an edit landing in between — the operator
		// extending the validity, a correction — allocated a newer version
		// and must win instead of being overwritten with the stale content
		// (reported P1). A failed save must not dispatch the expiry either:
		// the communication stays active and the manual expire remains
		// available.
		transition := composeTransition(h, dispatch.TransitionExpired)
		id, stamped, applied, err := cs.ExpireComposeHazardIfCurrent(ctx, storage.ComposeHazard{
			EventKey: row.EventKey, State: row.State, Status: "expired", UpdatedAt: now,
		}, transition, row.Version)
		if err != nil {
			s.logger.Warn("compose: auto-expiry local record failed", "event_key", row.EventKey, "error", err)
			continue
		}
		if !applied {
			s.logger.Info("compose: auto-expiry skipped, the communication changed or carries no version", "event_key", row.EventKey)
			continue
		}
		transition = stamped
		transition.InboxID = id
		if s.ingress.Enqueue(transition) == dispatch.Rejected {
			// The row is durable: recovery delivers it after the restart.
			s.logger.Warn("compose: auto-expiry transition rejected by the stopped intake (recovery will deliver it)", "event_key", row.EventKey, "inbox_id", id)
			continue
		}
		if s.pub != nil {
			go func(key string) {
				if err := s.pub.ExpireActive(composeSource, key); err != nil {
					s.logger.Warn("compose: auto-expiry broker sync failed", "event_key", key, "error", err)
				}
			}(row.EventKey)
		}
		s.logger.Info("compose: communication auto-expired (reconciliation)", "event_key", row.EventKey)
		expired++
	}
	return expired, nil
}

// composeStateJSON marshals one hazard for the local record (nil on
// failure; callers log and continue — the dispatch inbox already holds
// the transition).
func composeStateJSON(h state.Hazard) []byte {
	data, err := json.Marshal(h)
	if err != nil {
		return nil
	}
	return data
}

// composeTransition builds the canonical ingress event for one compose
// action (new / updated / expired).
func composeTransition(h state.Hazard, typ dispatch.TransitionType) dispatch.Event {
	now := time.Now()
	return dispatch.Event{
		Kind:       dispatch.EventHazardTransition,
		ReceivedAt: now,
		Origin:     dispatch.Origin{Type: "web", ReceiverID: "compose"},
		Hazard: &dispatch.HazardTransition{
			Type:      typ,
			Key:       h.EventKey,
			Source:    composeSource,
			Timestamp: now,
			Hazard: dispatch.Hazard{
				EventKey:  h.EventKey,
				MsgID:     core.MessageID(h.EventKey),
				Source:    h.Source,
				SourceID:  h.SourceID,
				Event:     h.Event,
				Severity:  h.Severity,
				Urgency:   h.Urgency,
				Certainty: h.Certainty,
				Headline:  h.Headline, Description: h.Description,
				Instruction: h.Instruction, Areas: h.Areas,
				Latitude:    h.Latitude,
				Longitude:   h.Longitude,
				EffectiveAt: h.EffectiveAt,
				ExpiresAt:   h.ExpiresAt,
				ReceivedAt:  h.ReceivedAt,
				UpdatedAt:   h.UpdatedAt,
			},
		},
	}
}

// validateComposeForm returns a user-facing message for invalid input,
// localized to the UI language.
func validateComposeForm(lang string, form composeForm) string {
	if form.EventKey != "" && !composeEventKeyRe.MatchString(form.EventKey) {
		return i18n.T(lang, "compose.err.invalid_key")
	}
	if form.Event == "" {
		return i18n.T(lang, "compose.err.empty_event")
	}
	if len(form.Event) > maxComposeEvent {
		return fmt.Sprintf(i18n.T(lang, "compose.err.event_long"), maxComposeEvent)
	}
	if form.Headline == "" {
		return i18n.T(lang, "compose.err.empty_headline")
	}
	if len(form.Headline) > maxComposeHeadline {
		return fmt.Sprintf(i18n.T(lang, "compose.err.headline_long"), maxComposeHeadline)
	}
	if !severity.Valid(form.Severity) {
		return i18n.T(lang, "compose.err.invalid_severity")
	}
	if form.Urgency != "" && !oneOf(form.Urgency, optionList(lang, testUrgencyValues)) {
		return i18n.T(lang, "compose.err.invalid_urgency")
	}
	if form.Certainty != "" && !oneOf(form.Certainty, optionList(lang, testCertaintyValues)) {
		return i18n.T(lang, "compose.err.invalid_certainty")
	}
	if form.Status != "active" && form.Status != "expired" {
		return i18n.T(lang, "compose.err.invalid_status")
	}
	if len(form.Description) > maxComposeText || len(form.Instruction) > maxComposeText {
		return fmt.Sprintf(i18n.T(lang, "compose.err.text_limit"), maxComposeText)
	}
	if len(form.Areas) > maxComposeAreasLen {
		return fmt.Sprintf(i18n.T(lang, "compose.err.areas_long"), maxComposeAreasLen)
	}
	if (form.Latitude == "") != (form.Longitude == "") {
		return i18n.T(lang, "compose.err.coords_together")
	}
	if form.Latitude != "" {
		lat, err1 := strconv.ParseFloat(form.Latitude, 64)
		lon, err2 := strconv.ParseFloat(form.Longitude, 64)
		if err1 != nil || err2 != nil {
			return i18n.T(lang, "compose.err.invalid_coords")
		}
		if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
			return i18n.T(lang, "compose.err.coords_range")
		}
	}
	if form.EffectiveAt != "" && parseComposeTime(form.EffectiveAt) == nil {
		return i18n.T(lang, "compose.err.invalid_effective")
	}
	if form.ExpiresAt != "" && parseComposeTime(form.ExpiresAt) == nil {
		return i18n.T(lang, "compose.err.invalid_expires")
	}
	return ""
}

// parseComposeTime parses one time field in the server's local zone. The
// form shows ISO stamps ("2026-10-12 14:30"); the older datetime-local "T"
// form and RFC 3339 strings (hidden round-trips) are accepted too, so a
// value pasted from elsewhere still works.
func parseComposeTime(v string) *time.Time {
	for _, layout := range []string{"2006-01-02 15:04", "2006-01-02T15:04"} {
		if t, err := time.ParseInLocation(layout, v, time.Local); err == nil {
			return &t
		}
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return &t
	}
	return nil
}

// canonicalComposeStamp rewrites an accepted stamp into the one the form
// displays, so a value pasted in another shape comes back in the format
// the station speaks. Unparsable input is returned as typed: validation
// reports it and the operator still sees what to fix.
func canonicalComposeStamp(v string) string {
	t := parseComposeTime(v)
	if t == nil {
		return v
	}
	return t.Format("2006-01-02 15:04")
}

// composeTimeValue formats an optional time for the compose time field in
// the server's display timezone.
func composeTimeValue(t *time.Time, loc *time.Location) string {
	if t == nil {
		return ""
	}
	return t.In(loc).Format("2006-01-02 15:04")
}

// composeAreas splits the comma/semicolon-separated areas field.
func composeAreas(raw string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ';' })
	areas := make([]string, 0, len(parts))
	for _, p := range parts {
		if a := strings.TrimSpace(p); a != "" {
			areas = append(areas, a)
		}
	}
	if len(areas) > maxComposeAreas {
		areas = areas[:maxComposeAreas]
	}
	return areas
}

// composeHazardFromForm assembles the mirror hazard for publication.
func composeHazardFromForm(form composeForm, now time.Time) state.Hazard {
	key := form.EventKey
	if key == "" {
		key = fmt.Sprintf("%s:%d", composeSource, now.UnixMilli())
	}
	received := parseComposeTime(form.ReceivedAt)
	if received == nil {
		received = &now
	}
	return state.Hazard{
		EventKey:    key,
		Source:      composeSource,
		SourceID:    "ops",
		Event:       form.Event,
		Severity:    form.Severity,
		Urgency:     form.Urgency,
		Certainty:   form.Certainty,
		Headline:    form.Headline,
		Description: form.Description,
		Instruction: form.Instruction,
		Areas:       composeAreas(form.Areas),
		Latitude:    parseComposeCoord(form.Latitude),
		Longitude:   parseComposeCoord(form.Longitude),
		Status:      form.Status,
		EffectiveAt: parseComposeTime(form.EffectiveAt),
		ExpiresAt:   parseComposeTime(form.ExpiresAt),
		ReceivedAt:  *received,
		UpdatedAt:   now,
	}
}

// parseComposeCoord parses one optional coordinate form value; empty
// yields nil (validated elsewhere).
func parseComposeCoord(raw string) *float64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil
	}
	return &v
}

// composeCoordValue formats an optional coordinate for the form.
func composeCoordValue(v *float64) string {
	if v == nil {
		return ""
	}
	return strconv.FormatFloat(*v, 'f', 5, 64)
}

// composeFormFromHazard prefills the form for an edit.
func composeFormFromHazard(h state.Hazard, loc *time.Location) composeForm {
	return composeForm{
		EventKey:    h.EventKey,
		Event:       h.Event,
		Severity:    h.Severity,
		Urgency:     h.Urgency,
		Certainty:   h.Certainty,
		Status:      h.Status,
		Headline:    h.Headline,
		Description: h.Description,
		Instruction: h.Instruction,
		Areas:       strings.Join(h.Areas, ", "),
		Latitude:    composeCoordValue(h.Latitude),
		Longitude:   composeCoordValue(h.Longitude),
		EffectiveAt: composeTimeValue(h.EffectiveAt, loc),
		ExpiresAt:   composeTimeValue(h.ExpiresAt, loc),
		ReceivedAt:  h.ReceivedAt.Format(time.RFC3339),
	}
}

// composeHazard returns one module-issued hazard: the local record wins
// (authoritative and broker-independent); the mirror covers
// communications issued by other instances.
func (s *Server) composeHazard(eventKey string) (state.Hazard, bool) {
	if st, ok := s.users.(composeStore); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		rows, err := st.ComposeHazards(ctx)
		cancel()
		if err == nil {
			for _, row := range rows {
				if row.EventKey != eventKey {
					continue
				}
				var h state.Hazard
				if err := json.Unmarshal(row.State, &h); err != nil {
					s.logger.Warn("compose: local record corrupt", "event_key", eventKey, "error", err)
					return state.Hazard{}, false
				}
				h.Status = row.Status
				return h, true
			}
		} else {
			s.logger.Warn("compose: local record read failed", "error", err)
		}
		// Not in the local record (or it is unreadable): fall back to the
		// mirror, which also covers communications issued by other
		// instances.
	}
	for _, h := range s.st.Snapshot().Hazards {
		if h.Source == composeSource && h.EventKey == eventKey {
			return h, true
		}
	}
	return state.Hazard{}, false
}

// composeItems returns the module-issued communications, newest first.
// The local record is authoritative for keys it owns; mirror-only
// entries (other instances' communications) fill in the rest.
func (s *Server) composeItems() []composeItem {
	if st, ok := s.users.(composeStore); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		rows, err := st.ComposeHazards(ctx)
		cancel()
		if err != nil {
			s.logger.Warn("compose: local record read failed", "error", err)
		} else {
			items := make([]composeItem, 0, len(rows))
			seen := make(map[string]bool, len(rows))
			for _, row := range rows {
				seen[row.EventKey] = true
				if row.Status != "active" {
					continue // expired tombstones stay invisible
				}
				var h state.Hazard
				if err := json.Unmarshal(row.State, &h); err != nil {
					s.logger.Warn("compose: local record corrupt", "event_key", row.EventKey, "error", err)
					continue
				}
				items = append(items, composeItemFrom(h, row.Status))
			}
			for _, h := range s.st.Snapshot().Hazards {
				if h.Source != composeSource || seen[h.EventKey] {
					continue
				}
				items = append(items, composeItemFrom(h, h.Status))
			}
			sort.Slice(items, func(i, j int) bool { return items[i].UpdatedAt.After(items[j].UpdatedAt) })
			return items
		}
	}
	// Mirror-only installations: the retained state is the list.
	hazards := s.st.Snapshot().Hazards
	items := make([]composeItem, 0, len(hazards))
	for _, h := range hazards {
		if h.Source != composeSource {
			continue
		}
		items = append(items, composeItemFrom(h, h.Status))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].UpdatedAt.After(items[j].UpdatedAt) })
	return items
}

// composeItemFrom projects one hazard into a list item.
func composeItemFrom(h state.Hazard, status string) composeItem {
	return composeItem{
		EventKey:    h.EventKey,
		MsgID:       core.MessageID(h.EventKey),
		Event:       h.Event,
		Severity:    h.Severity,
		Urgency:     h.Urgency,
		Certainty:   h.Certainty,
		Status:      status,
		Headline:    h.Headline,
		Areas:       strings.Join(h.Areas, ", "),
		EffectiveAt: h.EffectiveAt,
		ExpiresAt:   h.ExpiresAt,
		UpdatedAt:   h.UpdatedAt,
	}
}

// buildComposeView assembles the page model in a UI language.
func (s *Server) buildComposeView(lang string, form composeForm) composeView {
	view := composeView{
		AppTitle:    s.cfg.Title,
		Name:        s.displayName(),
		Header1:     s.displayHeader1(),
		Header2:     s.DisplayHeader2(),
		Tagline:     s.DisplayTagline(),
		Version:     s.version,
		Commit:      s.commit,
		RepoURL:     repoURL,
		Source:      composeSource,
		Form:        form,
		Items:       s.composeItems(),
		External:    s.externalHazards(),
		Severities:  optionList(lang, testSeverityValues),
		Urgencies:   optionList(lang, testUrgencyValues),
		Certainties: optionList(lang, testCertaintyValues),
		Statuses:    composeStatusesFor(lang),
		NavCompose:  true,
		OfflineMode: s.OfflineMode(),
		ForceTiles:  s.forceTiles.Load(),
		// Editing opens the panel on the initial render; a fresh page
		// starts with the issued list and a collapsed panel.
		ShowForm: form.EventKey != "",
	}
	// The map picker centers on the operational area (the territory we
	// serve); 0 = no picker (APRS hub disabled).
	if s.aprs != nil && s.aprs.Enabled() {
		view.AprsLat, view.AprsLon = s.aprs.AreaLat(), s.aprs.AreaLon()
	}
	return view
}

// renderComposeError re-renders the page with an error banner, preserving
// the submitted form values.
func (s *Server) renderComposeError(w http.ResponseWriter, r *http.Request, status int, form composeForm, msg string) {
	sess := s.sessions.currentSession(r)
	view := s.buildComposeView(s.langFor(r), form)
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	view.Error = msg
	view.ShowForm = true // keep the form visible so the operator sees what failed
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	s.renderL(w, r, "compose", view)
}
