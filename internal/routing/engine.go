// Package routing implements the group notification rule engine: the
// consumer of the canonical dispatch ingress.
//
//	receiver callback -> canonical Event -> ingress queue
//	    -> rule engine: per-cell (source × action) severity matrix
//	       -> assigned actions
//
// Every group is a notification channel: a routing matrix in which every
// cell reads "events from input plugin S at severity ≥ T fire action A".
// A cell without a source (empty) matches every source and acts as the
// fallback when no source-specific cell for that action matches. Only
// NEW and UPDATED transitions notify: cancelled/expired transitions
// retire the dashboard view and never start the notification machine.
// Output plugins need no routing here — they receive every journal change
// by default. Rules are reloaded from storage on an interval, so edits
// made in the web UI take effect without a restart.
package routing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/metrics"
	"github.com/szporwolik/WarnFlux/internal/severity"
	"github.com/szporwolik/WarnFlux/internal/storage"
	"github.com/szporwolik/WarnFlux/internal/trail"
)

// ActionSubmitter is the action half of rule evaluation. It is satisfied
// by *action.Manager.
type ActionSubmitter interface {
	Submit(id string, req action.ActionRequest) error
}

// RuleStore supplies the authoritative group routings. It is satisfied by
// the SQLite directory store.
type RuleStore interface {
	ListGroupRoutings() ([]storage.GroupRouting, error)
	// GroupRecipientEmails returns the group members' contact addresses
	// (empty list when the group has none).
	GroupRecipientEmails(groupID int64) ([]string, error)
	// GroupRecipientAPRS returns the group members' registered APRS
	// callsigns (empty list when the group has none).
	GroupRecipientAPRS(groupID int64) ([]string, error)
	// GroupRecipientDiscord returns the group members' registered
	// Discord handles (empty list when the group has none).
	GroupRecipientDiscord(groupID int64) ([]string, error)
	// EnqueueDelivery persists the durable delivery job — full payload
	// (event + recipients), attempt counter and next-attempt deadline
	// all live in the row. queued=true means this transition made the
	// job pending (fresh insert, or a terminally failed job re-armed);
	// queued=false means the transition was deduplicated and st
	// explains why (already pending or already delivered).
	EnqueueDelivery(ctx context.Context, job storage.DeliveryJob) (storage.DeliveryStatus, bool, error)
}

// Inbox is the optional durable dispatch inbox: events persisted before
// routing are re-delivered after a restart or a full live queue, and
// acknowledged once the engine evaluated them.
type Inbox interface {
	PendingInboxEvents(ctx context.Context, limit int) ([]storage.InboxItem, error)
	AckInboxEvent(ctx context.Context, id int64) error
}

// inboxBatch bounds one recovery pass over the durable inbox.
const inboxBatch = 64

// defaultRefreshInterval is how often rules are reloaded from storage.
const defaultRefreshInterval = 10 * time.Second

// Engine evaluates group notification rules for every canonical hazard
// transition drained from the dispatch ingress. It never blocks the
// ingress: deliveries that fail are counted and logged.
type Engine struct {
	store   RuleStore
	actions ActionSubmitter
	logger  *slog.Logger
	trail   *trail.Recorder

	// reg is the optional metrics registry; notifCells caches the
	// per-action notification counters (action IDs are configuration).
	reg        *metrics.Registry
	notifCells sync.Map // actionID|result -> func(int64)

	// app is stamped onto every action request (footers, links).
	app action.AppInfo

	refreshInterval time.Duration

	mu    sync.RWMutex
	rules []storage.GroupRouting
	// bcc caches each group's member contact addresses (groupID -> emails),
	// loaded alongside the rules; only groups with assigned actions are
	// queried.
	bcc map[int64][]string
	// aprsBcc caches each group's members' registered APRS callsigns
	// (groupID -> callsigns).
	aprsBcc map[int64][]string
	// discordBcc caches each group's members' registered Discord handles
	// (groupID -> handles).
	discordBcc map[int64][]string

	// Stats counters (atomic).
	eventsSeen         atomic.Int64
	transitionsSkipped atomic.Int64
	rulesMatched       atomic.Int64
	actionsFired       atomic.Int64
	actionsFailed      atomic.Int64
	actionsDeduped     atomic.Int64
	ruleLoadErrors     atomic.Int64
	rulesLoaded        atomic.Int64
	// lastRefresh is the unix-nano timestamp of the last successfully
	// installed routing snapshot (0 = never).
	lastRefresh atomic.Int64

	// inbox is the optional durable dispatch inbox (see Inbox): events
	// recovered from it are evaluated like live ones and acknowledged
	// afterwards.
	inbox Inbox
}

// New builds an engine with the default refresh interval. trail is the
// optional per-alert audit recorder (may be nil); reg is the optional
// metrics registry (may be nil).
func New(store RuleStore, actions ActionSubmitter, logger *slog.Logger, app action.AppInfo, trail *trail.Recorder, reg *metrics.Registry) *Engine {
	return &Engine{
		store:           store,
		actions:         actions,
		logger:          logger,
		trail:           trail,
		reg:             reg,
		app:             app,
		refreshInterval: defaultRefreshInterval,
	}
}

// notifCount increments (or creates) the warnflux_notifications_total
// counter for one (action, result) pair.
func (e *Engine) notifCount(actionID, result string, delta int64) {
	if e.reg == nil {
		return
	}
	k := actionID + "|" + result
	fn, ok := e.notifCells.Load(k)
	if !ok {
		fn = e.reg.Counter("warnflux_notifications_total",
			"Notification outcomes per action.",
			"action", actionID, "result", result)
		e.notifCells.Store(k, fn)
	}
	fn.(func(int64))(delta)
}

// SetInbox attaches the durable dispatch inbox (optional): pending rows
// re-enter the engine on the refresh tick and after restarts.
func (e *Engine) SetInbox(in Inbox) {
	e.inbox = in
}

// Run drains events until ctx is cancelled or the channel is closed
// (ingress StopIntake). It also reloads the rules and recovers the
// durable inbox on an interval.
func (e *Engine) Run(ctx context.Context, events <-chan dispatch.Event) {
	e.refresh()
	ticker := time.NewTicker(e.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			e.handle(ctx, ev)
		case <-ticker.C:
			e.refresh()
			e.recoverInbox(ctx)
		}
	}
}

// recoverInbox re-evaluates durable inbox rows that were accepted but
// never acknowledged (crash between acceptance and evaluation, or a full
// live queue). The per-(group, action) delivery jobs deduplicate, so
// re-evaluation is safe.
func (e *Engine) recoverInbox(ctx context.Context) {
	if e.inbox == nil {
		return
	}
	items, err := e.inbox.PendingInboxEvents(ctx, inboxBatch)
	if err != nil {
		e.logger.Warn("routing: inbox recovery failed", "error", err)
		return
	}
	for _, it := range items {
		e.handle(ctx, it.Event)
	}
}

// Ready reports whether the engine has completed at least one rule load:
// events fed before that moment evaluate against an empty matrix and are
// skipped, so callers (and tests) can wait for it.
func (e *Engine) Ready() bool {
	return e.rulesLoaded.Load() > 0
}

// SnapshotAge reports how long ago the installed routing snapshot was
// refreshed successfully. Zero means no snapshot has been installed yet.
func (e *Engine) SnapshotAge() time.Duration {
	ts := e.lastRefresh.Load()
	if ts == 0 {
		return 0
	}
	return time.Since(time.Unix(0, ts))
}

// RefreshFailures counts reloads that failed (rule list or any
// recipient-channel lookup); the previous snapshot stays installed.
func (e *Engine) RefreshFailures() int64 {
	return e.ruleLoadErrors.Load()
}

// refresh reloads the rules and every group's recipient channels into a
// complete new snapshot. The snapshot is installed ONLY when every read
// succeeded: a failed email, APRS or Discord lookup must never replace
// working routing data with empty recipients. A failed load keeps the
// previous snapshot and is retried on the next tick.
func (e *Engine) refresh() {
	rules, err := e.store.ListGroupRoutings()
	if err != nil {
		e.ruleLoadErrors.Add(1)
		e.logger.Warn("routing: rule reload failed", "error", err)
		return
	}
	bcc := make(map[int64][]string)
	aprsBcc := make(map[int64][]string)
	discordBcc := make(map[int64][]string)
	for _, rule := range rules {
		if len(rule.Actions) == 0 {
			continue
		}
		emails, err := e.store.GroupRecipientEmails(rule.GroupID)
		if err != nil {
			e.ruleLoadErrors.Add(1)
			e.logger.Warn("routing: recipient load failed, keeping the previous snapshot",
				"group", rule.Name, "channel", "email", "error", err)
			return
		}
		callsigns, err := e.store.GroupRecipientAPRS(rule.GroupID)
		if err != nil {
			e.ruleLoadErrors.Add(1)
			e.logger.Warn("routing: recipient load failed, keeping the previous snapshot",
				"group", rule.Name, "channel", "aprs", "error", err)
			return
		}
		handles, err := e.store.GroupRecipientDiscord(rule.GroupID)
		if err != nil {
			e.ruleLoadErrors.Add(1)
			e.logger.Warn("routing: recipient load failed, keeping the previous snapshot",
				"group", rule.Name, "channel", "discord", "error", err)
			return
		}
		bcc[rule.GroupID] = emails
		aprsBcc[rule.GroupID] = callsigns
		discordBcc[rule.GroupID] = handles
	}
	e.mu.Lock()
	e.rules = rules
	e.bcc = bcc
	e.aprsBcc = aprsBcc
	e.discordBcc = discordBcc
	e.mu.Unlock()
	e.rulesLoaded.Add(1)
	e.lastRefresh.Store(time.Now().UnixNano())
}

// handle evaluates one canonical event against the cached rules.
func (e *Engine) handle(ctx context.Context, ev dispatch.Event) {
	// Durable acceptance: the inbox row is acknowledged once the
	// evaluation finishes (on every path), so a crash mid-evaluation
	// re-delivers the event after a restart.
	if ev.InboxID != 0 && e.inbox != nil {
		defer func() {
			if err := e.inbox.AckInboxEvent(context.Background(), ev.InboxID); err != nil {
				e.logger.Warn("routing: inbox ack failed",
					"id", ev.InboxID, "error", err)
			}
		}()
	}
	if ev.Kind != dispatch.EventHazardTransition || ev.Hazard == nil {
		return
	}
	e.eventsSeen.Add(1)

	key := ev.Hazard.Key
	sev := strings.ToLower(strings.TrimSpace(ev.Hazard.Hazard.Severity))
	rank, ok := severity.Rank(sev)
	if !ok {
		// Unranked provider vocabularies count as the lowest rank: only
		// the permissive "unknown" threshold (deliver everything) matches.
		rank = 0
	}
	src := strings.ToLower(strings.TrimSpace(ev.Hazard.Hazard.Source))

	// Audit trail: open the per-alert trail for every transition, even
	// ones that end up skipped — "why was this alert not sent?" is the
	// question the trail exists to answer.
	e.trail.Receive(key, src, sev, ev.Hazard.Hazard.Event, ev.Hazard.Hazard.Headline, ev.Hazard.Timestamp)

	// Cancellations and expirations only retire the active view (the
	// dashboard hides the hazard); starting the notification machine for
	// them makes no sense, so only new and updated transitions are
	// routed to actions.
	switch ev.Hazard.Type {
	case dispatch.TransitionCancelled, dispatch.TransitionExpired:
		e.transitionsSkipped.Add(1)
		e.logger.Debug("routing: terminal transition skipped",
			"type", ev.Hazard.Type, "event_key", ev.Hazard.Key)
		e.trail.Add(key, trail.StepSkipped,
			"skipped: "+string(ev.Hazard.Type)+" transition — notifications are never started for cancelled/expired hazards",
			time.Now())
		e.trail.SetOutcome(key, trail.OutcomeSkipped)
		return
	}

	e.mu.RLock()
	rules := e.rules
	bcc := e.bcc
	aprsBcc := e.aprsBcc
	e.mu.RUnlock()

	anyCell := false
	for _, rule := range rules {
		if len(rule.Actions) == 0 {
			continue
		}

		// Routing matrix: every cell reads (source, action, threshold).
		// For each action pick the most specific matching cell — a
		// source-specific cell wins over the empty "any source" cell, so
		// the fallback only applies where no specific cell exists.
		best := make(map[string]storage.ChannelAssignment, len(rule.Actions))
		for _, a := range rule.Actions {
			if a.Source != "" && a.Source != src {
				continue
			}
			cur, ok := best[a.ID]
			if !ok || len(a.Source) > len(cur.Source) {
				best[a.ID] = a
			}
		}
		if len(best) == 0 {
			e.trail.Add(key, trail.StepSkipped,
				"skipped: no matching route in group "+rule.Name, time.Now())
			continue
		}
		anyCell = true

		// One event can fire a subset of the actions.
		var fired int
		for _, a := range best {
			if !meetsThreshold(rank, a.MinSeverity) {
				e.notifCount(a.ID, "skipped", 1)
				e.trail.Add(key, trail.StepSkipped,
					fmt.Sprintf("skipped: severity below threshold — group %s action %s needs ≥ %s",
						rule.Name, a.ID, a.MinSeverity), time.Now())
				continue
			}
			// Durable delivery: the full job (payload, recipients) is
			// persisted BEFORE anything counts as "accepted", and the
			// action worker records the result AFTER each execution. A
			// power loss between "queued" and "transmitted" therefore
			// retries on restart, and an execution that burns all
			// attempts is re-armed by a replay instead of being
			// deduplicated away.
			dedup := fireDedupKey(ev)
			e.trail.Add(key, trail.StepMatched, "matched group "+rule.Name, time.Now())
			routeSrc := a.Source
			if routeSrc == "" {
				routeSrc = "any"
			}
			e.trail.Add(key, trail.StepRoute,
				fmt.Sprintf("%s → %s ≥ %s", routeSrc, a.ID, a.MinSeverity), time.Now())
			req := action.ActionRequest{
				ID:             fmt.Sprintf("%s/%s", ev.Hazard.Key, a.ID),
				CreatedAt:      time.Now(),
				Event:          ev,
				Bcc:            append([]string(nil), bcc[rule.GroupID]...),
				APRSCallsigns:  append([]string(nil), aprsBcc[rule.GroupID]...),
				DiscordHandles: append([]string(nil), e.discordBcc[rule.GroupID]...),
				App:            e.app,
			}
			payload, err := json.Marshal(req)
			if err != nil {
				// Defensive: this struct cannot fail JSON encoding in
				// practice, but an alert must never be silently dropped.
				e.actionsFailed.Add(1)
				e.notifCount(a.ID, "failed", 1)
				e.trail.Add(key, trail.StepFailed,
					fmt.Sprintf("%s job encode failed: %v", a.ID, err), time.Now())
				e.trail.SetOutcome(key, trail.OutcomeFailed)
				e.logger.Warn("routing: action request encode failed",
					"group", rule.Name, "action", a.ID, "error", err)
				continue
			}
			st, queued, err := e.store.EnqueueDelivery(ctx, storage.DeliveryJob{
				GroupID:  rule.GroupID,
				ActionID: a.ID,
				EventKey: ev.Hazard.Key,
				DedupKey: dedup,
				Payload:  payload,
				FiredAt:  time.Now(),
			})
			if err != nil {
				// A ledger failure must never suppress an alert: fall
				// back to the in-memory submission path (best-effort
				// deduplication).
				e.logger.Warn("routing: delivery job persist failed",
					"group", rule.Name, "action", a.ID, "error", err)
				if serr := e.actions.Submit(a.ID, req); serr != nil {
					e.actionsFailed.Add(1)
					e.notifCount(a.ID, "failed", 1)
					e.trail.Add(key, trail.StepFailed,
						fmt.Sprintf("%s failed to start: %v", a.ID, serr), time.Now())
					e.trail.SetOutcome(key, trail.OutcomeFailed)
					e.logger.Warn("routing: action submission failed",
						"group", rule.Name, "action", a.ID, "error", serr)
				} else {
					e.actionsFired.Add(1)
					fired++
					e.trail.Add(key, trail.StepSubmitted,
						fmt.Sprintf("%s action started (group %s, ledger unavailable)", a.ID, rule.Name), time.Now())
					e.trail.SetOutcome(key, trail.OutcomeSubmitted)
				}
				continue
			}
			if !queued {
				// The job is already pending or already delivered:
				// replays deduplicate (the worker executes queued jobs;
				// delivered jobs never fire twice).
				e.actionsDeduped.Add(1)
				e.notifCount(a.ID, "deduped", 1)
				e.trail.Add(key, trail.StepSkipped,
					fmt.Sprintf("skipped: already queued or delivered — group %s action %s (duplicate, %s)",
						rule.Name, a.ID, st), time.Now())
				continue
			}
			e.actionsFired.Add(1)
			fired++
			e.trail.Add(key, trail.StepSubmitted,
				fmt.Sprintf("%s job queued durably (group %s)", a.ID, rule.Name), time.Now())
			e.trail.SetOutcome(key, trail.OutcomeSubmitted)
		}

		if fired > 0 {
			e.rulesMatched.Add(1)
		}
	}

	if !anyCell {
		e.trail.Add(key, trail.StepSkipped,
			"skipped: no group has a matching route for this alert", time.Now())
	}
}

// meetsThreshold reports whether an event rank satisfies a channel's
// minimum severity.
func meetsThreshold(rank int, minSeverity string) bool {
	threshold, ok := severity.Rank(minSeverity)
	if !ok {
		threshold = 0
	}
	return rank >= threshold
}

// fireDedupKey builds the stable deduplication identity of one hazard
// transition. The publisher's journal ChangeID is the canonical identity
// plus the publisher UUID (independent instances can produce identical
// source/key/changeID tuples and must never suppress each other; receivers
// observing the SAME publisher deduplicate together). Without a publisher
// (legacy producers) the key falls back to the publisher-less form; when
// there is no ChangeID either, a content hash of the canonical fields
// stands in.
func fireDedupKey(ev dispatch.Event) string {
	h := ev.Hazard
	if h.ChangeID != 0 {
		if h.Publisher != "" {
			return fmt.Sprintf("c:%s:%s:%s:%d", h.Publisher, h.Source, h.Key, h.ChangeID)
		}
		return fmt.Sprintf("c:%s:%s:%d", h.Source, h.Key, h.ChangeID)
	}
	if h.Publisher != "" {
		var b strings.Builder
		fmt.Fprintf(&b, "%s|%s|%s|%s|%d|%s|%s|%s|%s|%s",
			h.Publisher, h.Type, h.Key, h.Source, h.Timestamp.UnixNano(),
			h.Hazard.EventKey, h.Hazard.Severity, h.Hazard.Urgency, h.Hazard.Certainty, h.Hazard.Headline)
		sum := sha256.Sum256([]byte(b.String()))
		return "h:" + hex.EncodeToString(sum[:16])
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s|%s|%s|%d|%s|%s|%s|%s|%s",
		h.Type, h.Key, h.Source, h.Timestamp.UnixNano(),
		h.Hazard.EventKey, h.Hazard.Severity, h.Hazard.Urgency, h.Hazard.Certainty, h.Hazard.Headline)
	sum := sha256.Sum256([]byte(b.String()))
	return "h:" + hex.EncodeToString(sum[:16])
}
