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
	"github.com/szporwolik/WarnFlux/internal/core"
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
	// CommitInboxDelivery persists every delivery job of one evaluation
	// — full payload (event + recipients), attempt counter and
	// next-attempt deadline all live in the row — and, for inbox events
	// (inboxID != 0), deletes the inbox row in the SAME transaction.
	// It returns one result per job: Queued=true means the transition
	// made the job pending (fresh insert, or a terminally failed job
	// re-armed); Queued=false means it was deduplicated.
	CommitInboxDelivery(ctx context.Context, inboxID int64, jobs []storage.DeliveryJob) ([]storage.DeliveryResult, error)
}

// Inbox is the optional durable dispatch inbox: events persisted before
// routing are re-delivered after a restart or a full live queue. Rows
// are consumed only through RuleStore.CommitInboxDelivery, atomically
// with the delivery jobs they produced — never on a failed evaluation.
type Inbox interface {
	PendingInboxEvents(ctx context.Context, limit int) ([]storage.InboxItem, error)
}

// HazardFreshness is the optional storage-side staleness oracle: a
// store that implements it lets the engine refuse to schedule
// notifications for hazards that already expired or were cancelled —
// an older update must never outrank a known cancellation. The verdict
// separates active / inactive / unknown; read errors are reported and
// handled fail-open by the engine.
type HazardFreshness interface {
	HazardActive(ctx context.Context, eventKey string, now time.Time) (storage.HazardVerdict, error)
}

// LifecycleRecorder is the optional storage-side message lifecycle
// ledger: every observed transition records its latest version and
// state per (publisher, event key), so later jobs — including ones
// built from REMOTE events that never touch the local events table —
// can be blocked by a superseding version, cancellation or expiry.
// Messages without a publisher identity (legacy, panel) are left to
// their own recorders.
type LifecycleRecorder interface {
	RecordLifecycle(ctx context.Context, publisher, eventKey string, version int64, status string) error
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
	inboxID := ev.InboxID
	if ev.Kind != dispatch.EventHazardTransition || ev.Hazard == nil {
		// Non-routed kinds are a deliberate no-op: consume the inbox
		// row (if any) so it never loops.
		e.consumeDeliberate(ctx, inboxID)
		return
	}
	e.eventsSeen.Add(1)

	// The common lifecycle ledger records EVERY observed transition —
	// local, remote and panel — so the pre-transmission gate can block
	// jobs superseded by a newer version, a remote cancellation or a
	// panel expiry. Recording happens before the terminal-transition
	// skip, so a remote 'cancelled' leaves durable knowledge even
	// though it starts no notification machine.
	e.recordLifecycle(ctx, ev)

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
		e.consumeDeliberate(ctx, inboxID)
		return
	}

	// Staleness gate BEFORE anything is scheduled: a recovered new or
	// updated transition must never notify for a hazard that already
	// expired, and an older update must not outrank a cancellation the
	// store already knows. This is a deliberate skip — the inbox row
	// (if any) is consumed.
	if !e.hazardFresh(ctx, ev) {
		e.transitionsSkipped.Add(1)
		e.logger.Debug("routing: stale transition skipped",
			"type", ev.Hazard.Type, "event_key", ev.Hazard.Key)
		e.trail.Add(key, trail.StepSkipped,
			"skipped: hazard no longer active (expired or superseded before scheduling)", time.Now())
		e.trail.SetOutcome(key, trail.OutcomeSkipped)
		e.consumeDeliberate(ctx, inboxID)
		return
	}

	// The notification machine needs a validly loaded routing snapshot.
	// Without one the event cannot be deliberately evaluated, so an
	// inbox event stays PENDING for recovery instead of being consumed
	// by a broken evaluation. ("No matching rule" must be a deliberate
	// result, never the side effect of a failed rule load.)
	if e.rulesLoaded.Load() == 0 {
		e.logger.Debug("routing: rules not loaded yet, event kept pending",
			"event_key", key, "inbox_id", inboxID)
		e.trail.Add(key, trail.StepSkipped,
			"skipped: rules not loaded — event kept pending for retry", time.Now())
		return
	}

	e.mu.RLock()
	rules := e.rules
	bcc := e.bcc
	aprsBcc := e.aprsBcc
	e.mu.RUnlock()

	// One evaluation first collects EVERY delivery job, then commits
	// them — together with the inbox consumption — in a single
	// transaction. A crash or failure therefore either persists all
	// jobs durably and removes the inbox row, or does neither.
	var jobs []storage.DeliveryJob
	var metas []jobMeta
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

		for _, a := range best {
			if !meetsThreshold(rank, a.MinSeverity) {
				e.notifCount(a.ID, "skipped", 1)
				e.trail.Add(key, trail.StepSkipped,
					fmt.Sprintf("skipped: severity below threshold — group %s action %s needs ≥ %s",
						rule.Name, a.ID, a.MinSeverity), time.Now())
				continue
			}
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
				// practice — but without a payload the job cannot be
				// made durable, so the whole event stays pending rather
				// than being consumed half-processed.
				e.actionsFailed.Add(1)
				e.notifCount(a.ID, "failed", 1)
				e.trail.Add(key, trail.StepFailed,
					fmt.Sprintf("%s job encode failed: %v", a.ID, err), time.Now())
				e.trail.SetOutcome(key, trail.OutcomeFailed)
				e.logger.Warn("routing: action request encode failed",
					"group", rule.Name, "action", a.ID, "error", err)
				return
			}
			jobs = append(jobs, storage.DeliveryJob{
				GroupID:  rule.GroupID,
				ActionID: a.ID,
				EventKey: ev.Hazard.Key,
				DedupKey: fireDedupKey(ev),
				Payload:  payload,
				FiredAt:  time.Now(),
			})
			metas = append(metas, jobMeta{
				groupID: rule.GroupID, ruleName: rule.Name, actionID: a.ID, req: req,
			})
		}
	}

	// Single transaction: every delivery job (full payload, recipients,
	// attempts, deadline) is persisted together with the inbox
	// consumption. Durable delivery then proceeds in the action workers,
	// which record the result after each execution.
	results, err := e.store.CommitInboxDelivery(ctx, inboxID, jobs)
	if err != nil {
		// A ledger failure must never suppress an alert and must never
		// consume the inbox row: fall back to the in-memory submission
		// path (best-effort, undeduplicated) and leave the event pending
		// so recovery re-evaluates it — at-least-once.
		e.logger.Warn("routing: delivery commit failed",
			"inbox_id", inboxID, "jobs", len(jobs), "error", err)
		e.deliverInMemory(key, metas)
		if !anyCell {
			e.trail.Add(key, trail.StepSkipped,
				"skipped: no group has a matching route for this alert", time.Now())
		}
		return
	}

	groupFired := make(map[int64]struct{}, len(jobs))
	for i, m := range metas {
		r := results[i]
		if !r.Queued {
			// The job is already pending or already delivered: replays
			// deduplicate (the worker executes queued jobs; delivered
			// jobs never fire twice).
			e.actionsDeduped.Add(1)
			e.notifCount(m.actionID, "deduped", 1)
			e.trail.Add(key, trail.StepSkipped,
				fmt.Sprintf("skipped: already queued or delivered — group %s action %s (duplicate, %s)",
					m.ruleName, m.actionID, r.Status), time.Now())
			continue
		}
		e.actionsFired.Add(1)
		groupFired[m.groupID] = struct{}{}
		e.trail.Add(key, trail.StepSubmitted,
			fmt.Sprintf("%s job queued durably (group %s)", m.actionID, m.ruleName), time.Now())
		e.trail.SetOutcome(key, trail.OutcomeSubmitted)
	}
	if len(groupFired) > 0 {
		e.rulesMatched.Add(int64(len(groupFired)))
	}

	if !anyCell {
		// A deliberate no-match: the evaluation was correct and the
		// inbox row (if any) was consumed atomically with the (empty)
		// commit above.
		e.trail.Add(key, trail.StepSkipped,
			"skipped: no group has a matching route for this alert", time.Now())
	}
}

// jobMeta is the in-memory companion of one delivery job collected
// during an evaluation: it links the persisted job back to the action
// request, rule and group for counters, the trail and the fallback path.
type jobMeta struct {
	groupID  int64
	ruleName string
	actionID string
	req      action.ActionRequest
}

// deliverInMemory submits every collected request through the
// in-memory fallback path. It is used only when the durable ledger is
// unavailable: alerts fire (at-least-once) but are not deduplicated.
func (e *Engine) deliverInMemory(key string, metas []jobMeta) {
	groupFired := make(map[int64]struct{}, len(metas))
	for _, m := range metas {
		if err := e.actions.Submit(m.actionID, m.req); err != nil {
			e.actionsFailed.Add(1)
			e.notifCount(m.actionID, "failed", 1)
			e.trail.Add(key, trail.StepFailed,
				fmt.Sprintf("%s failed to start: %v", m.actionID, err), time.Now())
			e.trail.SetOutcome(key, trail.OutcomeFailed)
			e.logger.Warn("routing: action submission failed",
				"group", m.ruleName, "action", m.actionID, "error", err)
			continue
		}
		e.actionsFired.Add(1)
		groupFired[m.groupID] = struct{}{}
		e.trail.Add(key, trail.StepSubmitted,
			fmt.Sprintf("%s action started (group %s, ledger unavailable)", m.actionID, m.ruleName), time.Now())
		e.trail.SetOutcome(key, trail.OutcomeSubmitted)
	}
	if len(groupFired) > 0 {
		e.rulesMatched.Add(int64(len(groupFired)))
	}
}

// consumeDeliberate marks an inbox event as processed with no delivery
// jobs: the result was deliberate (non-routed kind or terminal
// transition), so the inbox row is deleted in its own transaction. A
// failing ledger keeps the row pending for recovery.
func (e *Engine) consumeDeliberate(ctx context.Context, inboxID int64) {
	if inboxID == 0 {
		return
	}
	if _, err := e.store.CommitInboxDelivery(ctx, inboxID, nil); err != nil {
		e.logger.Warn("routing: inbox consume failed, event stays pending",
			"id", inboxID, "error", err)
	}
}

// recordLifecycle feeds the observed transition into the shared
// lifecycle ledger. Legacy payloads without a publisher identity are
// skipped (fail-open: nothing to record against); the panel records its
// own state through the compose store.
func (e *Engine) recordLifecycle(ctx context.Context, ev dispatch.Event) {
	h := ev.Hazard
	if h == nil || h.Publisher == "" {
		return
	}
	lr, ok := e.store.(LifecycleRecorder)
	if !ok {
		return
	}
	status := string(core.StatusActive)
	switch h.Type {
	case dispatch.TransitionCancelled:
		status = string(core.StatusCancelled)
	case dispatch.TransitionExpired:
		status = string(core.StatusExpired)
	}
	if err := lr.RecordLifecycle(ctx, h.Publisher, h.Key, h.ChangeID, status); err != nil {
		e.logger.Warn("routing: lifecycle record failed",
			"event_key", h.Key, "change_id", h.ChangeID, "error", err)
	}
}

// hazardFresh is the scheduling-time staleness gate: the transition's
// own expiry is always checked, and — when the store offers a
// freshness oracle — the current stored state too, so an older update
// never outranks a known cancellation. Lookup failures never suppress
// an alert.
func (e *Engine) hazardFresh(ctx context.Context, ev dispatch.Event) bool {
	if exp := ev.Hazard.Hazard.ExpiresAt; exp != nil && !exp.After(time.Now()) {
		return false
	}
	fh, ok := e.store.(HazardFreshness)
	if !ok {
		return true // no oracle: the expiry check above is all we have
	}
	verdict, err := fh.HazardActive(ctx, ev.Hazard.Key, time.Now())
	if err != nil {
		e.logger.Warn("routing: hazard freshness lookup failed",
			"event_key", ev.Hazard.Key, "error", err)
		return true // read error: fail-open, never suppress
	}
	return verdict != storage.HazardInactive
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
