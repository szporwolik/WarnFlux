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
	// ClaimActionFire records a delivery claim for (group, action, event)
	// and reports whether it is new (true) or already recorded (false).
	ClaimActionFire(groupID int64, actionID, eventKey, dedupKey string, at time.Time) (bool, error)
}

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

	// Stats counters (atomic).
	eventsSeen         atomic.Int64
	transitionsSkipped atomic.Int64
	rulesMatched       atomic.Int64
	actionsFired       atomic.Int64
	actionsFailed      atomic.Int64
	actionsDeduped     atomic.Int64
	ruleLoadErrors     atomic.Int64
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

// Run drains events until ctx is cancelled or the channel is closed
// (ingress StopIntake). It also reloads the rules on an interval.
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
		}
	}
}

// refresh reloads the rules; a failed load keeps the previous rules and
// is retried on the next tick.
func (e *Engine) refresh() {
	rules, err := e.store.ListGroupRoutings()
	if err != nil {
		e.ruleLoadErrors.Add(1)
		e.logger.Warn("routing: rule reload failed", "error", err)
		return
	}
	bcc := make(map[int64][]string)
	aprsBcc := make(map[int64][]string)
	for _, rule := range rules {
		if len(rule.Actions) == 0 {
			continue
		}
		emails, err := e.store.GroupRecipientEmails(rule.GroupID)
		if err != nil {
			e.logger.Warn("routing: recipient load failed",
				"group", rule.Name, "error", err)
			continue
		}
		callsigns, err := e.store.GroupRecipientAPRS(rule.GroupID)
		if err != nil {
			e.logger.Warn("routing: aprs recipient load failed",
				"group", rule.Name, "error", err)
			continue
		}
		bcc[rule.GroupID] = emails
		aprsBcc[rule.GroupID] = callsigns
	}
	e.mu.Lock()
	e.rules = rules
	e.bcc = bcc
	e.aprsBcc = aprsBcc
	e.mu.Unlock()
}

// handle evaluates one canonical event against the cached rules.
func (e *Engine) handle(ctx context.Context, ev dispatch.Event) {
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
			// Durable deduplication: claim the delivery in the ledger
			// before submitting. A retained/replayed transition with the
			// same identity never re-fires the same (group, action) pair.
			claimed, err := e.store.ClaimActionFire(rule.GroupID, a.ID, ev.Hazard.Key, fireDedupKey(ev), time.Now())
			if err != nil {
				// A ledger failure must never suppress an alert: log and
				// deliver anyway (best-effort deduplication).
				e.logger.Warn("routing: fire claim failed",
					"group", rule.Name, "action", a.ID, "error", err)
			} else if !claimed {
				e.actionsDeduped.Add(1)
				e.notifCount(a.ID, "deduped", 1)
				e.trail.Add(key, trail.StepSkipped,
					fmt.Sprintf("skipped: already delivered — group %s action %s (duplicate)",
						rule.Name, a.ID), time.Now())
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
				ID:            fmt.Sprintf("%s/%s", ev.Hazard.Key, a.ID),
				CreatedAt:     time.Now(),
				Event:         ev,
				Bcc:           append([]string(nil), bcc[rule.GroupID]...),
				APRSCallsigns: append([]string(nil), aprsBcc[rule.GroupID]...),
				App:           e.app,
			}
			if err := e.actions.Submit(a.ID, req); err != nil {
				e.actionsFailed.Add(1)
				e.notifCount(a.ID, "failed", 1)
				e.trail.Add(key, trail.StepFailed,
					fmt.Sprintf("%s failed to start: %v", a.ID, err), time.Now())
				e.trail.SetOutcome(key, trail.OutcomeFailed)
				e.logger.Warn("routing: action submission failed",
					"group", rule.Name, "action", a.ID, "error", err)
			} else {
				e.actionsFired.Add(1)
				fired++
				e.trail.Add(key, trail.StepSubmitted,
					fmt.Sprintf("%s action started (group %s)", a.ID, rule.Name), time.Now())
				e.trail.SetOutcome(key, trail.OutcomeSubmitted)
			}
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

// Stats returns a snapshot of the engine counters (monitoring/tests).
func (e *Engine) Stats() EngineStats {
	return EngineStats{
		EventsSeen:         e.eventsSeen.Load(),
		TransitionsSkipped: e.transitionsSkipped.Load(),
		RulesMatched:       e.rulesMatched.Load(),
		ActionsFired:       e.actionsFired.Load(),
		ActionsFailed:      e.actionsFailed.Load(),
		ActionsDeduped:     e.actionsDeduped.Load(),
		RuleLoadErrors:     e.ruleLoadErrors.Load(),
	}
}

// EngineStats is a point-in-time snapshot of the engine counters.
type EngineStats struct {
	EventsSeen         int64
	TransitionsSkipped int64 // cancelled/expired: dashboard-only, never notify
	RulesMatched       int64
	ActionsFired       int64
	ActionsFailed      int64
	ActionsDeduped     int64
	RuleLoadErrors     int64
}

// fireDedupKey builds the stable deduplication identity of one hazard
// transition. The publisher's journal ChangeID is the canonical identity;
// when it is absent (synthetic or foreign events) a content hash of the
// canonical fields stands in.
func fireDedupKey(ev dispatch.Event) string {
	h := ev.Hazard
	if h.ChangeID != 0 {
		return fmt.Sprintf("c:%s:%s:%d", h.Source, h.Key, h.ChangeID)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s|%s|%s|%d|%s|%s|%s|%s|%s",
		h.Type, h.Key, h.Source, h.Timestamp.UnixNano(),
		h.Hazard.EventKey, h.Hazard.Severity, h.Hazard.Urgency, h.Hazard.Certainty, h.Hazard.Headline)
	sum := sha256.Sum256([]byte(b.String()))
	return "h:" + hex.EncodeToString(sum[:16])
}
