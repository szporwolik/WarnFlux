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
// fallback when no source-specific cell for that action matches. Output
// plugins need no routing here — they receive every journal change by
// default. Rules are reloaded from storage on an interval, so edits made
// in the web UI take effect without a restart.
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
	"github.com/szporwolik/WarnFlux/internal/storage"
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

	// app is stamped onto every action request (footers, links).
	app action.AppInfo

	refreshInterval time.Duration

	mu    sync.RWMutex
	rules []storage.GroupRouting
	// bcc caches each group's member contact addresses (groupID -> emails),
	// loaded alongside the rules; only groups with assigned actions are
	// queried.
	bcc map[int64][]string

	// Stats counters (atomic).
	eventsSeen     atomic.Int64
	rulesMatched   atomic.Int64
	actionsFired   atomic.Int64
	actionsFailed  atomic.Int64
	actionsDeduped atomic.Int64
	ruleLoadErrors atomic.Int64
}

// New builds an engine with the default refresh interval.
func New(store RuleStore, actions ActionSubmitter, logger *slog.Logger, app action.AppInfo) *Engine {
	return &Engine{
		store:           store,
		actions:         actions,
		logger:          logger,
		app:             app,
		refreshInterval: defaultRefreshInterval,
	}
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
		bcc[rule.GroupID] = emails
	}
	e.mu.Lock()
	e.rules = rules
	e.bcc = bcc
	e.mu.Unlock()
}

// handle evaluates one canonical event against the cached rules.
func (e *Engine) handle(ctx context.Context, ev dispatch.Event) {
	if ev.Kind != dispatch.EventHazardTransition || ev.Hazard == nil {
		return
	}
	e.eventsSeen.Add(1)

	sev := strings.ToLower(strings.TrimSpace(ev.Hazard.Hazard.Severity))
	rank, ok := storage.SeverityRank(sev)
	if !ok {
		// Unranked provider vocabularies count as the lowest rank: only
		// the permissive "unknown" threshold (deliver everything) matches.
		rank = 0
	}
	src := strings.ToLower(strings.TrimSpace(ev.Hazard.Hazard.Source))

	e.mu.RLock()
	rules := e.rules
	bcc := e.bcc
	e.mu.RUnlock()

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

		// One event can fire a subset of the actions.
		var fired int
		for _, a := range best {
			if !meetsThreshold(rank, a.MinSeverity) {
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
				continue
			}
			req := action.ActionRequest{
				ID:        fmt.Sprintf("%s/%s", ev.Hazard.Key, a.ID),
				CreatedAt: time.Now(),
				Event:     ev,
				Bcc:       append([]string(nil), bcc[rule.GroupID]...),
				App:       e.app,
			}
			if err := e.actions.Submit(a.ID, req); err != nil {
				e.actionsFailed.Add(1)
				e.logger.Warn("routing: action submission failed",
					"group", rule.Name, "action", a.ID, "error", err)
			} else {
				e.actionsFired.Add(1)
				fired++
			}
		}

		if fired > 0 {
			e.rulesMatched.Add(1)
		}
	}
}

// meetsThreshold reports whether an event rank satisfies a channel's
// minimum severity.
func meetsThreshold(rank int, minSeverity string) bool {
	threshold, ok := storage.SeverityRank(minSeverity)
	if !ok {
		threshold = 0
	}
	return rank >= threshold
}

// Stats returns a snapshot of the engine counters (monitoring/tests).
func (e *Engine) Stats() EngineStats {
	return EngineStats{
		EventsSeen:     e.eventsSeen.Load(),
		RulesMatched:   e.rulesMatched.Load(),
		ActionsFired:   e.actionsFired.Load(),
		ActionsFailed:  e.actionsFailed.Load(),
		ActionsDeduped: e.actionsDeduped.Load(),
		RuleLoadErrors: e.ruleLoadErrors.Load(),
	}
}

// EngineStats is a point-in-time snapshot of the engine counters.
type EngineStats struct {
	EventsSeen     int64
	RulesMatched   int64
	ActionsFired   int64
	ActionsFailed  int64
	ActionsDeduped int64
	RuleLoadErrors int64
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
