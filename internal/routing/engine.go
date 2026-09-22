// Package routing implements the group notification rule engine: the
// consumer of the canonical dispatch ingress.
//
//	receiver callback -> canonical Event -> ingress queue
//	    -> rule engine: severity threshold -> assigned actions + outputs
//
// Every group is a notification channel: a minimum severity threshold plus
// a set of assigned action instances (invoked via action.Manager.Submit)
// and output instances (invoked via their optional RuleFeed capability).
// Rules are reloaded from storage on an interval, so edits made in the web
// UI take effect without a restart.
package routing

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// ActionSubmitter is the action half of rule evaluation. It is satisfied
// by *action.Manager.
type ActionSubmitter interface {
	Submit(id string, req action.ActionRequest) error
}

// RuleOutputRouter delivers one group-routed change to output instances.
// It is satisfied by *plugin.Manager.
type RuleOutputRouter interface {
	SubmitRule(ctx context.Context, outputIDs []string, change core.EventChange, ref plugin.RuleRef) error
}

// RuleStore supplies the authoritative group routings. It is satisfied by
// the SQLite directory store.
type RuleStore interface {
	ListGroupRoutings() ([]storage.GroupRouting, error)
}

const (
	// defaultRefreshInterval is how often rules are reloaded from storage.
	defaultRefreshInterval = 10 * time.Second
	// outputRoundTimeout bounds one SubmitRule round (all outputs of one
	// matched group) so a slow output can never stall the ingress consumer.
	outputRoundTimeout = 5 * time.Second
)

// Engine evaluates group notification rules for every canonical hazard
// transition drained from the dispatch ingress. It never blocks the
// ingress: deliveries that fail are counted and logged.
type Engine struct {
	store   RuleStore
	actions ActionSubmitter
	outputs RuleOutputRouter
	logger  *slog.Logger

	refreshInterval time.Duration

	mu    sync.RWMutex
	rules []storage.GroupRouting

	// Stats counters (atomic).
	eventsSeen     atomic.Int64
	rulesMatched   atomic.Int64
	actionsFired   atomic.Int64
	actionsFailed  atomic.Int64
	outputRounds   atomic.Int64
	outputErrors   atomic.Int64
	ruleLoadErrors atomic.Int64
}

// New builds an engine with the default refresh interval.
func New(store RuleStore, actions ActionSubmitter, outputs RuleOutputRouter, logger *slog.Logger) *Engine {
	return &Engine{
		store:           store,
		actions:         actions,
		outputs:         outputs,
		logger:          logger,
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
	e.mu.Lock()
	e.rules = rules
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

	e.mu.RLock()
	rules := e.rules
	e.mu.RUnlock()

	for _, rule := range rules {
		threshold, ok := storage.SeverityRank(rule.MinSeverity)
		if !ok {
			threshold = 0
		}
		if rank < threshold {
			continue
		}
		if len(rule.Actions) == 0 && len(rule.Outputs) == 0 {
			continue
		}
		e.rulesMatched.Add(1)

		for _, actionID := range rule.Actions {
			req := action.ActionRequest{
				ID:        fmt.Sprintf("%s/%s", ev.Hazard.Key, actionID),
				CreatedAt: time.Now(),
				Event:     ev,
			}
			if err := e.actions.Submit(actionID, req); err != nil {
				e.actionsFailed.Add(1)
				e.logger.Warn("routing: action submission failed",
					"group", rule.Name, "action", actionID, "error", err)
			} else {
				e.actionsFired.Add(1)
			}
		}

		if len(rule.Outputs) > 0 {
			e.outputRounds.Add(1)
			ref := plugin.RuleRef{
				GroupID:     rule.GroupID,
				GroupName:   rule.Name,
				MinSeverity: rule.MinSeverity,
			}
			roundCtx, cancel := context.WithTimeout(ctx, outputRoundTimeout)
			err := e.outputs.SubmitRule(roundCtx, rule.Outputs, changeOf(ev), ref)
			cancel()
			if err != nil {
				e.outputErrors.Add(1)
				e.logger.Warn("routing: output delivery failed",
					"group", rule.Name, "error", err)
			}
		}
	}
}

// changeOf maps a canonical hazard transition to the EventChange shape
// outputs understand. Transitions carry the compact hazard block, so the
// change is a best-effort projection (fields absent from the wire stay
// zero-valued).
func changeOf(ev dispatch.Event) core.EventChange {
	h := ev.Hazard
	ct := core.ChangeNew
	status := core.StatusActive
	switch h.Type {
	case dispatch.TransitionUpdated:
		ct = core.ChangeUpdated
	case dispatch.TransitionCancelled:
		ct = core.ChangeCancelled
		status = core.StatusCancelled
	case dispatch.TransitionExpired:
		ct = core.ChangeExpired
		status = core.StatusExpired
	}
	return core.EventChange{
		Type: ct,
		Event: core.HazardEvent{
			Source:      h.Hazard.Source,
			SourceID:    h.Hazard.SourceID,
			Event:       h.Hazard.Event,
			Severity:    h.Hazard.Severity,
			Headline:    h.Hazard.Headline,
			Areas:       h.Hazard.Areas,
			EffectiveAt: h.Hazard.EffectiveAt,
			ExpiresAt:   h.Hazard.ExpiresAt,
			ReceivedAt:  h.Hazard.ReceivedAt,
			UpdatedAt:   h.Hazard.UpdatedAt,
			Status:      status,
		},
	}
}

// Stats returns a snapshot of the engine counters (monitoring/tests).
func (e *Engine) Stats() EngineStats {
	return EngineStats{
		EventsSeen:     e.eventsSeen.Load(),
		RulesMatched:   e.rulesMatched.Load(),
		ActionsFired:   e.actionsFired.Load(),
		ActionsFailed:  e.actionsFailed.Load(),
		OutputRounds:   e.outputRounds.Load(),
		OutputErrors:   e.outputErrors.Load(),
		RuleLoadErrors: e.ruleLoadErrors.Load(),
	}
}

// EngineStats is a point-in-time snapshot of the engine counters.
type EngineStats struct {
	EventsSeen     int64
	RulesMatched   int64
	ActionsFired   int64
	ActionsFailed  int64
	OutputRounds   int64
	OutputErrors   int64
	RuleLoadErrors int64
}
