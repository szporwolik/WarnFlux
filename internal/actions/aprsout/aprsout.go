// Package aprsout implements the built-in aprs-out action: it sends APRS
// text messages to configured ham callsigns (plus the matched group's
// members' registered callsigns) and WAITS for the addressee's ack when
// one can be obtained. Delivery is paced — a per-station cooldown and a
// global transmission interval keep the action from spamming the channel —
// and unacknowledged messages fail the call so the manager's retry
// machinery can try again, exactly like the SMTP action.
package aprsout

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/aprs"
)

// Type is the action type name used in the YAML configuration.
const Type = "aprs-out"

// maxRecipients bounds the static recipient list.
const maxRecipients = 32

// Config is the action-specific configuration.
type Config struct {
	// Callsigns lists the ham stations that receive the notification;
	// may be empty when routing relies entirely on the matched group's
	// members' registered callsigns.
	Callsigns []string `yaml:"callsigns"`
	// Prefix is prepended to every message (e.g. the system name).
	Prefix string `yaml:"prefix"`
	// Cooldown is the minimum interval between two messages to the SAME
	// station (default 2m). Negative disables the per-station cooldown.
	Cooldown time.Duration `yaml:"cooldown"`
	// AckTimeout bounds how long one message waits for the addressee's
	// ack before the call fails (and the manager may retry).
	AckTimeout time.Duration `yaml:"ack_timeout"`
	// TxInterval is the minimum spacing between any two transmissions of
	// this action (default 5s) — the APRS channel is shared. Negative
	// disables it.
	TxInterval time.Duration `yaml:"tx_interval"`
}

// Defaults and bounds.
const (
	defaultCooldown   = 2 * time.Minute
	defaultAckTimeout = 30 * time.Second
	defaultTxInterval = 5 * time.Second
	maxAckTimeout     = 5 * time.Minute
	maxCooldown       = time.Hour
	maxTxInterval     = time.Minute
)

// Register adds the aprs-out action factory to the action registry.
func Register(reg *action.Registry, hub *aprs.Hub) error {
	return reg.Register(Type, func(node *yaml.Node) (action.Plugin, error) {
		return New(node, hub)
	})
}

// New builds one aprs-out action instance from its raw YAML config.
func New(node *yaml.Node, hub *aprs.Hub) (action.Plugin, error) {
	if hub == nil || !hub.Enabled() {
		return nil, errors.New("aprs-out: the APRS hub is disabled (set aprs.enabled: true)")
	}
	var cfg Config
	if node != nil {
		if err := node.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("aprs-out: decode config: %w", err)
		}
	}
	if cfg.Cooldown == 0 {
		cfg.Cooldown = defaultCooldown
	}
	if cfg.AckTimeout == 0 {
		cfg.AckTimeout = defaultAckTimeout
	}
	if cfg.TxInterval == 0 {
		cfg.TxInterval = defaultTxInterval
	}
	if cfg.Cooldown > maxCooldown {
		return nil, fmt.Errorf("aprs-out: cooldown must not exceed %s, got %s", maxCooldown, cfg.Cooldown)
	}
	if cfg.AckTimeout <= 0 || cfg.AckTimeout > maxAckTimeout {
		return nil, fmt.Errorf("aprs-out: ack_timeout must be between 1s and %s, got %s", maxAckTimeout, cfg.AckTimeout)
	}
	if cfg.TxInterval > maxTxInterval {
		return nil, fmt.Errorf("aprs-out: tx_interval must not exceed %s, got %s", maxTxInterval, cfg.TxInterval)
	}
	if len(cfg.Callsigns) > maxRecipients {
		return nil, fmt.Errorf("aprs-out: config.callsigns has %d recipients, maximum %d", len(cfg.Callsigns), maxRecipients)
	}
	for i, cs := range cfg.Callsigns {
		cfg.Callsigns[i] = aprs.NormalizeCallsign(cs)
		if !aprs.ValidCallsign(cfg.Callsigns[i]) {
			return nil, fmt.Errorf("aprs-out: callsign %q is not a valid APRS callsign", cs)
		}
	}
	return &aprsOutAction{hub: hub, cfg: cfg, lastSend: make(map[string]time.Time)}, nil
}

type aprsOutAction struct {
	hub *aprs.Hub
	cfg Config

	mu       sync.Mutex
	lastSend map[string]time.Time
	nextTx   time.Time
}

func (a *aprsOutAction) Name() string { return Type }

func (a *aprsOutAction) Close(context.Context) error { return nil }

// Execute sends one APRS message per recipient and waits for acks,
// paced by the per-station cooldown and the global tx interval. Any
// failure (including a missing ack) fails the call so the manager's
// retry machinery handles it.
func (a *aprsOutAction) Execute(ctx context.Context, req action.ActionRequest) error {
	text := a.messageText(req)
	recipients := a.recipients(req)
	if len(recipients) == 0 {
		// Nothing to do: the config may rely entirely on the matched
		// group's members, and none of them registered a callsign.
		return nil
	}
	var firstErr error
	failed := 0
	for _, callsign := range recipients {
		if err := a.waitUntil(ctx, a.nextSlot(callsign)); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			failed++
			continue
		}
		ack, err := a.hub.SendMessageWaitAck(ctx, callsign, text, a.cfg.AckTimeout)
		if err == nil && ack {
			// Transmitted (and acknowledged): the cooldown and spacing
			// windows now start from this transmission.
			a.mu.Lock()
			a.lastSend[callsign] = time.Now()
			if a.cfg.TxInterval > 0 {
				a.nextTx = time.Now().Add(a.cfg.TxInterval)
			}
			a.mu.Unlock()
			continue
		}
		if err == nil {
			err = fmt.Errorf("no ack from %s within %s", callsign, a.cfg.AckTimeout)
		}
		if firstErr == nil {
			firstErr = err
		}
		failed++
	}
	if firstErr != nil {
		return fmt.Errorf("aprs-out: %d of %d messages failed (first: %w)", failed, len(recipients), firstErr)
	}
	return nil
}

// nextSlot returns the earliest time the next transmission to callsign may
// leave: now, shifted by the per-station cooldown and the global tx
// interval.
func (a *aprsOutAction) nextSlot(callsign string) time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	t := time.Now()
	if a.cfg.Cooldown > 0 {
		if last, ok := a.lastSend[callsign]; ok {
			if until := last.Add(a.cfg.Cooldown); until.After(t) {
				t = until
			}
		}
	}
	if a.nextTx.After(t) {
		t = a.nextTx
	}
	return t
}

// waitUntil blocks until t (or ctx cancellation).
func (a *aprsOutAction) waitUntil(ctx context.Context, t time.Time) error {
	for {
		d := time.Until(t)
		if d <= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
		}
	}
}

// recipients merges the configured callsigns with the group members'
// registered callsigns (normalized, de-duplicated, bounded).
func (a *aprsOutAction) recipients(req action.ActionRequest) []string {
	seen := make(map[string]bool, len(a.cfg.Callsigns)+len(req.APRSCallsigns))
	var out []string
	add := func(callsign string) {
		callsign = aprs.NormalizeCallsign(callsign)
		if callsign == "" || seen[callsign] {
			return
		}
		seen[callsign] = true
		out = append(out, callsign)
	}
	for _, cs := range a.cfg.Callsigns {
		add(cs)
	}
	for _, cs := range req.APRSCallsigns {
		add(cs)
	}
	return out
}

// messageText renders the notification from the canonical event metadata:
// headline first, then event, severity and prefix, with less important
// components dropped before the headline is ever shortened. The ack {id}
// suffix room is reserved so the final frame always fits the APRS limit.
func (a *aprsOutAction) messageText(req action.ActionRequest) string {
	prefix := strings.TrimSpace(a.cfg.Prefix)
	if prefix == "" {
		prefix = strings.TrimSpace(req.App.Header1)
	}
	if h := req.Event.Hazard; h != nil {
		return aprs.BuildAlertMessage(prefix, h.Hazard.Severity, h.Hazard.Event, h.Hazard.Headline, aprs.AckSuffixLen)
	}
	return aprs.LimitMessageText(strings.Join([]string{prefix, "WarnFlux notification"}, " "), aprs.AckSuffixLen)
}

var _ action.Plugin = (*aprsOutAction)(nil)
