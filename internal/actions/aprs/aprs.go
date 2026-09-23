// Package aprs implements the built-in APRS action: it sends an APRS text
// message to configured ham callsigns when the rule engine routes a
// dispatch event to this action. The message travels through the shared
// APRS hub, which uses the first ready transmitter (aprs-inet today,
// aprs-radio later) — so the same action works no matter which backend is
// connected.
package aprs

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/aprs"
)

// Type is the action type name used in the YAML configuration.
const Type = "aprs"

// maxRecipients bounds the static recipient list.
const maxRecipients = 32

// Config is the action-specific configuration.
type Config struct {
	// Callsigns lists the ham stations that receive the notification.
	Callsigns []string `yaml:"callsigns"`
	// Prefix is prepended to every message (e.g. the system name).
	Prefix string `yaml:"prefix"`
}

// Register adds the aprs action factory to the action registry.
func Register(reg *action.Registry, hub *aprs.Hub) error {
	return reg.Register(Type, func(node *yaml.Node) (action.Plugin, error) {
		return New(node, hub)
	})
}

// New builds one APRS action instance from its raw YAML configuration.
func New(node *yaml.Node, hub *aprs.Hub) (action.Plugin, error) {
	if hub == nil || !hub.Enabled() {
		return nil, errors.New("aprs: the APRS hub is disabled (set aprs.enabled: true)")
	}
	var cfg Config
	if node != nil {
		if err := node.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("aprs: decode config: %w", err)
		}
	}
	if len(cfg.Callsigns) == 0 {
		return nil, errors.New("aprs: config.callsigns must contain at least one callsign")
	}
	if len(cfg.Callsigns) > maxRecipients {
		return nil, fmt.Errorf("aprs: config.callsigns has %d recipients, maximum %d", len(cfg.Callsigns), maxRecipients)
	}
	for i, cs := range cfg.Callsigns {
		cfg.Callsigns[i] = aprs.NormalizeCallsign(cs)
		if !aprs.ValidCallsign(cfg.Callsigns[i]) {
			return nil, fmt.Errorf("aprs: callsign %q is not a valid APRS callsign", cs)
		}
	}
	return &aprsAction{hub: hub, cfg: cfg}, nil
}

type aprsAction struct {
	hub *aprs.Hub
	cfg Config
}

func (a *aprsAction) Name() string { return Type }

func (a *aprsAction) Close(context.Context) error { return nil }

// Execute sends one APRS message per configured recipient. The hub routes
// through the first ready transmitter; failures are reported per-recipient
// and the first error is returned.
func (a *aprsAction) Execute(ctx context.Context, req action.ActionRequest) error {
	text := a.messageText(req)
	var firstErr error
	failed := 0
	for _, callsign := range a.cfg.Callsigns {
		if err := a.hub.SendMessage(ctx, callsign, text); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			failed++
		}
	}
	if firstErr != nil {
		return fmt.Errorf("aprs: %d of %d messages failed (first: %w)", failed, len(a.cfg.Callsigns), firstErr)
	}
	return nil
}

// messageText renders the notification from the canonical event metadata.
// APRS messages are a single line of at most 67 characters.
func (a *aprsAction) messageText(req action.ActionRequest) string {
	prefix := strings.TrimSpace(a.cfg.Prefix)
	if prefix == "" {
		prefix = strings.TrimSpace(req.App.Header1)
	}
	var text string
	if h := req.Event.Hazard; h != nil {
		sev := strings.ToUpper(strings.TrimSpace(h.Hazard.Severity))
		event := strings.TrimSpace(h.Hazard.Event)
		headline := strings.TrimSpace(h.Hazard.Headline)
		text = strings.Join([]string{prefix, sev, event, headline}, " ")
	} else {
		text = strings.Join([]string{prefix, "WarnFlux notification"}, " ")
	}
	return aprs.TrimMessageText(text)
}

var _ action.Plugin = (*aprsAction)(nil)
