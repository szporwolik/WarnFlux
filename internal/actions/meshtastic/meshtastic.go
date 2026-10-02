// Package meshtastic implements the built-in meshtastic outbound action: it
// sends SOSNA alerts as Meshtastic channel text messages through the shared
// mesh hub (the Companion serial link).
package meshtastic

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	mesh "github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/sanity"
)

// Type is the action type name used in the YAML configuration.
const Type = "meshtastic"

// maxMeshMessageChars bounds one channel message (133 chars per the
// Meshtastic spec).
const maxMeshMessageChars = 133

// Config is the action-specific configuration.
type Config struct {
	// Prefix is prepended to every message (e.g. the system name).
	Prefix string `yaml:"prefix"`
	// TxInterval is the minimum spacing between two transmissions of this
	// action (default 5s) — the LoRa channel is shared.
	TxInterval time.Duration `yaml:"tx_interval"`
}

var errHubDisabled = errors.New("meshtastic: hub is not configured")

// sender is the hub seam the action transmits through: direct messages
// to the routed group members' node IDs, or a channel broadcast when the
// group has none. *mesh.Hub satisfies it; tests use a stub.
type sender interface {
	SendContactMessage(ctx context.Context, addr, text, operator string) error
	SendChannelMessage(ctx context.Context, text, operator string) error
}

// Action sends SOSNA alerts as direct messages to the routed group
// members' node IDs; a group without any registered node IDs falls back
// to one channel broadcast (channel 0).
type Action struct {
	cfg  Config
	hub  sender
	last time.Time
}

// Register wires the action type into the action registry.
func Register(reg *action.Registry, hub *mesh.Hub) error {
	return reg.Register(Type, func(node *yaml.Node) (action.Plugin, error) {
		var cfg Config
		if node != nil {
			if err := node.Decode(&cfg); err != nil {
				return nil, err
			}
		}
		if hub == nil {
			return nil, errHubDisabled
		}
		if cfg.TxInterval <= 0 {
			cfg.TxInterval = 5 * time.Second
		}
		return &Action{cfg: cfg, hub: hub}, nil
	})
}

func (a *Action) Name() string { return Type }

// Execute formats the routed hazard as one message and delivers it to
// every routed group member's registered node ID (direct message), or —
// when the group has none — as one channel broadcast. The hub records
// each transmission in the durable history and tracks its delivery
// state (sent/delivered/failed).
func (a *Action) Execute(ctx context.Context, req action.ActionRequest) error {
	if req.Event.Kind != dispatch.EventHazardTransition || req.Event.Hazard == nil {
		return nil // nothing to say for non-hazard events
	}
	text := a.textFor(ctx, req)
	if len(req.MeshNodeIDs) == 0 {
		if err := a.pace(ctx); err != nil {
			return err
		}
		if err := a.hub.SendChannelMessage(ctx, text, "system"); err != nil {
			return fmt.Errorf("meshtastic: %w", err)
		}
		a.last = time.Now()
		return nil
	}
	for _, id := range req.MeshNodeIDs {
		if err := a.pace(ctx); err != nil {
			return err
		}
		if err := a.hub.SendContactMessage(ctx, id, text, "system"); err != nil {
			return fmt.Errorf("meshtastic: %s: %w", id, err)
		}
		a.last = time.Now()
	}
	return nil
}

// textFor builds the bounded outbound message for one routed hazard.
func (a *Action) textFor(ctx context.Context, req action.ActionRequest) string {
	h := req.Event.Hazard.Hazard
	text := strings.TrimSpace(h.Headline)
	if text == "" {
		text = h.Event
	}
	text = fmt.Sprintf("%s %s %s", a.cfg.Prefix, strings.ToUpper(h.Severity), text)
	text = sanity.NormalizeText(ctx, sanity.ChannelAPRS, text)
	if len([]rune(text)) > maxMeshMessageChars {
		text = string([]rune(text)[:maxMeshMessageChars])
	}
	return text
}

// pace waits out the minimum spacing between transmissions; the mesh
// channel is shared airtime.
func (a *Action) pace(ctx context.Context) error {
	if wait := a.cfg.TxInterval - time.Since(a.last); wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil
}

// Close releases nothing (the hub owns the serial link).
func (a *Action) Close(ctx context.Context) error { return nil }

var _ action.Plugin = (*Action)(nil)
