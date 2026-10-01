// Package meshcore implements the built-in meshcore outbound action: it
// sends SOSNA alerts as MeshCore channel text messages through the shared
// mesh hub (the Companion serial link).
package meshcore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	mesh "github.com/szporwolik/WarnFlux/internal/meshcore"
	"github.com/szporwolik/WarnFlux/internal/sanity"
)

// Type is the action type name used in the YAML configuration.
const Type = "meshcore"

// maxMeshMessageChars bounds one channel message (133 chars per the
// MeshCore spec).
const maxMeshMessageChars = 133

// Config is the action-specific configuration.
type Config struct {
	// Prefix is prepended to every message (e.g. the system name).
	Prefix string `yaml:"prefix"`
	// TxInterval is the minimum spacing between two transmissions of this
	// action (default 5s) — the LoRa channel is shared.
	TxInterval time.Duration `yaml:"tx_interval"`
}

var errHubDisabled = errors.New("meshcore: hub is not configured")

// Action sends channel text messages through the hub.
type Action struct {
	cfg  Config
	hub  *mesh.Hub
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

// Execute formats the routed hazard as one channel message and hands it to
// the hub. The hub records the tx in the durable history.
func (a *Action) Execute(ctx context.Context, req action.ActionRequest) error {
	if req.Event.Kind != dispatch.EventHazardTransition || req.Event.Hazard == nil {
		return nil // nothing to say for non-hazard events
	}
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

	// Pace transmissions: the mesh channel is shared airtime.
	if wait := a.cfg.TxInterval - time.Since(a.last); wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	if err := a.hub.SendChannelMessage(ctx, text, "system"); err != nil {
		return fmt.Errorf("meshcore: %w", err)
	}
	a.last = time.Now()
	return nil
}

// Close releases nothing (the hub owns the serial link).
func (a *Action) Close(ctx context.Context) error { return nil }

var _ action.Plugin = (*Action)(nil)
