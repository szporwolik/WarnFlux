// Package meshtastic implements the built-in Meshtastic source plugin: it runs
// the shared mesh hub (the Companion serial session) and publishes mesh
// node snapshots as informational messages so downstream layers can
// consume them.
package meshtastic

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/core"
	mesh "github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// Type is the plugin type name used in the YAML configuration.
const Type = "meshtastic"

var errHubDisabled = errors.New("meshtastic: hub is not configured")

// Source runs the hub and periodically publishes the node list.
type Source struct {
	hub *mesh.Hub
}

// Register wires the source type into the plugin registry.
func Register(reg *plugin.Registry, hub *mesh.Hub) error {
	return reg.RegisterSource(Type, func(node *yaml.Node) (plugin.SourcePlugin, error) {
		if hub == nil {
			return nil, errHubDisabled
		}
		return &Source{hub: hub}, nil
	})
}

func (s *Source) Name() string { return Type }

// Run pumps the hub until ctx is cancelled and publishes the node snapshot
// every 30 seconds for map-layer consumers.
func (s *Source) Run(ctx context.Context, emit plugin.Emitter) error {
	errCh := make(chan error, 1)
	go func() { errCh <- s.hub.Run(ctx) }()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			return err
		case <-ticker.C:
			s.publishNodes(emit)
		}
	}
}

func (s *Source) publishNodes(emit plugin.Emitter) {
	snap := s.hub.Snapshot()
	payload, err := json.Marshal(snap)
	if err != nil {
		return
	}
	_ = emit.EmitInformation(context.Background(), core.InformationMessage{
		Source:      "meshtastic",
		Key:         "nodes",
		Kind:        "mesh-nodes",
		GeneratedAt: time.Now(),
		Payload:     payload,
	})
}
