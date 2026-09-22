// Package action contains the ActionPlugin contract, registry, manager and
// per-instance isolated worker.
//
// ActionPlugins are NOT OutputPlugins:
//   - an OutputPlugin is a durable subscriber to Router core transitions
//     (example: MQTT);
//   - an ActionPlugin is explicitly invoked by future dispatch rules via
//     Manager.Submit (examples: SMS, email, Discord, ntfy, CAT).
//
// ActionPlugins never automatically receive MQTT events. The dispatch
// ingress only normalizes events; the rule engine (later task) selects
// which action runs for which event.
package action

import (
	"context"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

// ActionRequest is the minimal execution request handed to one action.
// No rule/template/retry complexity is modeled yet.
type ActionRequest struct {
	// ID is a request identifier (e.g. a future action_jobs row ID).
	ID string
	// CreatedAt is when the request was created.
	CreatedAt time.Time
	// Event is the canonical dispatch event that triggered the action.
	Event dispatch.Event
	// Bcc carries the matched group's member addresses (e.g. emails). It
	// is populated by the rule engine when the matched group has members
	// with contact data; actions treat it as read-only.
	Bcc []string
}

// Plugin is the minimal contract every action implements.
type Plugin interface {
	// Name returns the configured instance ID.
	Name() string
	// Execute runs one action invocation. It MUST respect ctx: a normal
	// plugin returns promptly when ctx is cancelled.
	Execute(ctx context.Context, request ActionRequest) error
	// Close releases plugin-owned resources (files, connections). It MUST
	// respect ctx. It is called exactly once, even if Execute never ran.
	Close(ctx context.Context) error
}

// Factory builds one action instance from its raw YAML configuration node.
type Factory func(node *yaml.Node) (Plugin, error)

// Registry maps action type names to factories.
type Registry struct {
	factories map[string]Factory
}

// NewRegistry returns an empty action registry.
func NewRegistry() *Registry {
	return &Registry{factories: make(map[string]Factory)}
}

// Register adds a factory for an action type. Duplicate types are rejected.
func (r *Registry) Register(typ string, f Factory) error {
	if typ == "" {
		return fmt.Errorf("action type must not be empty")
	}
	if _, exists := r.factories[typ]; exists {
		return fmt.Errorf("duplicate action type %q registration", typ)
	}
	r.factories[typ] = f
	return nil
}

// Known reports whether the type is registered.
func (r *Registry) Known(typ string) bool {
	_, ok := r.factories[typ]
	return ok
}

// Create builds an action instance via the factory registered for typ.
func (r *Registry) Create(typ string, node *yaml.Node) (Plugin, error) {
	f, ok := r.factories[typ]
	if !ok {
		return nil, fmt.Errorf("action type %q is not registered", typ)
	}
	return f(node)
}
