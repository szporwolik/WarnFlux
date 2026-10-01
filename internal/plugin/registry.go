package plugin

import (
	"fmt"
	"sync"
)

// Registry holds the factories of all available plugin types. Registration
// is explicit (no magic reflection); a contributor searches for
// RegisterSource / RegisterOutput to see how integrations are wired.
type Registry struct {
	mu       sync.Mutex
	sources  map[string]SourceFactory
	outputs  map[string]OutputFactory
	internet map[string]bool
}

// NewRegistry creates an empty plugin registry.
func NewRegistry() *Registry {
	return &Registry{
		sources:  make(map[string]SourceFactory),
		outputs:  make(map[string]OutputFactory),
		internet: make(map[string]bool),
	}
}

// RegisterSource registers a source plugin type. Duplicate types are
// rejected instead of being silently overwritten.
func (r *Registry) RegisterSource(name string, factory SourceFactory) error {
	if name == "" {
		return fmt.Errorf("source plugin type must not be empty")
	}
	if factory == nil {
		return fmt.Errorf("source plugin type %q has a nil factory", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.sources[name]; exists {
		return fmt.Errorf("source plugin type %q is already registered", name)
	}
	r.sources[name] = factory
	return nil
}

// RegisterOutput registers an output plugin type. Duplicate types are
// rejected instead of being silently overwritten.
func (r *Registry) RegisterOutput(name string, factory OutputFactory) error {
	if name == "" {
		return fmt.Errorf("output plugin type must not be empty")
	}
	if factory == nil {
		return fmt.Errorf("output plugin type %q has a nil factory", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.outputs[name]; exists {
		return fmt.Errorf("output plugin type %q is already registered", name)
	}
	r.outputs[name] = factory
	return nil
}

// Source returns the factory for the named source plugin type.
func (r *Registry) Source(name string) (SourceFactory, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	factory, ok := r.sources[name]
	if !ok {
		return nil, fmt.Errorf("unknown source plugin type %q", name)
	}
	return factory, nil
}

// MarkSourceInternet classifies a source type as internet-backed: the
// offline-mode switch suspends every enabled instance of such types and
// resumes them when the station goes online again.
func (r *Registry) MarkSourceInternet(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.internet[name] = true
}

// SourceInternet reports whether the source type was classified as
// internet-backed.
func (r *Registry) SourceInternet(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.internet[name]
}

// Output returns the factory for the named output plugin type.
func (r *Registry) Output(name string) (OutputFactory, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	factory, ok := r.outputs[name]
	if !ok {
		return nil, fmt.Errorf("unknown output plugin type %q", name)
	}
	return factory, nil
}
