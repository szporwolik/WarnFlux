package plugin

import (
	"fmt"
	"sync"
	"time"
)

// Registry holds the factories of all available plugin types. Registration
// is explicit (no magic reflection); a contributor searches for
// RegisterSource / RegisterOutput to see how integrations are wired.
type Registry struct {
	mu       sync.Mutex
	sources  map[string]SourceFactory
	outputs  map[string]OutputFactory
	internet map[string]bool
	// pollMin / pollMax hold the poll_interval range each source plugin
	// accepts (the Config page validates edits against it).
	pollMin map[string]time.Duration
	pollMax map[string]time.Duration
}

// NewRegistry creates an empty plugin registry.
func NewRegistry() *Registry {
	return &Registry{
		sources:  make(map[string]SourceFactory),
		outputs:  make(map[string]OutputFactory),
		internet: make(map[string]bool),
		pollMin:  make(map[string]time.Duration),
		pollMax:  make(map[string]time.Duration),
	}
}

// SetSourcePollBounds records the poll_interval range a source plugin
// accepts. The Config page edits the value directly, so it must refuse
// what the plugin would refuse at startup — this is where the no-typo
// guarantee comes from.
func (r *Registry) SetSourcePollBounds(name string, min, max time.Duration) {
	if name == "" || min <= 0 || max < min {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pollMin[name] = min
	r.pollMax[name] = max
}

// SourcePollBounds returns the range recorded for a source plugin type;
// ok is false when the plugin declared none (the page then says so and
// leaves the value to the plugin's own check).
func (r *Registry) SourcePollBounds(name string) (min, max time.Duration, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	min, hasMin := r.pollMin[name]
	max, hasMax := r.pollMax[name]
	if !hasMin || !hasMax {
		return 0, 0, false
	}
	return min, max, true
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
