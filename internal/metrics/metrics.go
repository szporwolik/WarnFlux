// Package metrics is a minimal, dependency-free Prometheus text-format
// registry. Counters and gauges are pre-registered with fixed label sets
// and rendered on the /metrics endpoint; label VALUES are application
// identity (action id, receiver id, source id), so the cardinality is
// tiny and bounded by configuration.
package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// kind distinguishes counters from gauges in the rendered output.
type kind uint8

const (
	kindCounter kind = iota
	kindGauge
)

// entry is one named, labeled metric value.
type entry struct {
	name   string
	help   string
	kind   kind
	labels []string // alternating name, value
	val    atomic.Int64
}

// Registry collects named metric values. It is safe for concurrent use.
type Registry struct {
	mu      sync.Mutex
	entries map[string]*entry // key: name + "{" + labels + "}"
}

// New builds an empty registry.
func New() *Registry {
	return &Registry{entries: make(map[string]*entry)}
}

// key renders the unique identity of a labeled metric.
func key(name string, labels ...string) string {
	if len(labels) == 0 {
		return name
	}
	return name + "{" + strings.Join(labels, ",") + "}"
}

// register returns (creating on first use) the entry for a labeled
// metric.
func (r *Registry) register(t kind, name, help string, labels ...string) *entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := key(name, labels...)
	if e, ok := r.entries[k]; ok {
		return e
	}
	e := &entry{name: name, help: help, kind: t, labels: append([]string(nil), labels...)}
	r.entries[k] = e
	return e
}

// Counter returns an increment function for a labeled counter.
// Counter(..., "action", "smtp") — every pair is label name + value.
func (r *Registry) Counter(name, help string, labels ...string) func(delta int64) {
	e := r.register(kindCounter, name, help, labels...)
	return func(delta int64) { e.val.Add(delta) }
}

// Gauge returns a set function for a labeled gauge.
func (r *Registry) Gauge(name, help string, labels ...string) func(value int64) {
	e := r.register(kindGauge, name, help, labels...)
	return func(value int64) { e.val.Store(value) }
}

// GaugeAdd returns an add function for a labeled gauge.
func (r *Registry) GaugeAdd(name, help string, labels ...string) func(delta int64) {
	e := r.register(kindGauge, name, help, labels...)
	return func(delta int64) { e.val.Add(delta) }
}

// Render emits the Prometheus exposition text, sorted by metric name and
// labels for stable diffs.
func (r *Registry) Render() string {
	r.mu.Lock()
	type kv struct {
		key string
		e   *entry
	}
	items := make([]kv, 0, len(r.entries))
	for k, e := range r.entries {
		items = append(items, kv{key: k, e: e})
	}
	r.mu.Unlock()

	sort.Slice(items, func(i, j int) bool { return items[i].key < items[j].key })

	var b strings.Builder
	for _, it := range items {
		e := it.e
		fmt.Fprintf(&b, "# HELP %s %s\n", e.name, e.help)
		if e.kind == kindCounter {
			fmt.Fprintf(&b, "# TYPE %s counter\n", e.name)
		} else {
			fmt.Fprintf(&b, "# TYPE %s gauge\n", e.name)
		}
		if len(e.labels) == 0 {
			fmt.Fprintf(&b, "%s %d\n", e.name, e.val.Load())
		} else {
			var lv []string
			for i := 0; i+1 < len(e.labels); i += 2 {
				lv = append(lv, fmt.Sprintf("%s=%q", e.labels[i], e.labels[i+1]))
			}
			fmt.Fprintf(&b, "%s{%s} %d\n", e.name, strings.Join(lv, ","), e.val.Load())
		}
	}
	return b.String()
}
