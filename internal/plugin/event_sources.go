package plugin

import (
	"sort"
	"sync"
)

// eventSources declares, per plugin TYPE, the event source slugs a source
// plugin publishes when they differ from the type itself.
//
// The station reconciles events against the configuration, and the two
// vocabularies do not always match: the IMGW plugin's type is "imgw" while its
// events carry "imgw-meteo" and "imgw-hydro". Guessing the relation (by name
// prefix, or by treating every configured type as the only owner) silently
// withdrew nine valid road-work warnings in review — the reconciliation must
// know the mapping, not infer it.
//
// A plugin whose event source IS its type needs no registration: the type is
// used as the single slug.
var (
	eventSourcesMu sync.RWMutex
	eventSources   = map[string][]string{}
)

// RegisterEventSources declares the event source slugs of a source plugin type.
// It is meant to be called from the plugin package's init.
func RegisterEventSources(pluginType string, slugs ...string) {
	if pluginType == "" || len(slugs) == 0 {
		return
	}
	out := make([]string, 0, len(slugs))
	seen := make(map[string]bool, len(slugs))
	for _, s := range slugs {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	sort.Strings(out)
	eventSourcesMu.Lock()
	defer eventSourcesMu.Unlock()
	eventSources[pluginType] = out
}

// EventSourcesOf reports the event source slugs a plugin type publishes. A type
// that registered nothing owns exactly its own name.
func EventSourcesOf(pluginType string) []string {
	eventSourcesMu.RLock()
	defer eventSourcesMu.RUnlock()
	if slugs, ok := eventSources[pluginType]; ok && len(slugs) > 0 {
		return append([]string(nil), slugs...)
	}
	if pluginType == "" {
		return nil
	}
	return []string{pluginType}
}
