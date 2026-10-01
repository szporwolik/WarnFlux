// Package mqttpolicy holds the runtime publish mask: which document
// families WarnFlux may publish to the MQTT broker. The admin Config page
// toggles the mask at runtime (changes apply immediately — every publisher
// consults it before each publish); the config file sets the startup
// state. Disabling noisy categories cuts broker traffic and CPU.
package mqttpolicy

import (
	"sync/atomic"
)

// Category is one publishable document family. The numeric values are
// single bits so the whole mask fits one atomic word.
type Category uint32

const (
	// CatEvents is the hazard-transition journal (<prefix>/events).
	CatEvents Category = 1 << iota
	// CatActive is the retained active-hazard view (<prefix>/active/...,
	// <prefix>/active-list) — the source plugins, the panel and the
	// output worker all materialize it.
	CatActive
	// CatInfo is the retained informational state (<prefix>/info/...):
	// source information messages and the EMCOM network states.
	CatInfo
	// CatStatus is the retained station status (<prefix>/status).
	CatStatus
	// CatAPRSStations is the retained per-station APRS feed
	// (<prefix>/aprs/stations/<callsign>).
	CatAPRSStations
	// CatAPRSBulletins is the retained APRS bulletin feed
	// (<prefix>/aprs/bulletins/...).
	CatAPRSBulletins
	// CatAPRSPackets is the non-retained APRS packet feed
	// (<prefix>/aprs/packets) — the noisiest APRS topic.
	CatAPRSPackets
	// CatAPRSMessages is the non-retained APRS message feed
	// (<prefix>/aprs/messages, rx and tx).
	CatAPRSMessages
	// CatMeshcoreStations is the retained MeshCore node feed
	// (<prefix>/meshcore/stations/<key>).
	CatMeshcoreStations
	// CatMeshcoreMessages is the non-retained MeshCore message feed
	// (<prefix>/meshcore/messages).
	CatMeshcoreMessages
)

// CatAll is the default mask: everything publishes.
const CatAll = CatEvents | CatActive | CatInfo | CatStatus |
	CatAPRSStations | CatAPRSBulletins | CatAPRSPackets | CatAPRSMessages |
	CatMeshcoreStations | CatMeshcoreMessages

// orderedNames maps the canonical config keys to their categories; the
// order is the UI order.
var orderedNames = []struct {
	Key  string
	Cat  Category
	Desc string
}{
	{"events", CatEvents, "hazard transition journal"},
	{"active", CatActive, "retained active-hazard view"},
	{"info", CatInfo, "information messages and EMCOM states"},
	{"status", CatStatus, "station status heartbeat"},
	{"aprs_stations", CatAPRSStations, "APRS station feed"},
	{"aprs_bulletins", CatAPRSBulletins, "APRS bulletins"},
	{"aprs_packets", CatAPRSPackets, "APRS packet feed"},
	{"aprs_messages", CatAPRSMessages, "APRS message feed"},
	{"meshcore_stations", CatMeshcoreStations, "MeshCore node feed"},
	{"meshcore_messages", CatMeshcoreMessages, "MeshCore message feed"},
}

// mask is the process-wide publish mask. The zero value means "nothing
// publishes", so the package default must be explicit.
var mask atomic.Uint32

// onChange is the optional change hook: after every effective Set it
// receives (old, new). Consumers that keep local state derived from the
// mask (the MQTT output's active-view rehydration) use it to resync.
var onChange atomic.Value // func(old, new uint32)

func init() { mask.Store(uint32(CatAll)) }

// Allowed reports whether the category may be published right now.
func Allowed(c Category) bool {
	return c != 0 && mask.Load()&uint32(c) != 0
}

// Mask returns the current mask.
func Mask() uint32 { return mask.Load() }

// SetOnChange registers the mask-change hook (nil clears it). The last
// registration wins; the hook runs synchronously after every Set that
// actually changes the mask.
func SetOnChange(fn func(old, new uint32)) { onChange.Store(fn) }

// Set replaces the mask atomically. Unknown bits are kept.
func Set(m uint32) {
	old := mask.Swap(m)
	if old == m {
		return
	}
	if fn, _ := onChange.Load().(func(old, new uint32)); fn != nil {
		fn(old, m)
	}
}

// Key returns the config-file key of a category ("" for unknown bits).
func Key(c Category) string {
	for _, e := range orderedNames {
		if e.Cat == c {
			return e.Key
		}
	}
	return ""
}

// Parse builds a mask from config keys; unknown keys are ignored.
func Parse(keys []string) uint32 {
	var m uint32
	for _, k := range keys {
		for _, e := range orderedNames {
			if e.Key == k {
				m |= uint32(e.Cat)
				break
			}
		}
	}
	return m
}

// EnabledKeys lists the config keys of the enabled categories, ordered
// like the UI.
func EnabledKeys(m uint32) []string {
	var out []string
	for _, e := range orderedNames {
		if m&uint32(e.Cat) != 0 {
			out = append(out, e.Key)
		}
	}
	return out
}

// List returns every category in UI order.
func List() []Category {
	out := make([]Category, 0, len(orderedNames))
	for _, e := range orderedNames {
		out = append(out, e.Cat)
	}
	return out
}
