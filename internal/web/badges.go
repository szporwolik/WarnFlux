package web

import (
	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// The interface has ONE severity vocabulary, and these are its four words:
//
//	ok    — green:  everything is as it should be
//	warn  — amber:  working, but limited or held (attention, not an outage)
//	bad   — red:    not working: it should run and does not, or delivery is
//	                impossible
//	muted — gray:   no verdict: switched off, not configured, or in transit
//
// Every card maps its condition through these constants, so the same
// condition can never read amber in one card and red in the next. That is
// exactly what the previous per-view mappings did: "degraded" was amber in
// the source table and red in the health card above it, while a source
// switched off in the configuration was painted red by the "disabled"
// class, which read as a fault.
const (
	badgeOK    = "ok"
	badgeWarn  = "warn"
	badgeBad   = "bad"
	badgeMuted = "muted"
)

// pluginBadgeClass maps a plugin instance state onto that vocabulary:
//
//	running              → ok     reporting normally
//	degraded             → warn   partial delivery (some feeds failing)
//	suspended            → warn   held: the offline-mode switch, or paused
//	                              after repeated failures
//	stopped              → bad    it is enabled and should be running
//	disabled             → muted  switched off in the configuration
//	starting, stopping   → muted  transitional, no verdict yet
//
// A halted instance is red on purpose: the station depends on it, and the
// supervisor restarting it is a problem the operator has to see, not a
// colour that blends into the gray of the switched-off ones.
func pluginBadgeClass(st plugin.PluginState) string {
	switch st {
	case plugin.StateRunning:
		return badgeOK
	case plugin.StateDegraded, plugin.StateSuspended:
		return badgeWarn
	case plugin.StateStopped:
		return badgeBad
	default: // disabled, starting, stopping, and any future state
		return badgeMuted
	}
}

// actionBadgeClass maps an action instance state onto the same vocabulary:
// healthy → ok, degraded → warn (the worker is retrying; deliveries can be
// late), disabled → muted (the operator switched the action off).
func actionBadgeClass(st action.InstanceState) string {
	switch st {
	case action.StateHealthy:
		return badgeOK
	case action.StateDegraded:
		return badgeWarn
	default:
		return badgeMuted
	}
}
