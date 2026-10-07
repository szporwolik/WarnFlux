package meshtastic

import "sync/atomic"

// channelAlerts gates hazard broadcasts on the group channel; dmAlerts
// gates hazard direct messages to users' registered node IDs. Both are
// runtime switches on the admin Config page and both default to ON —
// the routing matrix decides WHAT goes out, these switches decide WHICH
// mesh paths carry it.
var (
	channelAlerts atomic.Bool
	dmAlerts      atomic.Bool
)

func init() {
	channelAlerts.Store(true)
	dmAlerts.Store(true)
}

// SetChannelAlerts flips the group-channel announcements switch.
func SetChannelAlerts(on bool) { channelAlerts.Store(on) }

// ChannelAlerts reports whether hazard broadcasts on the group channel
// may be transmitted.
func ChannelAlerts() bool { return channelAlerts.Load() }

// SetDMAlerts flips the direct-message announcements switch.
func SetDMAlerts(on bool) { dmAlerts.Store(on) }

// DMAlerts reports whether hazard direct messages to registered node
// IDs may be transmitted.
func DMAlerts() bool { return dmAlerts.Load() }
