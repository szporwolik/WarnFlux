package meshtastic

import "sync/atomic"

// channelAlerts gates hazard broadcasts on the group channel; dmAlerts
// gates hazard direct messages to users' registered node IDs; stationAlerts
// gates the APRS station range announcements on the group channel. All
// three are runtime switches on the admin Config page. Channel and DM
// default to ON — the routing matrix decides WHAT goes out, those
// switches decide WHICH mesh paths carry it. Station announcements
// default to OFF: they are useful but noisy.
var (
	channelAlerts atomic.Bool
	dmAlerts      atomic.Bool
	stationAlerts atomic.Bool
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

// SetStationAlerts flips the APRS station announcement switch.
func SetStationAlerts(on bool) { stationAlerts.Store(on) }

// StationAlerts reports whether APRS stations entering or leaving the
// operational range are announced on the group channel.
func StationAlerts() bool { return stationAlerts.Load() }
