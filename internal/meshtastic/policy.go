package meshtastic

import "sync/atomic"

// alertsEnabled is the runtime master switch for hazard announcements on
// the mesh (channel broadcasts and direct messages). The operator toggles
// it on the Config page; the default is ON — the routing matrix decides
// WHAT goes out, this switch decides WHETHER anything goes out at all
// (the announcements are spammy, but invaluable during operations).
var alertsEnabled atomic.Bool

func init() { alertsEnabled.Store(true) }

// SetAlertsEnabled flips the runtime switch.
func SetAlertsEnabled(on bool) { alertsEnabled.Store(on) }

// AlertsEnabled reports whether hazard announcements may be transmitted.
func AlertsEnabled() bool { return alertsEnabled.Load() }
