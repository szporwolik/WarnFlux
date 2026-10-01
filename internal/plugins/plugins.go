// Package plugins wires the built-in plugins into a plugin.Registry.
// Adding a new built-in plugin means registering its factory here and
// adding its package under plugins/sources or plugins/outputs.
package plugins

import (
	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/plugins/outputs/mqtt"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/adsb"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/aprsinet"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/aprsradio"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/gddkia"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/gios"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/giosaq"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/imgw"
	meshtasticsource "github.com/szporwolik/WarnFlux/internal/plugins/sources/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/metar"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/openmeteo"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/rso"
)

// RegisterBuiltins registers every built-in plugin type. Registration is
// explicit so it is easy to audit, test and search. hub is the shared APRS
// hub and meshtasticHub the shared Meshtastic hub (each may be nil when its
// integration is disabled; the affected plugins then fail fast).
func RegisterBuiltins(reg *plugin.Registry, hub *aprs.Hub, meshtasticHub *meshtastic.Hub) error {
	if err := openmeteo.Register(reg); err != nil {
		return err
	}
	if err := metar.Register(reg); err != nil {
		return err
	}
	if err := meshtasticsource.Register(reg, meshtasticHub); err != nil {
		return err
	}
	if err := imgw.Register(reg); err != nil {
		return err
	}
	if err := rso.Register(reg); err != nil {
		return err
	}
	if err := gddkia.Register(reg, hub); err != nil {
		return err
	}
	if err := gios.Register(reg, hub); err != nil {
		return err
	}
	if err := giosaq.Register(reg, hub); err != nil {
		return err
	}
	if err := adsb.Register(reg, hub); err != nil {
		return err
	}
	if err := aprsinet.Register(reg, hub); err != nil {
		return err
	}
	if err := aprsradio.Register(reg, hub); err != nil {
		return err
	}
	if err := mqtt.Register(reg); err != nil {
		return err
	}
	// Internet-backed sources: the admin offline-mode switch suspends
	// these (and resumes them on exit from offline mode). Local-only
	// sources — aprs-radio (KISS/TNC), meshtastic (serial), snapshotutil —
	// keep running off-grid.
	for _, internet := range []string{
		"openmeteo", "metar", "imgw", "rso", "gddkia",
		"gios", "giosaq", "adsb", "aprs-inet",
	} {
		reg.MarkSourceInternet(internet)
	}
	return nil
}
