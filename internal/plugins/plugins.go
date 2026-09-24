// Package plugins wires the built-in plugins into a plugin.Registry.
// Adding a new built-in plugin means registering its factory here and
// adding its package under plugins/sources or plugins/outputs.
package plugins

import (
	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/plugins/outputs/mqtt"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/aprsinet"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/aprsradio"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/gddkia"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/imgw"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/openmeteo"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/rso"
)

// RegisterBuiltins registers every built-in plugin type. Registration is
// explicit so it is easy to audit, test and search. hub is the shared APRS
// hub (may be nil when the hub is disabled; APRS plugins then fail fast).
func RegisterBuiltins(reg *plugin.Registry, hub *aprs.Hub) error {
	if err := openmeteo.Register(reg); err != nil {
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
	if err := aprsinet.Register(reg, hub); err != nil {
		return err
	}
	if err := aprsradio.Register(reg, hub); err != nil {
		return err
	}
	if err := mqtt.Register(reg); err != nil {
		return err
	}
	return nil
}
