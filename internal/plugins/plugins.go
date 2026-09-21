// Package plugins wires the built-in plugins into a plugin.Registry.
// Adding a new built-in plugin means registering its factory here and
// adding its package under plugins/sources or plugins/outputs.
package plugins

import (
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/plugins/outputs/mqtt"
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/demo"
)

// RegisterBuiltins registers every built-in plugin type. Registration is
// explicit so it is easy to audit, test and search.
func RegisterBuiltins(reg *plugin.Registry) error {
	if err := demo.Register(reg); err != nil {
		return err
	}
	if err := mqtt.Register(reg); err != nil {
		return err
	}
	return nil
}
