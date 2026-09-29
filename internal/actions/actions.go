// Package actions registers the built-in action types.
//
// Adding a future built-in action (sms, ntfy, ...) means adding a
// subpackage and one registration line here — the action core is
// untouched.
package actions

import (
	"github.com/szporwolik/WarnFlux/internal/action"
	aprsaction "github.com/szporwolik/WarnFlux/internal/actions/aprs"
	aprsout "github.com/szporwolik/WarnFlux/internal/actions/aprsout"
	"github.com/szporwolik/WarnFlux/internal/actions/discord"
	httpwebhook "github.com/szporwolik/WarnFlux/internal/actions/httpwebhook"
	"github.com/szporwolik/WarnFlux/internal/actions/logger"
	meshaction "github.com/szporwolik/WarnFlux/internal/actions/meshcore"
	"github.com/szporwolik/WarnFlux/internal/actions/smtp"
	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/meshcore"
)

// RegisterAll registers every built-in action type. hub is the shared APRS
// hub and meshHub the shared MeshCore hub (each may be nil when its
// integration is disabled; the affected actions then fail fast when
// configured).
func RegisterAll(reg *action.Registry, hub *aprs.Hub, meshHub *meshcore.Hub) error {
	if err := reg.Register("logger", logger.New); err != nil {
		return err
	}
	if err := reg.Register("smtp", smtp.New); err != nil {
		return err
	}
	if err := reg.Register("http_webhook", httpwebhook.New); err != nil {
		return err
	}
	if err := reg.Register("discord", discord.New); err != nil {
		return err
	}
	if err := aprsaction.Register(reg, hub); err != nil {
		return err
	}
	if err := aprsout.Register(reg, hub); err != nil {
		return err
	}
	if err := meshaction.Register(reg, meshHub); err != nil {
		return err
	}
	return nil
}
