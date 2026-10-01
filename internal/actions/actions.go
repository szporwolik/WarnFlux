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
	meshtasticaction "github.com/szporwolik/WarnFlux/internal/actions/meshtastic"
	"github.com/szporwolik/WarnFlux/internal/actions/smtp"
	"github.com/szporwolik/WarnFlux/internal/aprs"
	"github.com/szporwolik/WarnFlux/internal/meshtastic"
)

// RegisterAll registers every built-in action type. hub is the shared APRS
// hub and meshtasticHub the shared Meshtastic hub (each may be nil when its
// integration is disabled; the affected actions then fail fast when
// configured).
func RegisterAll(reg *action.Registry, hub *aprs.Hub, meshtasticHub *meshtastic.Hub) error {
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
	if err := meshtasticaction.Register(reg, meshtasticHub); err != nil {
		return err
	}
	// Internet-backed actions: the admin offline-mode switch makes their
	// workers hold queued requests (nothing executed, nothing lost) until
	// the station is online again. Local actions (logger, aprs, aprsout,
	// meshtastic) keep running off-grid.
	for _, internet := range []string{"smtp", "http_webhook", "discord"} {
		reg.MarkInternet(internet)
	}
	return nil
}
