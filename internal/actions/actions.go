// Package actions registers the built-in action types.
//
// Adding a future built-in action (sms, discord, ntfy, ...) means adding a
// subpackage and one registration line here — the action core is
// untouched.
package actions

import (
	"github.com/szporwolik/WarnFlux/internal/action"
	aprsaction "github.com/szporwolik/WarnFlux/internal/actions/aprs"
	"github.com/szporwolik/WarnFlux/internal/actions/httpwebhook"
	"github.com/szporwolik/WarnFlux/internal/actions/logger"
	"github.com/szporwolik/WarnFlux/internal/actions/smtp"
	"github.com/szporwolik/WarnFlux/internal/aprs"
)

// RegisterAll registers every built-in action type. hub is the shared APRS
// hub (may be nil when the hub is disabled; the aprs action then fails
// fast when configured).
func RegisterAll(reg *action.Registry, hub *aprs.Hub) error {
	if err := reg.Register("logger", logger.New); err != nil {
		return err
	}
	if err := reg.Register("smtp", smtp.New); err != nil {
		return err
	}
	if err := reg.Register("http_webhook", httpwebhook.New); err != nil {
		return err
	}
	if err := aprsaction.Register(reg, hub); err != nil {
		return err
	}
	return nil
}
