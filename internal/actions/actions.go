// Package actions registers the built-in action types.
//
// Adding a future built-in action (sms, discord, ntfy, ...) means adding a
// subpackage and one registration line here — the action core is
// untouched.
package actions

import (
	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/actions/logger"
	"github.com/szporwolik/WarnFlux/internal/actions/smtp"
)

// RegisterAll registers every built-in action type.
func RegisterAll(reg *action.Registry) error {
	if err := reg.Register("logger", logger.New); err != nil {
		return err
	}
	if err := reg.Register("smtp", smtp.New); err != nil {
		return err
	}
	return nil
}
