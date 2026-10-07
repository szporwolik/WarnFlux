package smtp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

// hazardReq builds a routed hazard request (same shape the rule engine
// hands to the smtp action).
func hazardReq() action.ActionRequest {
	return action.ActionRequest{
		ID:        "r1",
		CreatedAt: time.Now(),
		Event: dispatch.Event{
			Kind:   dispatch.EventHazardTransition,
			Origin: dispatch.Origin{Type: "mqtt", ReceiverID: "local"},
			Hazard: &dispatch.HazardTransition{
				Type:   dispatch.TransitionNew,
				Key:    "emcom:sp9moa-emcom",
				Source: "emcom",
				Hazard: dispatch.Hazard{
					EventKey: "emcom:sp9moa-emcom",
					Event:    "EMCOM",
					Severity: "severe",
					Headline: "SP9MOA EMCOM: level 1",
				},
			},
		},
	}
}

// TestBuildMessagePerRecipientLanguage pins the per-recipient mail
// language: a recipient whose member language is Polish gets the Polish
// rendering of a localized hazard (EMCOM) plus localized boilerplate,
// while the system default stays English.
func TestBuildMessagePerRecipientLanguage(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	req := hazardReq()
	req.Lang = "en" // system language
	req.Bcc = []string{"pl@example.com", "en@example.com"}
	req.BccLangs = []string{"pl", "en"}
	req.Event.Hazard.Hazard.Headline = "SP9MOA EMCOM: level 1 – Increased readiness"
	req.Event.Hazard.Hazard.Description = "SP9MOA EMCOM is now at level 1."
	req.Event.Hazard.Hazard.Localized = map[string]dispatch.HazardText{
		"en": {
			Headline:    "SP9MOA EMCOM: level 1 – Increased readiness",
			Description: "SP9MOA EMCOM is now at level 1 – Increased readiness.",
			Instruction: "Operators: prepare radio equipment.",
		},
		"pl": {
			Headline:    "SP9MOA EMCOM: poziom 1 – Podwyższona gotowość",
			Description: "SP9MOA EMCOM jest teraz na poziomie 1 – Podwyższona gotowość.",
			Instruction: "Operatorzy: przygotujcie sprzęt radiowy.",
		},
	}

	enMsg := string(buildMessage(context.Background(), Config{From: "a@b.c", To: []string{"x@y.z"}}, req, "en", now))
	if !strings.Contains(enMsg, "Increased readiness") || !strings.Contains(enMsg, "instruction:") {
		t.Errorf("English mail missing English rendering:\n%s", enMsg)
	}
	if strings.Contains(enMsg, "Podwyższona gotowość") {
		t.Errorf("English mail leaked the Polish rendering:\n%s", enMsg)
	}

	plMsg := string(buildMessage(context.Background(), Config{From: "a@b.c", To: []string{"x@y.z"}}, req, "pl", now))
	if !strings.Contains(plMsg, "Podwyższona gotowość") || !strings.Contains(plMsg, "zalecenia:") {
		t.Errorf("Polish mail missing Polish rendering:\n%s", plMsg)
	}
	if strings.Contains(plMsg, "Increased readiness") {
		t.Errorf("Polish mail leaked the English rendering:\n%s", plMsg)
	}

	// recipientLang resolution: members use their own choice, the
	// configured To list and unknown addresses fall back to the system
	// language.
	if got := recipientLang(req, "pl@example.com"); got != "pl" {
		t.Errorf("recipientLang(pl member) = %q, want pl", got)
	}
	if got := recipientLang(req, "en@example.com"); got != "en" {
		t.Errorf("recipientLang(en member) = %q, want en", got)
	}
	if got := recipientLang(req, "ops@station"); got != "en" {
		t.Errorf("recipientLang(static To) = %q, want system language en", got)
	}
}
