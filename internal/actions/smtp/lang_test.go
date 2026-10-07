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
			Description: "SP9MOA EMCOM is now at level 1 – Increased readiness.\n\nOperators ready to act.\n\nOperational readiness levels:\nLevel 0 – Monitoring: ...",
			Instruction: "Operators: prepare radio equipment.",
			Summary:     "SP9MOA EMCOM is now at level 1 – Increased readiness.",
			Definition:  "Operators ready to act.",
			Legend: []dispatch.HazardLegendLine{
				{Level: 0, Name: "Monitoring", Description: "Ongoing monitoring."},
				{Level: 1, Name: "Increased readiness", Description: "Operators ready to act."},
			},
		},
		"pl": {
			Headline:    "SP9MOA EMCOM: poziom 1 – Podwyższona gotowość",
			Description: "SP9MOA EMCOM jest teraz na poziomie 1 – Podwyższona gotowość.\n\nOperatorzy gotowi do działań.\n\nPoziomy gotowości operacyjnej:\nPoziom 0 – Monitoring: ...",
			Instruction: "Operatorzy: przygotujcie sprzęt radiowy.",
			Summary:     "SP9MOA EMCOM jest teraz na poziomie 1 – Podwyższona gotowość.",
			Definition:  "Operatorzy gotowi do działań.",
			Legend: []dispatch.HazardLegendLine{
				{Level: 0, Name: "Monitoring", Description: "Prowadzenie bieżącego nasłuchu."},
				{Level: 1, Name: "Podwyższona gotowość", Description: "Operatorzy gotowi do działań."},
			},
		},
	}

	enMsg := string(buildMessage(context.Background(), Config{From: "a@b.c", To: []string{"x@y.z"}}, req, "en", now))
	if !strings.Contains(enMsg, "Increased readiness") || !strings.Contains(enMsg, "instruction:") {
		t.Errorf("English mail missing English rendering:\n%s", enMsg)
	}
	if strings.Contains(enMsg, "Podwyższona gotowość") {
		t.Errorf("English mail leaked the Polish rendering:\n%s", enMsg)
	}
	// The styled HTML uses the structured pieces: the summary callout,
	// the definition paragraph and the legend rows — never the generic
	// plain description block (the plain-text part keeps the full text).
	enHTML := enMsg[strings.Index(enMsg, "text/html"):]
	if !strings.Contains(enHTML, "Operational readiness levels") {
		t.Errorf("English mail misses the legend heading:\n%s", enHTML)
	}
	if !strings.Contains(enHTML, "<strong>Monitoring</strong>") ||
		!strings.Contains(enHTML, "<strong>Increased readiness</strong>") {
		t.Errorf("English mail misses the legend rows:\n%s", enHTML)
	}
	if strings.Contains(enHTML, ">description</div>") {
		t.Errorf("structured EMCOM mail leaked the generic description block:\n%s", enHTML)
	}

	plMsg := string(buildMessage(context.Background(), Config{From: "a@b.c", To: []string{"x@y.z"}}, req, "pl", now))
	if !strings.Contains(plMsg, "Podwyższona gotowość") || !strings.Contains(plMsg, "zalecenia:") {
		t.Errorf("Polish mail missing Polish rendering:\n%s", plMsg)
	}
	if strings.Contains(plMsg, "Increased readiness") {
		t.Errorf("Polish mail leaked the English rendering:\n%s", plMsg)
	}
	plHTML := plMsg[strings.Index(plMsg, "text/html"):]
	if !strings.Contains(plHTML, "Poziomy gotowości operacyjnej") ||
		!strings.Contains(plHTML, "<strong>Monitoring</strong>") ||
		!strings.Contains(plHTML, "<strong>Podwyższona gotowość</strong>") {
		t.Errorf("Polish mail misses the styled legend:\n%s", plHTML)
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
