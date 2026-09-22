package core

import (
	"strings"
	"testing"
	"time"
)

func validInformationMessage() InformationMessage {
	return InformationMessage{
		Source:      "openmeteo",
		Key:         "home",
		Kind:        "weather",
		GeneratedAt: time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC),
		Payload:     []byte(`{"schema_version":1}`),
	}
}

func TestInformationMessageValidateAccepts(t *testing.T) {
	if err := validInformationMessage().Validate(); err != nil {
		t.Fatalf("valid message rejected: %v", err)
	}
	// Multi-segment slugs are allowed.
	m := validInformationMessage()
	m.Key = "cabin.b-1"
	if err := m.Validate(); err != nil {
		t.Errorf("slug key rejected: %v", err)
	}
}

func TestInformationMessageValidateRejects(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*InformationMessage)
	}{
		{"empty source", func(m *InformationMessage) { m.Source = "" }},
		{"uppercase source", func(m *InformationMessage) { m.Source = "OpenMeteo" }},
		{"source with slash", func(m *InformationMessage) { m.Source = "open/meteo" }},
		{"source with wildcard", func(m *InformationMessage) { m.Source = "openmeteo+" }},
		{"empty key", func(m *InformationMessage) { m.Key = "" }},
		{"uppercase key", func(m *InformationMessage) { m.Key = "Home" }},
		{"key with hash", func(m *InformationMessage) { m.Key = "home#" }},
		{"empty kind", func(m *InformationMessage) { m.Kind = "" }},
		{"kind with space", func(m *InformationMessage) { m.Kind = "wea ther" }},
		{"zero generated", func(m *InformationMessage) { m.GeneratedAt = time.Time{} }},
		{"invalid JSON payload", func(m *InformationMessage) { m.Payload = []byte(`{`) }},
		{"empty payload", func(m *InformationMessage) { m.Payload = nil }},
		{"oversized payload", func(m *InformationMessage) {
			m.Payload = []byte(`"` + strings.Repeat("x", MaxInformationPayloadBytes) + `"`)
		}},
		{"oversized slug", func(m *InformationMessage) { m.Key = strings.Repeat("a", 65) }},
	}
	for _, c := range cases {
		m := validInformationMessage()
		c.mut(&m)
		if err := m.Validate(); err == nil {
			t.Errorf("%s: expected rejection, got nil", c.name)
		}
	}
}

func TestInformationMessageCloneDeepCopiesPayload(t *testing.T) {
	m := validInformationMessage()
	cp := m.Clone()
	cp.Payload[0] = 'x'
	if m.Payload[0] == 'x' {
		t.Error("Clone must not alias the payload")
	}
	if string(cp.Payload) == string(m.Payload) {
		t.Error("clone payload unexpectedly equal after mutation")
	}
}
