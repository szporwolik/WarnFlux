package aprs

// APRS-message routing wire documents: an APRS message addressed to our
// station and heard over the radio (KISS) is re-published as a canonical
// /events payload on the WarnFlux topic prefix, so it enters the normal
// MQTT → routing matrix flow as the "aprs" source (groups can forward it
// to Discord, SMTP, ...). The schema mirrors mqttreceiver.WireSchemaVersion
// (both live on the same /events stream, so the version must never drift).
const messageEventSchemaVersion = 1

// MessageEventWire is the /events envelope for one routed APRS message.
type MessageEventWire struct {
	SchemaVersion int                `json:"schema_version"`
	ChangeID      int64              `json:"change_id"`
	ChangeType    string             `json:"change_type"`
	EventKey      string             `json:"event_key"`
	Event         MessageEventHazard `json:"event"`
}

// MessageEventHazard is the hazard block: the forwarded content starts
// with "Message from: <callsign with SSID>", the message text follows,
// and the receiving station's display name is carried as context.
type MessageEventHazard struct {
	Source      string   `json:"source"`
	SourceID    string   `json:"source_id"`
	Category    string   `json:"category"`
	Event       string   `json:"event"`
	Severity    string   `json:"severity"`
	Urgency     string   `json:"urgency"`
	Certainty   string   `json:"certainty"`
	Headline    string   `json:"headline"`
	Description string   `json:"description"`
	Instruction string   `json:"instruction"`
	EffectiveAt *string  `json:"effective_at,omitempty"`
	ExpiresAt   *string  `json:"expires_at,omitempty"`
	Areas       []string `json:"areas"`
	Status      string   `json:"status"`
	SourceURL   string   `json:"source_url"`
	ReceivedAt  string   `json:"received_at"`
	UpdatedAt   string   `json:"updated_at"`
}
