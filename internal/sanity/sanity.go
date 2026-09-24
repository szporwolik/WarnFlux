// Package sanity is the single chokepoint every outbound human-readable
// message passes through before transmission. It normalizes text per
// channel (APRS radio messages, email subject/body) so operators never
// transmit control characters, doubled whitespace or unreadable junk, and
// it is the prepared hook point for an OPTIONAL LLM reviewer: a future
// integration only needs to implement the review step inside Prepare —
// no caller changes.
//
// Egress map (all outbound human-readable channels):
//
//	aprs message text → aprs.Hub.SendMessage / SendMessageWaitAck
//	  (both radio and internet transmitters; callers: actions aprs,
//	  aprs-out)                                    → sanity.NormalizeText
//	email subject + plain body → actions/smtp
//	  (subjectOf + bodyOfPlain)                    → sanity.NormalizeText
//
// Out of scope by design: the MQTT output publishes machine-format JSON
// events (canonical payloads), not human prose — normalizing those would
// corrupt the schema.
package sanity

import (
	"context"
)

// Channel identifies the outbound medium the text travels on. Each
// channel has its own normalization rules.
type Channel string

const (
	// ChannelAPRS is a single-line APRS message frame: 7-bit ASCII,
	// Polish diacritics transliterated, no control characters.
	ChannelAPRS Channel = "aprs-message"
	// ChannelEmailSubject is a single-line RFC 5322 subject (UTF-8
	// allowed, bounded length).
	ChannelEmailSubject Channel = "email-subject"
	// ChannelEmailBody is a plain-text email body (newlines preserved).
	ChannelEmailBody Channel = "email-body"
)

// Message is one outbound human-readable message prepared for review and
// normalization. Subject is only used for email channels.
type Message struct {
	Channel Channel
	To      string // addressee: APRS callsign or email address
	Subject string // email subject line (email channels only)
	Text    string // APRS message text or plain email body
	// Note is an informational audit trail entry (normalization summary
	// today, LLM review annotation in the future). Callers may log it.
	Note string
}

// Service prepares outbound messages. It is deliberately a struct so the
// future LLM reviewer can be configured here without touching callers.
type Service struct {
	// TODO(llm): optional LLM reviewer configuration goes here:
	//   Endpoint string // OpenAI-compatible base URL
	//   Model    string // e.g. "gpt-4o-mini"
	//   APIKey   string // loaded from a file, never YAML
	//   Timeout  time.Duration
	// and a reviewer interface invoked from Prepare before normalization.
}

// defaultService is the process-wide service used by NormalizeText.
// TODO(llm): the LLM configuration will live on the service and be loaded
// here from the application config.
var defaultService = &Service{}

// NormalizeText normalizes a single text for the channel (the common
// case: callers that only have text, e.g. the APRS hub). For email
// subject channels the text is treated as the subject line.
func NormalizeText(ctx context.Context, ch Channel, text string) string {
	m := Message{Channel: ch}
	if ch == ChannelEmailSubject {
		m.Subject = text
	} else {
		m.Text = text
	}
	m = defaultService.Prepare(ctx, m)
	if ch == ChannelEmailSubject {
		return m.Subject
	}
	return m.Text
}

// Prepare normalizes one outbound message in place and appends a Note
// describing what changed. ctx is unused today but is the lifetime of any
// future blocking review step (LLM call) and must not be dropped.
func (s *Service) Prepare(ctx context.Context, m Message) Message {
	_ = ctx // reserved: TODO(llm) review call will use it
	// TODO(llm): if an LLM reviewer is configured on the service, run it
	// here FIRST (review and rewrite m.Text / m.Subject, record the
	// verdict in m.Note), then fall through to deterministic
	// normalization below — normalization always runs last so even an
	// LLM cannot bypass the channel limits.
	//
	// Example integration point:
	//   if s.reviewer != nil {
	//       reviewed, err := s.reviewer.Review(ctx, m)
	//       if err != nil { m.Note = "llm review failed: " + err.Error() }
	//       else { m = reviewed }
	//   }
	switch m.Channel {
	case ChannelAPRS:
		m.Text, m.Note = normalizeAPRS(m.Text)
	case ChannelEmailSubject:
		m.Subject, m.Note = normalizeSubject(m.Subject)
	case ChannelEmailBody:
		m.Text, m.Note = normalizeEmailBody(m.Text)
	default:
		// Unknown channels are left untouched rather than mangled.
	}
	return m
}
