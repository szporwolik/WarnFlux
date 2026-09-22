// Package smtp implements the built-in SMTP email action: it sends one
// plain-text email per routed dispatch event (subject from the hazard
// severity/headline, body from the canonical event metadata).
//
// It follows the ActionPlugin architecture exactly: YAML config decoding,
// factory validation (a missing secret or an invalid configuration is a
// startup error, never a runtime surprise), per-call connection handling,
// context-bounded execution and a no-op Close (no resources survive
// between calls).
//
// Secrets (password / password_file) are read at construction and are
// never logged.
package smtp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"mime"
	"net"
	"net/smtp"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

// Type is the action type name used in the YAML configuration.
const Type = "smtp"

const (
	// maxPasswordFileBytes bounds the password file read at construction.
	maxPasswordFileBytes = 64 * 1024
	// maxToRecipients bounds the static recipient list so a config typo
	// can never build an unbounded RCPT fan-out.
	maxToRecipients = 64
	// defaultDeadline bounds one send when the caller context carries no
	// deadline of its own.
	defaultDeadline = 30 * time.Second
)

// Config is the action-specific configuration.
type Config struct {
	// Host is the SMTP server hostname (used for dialing, the SMTP hello
	// and TLS certificate verification). Required.
	Host string `yaml:"host"`
	// Port is the SMTP port; 0 falls back to 587.
	Port int `yaml:"port"`
	// Username is the SMTP AUTH PLAIN user. Empty disables authentication.
	Username string `yaml:"username"`
	// Password is the plaintext password. Mutually exclusive with
	// PasswordFile.
	Password string `yaml:"password"`
	// PasswordFile reads the password from a file (e.g. a Docker secret).
	// Mutually exclusive with Password.
	PasswordFile string `yaml:"password_file"`
	// From is the envelope and header sender. Required.
	From string `yaml:"from"`
	// To lists the static recipients. Required (at least one).
	To []string `yaml:"to"`
	// StartTLS enables the SMTP STARTTLS upgrade when the server offers
	// it. Defaults to true when omitted.
	StartTLS *bool `yaml:"starttls"`
	// CAFile optionally appends a PEM CA bundle to the system roots, for
	// private SMTP servers with their own certificate authority.
	CAFile string `yaml:"ca_file"`
	// SubjectPrefix is prepended to every subject line.
	SubjectPrefix string `yaml:"subject_prefix"`
}

type emailAction struct {
	id       string
	cfg      Config
	password string
	roots    *x509.CertPool
	logger   *slog.Logger
}

// New builds an SMTP action instance from its raw YAML configuration.
func New(node *yaml.Node) (action.Plugin, error) {
	var cfg Config
	if node != nil {
		if err := node.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("smtp: decode config: %w", err)
		}
	}

	if strings.TrimSpace(cfg.Host) == "" {
		return nil, fmt.Errorf("smtp: config.host is required")
	}
	if cfg.Port == 0 {
		cfg.Port = 587
	}
	if strings.TrimSpace(cfg.From) == "" {
		return nil, fmt.Errorf("smtp: config.from is required")
	}
	if len(cfg.To) == 0 {
		return nil, fmt.Errorf("smtp: config.to must contain at least one recipient")
	}
	if len(cfg.To) > maxToRecipients {
		return nil, fmt.Errorf("smtp: config.to has %d recipients, maximum %d", len(cfg.To), maxToRecipients)
	}
	for _, rcpt := range cfg.To {
		if strings.TrimSpace(rcpt) == "" {
			return nil, fmt.Errorf("smtp: config.to contains an empty recipient")
		}
	}
	if cfg.Password != "" && cfg.PasswordFile != "" {
		return nil, fmt.Errorf("smtp: config.password and config.password_file are mutually exclusive")
	}
	if cfg.StartTLS == nil {
		tls := true
		cfg.StartTLS = &tls
	}

	p := &emailAction{id: Type, cfg: cfg, logger: slog.Default()}

	if cfg.PasswordFile != "" {
		if info, err := os.Stat(cfg.PasswordFile); err != nil {
			return nil, fmt.Errorf("smtp: stat config.password_file: %w", err)
		} else if info.Size() > maxPasswordFileBytes {
			return nil, fmt.Errorf("smtp: config.password_file is %d bytes, maximum %d", info.Size(), maxPasswordFileBytes)
		}
		data, err := os.ReadFile(cfg.PasswordFile)
		if err != nil {
			return nil, fmt.Errorf("smtp: read config.password_file: %w", err)
		}
		p.password = strings.TrimRight(string(data), "\r\n")
	} else {
		p.password = cfg.Password
	}

	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("smtp: read config.ca_file: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("smtp: system cert pool: %w", err)
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("smtp: config.ca_file contains no PEM certificates")
		}
		p.roots = roots
	}

	return p, nil
}

func (p *emailAction) Name() string { return p.id }

// Execute sends one email for the routed event. A fresh SMTP connection is
// used per call: the action holds no persistent network resources, so
// there is no reconnect state to corrupt. The connection is closed by the
// context's AfterFunc when ctx is cancelled before the deadline, which
// unblocks any in-flight protocol operation.
func (p *emailAction) Execute(ctx context.Context, req action.ActionRequest) error {
	deadline := time.Now().Add(defaultDeadline)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}

	addr := net.JoinHostPort(p.cfg.Host, fmt.Sprintf("%d", p.cfg.Port))
	dialer := net.Dialer{Deadline: deadline}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("smtp: connect to %s: %w", addr, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	client, err := smtp.NewClient(conn, p.cfg.Host)
	if err != nil {
		return fmt.Errorf("smtp: handshake: %w", err)
	}
	defer client.Close()

	if *p.cfg.StartTLS {
		if ok, _ := client.Extension("STARTTLS"); ok {
			tlsCfg := &tls.Config{
				ServerName: p.cfg.Host,
				MinVersion: tls.VersionTLS12,
				RootCAs:    p.roots,
			}
			if err := client.StartTLS(tlsCfg); err != nil {
				return fmt.Errorf("smtp: starttls: %w", err)
			}
		}
	}

	if p.cfg.Username != "" {
		auth := smtp.PlainAuth("", p.cfg.Username, p.password, p.cfg.Host)
		if err := client.Auth(auth); err != nil {
			return fmt.Errorf("smtp: auth: %w", err)
		}
	}

	if err := client.Mail(p.cfg.From); err != nil {
		return fmt.Errorf("smtp: mail from: %w", err)
	}
	for _, rcpt := range p.cfg.To {
		if err := client.Rcpt(rcpt); err != nil {
			return fmt.Errorf("smtp: rcpt %q: %w", rcpt, err)
		}
	}
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("smtp: data: %w", err)
	}
	msg := buildMessage(p.cfg, req, time.Now())
	if _, err := w.Write(msg); err != nil {
		return fmt.Errorf("smtp: write message: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("smtp: end message: %w", err)
	}
	if err := client.Quit(); err != nil {
		return fmt.Errorf("smtp: quit: %w", err)
	}
	return nil
}

// Close releases plugin-owned resources. This action is stateless between
// calls (each Execute uses its own connection), so there is nothing to
// release.
func (p *emailAction) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return nil
}

// buildMessage assembles the RFC 5322 message for one routed event. The
// subject line is MIME word-encoded so non-ASCII (e.g. Polish) text stays
// intact.
func buildMessage(cfg Config, req action.ActionRequest, now time.Time) []byte {
	subject := subjectOf(cfg, req)
	body := bodyOf(req, now)

	var b strings.Builder
	fmt.Fprintf(&b, "From: %s\r\n", cfg.From)
	fmt.Fprintf(&b, "To: %s\r\n", strings.Join(cfg.To, ", "))
	fmt.Fprintf(&b, "Subject: %s\r\n", mime.QEncoding.Encode("utf-8", subject))
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	b.WriteString("Content-Transfer-Encoding: 8bit\r\n")
	b.WriteString("\r\n")
	b.WriteString(body)
	return []byte(b.String())
}

// subjectOf builds a concise, severity-first subject line.
func subjectOf(cfg Config, req action.ActionRequest) string {
	var base string
	switch req.Event.Kind {
	case dispatch.EventHazardTransition:
		h := req.Event.Hazard
		if h == nil {
			base = "hazard transition"
			break
		}
		text := strings.TrimSpace(h.Hazard.Headline)
		if text == "" {
			text = h.Hazard.Event
		}
		base = fmt.Sprintf("%s: %s", strings.ToUpper(h.Hazard.Severity), text)
	case dispatch.EventMQTTMessage:
		if req.Event.MQTT != nil {
			base = fmt.Sprintf("MQTT message on %s", req.Event.MQTT.Topic)
		} else {
			base = "MQTT message"
		}
	default:
		base = "dispatch event"
	}
	if cfg.SubjectPrefix != "" {
		return cfg.SubjectPrefix + " " + base
	}
	return base
}

// bodyOf renders the canonical event metadata as plain text. It never
// includes raw payloads.
func bodyOf(req action.ActionRequest, now time.Time) string {
	var b strings.Builder
	b.WriteString("WarnFlux notification\n")
	fmt.Fprintf(&b, "Time: %s\n\n", now.Format(time.RFC3339))

	ev := req.Event
	fmt.Fprintf(&b, "Kind: %s\n", ev.Kind)
	if ev.Origin.ReceiverID != "" {
		fmt.Fprintf(&b, "Receiver: %s\n", ev.Origin.ReceiverID)
	}

	switch ev.Kind {
	case dispatch.EventHazardTransition:
		h := ev.Hazard
		if h == nil {
			b.WriteString("Hazard: <empty transition>\n")
			break
		}
		fmt.Fprintf(&b, "Transition: %s\n", h.Type)
		fmt.Fprintf(&b, "Source: %s\n", h.Source)
		fmt.Fprintf(&b, "Event key: %s\n", h.Key)
		fmt.Fprintf(&b, "Event: %s\n", h.Hazard.Event)
		fmt.Fprintf(&b, "Severity: %s\n", h.Hazard.Severity)
		if h.Hazard.Headline != "" {
			fmt.Fprintf(&b, "Headline: %s\n", h.Hazard.Headline)
		}
		if len(h.Hazard.Areas) > 0 {
			fmt.Fprintf(&b, "Areas: %s\n", strings.Join(h.Hazard.Areas, ", "))
		}
		if h.Hazard.EffectiveAt != nil {
			fmt.Fprintf(&b, "Effective: %s\n", h.Hazard.EffectiveAt.Format(time.RFC3339))
		}
		if h.Hazard.ExpiresAt != nil {
			fmt.Fprintf(&b, "Expires: %s\n", h.Hazard.ExpiresAt.Format(time.RFC3339))
		}
	case dispatch.EventMQTTMessage:
		m := ev.MQTT
		if m != nil {
			fmt.Fprintf(&b, "Topic: %s\n", m.Topic)
			fmt.Fprintf(&b, "QoS: %d\n", m.QoS)
			fmt.Fprintf(&b, "Payload bytes: %d\n", len(m.Payload))
		}
	}
	return b.String()
}
