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
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net"
	"net/smtp"
	"os"
	"strings"
	"sync"
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
	// it. Defaults to true when omitted; ignored when ImplicitTLS is set.
	StartTLS *bool `yaml:"starttls"`
	// ImplicitTLS encrypts the connection from the first byte (SMTPS,
	// typically port 465) instead of upgrading via STARTTLS. Defaults to
	// false when omitted.
	ImplicitTLS *bool `yaml:"implicit_tls"`
	// CAFile optionally appends a PEM CA bundle to the system roots, for
	// private SMTP servers with their own certificate authority.
	CAFile string `yaml:"ca_file"`
	// SubjectPrefix is prepended to every subject line.
	SubjectPrefix string `yaml:"subject_prefix"`
	// RateLimitPerMinute bounds how many emails this action sends per
	// minute (evenly spaced). 0 falls back to the default (30); a
	// negative value disables the limit entirely.
	RateLimitPerMinute int `yaml:"rate_limit_per_minute"`
}

// defaultRateLimitPerMinute protects against provider blocking when the
// configuration omits the limit.
const defaultRateLimitPerMinute = 30

type emailAction struct {
	id       string
	cfg      Config
	password string
	roots    *x509.CertPool
	limiter  *rateLimiter
	logger   *slog.Logger
}

// rateLimiter spaces sends evenly: at most limit emails per minute, one
// slot at a time. It is deliberately primitive — a token bucket or a
// sliding window is not needed for a single sequential worker.
type rateLimiter struct {
	mu       sync.Mutex
	next     time.Time
	interval time.Duration
}

func newRateLimiter(perMinute int) *rateLimiter {
	if perMinute <= 0 {
		return nil
	}
	return &rateLimiter{interval: time.Minute / time.Duration(perMinute)}
}

// wait claims the next send slot. It blocks until the slot's time has
// arrived or ctx is cancelled; a cancelled ctx returns ctx.Err() so the
// action worker counts a failure instead of silently sending anyway.
func (l *rateLimiter) wait(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	start := l.next
	if start.Before(now) {
		start = now
	}
	l.next = start.Add(l.interval)
	l.mu.Unlock()

	delay := time.Until(start)
	if delay <= 0 {
		return nil
	}
	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
	if cfg.ImplicitTLS == nil {
		no := false
		cfg.ImplicitTLS = &no
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

	if cfg.RateLimitPerMinute == 0 {
		cfg.RateLimitPerMinute = defaultRateLimitPerMinute
	}
	p.limiter = newRateLimiter(cfg.RateLimitPerMinute)

	return p, nil
}

func (p *emailAction) Name() string { return p.id }

// Execute sends one email for the routed event, one SMTP transaction per
// unique recipient (config.to plus the matched group's member addresses
// carried in request.Bcc). A fresh SMTP connection is used per call: the
// action holds no persistent network resources, so there is no reconnect
// state to corrupt. The connection is closed by the context's AfterFunc
// when ctx is cancelled before the deadline, which unblocks any in-flight
// protocol operation. Each email claims a rate-limit slot before it is
// sent, so bursts drain as a paced queue instead of hammering the server.
func (p *emailAction) Execute(ctx context.Context, req action.ActionRequest) error {
	recipients := dedupeRecipients(p.cfg.To, req.Bcc)
	if len(recipients) == 0 {
		return fmt.Errorf("smtp: no recipients (config.to and request.bcc are empty)")
	}

	deadline := time.Now().Add(defaultDeadline)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}

	addr := net.JoinHostPort(p.cfg.Host, fmt.Sprintf("%d", p.cfg.Port))
	dialer := net.Dialer{Deadline: deadline}

	var conn net.Conn
	if *p.cfg.ImplicitTLS {
		// SMTPS: TLS from the first byte (typically port 465); STARTTLS
		// is never attempted on an already-encrypted connection.
		tlsCfg := &tls.Config{
			ServerName: p.cfg.Host,
			MinVersion: tls.VersionTLS12,
			RootCAs:    p.roots,
		}
		tlsConn, err := tls.DialWithDialer(&dialer, "tcp", addr, tlsCfg)
		if err != nil {
			return fmt.Errorf("smtp: tls connect to %s: %w", addr, err)
		}
		conn = tlsConn
	} else {
		c, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return fmt.Errorf("smtp: connect to %s: %w", addr, err)
		}
		conn = c
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

	if *p.cfg.StartTLS && !*p.cfg.ImplicitTLS {
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

	msg := buildMessage(p.cfg, req, time.Now())
	var errs []error
	for _, rcpt := range recipients {
		// One rate slot per email; a cancelled wait fails the remaining
		// sends instead of bypassing the limit.
		if err := p.limiter.wait(ctx); err != nil {
			errs = append(errs, fmt.Errorf("rate limit: %w", err))
			break
		}
		if err := p.sendOne(client, rcpt, msg); err != nil {
			errs = append(errs, err)
			// A transport-level failure leaves the connection unusable;
			// give up on the rest of the batch.
			if !isProtocolError(err) {
				break
			}
		}
	}
	if err := client.Quit(); err != nil && len(errs) == 0 {
		errs = append(errs, fmt.Errorf("smtp: quit: %w", err))
	}
	return errors.Join(errs...)
}

// sendOne runs one SMTP transaction (MAIL/RCPT/DATA) for a single
// envelope recipient on the shared connection.
func (p *emailAction) sendOne(client *smtp.Client, rcpt string, msg []byte) error {
	if err := client.Mail(p.cfg.From); err != nil {
		return fmt.Errorf("smtp: mail from: %w", err)
	}
	if err := client.Rcpt(rcpt); err != nil {
		return fmt.Errorf("smtp: rcpt %q: %w", rcpt, err)
	}
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("smtp: data: %w", err)
	}
	if _, err := w.Write(msg); err != nil {
		return fmt.Errorf("smtp: write message: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("smtp: end message: %w", err)
	}
	return nil
}

// isProtocolError reports whether the error happened after the server
// accepted the transaction (rejected RCPT, rejected DATA...), i.e. the
// connection is still usable for the next recipient.
func isProtocolError(err error) bool {
	msg := err.Error()
	return strings.HasPrefix(msg, "smtp: rcpt ") ||
		strings.HasPrefix(msg, "smtp: data") ||
		strings.HasPrefix(msg, "smtp: end message")
}

// dedupeRecipients merges the configured To list with the request Bcc,
// preserving order and dropping duplicates (case-insensitive).
func dedupeRecipients(to, bcc []string) []string {
	seen := make(map[string]struct{}, len(to)+len(bcc))
	out := make([]string, 0, len(to)+len(bcc))
	for _, list := range [][]string{to, bcc} {
		for _, r := range list {
			r = strings.TrimSpace(r)
			if r == "" {
				continue
			}
			key := strings.ToLower(r)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, r)
		}
	}
	return out
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
		if h.Hazard.Urgency != "" {
			fmt.Fprintf(&b, "Urgency: %s\n", h.Hazard.Urgency)
		}
		if h.Hazard.Certainty != "" {
			fmt.Fprintf(&b, "Certainty: %s\n", h.Hazard.Certainty)
		}
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
