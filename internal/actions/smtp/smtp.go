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
	"encoding/base64"
	"errors"
	"fmt"
	"html"
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
	"github.com/szporwolik/WarnFlux/internal/appinfo"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/geo"
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

// logoCID identifies the inline logo part referenced from the HTML part.
const logoCID = "warnflux-logo"

// buildMessage assembles the RFC 5322 message for one routed event as
// multipart/related: the root carries a multipart/alternative body
// (plain-text fallback + styled HTML) plus the application logo as an
// inline CID image, so the brand renders without any external hosting.
// The subject line is MIME word-encoded so non-ASCII (e.g. Polish) text
// stays intact. All dynamic values are HTML-escaped in the HTML part.
func buildMessage(cfg Config, req action.ActionRequest, now time.Time) []byte {
	subject := subjectOf(cfg, req)
	relatedBoundary := fmt.Sprintf("warnflux-rel-%d", now.UnixNano())
	altBoundary := fmt.Sprintf("warnflux-alt-%d", now.UnixNano())
	plain := bodyOfPlain(req, now)
	htmlBody := bodyOfHTML(req, now)

	var b strings.Builder
	fmt.Fprintf(&b, "From: %s\r\n", cfg.From)
	fmt.Fprintf(&b, "To: %s\r\n", strings.Join(cfg.To, ", "))
	fmt.Fprintf(&b, "Subject: %s\r\n", mime.QEncoding.Encode("utf-8", subject))
	b.WriteString("MIME-Version: 1.0\r\n")
	fmt.Fprintf(&b, "Content-Type: multipart/related; boundary=\"%s\"\r\n", relatedBoundary)
	b.WriteString("\r\n")

	// Alternative body: plain text first, HTML second.
	fmt.Fprintf(&b, "--%s\r\n", relatedBoundary)
	fmt.Fprintf(&b, "Content-Type: multipart/alternative; boundary=\"%s\"\r\n\r\n", altBoundary)

	fmt.Fprintf(&b, "--%s\r\n", altBoundary)
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	b.WriteString("Content-Transfer-Encoding: 8bit\r\n\r\n")
	b.WriteString(plain)
	b.WriteString("\r\n")

	fmt.Fprintf(&b, "--%s\r\n", altBoundary)
	b.WriteString("Content-Type: text/html; charset=utf-8\r\n")
	b.WriteString("Content-Transfer-Encoding: 8bit\r\n\r\n")
	b.WriteString(htmlBody)
	b.WriteString("\r\n")
	fmt.Fprintf(&b, "--%s--\r\n", altBoundary)

	// Inline logo.
	fmt.Fprintf(&b, "--%s\r\n", relatedBoundary)
	b.WriteString("Content-Type: image/png; name=\"logo.png\"\r\n")
	fmt.Fprintf(&b, "Content-ID: <%s>\r\n", logoCID)
	b.WriteString("Content-Disposition: inline; filename=\"logo.png\"\r\n")
	b.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")
	b.WriteString(wrapBase64(appinfo.LogoPNG()))
	b.WriteString("\r\n")
	fmt.Fprintf(&b, "--%s--\r\n", relatedBoundary)
	return []byte(b.String())
}

// wrapBase64 line-wraps a base64 encoding at 76 columns (RFC 2045).
func wrapBase64(data []byte) string {
	enc := base64.StdEncoding.EncodeToString(data)
	const width = 76
	var b strings.Builder
	for len(enc) > width {
		b.WriteString(enc[:width])
		b.WriteString("\r\n")
		enc = enc[width:]
	}
	b.WriteString(enc)
	return b.String()
}

// subjectOf builds a concise, severity-first subject line. The bracket
// prefix is the application's header1 from the web configuration (e.g.
// "[SPOK]"); the action-level subject_prefix is only a fallback when the
// application identity is not populated.
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
	if h := strings.TrimSpace(req.App.Header1); h != "" {
		return fmt.Sprintf("[%s] %s", h, base)
	}
	if cfg.SubjectPrefix != "" {
		return cfg.SubjectPrefix + " " + base
	}
	return base
}

// bodyOfPlain renders the canonical event metadata as plain text. It never
// includes raw payloads.
func bodyOfPlain(req action.ActionRequest, now time.Time) string {
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
			fmt.Fprintf(&b, "Areas: %s\n", strings.Join(geo.DisplayAreas(h.Hazard.Areas), ", "))
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
	b.WriteString("\n--\n")
	b.WriteString(footerText(req))
	return b.String()
}

// severityColor maps a canonical severity to the UI palette used by the
// web dashboard, so outbound mails match the application branding.
func severityColor(severity string) (bg, fg string) {
	switch strings.ToLower(severity) {
	case "extreme":
		return "rgba(248,81,73,0.18)", "#f85149"
	case "severe":
		return "rgba(255,123,67,0.18)", "#ff7b43"
	case "moderate":
		return "rgba(210,153,34,0.18)", "#d29922"
	case "minor":
		return "rgba(88,166,255,0.18)", "#58a6ff"
	default:
		return "rgba(139,148,158,0.18)", "#8b949e"
	}
}

// bodyOfHTML renders a styled, human-first HTML version of the event: a
// focused summary for the reader on top, the technical metadata in a table
// below, and a branded footer. The card carries a severity-colored accent
// (the same palette as the web dashboard), so the alert level is visible
// at a glance.
func bodyOfHTML(req action.ActionRequest, now time.Time) string {
	accent := "#30363d"
	sevFG := ""
	if ev := req.Event; ev.Kind == dispatch.EventHazardTransition && ev.Hazard != nil {
		_, sevFG = severityColor(ev.Hazard.Hazard.Severity)
		accent = sevFG
	}

	var b strings.Builder
	fmt.Fprintf(&b, `<div style="background:#0d1117;padding:24px;font-family:-apple-system,'Segoe UI',Roboto,Arial,sans-serif;">
<table role="presentation" width="100%%" cellpadding="0" cellspacing="0" style="max-width:640px;margin:0 auto;border:1px solid #30363d;border-top:3px solid %s;border-radius:8px;background:#161b22;color:#c9d1d9;font-size:14px;">
<tr><td style="padding:24px 28px;">`, accent)

	// Brand row: the embedded logo next to the system header (header1).
	brand := strings.TrimSpace(req.App.Header1)
	if brand == "" {
		brand = "WarnFlux"
	}
	fmt.Fprintf(&b, `<table role="presentation" cellpadding="0" cellspacing="0" style="margin-bottom:16px;"><tr>
<td style="padding-right:10px;"><img src="cid:%s" width="36" height="36" alt="%s" style="width:36px;height:36px;border-radius:10px;display:block;border:0;"></td>
<td style="vertical-align:middle;font-size:16px;font-weight:700;color:#e6edf3;letter-spacing:.02em;">%s</td>
</tr></table>`,
		logoCID, htmlEscaper(brand), htmlEscaper(brand))

	b.WriteString(`<div style="font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#8b949e;">WarnFlux hazard alert</div>`)

	ev := req.Event
	switch ev.Kind {
	case dispatch.EventHazardTransition:
		b.WriteString(hazardHTML(ev))
	case dispatch.EventMQTTMessage:
		m := ev.MQTT
		if m != nil {
			fmt.Fprintf(&b, `<div style="font-size:22px;font-weight:700;color:#e6edf3;margin-top:14px;">%s</div>`,
				htmlEscaper("MQTT message on "+m.Topic))
		}
	default:
		b.WriteString(`<div style="font-size:22px;font-weight:700;color:#e6edf3;margin-top:14px;">Dispatch event</div>`)
	}

	b.WriteString(`<div style="margin-top:24px;border-top:1px solid #30363d;padding-top:16px;">
<div style="font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#8b949e;margin-bottom:8px;">Technical details</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="font-size:13px;">`)
	b.WriteString(technicalRows(req, now, sevFG))
	b.WriteString(`</table></div>`)

	b.WriteString(`<div style="margin-top:24px;border-top:1px solid #30363d;padding-top:14px;font-size:12px;color:#8b949e;">`)
	b.WriteString(footerHTML(req))
	b.WriteString(`</div>`)

	b.WriteString(`</td></tr></table></div>`)
	return b.String()
}

// hazardHTML renders the focused, human-readable summary block.
func hazardHTML(ev dispatch.Event) string {
	h := ev.Hazard
	if h == nil {
		return `<div style="font-size:22px;font-weight:700;color:#e6edf3;margin-top:14px;">Hazard transition</div>`
	}
	var b strings.Builder
	bg, fg := severityColor(h.Hazard.Severity)
	fmt.Fprintf(&b, `<div style="margin-top:16px;"><span style="background:%s;color:%s;padding:4px 14px;border-radius:999px;font-weight:700;text-transform:uppercase;font-size:12px;letter-spacing:.05em;">%s</span> <span style="color:#8b949e;font-size:12px;margin-left:8px;">%s</span></div>`,
		bg, fg, htmlEscaper(strings.ToUpper(h.Hazard.Severity)), htmlEscaper(string(h.Type)))

	headline := strings.TrimSpace(h.Hazard.Headline)
	if headline == "" {
		headline = h.Hazard.Event
	}
	fmt.Fprintf(&b, `<div style="font-size:22px;font-weight:700;color:#e6edf3;margin-top:14px;">%s</div>`, htmlEscaper(headline))
	if headline != h.Hazard.Event {
		fmt.Fprintf(&b, `<div style="color:#8b949e;margin-top:6px;">%s</div>`, htmlEscaper(h.Hazard.Event))
	}

	if len(h.Hazard.Areas) > 0 {
		b.WriteString(`<div style="margin-top:14px;color:#8b949e;font-size:12px;text-transform:uppercase;letter-spacing:.06em;">Areas</div>`)
		var chips strings.Builder
		for _, a := range geo.DisplayAreas(h.Hazard.Areas) {
			fmt.Fprintf(&chips, `<span style="display:inline-block;background:#21262d;border:1px solid #30363d;border-radius:999px;padding:3px 12px;margin:6px 6px 0 0;font-size:13px;color:#c9d1d9;">%s</span>`, htmlEscaper(a))
		}
		b.WriteString(chips.String())
	}
	if h.Hazard.EffectiveAt != nil {
		fmt.Fprintf(&b, `<div style="margin-top:14px;color:#8b949e;font-size:13px;">Effective: <span style="color:#c9d1d9;">%s</span></div>`, h.Hazard.EffectiveAt.Format(time.RFC3339))
	}
	if h.Hazard.ExpiresAt != nil {
		fmt.Fprintf(&b, `<div style="margin-top:4px;color:#8b949e;font-size:13px;">Expires: <span style="color:#c9d1d9;">%s</span></div>`, h.Hazard.ExpiresAt.Format(time.RFC3339))
	}
	return b.String()
}

// technicalRows renders the metadata table rows for any event kind. The
// severity value is tinted with the application's severity palette.
func technicalRows(req action.ActionRequest, now time.Time, sevFG string) string {
	row := func(k, v string) string {
		if v == "" {
			return ""
		}
		return fmt.Sprintf(`<tr><td style="padding:5px 8px;color:#8b949e;white-space:nowrap;vertical-align:top;">%s</td><td style="padding:5px 8px;color:#c9d1d9;">%s</td></tr>`,
			htmlEscaper(k), htmlEscaper(v))
	}
	var b strings.Builder
	ev := req.Event
	b.WriteString(row("Time", now.Format(time.RFC3339)))
	b.WriteString(row("Kind", string(ev.Kind)))
	if ev.Origin.ReceiverID != "" {
		b.WriteString(row("Receiver", ev.Origin.ReceiverID))
	}
	switch ev.Kind {
	case dispatch.EventHazardTransition:
		h := ev.Hazard
		if h == nil {
			break
		}
		b.WriteString(row("Transition", string(h.Type)))
		b.WriteString(row("Source", h.Source))
		b.WriteString(row("Event key", h.Key))
		b.WriteString(row("Event", h.Hazard.Event))
		if sevFG != "" {
			fmt.Fprintf(&b, `<tr><td style="padding:5px 8px;color:#8b949e;white-space:nowrap;vertical-align:top;">Severity</td><td style="padding:5px 8px;"><span style="color:%s;font-weight:600;">%s</span></td></tr>`,
				sevFG, htmlEscaper(h.Hazard.Severity))
		} else {
			b.WriteString(row("Severity", h.Hazard.Severity))
		}
		b.WriteString(row("Urgency", h.Hazard.Urgency))
		b.WriteString(row("Certainty", h.Hazard.Certainty))
		b.WriteString(row("Headline", h.Hazard.Headline))
		if len(h.Hazard.Areas) > 0 {
			b.WriteString(row("Areas", strings.Join(h.Hazard.Areas, ", ")))
		}
		if h.Hazard.EffectiveAt != nil {
			b.WriteString(row("Effective", h.Hazard.EffectiveAt.Format(time.RFC3339)))
		}
		if h.Hazard.ExpiresAt != nil {
			b.WriteString(row("Expires", h.Hazard.ExpiresAt.Format(time.RFC3339)))
		}
	case dispatch.EventMQTTMessage:
		m := ev.MQTT
		if m != nil {
			b.WriteString(row("Topic", m.Topic))
			b.WriteString(row("QoS", fmt.Sprintf("%d", m.QoS)))
			b.WriteString(row("Payload bytes", fmt.Sprintf("%d", len(m.Payload))))
		}
	}
	return b.String()
}

// normalizeDomain reduces the configured public domain to a display form:
// scheme and path removed, trailing slash trimmed.
func normalizeDomain(d string) string {
	d = strings.TrimSpace(d)
	if i := strings.Index(d, "://"); i >= 0 {
		d = d[i+3:]
	}
	if i := strings.IndexByte(d, '/'); i >= 0 {
		d = d[:i]
	}
	return strings.TrimSuffix(d, "/")
}

// footerParts returns the version and domain as presentable pieces.
func footerParts(req action.ActionRequest) (version, domain string) {
	version = strings.TrimSpace(req.App.Version)
	domain = normalizeDomain(req.App.Domain)
	return version, domain
}

// brandName returns the configured header1 plus the project name (e.g.
// "SOSNA · WarnFlux") that brands the footer, falling back to the project
// name alone when header1 is not populated.
func brandName(req action.ActionRequest) string {
	h := strings.TrimSpace(req.App.Header1)
	if h == "" || h == "WarnFlux" {
		return "WarnFlux"
	}
	return h + " · WarnFlux"
}

func footerText(req action.ActionRequest) string {
	version, domain := footerParts(req)
	var b strings.Builder
	fmt.Fprintf(&b, "Sent by %s", brandName(req))
	if version != "" {
		fmt.Fprintf(&b, " v%s", version)
	}
	if domain != "" {
		fmt.Fprintf(&b, " · %s", domain)
	}
	if req.App.RepoURL != "" {
		fmt.Fprintf(&b, " · %s", req.App.RepoURL)
	}
	return b.String()
}

func footerHTML(req action.ActionRequest) string {
	version, domain := footerParts(req)
	var b strings.Builder
	b.WriteString("Sent by ")
	fmt.Fprintf(&b, "<strong style=\"color:#c9d1d9;\">%s</strong>", htmlEscaper(brandName(req)))
	if version != "" {
		fmt.Fprintf(&b, " v%s", htmlEscaper(version))
	}
	if domain != "" {
		fmt.Fprintf(&b, " · %s", htmlEscaper(domain))
	}
	if req.App.RepoURL != "" {
		fmt.Fprintf(&b, ` · <a href="%s" style="color:#58a6ff;text-decoration:none;">%s</a>`,
			htmlEscaper(req.App.RepoURL), htmlEscaper(req.App.RepoURL))
	}
	return b.String()
}

// htmlEscaper escapes a value for safe embedding in the HTML part.
func htmlEscaper(s string) string {
	return html.EscapeString(s)
}
