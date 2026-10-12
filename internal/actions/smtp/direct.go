package smtp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"mime"
	"net"
	"net/smtp"
	"os"
	"strings"
	"time"
)

// Direct sends one plain-text email to the given recipients using the
// action configuration. It is the reusable transport behind the web
// password-reset mailer (the action worker keeps its own path). The
// password file form is honoured; callers must bound the context.
func Direct(ctx context.Context, cfg Config, to []string, subject, text string) error {
	msg := plainMessage(senderAddress(cfg), to, subject, text)
	return DirectMessage(ctx, cfg, to, msg)
}

// DirectMessage sends one already-built RFC 5322 message to the given
// recipients using the action configuration. It is the shared transport
// behind the plain (password reset) and the branded (mass notice) senders:
// a fresh SMTP connection per call, no persistent state. Callers must
// bound the context.
func DirectMessage(ctx context.Context, cfg Config, to []string, msg []byte) error {
	host := strings.TrimSpace(cfg.Host)
	if host == "" {
		return fmt.Errorf("smtp: config.host is required")
	}
	port := cfg.Port
	if port == 0 {
		port = 587
	}
	from := senderAddress(cfg)
	password := cfg.Password
	if cfg.PasswordFile != "" {
		data, err := os.ReadFile(cfg.PasswordFile)
		if err != nil {
			return fmt.Errorf("smtp: read password file: %w", err)
		}
		password = strings.TrimRight(string(data), "\r\n")
	}
	if len(to) == 0 {
		return fmt.Errorf("smtp: no recipients")
	}

	tlsConfig := func() (*tls.Config, error) {
		tc := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
		if ca := strings.TrimSpace(cfg.CAFile); ca != "" {
			pem, err := os.ReadFile(ca)
			if err != nil {
				return nil, fmt.Errorf("smtp: read ca file: %w", err)
			}
			roots, err := x509.SystemCertPool()
			if err != nil || roots == nil {
				roots = x509.NewCertPool()
			}
			if !roots.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("smtp: ca file contains no certificates")
			}
			tc.RootCAs = roots
		}
		return tc, nil
	}

	// One send is bounded as a whole: the caller's deadline when it has
	// one, defaultDeadline only when it does not (the action worker path
	// applies the same rule). Capping a caller's longer budget at the
	// default would cut a long recipient list in half.
	deadline := time.Now().Add(defaultDeadline)
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	}

	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	dialer := &net.Dialer{Timeout: 15 * time.Second, Deadline: deadline}

	// One slot per recipient from the budget shared with the action
	// instance: the provider counts routed notifications and one-off
	// notices together. The first slot is claimed BEFORE the connection,
	// so a paced send never holds an idle SMTP session open while it
	// waits for its turn.
	limiter := sharedLimiter(cfg)
	if err := limiter.wait(ctx); err != nil {
		return fmt.Errorf("smtp: rate limit: %w", err)
	}

	var conn net.Conn
	var err error
	implicit := cfg.ImplicitTLS != nil && *cfg.ImplicitTLS
	if implicit {
		tc, tErr := tlsConfig()
		if tErr != nil {
			return tErr
		}
		// DialContext covers the handshake too (HandshakeContext under
		// the hood). tls.DialWithDialer never saw the context, so a relay
		// that accepted the connection and then stayed silent during TLS
		// held the send until the deadline — password resets included
		// (reported P2).
		conn, err = (&tls.Dialer{NetDialer: dialer, Config: tc}).DialContext(ctx, "tcp", addr)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("smtp: dial %s: %w", addr, err)
	}
	defer conn.Close()

	// The deadline is armed BEFORE the greeting is read. net/smtp's
	// NewClient waits for the server's banner, so a server that accepts
	// the connection and then says nothing blocked this call for good —
	// the deadline used to be set only after the handshake, and the
	// context was never consulted at all (reported P2: password resets
	// and every send on this transport could hang forever).
	_ = conn.SetDeadline(deadline)

	// Cancelling the context closes the connection, which unblocks any
	// protocol operation already in flight instead of waiting for the
	// deadline.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	client, err := smtp.NewClient(conn, host)
	if err != nil {
		return fmt.Errorf("smtp: hello: %w", err)
	}
	defer client.Close()

	if !implicit {
		startTLS := cfg.StartTLS == nil || *cfg.StartTLS
		if startTLS {
			if ok, _ := client.Extension("STARTTLS"); ok {
				tc, tErr := tlsConfig()
				if tErr != nil {
					return tErr
				}
				if err := client.StartTLS(tc); err != nil {
					return fmt.Errorf("smtp: starttls: %w", err)
				}
			}
		}
	}
	if strings.TrimSpace(cfg.Username) != "" {
		if err := client.Auth(smtp.PlainAuth("", cfg.Username, password, host)); err != nil {
			return fmt.Errorf("smtp: auth: %w", err)
		}
	}

	for i, rcpt := range to {
		if i > 0 {
			if err := limiter.wait(ctx); err != nil {
				return fmt.Errorf("smtp: rate limit: %w", err)
			}
		}
		if err := client.Mail(from); err != nil {
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
			w.Close()
			return fmt.Errorf("smtp: write message: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("smtp: end message: %w", err)
		}
	}
	return client.Quit()
}

// senderAddress resolves the envelope/header From address from the config,
// falling back to noreply@<host>.
func senderAddress(cfg Config) string {
	from := strings.TrimSpace(cfg.From)
	if from == "" {
		from = "noreply@" + strings.TrimSpace(cfg.Host)
	}
	return from
}

// plainMessage builds a single-part text/plain RFC 5322 message.
func plainMessage(from string, to []string, subject, text string) []byte {
	return []byte("From: " + from + "\r\n" +
		"To: " + strings.Join(to, ", ") + "\r\n" +
		"Subject: " + mime.QEncoding.Encode("utf-8", subject) + "\r\n" +
		"MIME-Version: 1.0\r\n" +
		"Content-Type: text/plain; charset=utf-8\r\n" +
		"\r\n" + text + "\r\n")
}
