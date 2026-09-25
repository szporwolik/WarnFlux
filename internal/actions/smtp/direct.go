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
	host := strings.TrimSpace(cfg.Host)
	if host == "" {
		return fmt.Errorf("smtp: config.host is required")
	}
	port := cfg.Port
	if port == 0 {
		port = 587
	}
	from := strings.TrimSpace(cfg.From)
	if from == "" {
		from = "noreply@" + host
	}
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

	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	dialer := &net.Dialer{Timeout: 15 * time.Second}

	var conn net.Conn
	var err error
	implicit := cfg.ImplicitTLS != nil && *cfg.ImplicitTLS
	if implicit {
		tc, tErr := tlsConfig()
		if tErr != nil {
			return tErr
		}
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, tc)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("smtp: dial %s: %w", addr, err)
	}

	client, err := smtp.NewClient(conn, host)
	if err != nil {
		conn.Close()
		return fmt.Errorf("smtp: hello: %w", err)
	}
	defer client.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

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

	msg := "From: " + from + "\r\n" +
		"To: " + strings.Join(to, ", ") + "\r\n" +
		"Subject: " + mime.QEncoding.Encode("utf-8", subject) + "\r\n" +
		"MIME-Version: 1.0\r\n" +
		"Content-Type: text/plain; charset=utf-8\r\n" +
		"\r\n" + text + "\r\n"

	for _, rcpt := range to {
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
		if _, err := w.Write([]byte(msg)); err != nil {
			w.Close()
			return fmt.Errorf("smtp: write message: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("smtp: end message: %w", err)
		}
	}
	return client.Quit()
}
