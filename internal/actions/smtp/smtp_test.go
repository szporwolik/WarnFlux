package smtp_test

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/actions/smtp"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
)

// received is one accepted mail transaction.
type received struct {
	from     string
	rcpts    []string
	authUser string
	authPass string
	data     string
	at       time.Time
}

// smtpServer is a minimal SMTP server good enough for the net/smtp client:
// EHLO, optional STARTTLS, AUTH PLAIN, MAIL, RCPT, DATA, QUIT.
type smtpServer struct {
	mu       sync.Mutex
	received []received
	tlsCfg   *tls.Config
	listener net.Listener
	done     chan struct{}
	// implicitTLS wraps the connection in TLS before the SMTP greeting
	// (SMTPS on port 465 style).
	implicitTLS bool
	// refuseRecipient makes the server reject that one envelope recipient
	// (a mailbox the provider refuses), so a batch can fail part-way.
	refuseRecipient string
}

func (s *smtpServer) addr() string { return s.listener.Addr().String() }

func (s *smtpServer) snapshot() []received {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]received(nil), s.received...)
}

func (s *smtpServer) close() {
	s.listener.Close()
	<-s.done
}

func newSMTPServer(t *testing.T, tlsCfg *tls.Config) *smtpServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &smtpServer{listener: ln, tlsCfg: tlsCfg, done: make(chan struct{})}
	go s.acceptLoop()
	t.Cleanup(s.close)
	return s
}

// newImplicitSMTPServer is an SMTPS-style server: TLS from the first byte.
// implicitTLS is set BEFORE the accept loop starts: serve() reads it from
// the goroutine, so writing it after newSMTPServer races (race detector).
func newImplicitSMTPServer(t *testing.T, tlsCfg *tls.Config) *smtpServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &smtpServer{listener: ln, tlsCfg: tlsCfg, done: make(chan struct{}), implicitTLS: true}
	go s.acceptLoop()
	t.Cleanup(s.close)
	return s
}

func (s *smtpServer) acceptLoop() {
	defer close(s.done)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.serve(conn)
	}
}

func (s *smtpServer) serve(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if s.implicitTLS {
		tlsConn := tls.Server(conn, s.tlsCfg)
		if err := tlsConn.Handshake(); err != nil {
			return
		}
		conn = tlsConn
	}
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	write := func(line string) error {
		if _, err := w.WriteString(line + "\r\n"); err != nil {
			return err
		}
		return w.Flush()
	}
	if err := write("220 smtp.test ESMTP"); err != nil {
		return
	}

	var cur *received
	var authUser, authPass string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		verb, rest, _ := strings.Cut(line, " ")
		verb = strings.ToUpper(verb)
		switch verb {
		case "EHLO", "HELO":
			if err := write("250-smtp.test\r\n250 AUTH PLAIN STARTTLS"); err != nil {
				return
			}
		case "STARTTLS":
			if s.tlsCfg == nil {
				write("454 TLS not available")
				return
			}
			if err := write("220 ready"); err != nil {
				return
			}
			tlsConn := tls.Server(conn, s.tlsCfg)
			if err := tlsConn.Handshake(); err != nil {
				return
			}
			conn = tlsConn
			r = bufio.NewReader(conn)
			w = bufio.NewWriter(conn)
		case "AUTH":
			parts := strings.Split(rest, " ")
			if len(parts) != 2 || strings.ToUpper(parts[0]) != "PLAIN" {
				write("504 unsupported mechanism")
				return
			}
			raw, err := base64.StdEncoding.DecodeString(parts[1])
			if err != nil {
				write("501 bad auth")
				return
			}
			fields := strings.Split(string(raw), "\x00")
			if len(fields) != 3 {
				write("501 bad auth")
				return
			}
			authUser, authPass = fields[1], fields[2]
			if err := write("235 ok"); err != nil {
				return
			}
		case "MAIL":
			if err := write("250 ok"); err != nil {
				return
			}
			cur = &received{}
			if i := strings.Index(rest, "<"); i >= 0 {
				cur.from = strings.Trim(rest[i:], "<> ")
			}
		case "RCPT":
			addr := ""
			if i := strings.Index(rest, "<"); i >= 0 {
				addr = strings.Trim(rest[i:], "<> ")
			}
			if s.refuseRecipient != "" && strings.EqualFold(addr, s.refuseRecipient) {
				if err := write("550 mailbox unavailable"); err != nil {
					return
				}
				continue
			}
			if err := write("250 ok"); err != nil {
				return
			}
			if cur != nil && addr != "" {
				cur.rcpts = append(cur.rcpts, addr)
			}
		case "DATA":
			if err := write("354 go ahead"); err != nil {
				return
			}
			var body strings.Builder
			for {
				l, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if l == ".\r\n" || l == ".\n" {
					break
				}
				body.WriteString(strings.TrimRight(l, "\r\n"))
				body.WriteString("\n")
			}
			if cur != nil {
				cur.data = body.String()
				cur.authUser, cur.authPass = authUser, authPass
				cur.at = time.Now()
				s.mu.Lock()
				s.received = append(s.received, *cur)
				s.mu.Unlock()
			}
			cur = nil
			if err := write("250 accepted"); err != nil {
				return
			}
		case "QUIT":
			write("221 bye")
			return
		default:
			if err := write("250 ok"); err != nil {
				return
			}
		}
	}
}

// selfSignedCert generates an in-memory certificate for 127.0.0.1 and
// returns its PEM form (used as config.ca_file in tests).
func selfSignedCert(t *testing.T) (tlsConfig *tls.Config, pemBytes []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	cert, err := tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: mustMarshalEC(t, key)}))
	if err != nil {
		t.Fatalf("keypair: %v", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}, certPEM
}

func mustMarshalEC(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return der
}

func cfgNode(t *testing.T, yamlText string) *yaml.Node {
	t.Helper()
	var n yaml.Node
	if err := yaml.Unmarshal([]byte(yamlText), &n); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	return &n
}

func hazardReq() action.ActionRequest {
	return action.ActionRequest{
		ID:        "r1",
		CreatedAt: time.Now(),
		Event: dispatch.Event{
			Kind:   dispatch.EventHazardTransition,
			Origin: dispatch.Origin{Type: "mqtt", ReceiverID: "local"},
			Hazard: &dispatch.HazardTransition{
				Type:   dispatch.TransitionNew,
				Key:    "imgw-meteo:123",
				Source: "imgw-meteo",
				Hazard: dispatch.Hazard{
					EventKey: "imgw-meteo:123",
					Event:    "Burza",
					Severity: "severe",
					Headline: "Silny wiatr",
					Areas:    []string{"małopolskie"},
				},
			},
		},
	}
}

func TestFactoryValidation(t *testing.T) {
	cases := []struct {
		name string
		cfg  string
	}{
		{"missing host", "from: a@b.c\nto: [x@y.z]"},
		{"missing from", "host: 127.0.0.1\nto: [x@y.z]"},
		{"missing to", "host: 127.0.0.1\nfrom: a@b.c"},
		{"empty recipient", "host: 127.0.0.1\nfrom: a@b.c\nto: [\"\"]"},
		{"password conflict", "host: 127.0.0.1\nfrom: a@b.c\nto: [x@y.z]\npassword: p\npassword_file: /nonexistent"},
		{"missing password file", "host: 127.0.0.1\nfrom: a@b.c\nto: [x@y.z]\npassword_file: /nonexistent"},
	}
	for _, tc := range cases {
		if _, err := smtp.New(cfgNode(t, tc.cfg)); err == nil {
			t.Errorf("%s: want factory error", tc.name)
		}
	}
}

func TestSendPlainWithAuth(t *testing.T) {
	srv := newSMTPServer(t, nil)
	_, port, _ := net.SplitHostPort(srv.addr())

	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: 127.0.0.1
port: %s
from: warnflux@example.com
to: [ops@example.com, duty@example.com]
username: warnflux
password: secret-pw
starttls: false
rate_limit_per_minute: -1
`, port)))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := hazardReq()
	req.App = action.AppInfo{
		Version: "0.1.0",
		Header1: "SPOK",
		Domain:  "https://spok.sp9moa.pl",
		RepoURL: "https://github.com/szporwolik/WarnFlux",
	}
	if err := p.Execute(ctx, req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// One SMTP transaction per recipient.
	got := srv.snapshot()
	if len(got) != 2 {
		t.Fatalf("received %d mails, want 2 (one per recipient)", len(got))
	}
	m := got[0]
	if m.from != "warnflux@example.com" {
		t.Errorf("from = %q", m.from)
	}
	if len(m.rcpts) != 1 || m.rcpts[0] != "ops@example.com" {
		t.Errorf("first rcpts = %v, want [ops@example.com]", m.rcpts)
	}
	if len(got[1].rcpts) != 1 || got[1].rcpts[0] != "duty@example.com" {
		t.Errorf("second rcpts = %v, want [duty@example.com]", got[1].rcpts)
	}
	if m.authUser != "warnflux" || m.authPass != "secret-pw" {
		t.Errorf("auth = %q/%q", m.authUser, m.authPass)
	}
	for _, want := range []string{
		// The subject carries [Header1] from the application identity.
		"Subject: [SPOK] SEVERE: Silny wiatr",
		"WarnFlux notification",
		"Message: SEVERE · Burza",
		"Headline: Silny wiatr",
		"Message ID: WF-",
		"source: imgw-meteo",
		"areas: małopolskie",
		// The source (imgw) ships no instruction, so the mail carries no
		// instruction section at all — nothing is invented for it.
		// Multipart/alternative with the human-first HTML part.
		"Content-Type: multipart/related",
		"Content-Type: multipart/alternative",
		"Content-Type: text/html; charset=utf-8",
		"Message notification",
		"View details",
		// Embedded logo: inline CID image referenced from the HTML brand row.
		"Content-Type: image/png; name=\"logo.png\"",
		"Content-ID: <warnflux-logo>",
		"cid:warnflux-logo",
		">SPOK</td>",
		// The call-to-action renders as a solid button in every client:
		// a bgcolor table cell plus background-color on the anchor, white
		// text, no underline.
		">View details</a>",
		`bgcolor="#1f6feb"`,
		"background-color:#1f6feb",
		"color:#ffffff",
		// The severity is colored with the application palette: a solid
		// card accent strip and a solid severity chip (severe -> orange),
		// with the short message ID chip right next to it.
		"background-color:#f0784e;height:4px",
		"background-color:#f0784e;color:#0f1419",
		">SEVERE<",
		">WF-",
		// Branded footer: version and normalized domain. The repository
		// link must not appear in outbound mails.
		"Sent by <strong style=\"color:#eef2f5;\">SPOK · WarnFlux</strong> v0.1.0",
		"spok.sp9moa.pl",
	} {
		if !strings.Contains(m.data, want) {
			t.Errorf("mail body missing %q:\n%s", want, m.data)
		}
	}
	if strings.Contains(m.data, "github.com") {
		t.Errorf("mail body must not link the repository:\n%s", m.data)
	}

	// A non-ASCII subject must be MIME word-encoded (UTF-8 bytes).
	req2 := hazardReq()
	req2.App = req.App
	req2.Event.Hazard.Hazard.Headline = "Intensywne opady śniegu"
	if err := p.Execute(ctx, req2); err != nil {
		t.Fatalf("Execute non-ASCII: %v", err)
	}
	mails := srv.snapshot()
	if len(mails) != 4 {
		t.Fatalf("received %d mails after two events, want 4 (2 recipients each)", len(mails))
	}
	if !strings.Contains(mails[3].data, "Subject: =?utf-8?q?") || !strings.Contains(mails[3].data, "=C5=9B") {
		t.Errorf("non-ASCII subject not q-encoded:\n%s", mails[3].data)
	}
}

func TestSendImplicitTLSWithCAFile(t *testing.T) {
	tlsCfg, certPEM := selfSignedCert(t)
	srv := newImplicitSMTPServer(t, tlsCfg)
	_, port, _ := net.SplitHostPort(srv.addr())

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: 127.0.0.1
port: %s
from: warnflux@example.com
to: [ops@example.com]
implicit_tls: true
ca_file: %s
`, port, caPath)))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Execute(ctx, hazardReq()); err != nil {
		t.Fatalf("Execute over SMTPS: %v", err)
	}

	got := srv.snapshot()
	if len(got) != 1 {
		t.Fatalf("received %d mails, want 1", len(got))
	}
	if !strings.Contains(got[0].data, "Message: SEVERE · Burza") {
		t.Errorf("mail body:\n%s", got[0].data)
	}
}

func TestSendStartTLSWithCAFile(t *testing.T) {
	tlsCfg, certPEM := selfSignedCert(t)
	srv := newSMTPServer(t, tlsCfg)
	_, port, _ := net.SplitHostPort(srv.addr())

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: 127.0.0.1
port: %s
from: warnflux@example.com
to: [ops@example.com]
starttls: true
ca_file: %s
`, port, caPath)))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Execute(ctx, hazardReq()); err != nil {
		t.Fatalf("Execute over STARTTLS: %v", err)
	}

	got := srv.snapshot()
	if len(got) != 1 {
		t.Fatalf("received %d mails, want 1", len(got))
	}
	if !strings.Contains(got[0].data, "Message: SEVERE · Burza") {
		t.Errorf("mail body:\n%s", got[0].data)
	}
}

func TestExecuteGenericMQTTMessage(t *testing.T) {
	srv := newSMTPServer(t, nil)
	_, port, _ := net.SplitHostPort(srv.addr())

	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: 127.0.0.1
port: %s
from: a@b.c
to: [x@y.z]
starttls: false
`, port)))
	if err != nil {
		t.Fatal(err)
	}

	req := action.ActionRequest{
		ID: "r2",
		Event: dispatch.Event{
			Kind:   dispatch.EventMQTTMessage,
			Origin: dispatch.Origin{Type: "mqtt", ReceiverID: "remote"},
			MQTT:   &dispatch.MQTTMessage{Topic: "club/alarm", QoS: 1, Payload: []byte{1, 2, 3}},
		},
	}
	if err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	got := srv.snapshot()
	if len(got) != 1 {
		t.Fatalf("received %d mails, want 1", len(got))
	}
	for _, want := range []string{"MQTT message on club/alarm", "Topic: club/alarm", "Payload bytes: 3"} {
		if !strings.Contains(got[0].data, want) {
			t.Errorf("mail missing %q:\n%s", want, got[0].data)
		}
	}
}

func TestPasswordFile(t *testing.T) {
	srv := newSMTPServer(t, nil)
	_, port, _ := net.SplitHostPort(srv.addr())

	pwPath := filepath.Join(t.TempDir(), "pw.txt")
	if err := os.WriteFile(pwPath, []byte("file-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: 127.0.0.1
port: %s
from: a@b.c
to: [x@y.z]
username: u
password_file: %s
starttls: false
`, port, pwPath)))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	if err := p.Execute(context.Background(), hazardReq()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	got := srv.snapshot()
	if len(got) != 1 || got[0].authPass != "file-secret" {
		t.Fatalf("auth pass = %+v, want file-secret", got)
	}
}

func TestBccRecipients(t *testing.T) {
	srv := newSMTPServer(t, nil)
	_, port, _ := net.SplitHostPort(srv.addr())

	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: 127.0.0.1
port: %s
from: warnflux@example.com
to: [visible@example.com]
starttls: false
rate_limit_per_minute: -1
`, port)))
	if err != nil {
		t.Fatal(err)
	}

	req := hazardReq()
	req.Bcc = []string{"member1@example.com", "member2@example.com", "visible@example.com"}
	if err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	got := srv.snapshot()
	if len(got) != 3 {
		t.Fatalf("received %d mails, want 3 (visible + 2 members, deduped)", len(got))
	}
	wantEnvelope := []string{"visible@example.com", "member1@example.com", "member2@example.com"}
	for i, m := range got {
		if len(m.rcpts) != 1 || m.rcpts[0] != wantEnvelope[i] {
			t.Errorf("mail %d envelope = %v, want [%s]", i, m.rcpts, wantEnvelope[i])
		}
		if !strings.Contains(m.data, "To: visible@example.com") {
			t.Errorf("mail %d missing visible To header:\n%s", i, m.data)
		}
		if strings.Contains(m.data, "Bcc:") || strings.Contains(m.data, "member1@example.com") && !strings.Contains(m.data, "To:") {
			t.Errorf("mail %d leaks Bcc addresses:\n%s", i, m.data)
		}
	}
}

func TestBccEmptyRecipientsIsAnError(t *testing.T) {
	// An empty config.to list is rejected at construction; the runtime
	// guard (config.to + request.bcc both empty) can therefore only fire
	// on internal misuse and is exercised via validation here.
	if _, err := smtp.New(cfgNode(t, "host: 127.0.0.1\nfrom: a@b.c\nto: []\n")); err == nil {
		t.Fatal("factory must reject an empty recipient list")
	}
}

func TestRateLimiterPacesSends(t *testing.T) {
	srv := newSMTPServer(t, nil)
	_, port, _ := net.SplitHostPort(srv.addr())

	// 600/min -> ~100ms between emails; the two config.to recipients are
	// sent as two transactions and must be spaced accordingly.
	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: 127.0.0.1
port: %s
from: a@b.c
to: [x@y.z, y@y.z]
starttls: false
rate_limit_per_minute: 600
`, port)))
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := p.Execute(context.Background(), hazardReq()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 90*time.Millisecond {
		t.Errorf("two paced sends took %v, want >= ~100ms total", elapsed)
	}

	got := srv.snapshot()
	if len(got) != 2 {
		t.Fatalf("received %d mails, want 2", len(got))
	}
	if gap := got[1].at.Sub(got[0].at); gap < 80*time.Millisecond {
		t.Errorf("send spacing = %v, want >= ~100ms", gap)
	}
}

func TestRateLimitDisabledByNegative(t *testing.T) {
	srv := newSMTPServer(t, nil)
	_, port, _ := net.SplitHostPort(srv.addr())
	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: 127.0.0.1
port: %s
from: a@b.c
to: [x@y.z, y@y.z]
starttls: false
rate_limit_per_minute: -1
`, port)))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Execute(context.Background(), hazardReq()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := srv.snapshot(); len(got) != 2 {
		t.Fatalf("received %d mails, want 2", len(got))
	}
}

func TestRateLimitDisabledByDefaultConfigZero(t *testing.T) {
	// 0 in the config falls back to the protective default (30/min), so
	// a misconfigured instance can never flood a provider.
	srv := newSMTPServer(t, nil)
	_, port, _ := net.SplitHostPort(srv.addr())
	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: 127.0.0.1
port: %s
from: a@b.c
to: [x@y.z]
starttls: false
rate_limit_per_minute: 0
`, port)))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Execute(context.Background(), hazardReq()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := srv.snapshot(); len(got) != 1 {
		t.Fatalf("received %d mails, want 1", len(got))
	}
}

// TestSendNoticeIsBranded pins the Mass info broadcast mail: a one-off
// notice must render in the same branded shell as routed notifications
// (accent bar, embedded logo, system header, footer) with a plain-text
// alternative and an HTML-escaped body.
func TestSendNoticeIsBranded(t *testing.T) {
	srv := newSMTPServer(t, nil)
	_, port, _ := net.SplitHostPort(srv.addr())

	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: 127.0.0.1
port: %s
from: warnflux@example.com
to: [ops@example.com]
starttls: false
rate_limit_per_minute: -1
`, port)))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mailer, ok := p.(smtp.DirectMailer)
	if !ok {
		t.Fatal("smtp plugin does not implement DirectMailer")
	}
	app := action.AppInfo{
		Version: "0.7.1",
		Header1: "SPOK",
		Domain:  "https://spok.sp9moa.pl",
		RepoURL: "https://github.com/szporwolik/WarnFlux",
	}
	// A non-ASCII subject must be MIME word-encoded; the body must be
	// HTML-escaped in the HTML part.
	if err := mailer.SendNotice(ctx, []string{"alice@example.com"},
		"[SPOK] Wysyłka zbiorcza", "Zbiórka o 18:00 <ważne>", app, "en"); err != nil {
		t.Fatalf("SendNotice: %v", err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := srv.snapshot()
	if len(got) != 1 {
		t.Fatalf("mails = %d, want 1", len(got))
	}
	m := got[0]
	if len(m.rcpts) != 1 || m.rcpts[0] != "alice@example.com" {
		t.Errorf("rcpts = %v, want [alice@example.com]", m.rcpts)
	}
	for _, want := range []string{
		"To: alice@example.com",
		"Subject: =?utf-8?", // non-ASCII subject is word-encoded
		// Same branded shell as a routed notification.
		"Content-Type: multipart/related",
		"Content-Type: multipart/alternative",
		"Content-Type: text/html; charset=utf-8",
		"Content-Type: image/png; name=\"logo.png\"",
		"Content-ID: <warnflux-logo>",
		"cid:warnflux-logo",
		">SPOK</td>",
		// Notice label, escaped body, timestamp.
		"Mass info",
		"Zbiórka o 18:00 &lt;ważne&gt;",
		"Updated:",
		// Branded footer with version and normalized domain; no repo link.
		"Sent by <strong style=\"color:#eef2f5;\">SPOK · WarnFlux</strong> v0.7.1",
		"spok.sp9moa.pl",
	} {
		if !strings.Contains(m.data, want) {
			t.Errorf("notice mail missing %q:\n%s", want, m.data)
		}
	}
	if strings.Contains(m.data, "github.com") {
		t.Errorf("notice mail must not link the repository:\n%s", m.data)
	}
}

// TestSendNoticeMarkdownRendersBody pins the long-broadcast mail: the
// markdown subset a long Mass info notice is composed in must reach the
// reader as formatting — headings, lists, emphasis, code and links in the
// HTML part, flattened text in the plain-text alternative — while anything
// that only looks like markup stays escaped.
func TestSendNoticeMarkdownRendersBody(t *testing.T) {
	srv := newSMTPServer(t, nil)
	_, port, _ := net.SplitHostPort(srv.addr())

	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: 127.0.0.1
port: %s
from: warnflux@example.com
to: [ops@example.com]
starttls: false
rate_limit_per_minute: -1
`, port)))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mailer, ok := p.(smtp.DirectMailer)
	if !ok {
		t.Fatal("smtp plugin does not implement DirectMailer")
	}
	app := action.AppInfo{Version: "0.7.7", Header1: "SPOK", Domain: "spok.sp9moa.pl"}
	body := "# Radar\n**Pogrubienie** i `kod`.\n\n- jeden\n- dwa\n\nZobacz [mapę](https://sosna.sp9moa.pl) <script>alert(1)</script>"
	if err := mailer.SendNoticeMarkdown(ctx, []string{"alice@example.com"}, "[SPOK] Wysyłka zbiorcza", body, app, "en"); err != nil {
		t.Fatalf("SendNoticeMarkdown: %v", err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := srv.snapshot()
	if len(got) != 1 {
		t.Fatalf("mails = %d, want 1", len(got))
	}
	mail := got[0].data
	html := htmlPartOf(t, mail)
	for _, want := range []string{
		`<h4 style=`, "Radar</h4>",
		"<strong>Pogrubienie</strong>",
		`<code style=`, "kod</code>",
		`<ul style=`, `<li style="margin:4px 0;">jeden</li>`,
		`<a style="color:#7ab8ff;" href="https://sosna.sp9moa.pl"`,
		// Raw markup in the message never becomes markup.
		"&lt;script&gt;alert(1)&lt;/script&gt;",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("markdown HTML part missing %q:\n%s", want, html)
		}
	}
	if strings.Contains(html, "<script>") {
		t.Errorf("markdown HTML part leaked a script tag:\n%s", html)
	}
	// The plain-text alternative is flattened: no markers, the link keeps
	// its label and URL.
	plain := plainPartOf(t, mail)
	for _, want := range []string{"Radar", "Pogrubienie i kod.", "- jeden\n- dwa", "mapę (https://sosna.sp9moa.pl)"} {
		if !strings.Contains(plain, want) {
			t.Errorf("plain part missing %q:\n%s", want, plain)
		}
	}
	for _, bad := range []string{"**", "`", "](https://"} {
		if strings.Contains(plain, bad) {
			t.Errorf("plain part kept markdown %q:\n%s", bad, plain)
		}
	}
}

// htmlPartOf returns the text/html body of one captured mail.
func htmlPartOf(t *testing.T, mail string) string {
	t.Helper()
	return mimePartOf(t, mail, "Content-Type: text/html")
}

// plainPartOf returns the text/plain body of one captured mail.
func plainPartOf(t *testing.T, mail string) string {
	t.Helper()
	return mimePartOf(t, mail, "Content-Type: text/plain")
}

// mimePartOf slices one MIME part out of a captured mail: from its
// Content-Type header to the next boundary (whatever the test server did
// to the line endings).
func mimePartOf(t *testing.T, mail, contentType string) string {
	t.Helper()
	i := strings.Index(mail, contentType)
	if i < 0 {
		t.Fatalf("no %q part in:\n%s", contentType, mail)
	}
	rest := mail[i:]
	if j := strings.Index(rest, "\r\n--"); j >= 0 {
		return rest[:j]
	}
	if j := strings.Index(rest, "\n--"); j >= 0 {
		return rest[:j]
	}
	return rest
}

// shortenDefaultDeadline makes the "no caller budget" fallback short
// enough to observe in a test.
func shortenDefaultDeadline(t *testing.T, d time.Duration) {
	t.Helper()
	prev := smtp.DefaultDeadlineForTest()
	smtp.SetDefaultDeadlineForTest(d)
	t.Cleanup(func() { smtp.SetDefaultDeadlineForTest(prev) })
}

// TestBatchUsesTheCallersBudget pins the reported P2: the 30 s default
// acted as a hard cap, so a one-minute budget with a 20/minute rate limit
// delivered ten of twelve mails and the retry started over. The caller's
// deadline now decides.
func TestBatchUsesTheCallersBudget(t *testing.T) {
	shortenDefaultDeadline(t, 50*time.Millisecond)
	srv := newSMTPServer(t, nil)
	_, port, _ := net.SplitHostPort(srv.addr())

	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: 127.0.0.1
port: %s
from: warnflux@example.com
to: [visible@example.com]
starttls: false
rate_limit_per_minute: 6000
`, port)))
	if err != nil {
		t.Fatal(err)
	}
	req := hazardReq()
	for i := 0; i < 19; i++ {
		req.Bcc = append(req.Bcc, fmt.Sprintf("member%02d@example.com", i))
	}
	// 20 mails at 10 ms spacing need ~200 ms: three times the default the
	// old code capped the batch at, and inside the caller's budget.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := p.Execute(ctx, req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	got := srv.snapshot()
	if len(got) != 20 {
		t.Fatalf("delivered %d mails, want 20 (the caller's budget was cut short)", len(got))
	}
}

// TestPartialBatchIsNarrowedForTheRetry pins the second half: a batch cut
// short reports the addresses that DID go out (with THAT attempt's
// result), so the durable retry carries only the rest instead of starting
// from the beginning of the list.
func TestPartialBatchIsNarrowedForTheRetry(t *testing.T) {
	shortenDefaultDeadline(t, 50*time.Millisecond)
	srv := newSMTPServer(t, nil)
	_, port, _ := net.SplitHostPort(srv.addr())

	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: 127.0.0.1
port: %s
from: warnflux@example.com
to: [visible@example.com]
starttls: false
rate_limit_per_minute: 6000
`, port)))
	if err != nil {
		t.Fatal(err)
	}

	// The visible recipient plus ten members; the caller's budget only
	// covers a few of them.
	req := hazardReq()
	for i := 0; i < 10; i++ {
		req.Bcc = append(req.Bcc, fmt.Sprintf("member%02d@example.com", i))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Millisecond)
	defer cancel()
	err = p.Execute(ctx, req)
	if err == nil {
		t.Fatal("a batch cut by the deadline reported success")
	}
	var partial action.PartialDelivery
	if !errors.As(err, &partial) {
		t.Fatalf("the partial batch reported no progress: %v", err)
	}
	narrowed := req
	narrowed.DoneRecipients = append(append([]string(nil), req.DoneRecipients...), partial.Delivered()...)
	first := srv.snapshot()
	want := len(req.Bcc) + 1 // the visible recipient plus the members
	if len(first) == 0 || len(first) >= want {
		t.Fatalf("the first attempt delivered %d of %d mails", len(first), want)
	}
	if len(partial.Delivered()) == 0 {
		t.Fatal("a cut batch reported no delivered address")
	}

	// The retry sends the rest — and only the rest. The worker gives it a
	// fresh budget, which is what bounds it (the default only fills in for
	// a caller without one).
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer retryCancel()
	if err := p.Execute(retryCtx, narrowed); err != nil {
		t.Fatalf("retry Execute: %v", err)
	}
	counts := map[string]int{}
	for _, m := range srv.snapshot() {
		for _, rcpt := range m.rcpts {
			counts[rcpt]++
		}
	}
	// Whoever was REPORTED as delivered is never mailed again. Where the
	// deadline fell inside the last transaction the server may have
	// accepted it without the client seeing the confirmation, so the
	// remaining addresses are asserted as "reached", not as "exactly once".
	delivered := map[string]bool{}
	for _, addr := range partial.Delivered() {
		delivered[addr] = true
		if counts[addr] != 1 {
			t.Errorf("%s received %d mails although it was reported delivered", addr, counts[addr])
		}
	}
	for i := 0; i < 10; i++ {
		addr := fmt.Sprintf("member%02d@example.com", i)
		if !delivered[addr] && counts[addr] < 1 {
			t.Errorf("%s never went out", addr)
		}
	}
	if !delivered["visible@example.com"] && counts["visible@example.com"] < 1 {
		t.Error("visible@example.com never went out")
	}

	// Nothing left to send: a narrowed request whose addresses are all
	// delivered is a successful no-op, not a "no recipients" error.
	all := narrowed
	all.DoneRecipients = append(append([]string(nil), narrowed.DoneRecipients...),
		append([]string{"visible@example.com"}, req.Bcc...)...)
	if err := p.Execute(retryCtx, all); err != nil {
		t.Fatalf("retry with nothing left: %v", err)
	}
	if got := len(srv.snapshot()); got != len(counts) && got < want {
		t.Fatalf("the no-op retry delivered %d mails, want at least %d in total", got, want)
	}
}

// TestANewAttemptReportsOnlyItsOwnProgress pins the reported P1 on the
// action: two versions of the same alarm share their request identity, so
// progress must belong to the attempt that produced it. An attempt that
// delivered NOTHING (here: the server is gone, so the connect fails) must
// not report the addresses an earlier attempt delivered — otherwise the
// new version's retry would skip recipients it never reached.
func TestANewAttemptReportsOnlyItsOwnProgress(t *testing.T) {
	srv := newSMTPServer(t, nil)
	_, port, _ := net.SplitHostPort(srv.addr())
	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: 127.0.0.1
port: %s
from: warnflux@example.com
to: [visible@example.com]
starttls: false
rate_limit_per_minute: -1
`, port)))
	if err != nil {
		t.Fatal(err)
	}

	// Version 1: the mailbox of the second member is refused, so the first
	// one is delivered and the attempt reports exactly that.
	req := hazardReq()
	req.Bcc = []string{"sent@example.com", "broken@example.com"}
	srv.refuseRecipient = "broken@example.com"
	errV1 := p.Execute(context.Background(), req)
	var partial action.PartialDelivery
	if !errors.As(errV1, &partial) {
		t.Fatalf("version 1 reported no progress: %v", errV1)
	}
	// The configured recipient comes first, then the member; the refused
	// mailbox contributed nothing.
	want := []string{"visible@example.com", "sent@example.com"}
	if got := partial.Delivered(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("version 1 progress = %v, want %v", got, want)
	}

	// Version 2 of the SAME alarm (same request identity): nothing leaves
	// the station, because the server is gone.
	srv.close()
	errV2 := p.Execute(context.Background(), req)
	if errV2 == nil {
		t.Fatal("version 2 against a dead server reported success")
	}
	if errors.As(errV2, &partial) {
		t.Fatalf("version 2 reported progress it never made: %v", partial.Delivered())
	}
}

// TestNoticeSharesTheAccountBudget pins the reported P2: the panel's mass
// send went through DirectNotice, which never touched the instance's rate
// limiter, so a broadcast could exceed the provider's limit although
// rate_limit_per_minute was configured. Both paths now claim from the
// same account budget — one slot per recipient, so a notice waits for the
// slot the routed notification just took.
func TestNoticeSharesTheAccountBudget(t *testing.T) {
	smtp.ResetLimitersForTest()
	srv := newSMTPServer(t, nil)
	host, port, _ := net.SplitHostPort(srv.addr())

	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: %s
port: %s
from: from@example.net
to: [visible@example.net]
starttls: false
rate_limit_per_minute: 60
`, host, port)))
	if err != nil {
		t.Fatal(err)
	}
	mailer, ok := p.(smtp.DirectMailer)
	if !ok {
		t.Fatal("the smtp action does not expose the direct mailer")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Execute(ctx, hazardReq()); err != nil {
		t.Fatalf("routed send: %v", err)
	}
	if err := mailer.SendNotice(ctx, []string{"notice@example.net"}, "notice", "body", action.AppInfo{Header1: "SOS"}, "pl"); err != nil {
		t.Fatalf("notice send: %v", err)
	}

	got := srv.snapshot()
	if len(got) != 2 {
		t.Fatalf("deliveries = %d, want 2", len(got))
	}
	// 60 per minute is one per second: the notice must wait for its slot
	// instead of going out immediately (the report measured 0.57 ms).
	if gap := got[1].at.Sub(got[0].at); gap < 900*time.Millisecond {
		t.Fatalf("the notice went out %s after the routed mail; the account budget was bypassed", gap)
	}
}

// TestNoticeHonoursCancellationWhileWaiting pins the other half: a
// cancelled context ends the wait for a rate slot instead of sending
// anyway (the slot is claimed before the connection, so nothing reaches
// the provider).
func TestNoticeHonoursCancellationWhileWaiting(t *testing.T) {
	smtp.ResetLimitersForTest()
	srv := newSMTPServer(t, nil)
	host, port, _ := net.SplitHostPort(srv.addr())

	p, err := smtp.New(cfgNode(t, fmt.Sprintf(`
host: %s
port: %s
from: from@example.net
to: [visible@example.net]
starttls: false
rate_limit_per_minute: 6
`, host, port)))
	if err != nil {
		t.Fatal(err)
	}
	mailer := p.(smtp.DirectMailer)

	// The routed notification takes the first slot ...
	ctx := context.Background()
	if err := p.Execute(ctx, hazardReq()); err != nil {
		t.Fatalf("routed send: %v", err)
	}
	// ... and the mass send that follows is cancelled while it waits for
	// its own (ten seconds away).
	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = mailer.SendNotice(waitCtx, []string{"notice@example.net"}, "notice", "body", action.AppInfo{}, "pl")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled notice = %v, want the context error", err)
	}
	if got := srv.snapshot(); len(got) != 1 {
		t.Fatalf("deliveries = %d, want only the routed mail (the cancelled wait must not send)", len(got))
	}
}
