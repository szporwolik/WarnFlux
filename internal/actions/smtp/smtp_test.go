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
func newImplicitSMTPServer(t *testing.T, tlsCfg *tls.Config) *smtpServer {
	t.Helper()
	s := newSMTPServer(t, tlsCfg)
	s.implicitTLS = true
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
			if err := write("250 ok"); err != nil {
				return
			}
			if cur != nil {
				if i := strings.Index(rest, "<"); i >= 0 {
					cur.rcpts = append(cur.rcpts, strings.Trim(rest[i:], "<> "))
				}
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
subject_prefix: "[SPOK]"
`, port)))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Execute(ctx, hazardReq()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := srv.snapshot()
	if len(got) != 1 {
		t.Fatalf("received %d mails, want 1", len(got))
	}
	m := got[0]
	if m.from != "warnflux@example.com" {
		t.Errorf("from = %q", m.from)
	}
	if len(m.rcpts) != 2 || m.rcpts[0] != "ops@example.com" || m.rcpts[1] != "duty@example.com" {
		t.Errorf("rcpts = %v", m.rcpts)
	}
	if m.authUser != "warnflux" || m.authPass != "secret-pw" {
		t.Errorf("auth = %q/%q", m.authUser, m.authPass)
	}
	for _, want := range []string{
		"Subject: [SPOK] SEVERE: Silny wiatr",
		"Transition: hazard_new",
		"Source: imgw-meteo",
		"Severity: severe",
		"Areas: małopolskie",
	} {
		if !strings.Contains(m.data, want) {
			t.Errorf("mail body missing %q:\n%s", want, m.data)
		}
	}

	// A non-ASCII subject must be MIME word-encoded (UTF-8 bytes).
	req := hazardReq()
	req.Event.Hazard.Hazard.Headline = "Intensywne opady śniegu"
	if err := p.Execute(ctx, req); err != nil {
		t.Fatalf("Execute non-ASCII: %v", err)
	}
	mails := srv.snapshot()
	if len(mails) != 2 {
		t.Fatalf("received %d mails, want 2", len(mails))
	}
	if !strings.Contains(mails[1].data, "Subject: =?utf-8?q?") || !strings.Contains(mails[1].data, "=C5=9B") {
		t.Errorf("non-ASCII subject not q-encoded:\n%s", mails[1].data)
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
	if !strings.Contains(got[0].data, "Severity: severe") {
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
	if !strings.Contains(got[0].data, "Severity: severe") {
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
