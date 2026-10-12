package smtp

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
)

// shortenFallbackDeadline stretches the no-budget fallback so a send that
// ignores the context cannot hide behind it.
func shortenFallbackDeadline(t *testing.T, d time.Duration) {
	t.Helper()
	prev := DefaultDeadlineForTest()
	SetDefaultDeadlineForTest(d)
	t.Cleanup(func() { SetDefaultDeadlineForTest(prev) })
}

// waitForCancelledSend asserts the send ends long before the fallback
// deadline could have cut it.
func waitForCancelledSend(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a cancelled send reported success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the cancelled send did not return: the TLS handshake ignores the context")
	}
}

// silentServer accepts every connection and then answers nothing (or
// answers only the greeting), the way an overloaded relay or a network
// black hole behaves. It returns its address.
func silentServer(t *testing.T, banner bool) (host string, port int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				if banner {
					_, _ = c.Write([]byte("220 silent.test ESMTP\r\n"))
				}
				// Hold the connection open and never speak again.
				var buf [512]byte
				for {
					if _, err := c.Read(buf[:]); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	h, p, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split addr: %v", err)
	}
	n, err := strconv.Atoi(p)
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	return h, n
}

// TestDirectIsBoundedWhenServerStaysSilent pins the reported P2: the
// greeting was read by net/smtp's NewClient before any deadline was set
// and the context was never consulted, so a relay that accepted the
// connection without answering blocked the caller forever — password
// resets included. One send must end within the caller's deadline.
func TestDirectIsBoundedWhenServerStaysSilent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		banner bool
	}{
		{name: "no greeting at all", banner: false},
		{name: "greeting then silence", banner: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host, port := silentServer(t, tc.banner)
			cfg := Config{Host: host, Port: port, From: "wf@example.com"}

			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer cancel()

			done := make(chan error, 1)
			go func() {
				done <- Direct(ctx, cfg, []string{"someone@example.com"}, "reset", "body")
			}()

			select {
			case err := <-done:
				if err == nil {
					t.Fatal("send against a silent server reported success")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("send did not return within 5s: the deadline does not cover the greeting")
			}
		})
	}
}

// TestDirectCancellationClosesTheConnection pins the second half of the
// report: cancelling the context must end the send even when the server
// answered the greeting and then went quiet, instead of leaving the
// caller parked until the deadline.
func TestDirectCancellationClosesTheConnection(t *testing.T) {
	host, port := silentServer(t, true)
	cfg := Config{Host: host, Port: port, From: "wf@example.com"}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()

	done := make(chan error, 1)
	go func() {
		done <- Direct(ctx, cfg, []string{"someone@example.com"}, "reset", "body")
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled send reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled send did not return within 5s: the context does not close the connection")
	}
}

// TestDirectTLSHandshakeHonoursCancellation pins the reported P2: with
// implicit_tls the handshake went through tls.DialWithDialer, which never
// saw the context, so a relay that accepted the TCP connection and then
// stayed silent during TLS held the send until the deadline — cancellation
// or not. The handshake now runs under tls.Dialer.DialContext.
func TestDirectTLSHandshakeHonoursCancellation(t *testing.T) {
	shortenFallbackDeadline(t, 10*time.Second)
	host, port := silentServer(t, false)
	implicit := true
	cfg := Config{Host: host, Port: port, From: "wf@example.com", ImplicitTLS: &implicit}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	done := make(chan error, 1)
	go func() {
		done <- Direct(ctx, cfg, []string{"someone@example.com"}, "reset", "body")
	}()
	waitForCancelledSend(t, done)
}

// TestImplicitTLSHandshakeHonoursCancellation pins the same gap on the
// action path (smtp.go): a peer that stays silent during the SMTPS
// handshake must not hold the send when the caller gives up.
func TestImplicitTLSHandshakeHonoursCancellation(t *testing.T) {
	shortenFallbackDeadline(t, 10*time.Second)
	host, port := silentServer(t, false)

	var node yaml.Node
	if err := node.Encode(map[string]any{
		"host":                  host,
		"port":                  port,
		"from":                  "wf@example.com",
		"to":                    []string{"visible@example.com"},
		"implicit_tls":          true,
		"rate_limit_per_minute": 6000,
	}); err != nil {
		t.Fatal(err)
	}
	p, err := New(&node)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	done := make(chan error, 1)
	go func() {
		done <- p.Execute(ctx, action.ActionRequest{ID: "imgw:1/mail"})
	}()
	waitForCancelledSend(t, done)
}
