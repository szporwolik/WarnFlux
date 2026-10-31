package healthprobe

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A healthy answer is the only thing the probe accepts: the station answers 200
// with a short body, anything else means "do not trust this container".
func TestProbeAcceptsHealthyAnswer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Errorf("probed %q, want /healthz", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}))
	defer srv.Close()

	if err := Probe(context.Background(), srv.URL+"/healthz"); err != nil {
		t.Fatalf("Probe() = %v, want nil", err)
	}
}

// A station that answers but reports a failure is not healthy.
func TestProbeRejectsFailureStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready\n"))
	}))
	defer srv.Close()

	err := Probe(context.Background(), srv.URL+"/healthz")
	if err == nil {
		t.Fatal("Probe() = nil, want an error for HTTP 503")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Fatalf("Probe() error = %q, want it to name the status", err)
	}
}

// A redirect means the request never reached the liveness handler — for
// example a listener serving a login page. Following it would report whatever
// the login page returns and could look healthy.
func TestProbeRejectsRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("login page\n"))
	}))
	defer srv.Close()

	err := Probe(context.Background(), srv.URL+"/healthz")
	if err == nil {
		t.Fatal("Probe() = nil, want an error for a redirect")
	}
}

// Nothing listening is the classic "the process is gone but Docker still shows
// a container" case and must be reported as an error.
func TestProbeReportsUnreachableListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	if err := Probe(context.Background(), "http://"+addr+"/healthz"); err == nil {
		t.Fatal("Probe() = nil, want an error for a closed listener")
	}
}

// A hung listener must not hang the probe: the deadline is part of the
// contract, because Docker kills the probe at HEALTHCHECK --timeout.
func TestProbeGivesUpOnContextCancel(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := Probe(ctx, srv.URL+"/healthz")
	if err == nil {
		t.Fatal("Probe() = nil, want an error when the deadline passes")
	}
	if elapsed := time.Since(start); elapsed > Timeout {
		t.Fatalf("Probe() took %v, want it bounded by %v", elapsed, Timeout)
	}
}
