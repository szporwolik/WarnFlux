package gsm

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDeviceCandidatesPrefersTheAnsweringPort pins the interface rotation seen
// on the production modem: the AT port moved from if00 to if04 across a USB
// reset, so the list is walked and the remembered path goes first.
func TestDeviceCandidatesPrefersTheAnsweringPort(t *testing.T) {
	h := NewHub(Config{Enabled: true, Device: "/dev/modem-0, /dev/modem-3 ,/dev/modem-4"}, nil, nil)
	if got := h.deviceCandidates(); len(got) != 3 || got[0] != "/dev/modem-0" || got[2] != "/dev/modem-4" {
		t.Fatalf("candidates = %v", got)
	}
	h.device = "/dev/modem-4"
	got := h.deviceCandidates()
	if got[0] != "/dev/modem-4" {
		t.Fatalf("remembered path is not first: %v", got)
	}
	if len(got) != 3 {
		t.Fatalf("remembered path duplicated: %v", got)
	}
}

// TestOpenModemSkipsAnUnusablePath pins the actual failure mode seen in
// production: the first candidate on the list is not a working AT port (it may
// open and stay silent, or not be a tty at all), the second one answers. The hub
// must end up on the second and remember it, not sit on the first.
func TestOpenModemSkipsAnUnusablePath(t *testing.T) {
	// A plain file opens but is not a serial port: openSerial must skip it.
	notATty := filepath.Join(t.TempDir(), "not-a-tty")
	if err := os.WriteFile(notATty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	master, slave := openPTY(t)
	defer master.Close()

	// The "modem" on the pty answers OK to every command the init sends.
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 256)
		for {
			n, err := master.Read(buf)
			if err != nil {
				return
			}
			if n > 0 {
				if _, err := master.Write([]byte("\r\nOK\r\n")); err != nil {
					return
				}
			}
		}
	}()

	h, f, _ := newTestHub(t)
	_ = f
	h.mu.Lock()
	h.conn = nil
	h.connected.Store(false)
	h.cfg.Device = notATty + "," + slave
	ok, opened := h.openModemLocked()
	device := h.device
	h.mu.Unlock()
	if !ok || !opened {
		t.Fatalf("openModemLocked = %v/%v, want the pty to be taken", ok, opened)
	}
	if device != slave {
		t.Errorf("session runs on %q, want the answering port %q", device, slave)
	}
	h.Close()
	_ = master.Close()
	<-done
}
