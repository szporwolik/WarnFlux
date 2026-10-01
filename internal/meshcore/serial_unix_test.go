//go:build unix

package meshcore

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// openPTY returns the master side of a fresh pseudoterminal pair and the
// slave device path. Environments without /dev/ptmx skip the test — the
// hub suite keeps its net.Pipe transport independent of the OS facility.
func openPTY(t *testing.T) (*os.File, string) {
	t.Helper()
	m, err := os.OpenFile("/dev/ptmx", os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Skipf("no /dev/ptmx: %v", err)
	}
	if err := unix.IoctlSetPointerInt(int(m.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		m.Close()
		t.Skipf("unlock pty: %v", err)
	}
	n, err := unix.IoctlGetInt(int(m.Fd()), unix.TIOCGPTN)
	if err != nil {
		m.Close()
		t.Skipf("pty number: %v", err)
	}
	slave := fmt.Sprintf("/dev/pts/%d", n)
	t.Cleanup(func() { m.Close() })
	return m, slave
}

// openSerialPTY opens the production transport against the PTY slave.
func openSerialPTY(t *testing.T) (conn, *os.File) {
	t.Helper()
	m, slave := openPTY(t)
	c, err := openSerial(slave, 9600)
	if err != nil {
		t.Skipf("open serial pty: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, m
}

// waitRead runs one Read in a goroutine and returns its result; the
// watchdog keeps a hung transport from hanging the test suite (the exact
// regression this file guards against).
func waitRead(t *testing.T, c conn) (int, error) {
	t.Helper()
	type res struct {
		n   int
		err error
	}
	done := make(chan res, 1)
	go func() {
		buf := make([]byte, 64)
		n, err := c.Read(buf[:1])
		done <- res{n, err}
	}()
	select {
	case r := <-done:
		return r.n, r.err
	case <-time.After(3 * time.Second):
		t.Fatal("read still blocked after 3s — the serial deadline does not work")
		return 0, nil
	}
}

// TestSerialReadDeadline pins the reported P1: a production openSerial
// conn must honor a 30 ms read deadline on a silent device instead of
// parking in the runtime poller forever.
func TestSerialReadDeadline(t *testing.T) {
	c, _ := openSerialPTY(t)
	start := time.Now()
	if err := c.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	n, err := waitRead(t, c)
	if n != 0 {
		t.Fatalf("timed-out read returned %d bytes", n)
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read error = %v, want os.ErrDeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("deadline took %v, want well under 2s", elapsed)
	}
}

// TestSerialExpiredDeadline: a deadline already in the past fails
// immediately, without touching the descriptor.
func TestSerialExpiredDeadline(t *testing.T) {
	c, _ := openSerialPTY(t)
	if err := c.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	n, err := c.Read(make([]byte, 8))
	if n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read = (%d, %v), want (0, os.ErrDeadlineExceeded)", n, err)
	}
}

// TestSerialReadData: bytes written by the peer arrive through the
// production transport before the deadline.
func TestSerialReadData(t *testing.T) {
	c, m := openSerialPTY(t)
	if _, err := m.Write([]byte("XY")); err != nil {
		t.Fatalf("master write: %v", err)
	}
	if err := c.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, 4)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != 2 || string(buf[:n]) != "XY" {
		t.Fatalf("read %q, want %q", buf[:n], "XY")
	}
}

// TestSerialWriteData: bytes written through the production transport
// arrive on the master side.
func TestSerialWriteData(t *testing.T) {
	c, m := openSerialPTY(t)
	if _, err := c.Write([]byte("hello")); err != nil {
		t.Fatalf("serial write: %v", err)
	}
	if err := m.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("master SetReadDeadline: %v", err)
	}
	buf := make([]byte, 16)
	n, err := m.Read(buf)
	if err != nil {
		t.Fatalf("master read: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("master read %q, want %q", buf[:n], "hello")
	}
}

// TestSerialCloseUnblocksRead: closing the transport (session
// cancellation) releases a blocked reader immediately with a transport
// error, not a deadline.
func TestSerialCloseUnblocksRead(t *testing.T) {
	c, _ := openSerialPTY(t)
	if err := c.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	type res struct {
		n   int
		err error
	}
	done := make(chan res, 1)
	go func() {
		buf := make([]byte, 8)
		n, err := c.Read(buf)
		done <- res{n, err}
	}()
	time.Sleep(100 * time.Millisecond) // let the read park in poll
	start := time.Now()
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case r := <-done:
		if r.n > 0 {
			t.Fatalf("closed read returned %d bytes", r.n)
		}
		if r.err == nil || errors.Is(r.err, os.ErrDeadlineExceeded) {
			t.Fatalf("closed read error = %v, want a transport error", r.err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("close unblocked the read only after %v", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("read still blocked 3s after Close")
	}
}
