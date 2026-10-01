//go:build unix

package meshcore

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// conn is the transport seam: both the serial device and test pipes
// (net.Pipe) satisfy it.
type conn interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
	SetReadDeadline(t time.Time) error
}

// openSerial opens the MeshCore device in raw 8N1 mode at the configured
// baud and returns a conn with read-deadline support.
func openSerial(device string, baud int) (conn, error) {
	fd, err := unix.Open(device, unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", device, err)
	}

	tio, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("get termios %s: %w", device, err)
	}
	rate := baudToConst(baud)
	if rate == 0 {
		unix.Close(fd)
		return nil, fmt.Errorf("unsupported baud %d", baud)
	}
	tio.Cflag &^= unix.CBAUD
	tio.Cflag |= rate | unix.CS8 | unix.CREAD | unix.CLOCAL
	tio.Iflag = 0
	tio.Oflag = 0
	tio.Lflag = 0
	tio.Cc[unix.VMIN] = 1
	tio.Cc[unix.VTIME] = 0
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, tio); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("set termios %s: %w", device, err)
	}

	return &serialConn{fd: fd}, nil
}

// serialConn adapts a raw non-blocking serial fd to a deadline-aware
// conn. The descriptor is deliberately NOT wrapped in os.File: NewFile
// hands it to the runtime poller, whose Read parks the goroutine with
// nothing to wake it when the deadline passes — a silent device would
// block the session initialization and shutdown forever. All I/O goes
// through the raw descriptor with an explicit poll loop instead, so the
// deadline is honored by construction.
type serialConn struct {
	fd        int
	deadline  time.Time
	closeOnce sync.Once
}

func (c *serialConn) SetReadDeadline(t time.Time) error {
	c.deadline = t
	return nil
}

// Read reads from the raw descriptor, poll-waiting while it is not
// readable. The deadline is honored exactly: a timeout returns
// os.ErrDeadlineExceeded. A zero deadline means "block until data"
// (matching os.File semantics); the session cancellation closes the
// transport, which unblocks this read immediately.
func (c *serialConn) Read(p []byte) (int, error) {
	var timeout <-chan time.Time
	if !c.deadline.IsZero() {
		d := time.Until(c.deadline)
		if d <= 0 {
			return 0, os.ErrDeadlineExceeded
		}
		t := time.NewTimer(d)
		defer t.Stop()
		timeout = t.C
	}
	for {
		n, err := unix.Read(c.fd, p)
		if err == nil {
			return n, nil
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			if !c.awaitReadable(timeout) {
				return 0, os.ErrDeadlineExceeded
			}
			continue
		}
		return n, err
	}
}

// awaitReadable waits until the descriptor is readable (or closed).
// false means the deadline timer won. Poll and syscall errors are
// reported as readable: the following Read surfaces the real error.
func (c *serialConn) awaitReadable(timeout <-chan time.Time) bool {
	if timeout == nil {
		// No deadline: an indefinite poll is exactly the blocking
		// read the caller asked for. Close() makes the poll return
		// and the next Read report the closed descriptor.
		for {
			pfd := []unix.PollFd{{Fd: int32(c.fd), Events: unix.POLLIN}}
			if _, err := unix.Poll(pfd, -1); err != nil {
				if errors.Is(err, syscall.EINTR) {
					continue
				}
				return true
			}
			return true
		}
	}
	for {
		select {
		case <-timeout:
			return false
		default:
		}
		d := time.Until(c.deadline)
		if d <= 0 {
			return false
		}
		if d > 200*time.Millisecond {
			d = 200 * time.Millisecond
		}
		pfd := []unix.PollFd{{Fd: int32(c.fd), Events: unix.POLLIN}}
		if _, err := unix.Poll(pfd, int(d.Milliseconds())); err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return true
		}
		if pfd[0].Revents&(unix.POLLIN|unix.POLLHUP|unix.POLLERR|unix.POLLNVAL) != 0 {
			return true
		}
	}
}

// Write writes to the raw descriptor, poll-waiting while the line
// discipline buffer is full (EAGAIN). The hub bounds every command
// write with its own timeout, and closing the transport unblocks a
// stuck writer immediately.
func (c *serialConn) Write(p []byte) (int, error) {
	for {
		n, err := unix.Write(c.fd, p)
		if err == nil {
			return n, nil
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			pfd := []unix.PollFd{{Fd: int32(c.fd), Events: unix.POLLOUT}}
			if _, perr := unix.Poll(pfd, -1); perr != nil {
				if errors.Is(perr, syscall.EINTR) {
					continue
				}
				return 0, perr
			}
			continue
		}
		return n, err
	}
}

func (c *serialConn) Close() error {
	var err error
	c.closeOnce.Do(func() { err = unix.Close(c.fd) })
	return err
}

func baudToConst(baud int) uint32 {
	switch baud {
	case 9600:
		return unix.B9600
	case 19200:
		return unix.B19200
	case 38400:
		return unix.B38400
	case 57600:
		return unix.B57600
	case 115200:
		return unix.B115200
	case 230400:
		return unix.B230400
	case 460800:
		return unix.B460800
	case 921600:
		return unix.B921600
	}
	return 0
}
