//go:build unix

package meshcore

import (
	"errors"
	"fmt"
	"os"
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

	f := os.NewFile(uintptr(fd), device)
	return &serialConn{f: f}, nil
}

// serialConn adapts a non-blocking serial fd to a deadline-aware conn.
type serialConn struct {
	f        *os.File
	deadline time.Time
}

func (c *serialConn) SetReadDeadline(t time.Time) error {
	c.deadline = t
	return nil
}

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
		n, err := c.f.Read(p)
		if err == nil {
			return n, nil
		}
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			if timeout == nil {
				return 0, os.ErrDeadlineExceeded
			}
			select {
			case <-timeout:
				return 0, os.ErrDeadlineExceeded
			default:
			}
			pfd := []unix.PollFd{{Fd: int32(c.f.Fd()), Events: unix.POLLIN}}
			if _, err := unix.Poll(pfd, 50); err != nil {
				if errors.Is(err, syscall.EINTR) {
					continue
				}
				return 0, err
			}
			continue
		}
		return n, err
	}
}

func (c *serialConn) Write(p []byte) (int, error) { return c.f.Write(p) }
func (c *serialConn) Close() error                { return c.f.Close() }

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
