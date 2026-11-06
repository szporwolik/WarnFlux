package gsm

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

// openPTY returns a pseudo-terminal pair (master, slave path).
func openPTY(t *testing.T) (*os.File, string) {
	t.Helper()
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		t.Skipf("no pty available: %v", err)
	}
	// TIOCSPTLCK takes a pointer to an int, not the value.
	unlock := 0
	if err := unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, unlock); err != nil {
		master.Close()
		t.Skipf("cannot unlock pty: %v", err)
	}
	n, err := unix.IoctlGetInt(int(master.Fd()), unix.TIOCGPTN)
	if err != nil {
		master.Close()
		t.Skipf("cannot name pty: %v", err)
	}
	return master, "/dev/pts/" + itoa(n)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
