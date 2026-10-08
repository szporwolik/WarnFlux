// Package gsm owns the optional cellular modem integration: a serial AT
// modem receives and sends SMS messages. The architecture mirrors the
// APRS/Meshtastic hubs — one Hub owns the transport session, the durable
// history rides the MessageRecorder, and incoming text funnels through a
// single handler hook so commands can be added later without touching
// the transport.
package gsm

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// Config is the modem session configuration.
type Config struct {
	// Enabled switches the modem session on.
	Enabled bool
	// Device is the serial device path of the modem's AT port
	// (e.g. /dev/ttyUSB0).
	Device string
}

// MessageRecorder persists the SMS history (rx and tx). Implemented by
// the SQLite store; recording failures are best-effort.
type MessageRecorder interface {
	RecordGSMMessage(ctx context.Context, direction, from, to, text string, at time.Time) error
}

// phoneRE is the accepted addressee shape for outbound SMS.
var phoneRE = regexp.MustCompile(`^\+?[0-9]{7,15}$`)

// ValidNumber reports whether the phone number has an accepted shape.
func ValidNumber(s string) bool { return phoneRE.MatchString(strings.TrimSpace(s)) }

// ErrNoModem is returned when the hub has no session (disabled or not
// started).
var ErrNoModem = errors.New("gsm: modem session not available")

// ErrTimeout is returned when the modem does not answer an AT exchange.
var ErrTimeout = errors.New("gsm: modem response timeout")

// pollInterval is the spacing of the unread-message scan.
const pollInterval = 10 * time.Second

// atTimeout bounds one AT exchange.
const atTimeout = 15 * time.Second

// received is one consumed inbox message.
type received struct {
	from, text string
}

// Hub owns the AT session to one modem. All exchanges serialize under
// one mutex, so polling, sends and future commands never interleave on
// the serial line.
type Hub struct {
	cfg    Config
	logger *slog.Logger
	rec    MessageRecorder
	now    func() time.Time

	mu        sync.Mutex
	conn      io.ReadWriteCloser
	reader    *bufio.Reader
	connected atomic.Bool
	ready     atomic.Bool

	// handler receives every incoming SMS text (the future command
	// interpreter hook; nil = history only). Invoked WITHOUT the
	// session lock, so it may call Send.
	handler func(from, text string)
}

// NewHub builds the hub (nothing is opened yet).
func NewHub(cfg Config, rec MessageRecorder, logger *slog.Logger) *Hub {
	if logger == nil {
		logger = slog.Default()
	}
	return &Hub{cfg: cfg, logger: logger, rec: rec, now: time.Now}
}

// SetHandler installs the incoming-message hook (commands later). The
// last registration wins; nil clears it.
func (h *Hub) SetHandler(fn func(from, text string)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handler = fn
}

// Connected reports whether the serial session is up.
func (h *Hub) Connected() bool { return h.connected.Load() }

// Ready reports whether the modem answered the init exchange.
func (h *Hub) Ready() bool { return h.ready.Load() }

// Enabled reports whether the modem integration is configured on.
func (h *Hub) Enabled() bool { return h.cfg.Enabled }

// SetTransport injects an already-open transport (tests); nil clears it
// so Start opens the configured device.
func (h *Hub) SetTransport(rw io.ReadWriteCloser) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conn != nil {
		_ = h.conn.Close()
	}
	h.conn = rw
	h.reader = nil
}

// Start opens the device (unless a transport was injected), runs the AT
// init and launches the unread-message poll loop until ctx is done.
func (h *Hub) Start(ctx context.Context) {
	if !h.cfg.Enabled {
		return
	}
	h.mu.Lock()
	conn := h.conn
	if conn == nil {
		dev, err := openSerial(h.cfg.Device)
		if err != nil {
			h.mu.Unlock()
			h.logger.Warn("gsm: cannot open modem", "device", h.cfg.Device, "error", err)
			return
		}
		conn = dev
		h.conn = dev
	}
	if h.reader == nil {
		h.reader = bufio.NewReader(conn)
	}
	h.mu.Unlock()
	h.connected.Store(true)

	h.mu.Lock()
	ok := h.initLocked()
	h.mu.Unlock()
	if !ok {
		h.logger.Warn("gsm: modem init failed — session closed", "device", h.cfg.Device)
		h.closeTransport()
		return
	}
	h.ready.Store(true)
	h.logger.Info("gsm: modem ready", "device", h.cfg.Device)

	go h.pollLoop(ctx)
}

// Close releases the transport (Start may be called again afterwards).
func (h *Hub) Close() {
	h.mu.Lock()
	h.closeTransport()
	h.mu.Unlock()
}

func (h *Hub) closeTransport() {
	h.connected.Store(false)
	h.ready.Store(false)
	h.reader = nil
	if h.conn != nil {
		_ = h.conn.Close()
		h.conn = nil
	}
}

// openSerial opens the device in raw 115200 8N1 mode.
func openSerial(path string) (io.ReadWriteCloser, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	term, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	term.Cflag &^= unix.CSIZE | unix.PARENB | unix.CSTOPB | unix.CRTSCTS
	term.Cflag |= unix.CS8 | unix.CLOCAL | unix.CREAD
	term.Iflag &^= unix.IGNBRK | unix.BRKINT | unix.PARMRK | unix.ISTRIP | unix.INLCR | unix.IGNCR | unix.ICRNL | unix.IXON
	term.Oflag &^= unix.OPOST
	term.Lflag &^= unix.ECHO | unix.ECHONL | unix.ICANON | unix.ISIG | unix.IEXTEN
	term.Cc[unix.VMIN] = 0
	term.Cc[unix.VTIME] = 1
	term.Cflag &^= unix.CBAUD
	term.Cflag |= unix.B115200
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, term); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

// initLocked runs the modem setup: echo off + text-mode SMS. The caller
// holds h.mu.
func (h *Hub) initLocked() bool {
	for _, cmd := range []string{"ATE0", "AT+CMGF=1"} {
		if _, err := h.exchangeLocked(cmd, "OK"); err != nil {
			return false
		}
	}
	return true
}

// exchangeLocked writes one AT command and waits for the expected
// response (a "" command writes nothing — used to collect a pending
// response). The caller holds h.mu.
func (h *Hub) exchangeLocked(cmd, expect string) ([]string, error) {
	if h.reader == nil {
		return nil, ErrNoModem
	}
	if cmd != "" {
		if _, err := io.WriteString(h.conn, cmd+"\r"); err != nil {
			return nil, fmt.Errorf("gsm: write %q: %w", cmd, err)
		}
	}
	deadline := h.now().Add(atTimeout)
	var lines []string
	for {
		if h.now().After(deadline) {
			return lines, ErrTimeout
		}
		line, err := h.readLine(deadline)
		if err != nil {
			return lines, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}
		lines = append(lines, line)
		switch {
		case line == expect:
			return lines, nil
		case expect == "> " && strings.TrimSpace(line) == ">":
			return lines, nil
		case line == "ERROR":
			return lines, fmt.Errorf("gsm: modem error on %q", cmd)
		}
	}
}

// readLine reads one line from the persistent reader, honoring the
// deadline.
func (h *Hub) readLine(deadline time.Time) (string, error) {
	var sb strings.Builder
	for {
		s, err := h.reader.ReadString('\n')
		sb.WriteString(s)
		if strings.HasSuffix(s, "\n") {
			return sb.String(), nil
		}
		if err != nil {
			if sb.Len() > 0 {
				return sb.String(), nil
			}
			if h.now().After(deadline) {
				return "", ErrTimeout
			}
			return "", err
		}
	}
}

// Send transmits one SMS. The tx row lands in the durable history only
// after the modem accepted the message.
func (h *Hub) Send(ctx context.Context, number, text string) error {
	number = strings.TrimSpace(number)
	text = strings.TrimSpace(text)
	if !ValidNumber(number) {
		return fmt.Errorf("gsm: invalid addressee number %q", number)
	}
	if text == "" {
		return fmt.Errorf("gsm: message text must not be empty")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conn == nil || !h.connected.Load() || h.reader == nil {
		return ErrNoModem
	}
	// Wait for the prompt, then submit the body with Ctrl-Z.
	if _, err := h.exchangeLocked(`AT+CMGS="`+number+`"`, "> "); err != nil {
		return fmt.Errorf("gsm: send to %s: %w", number, err)
	}
	if _, err := io.WriteString(h.conn, text+"\x1A"); err != nil {
		return fmt.Errorf("gsm: write body: %w", err)
	}
	if _, err := h.exchangeLocked("", "OK"); err != nil {
		return fmt.Errorf("gsm: send to %s: %w", number, err)
	}
	if h.rec != nil {
		recCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := h.rec.RecordGSMMessage(recCtx, "tx", "self", number, text, h.now())
		cancel()
		if err != nil {
			h.logger.Warn("gsm: tx history record failed", "to", number, "error", err)
		}
	}
	return nil
}

// pollLoop scans unread inbox messages every pollInterval.
func (h *Hub) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, m := range h.pollUnread() {
				h.mu.Lock()
				fn := h.handler
				h.mu.Unlock()
				if fn != nil {
					fn(m.from, m.text)
				}
			}
		}
	}
}

// pollUnread lists "REC UNREAD" messages, records them, deletes the
// inbox and returns the received set (handlers run outside the lock).
func (h *Hub) pollUnread() []received {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conn == nil || !h.connected.Load() {
		return nil
	}
	lines, err := h.exchangeLocked(`AT+CMGL="REC UNREAD"`, "OK")
	if err != nil {
		h.logger.Debug("gsm: unread scan failed", "error", err)
		return nil
	}
	var out []received
	var cur received
	got := false
	flush := func() {
		if !got || cur.from == "" {
			got, cur = false, received{}
			return
		}
		if h.rec != nil {
			recCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := h.rec.RecordGSMMessage(recCtx, "rx", cur.from, "self", cur.text, h.now())
			cancel()
			if err != nil {
				h.logger.Warn("gsm: rx history record failed", "from", cur.from, "error", err)
			}
		}
		h.logger.Info("gsm: sms received", "from", cur.from)
		out = append(out, cur)
		got, cur = false, received{}
	}
	for _, l := range lines {
		if strings.HasPrefix(l, "+CMGL:") {
			flush()
			// +CMGL: 1,"REC UNREAD","+48123456789",,"25/10/08,.."
			rest := strings.TrimSpace(strings.TrimPrefix(l, "+CMGL:"))
			if i := strings.IndexByte(rest, ','); i >= 0 {
				rest = strings.TrimSpace(rest[i+1:])
				if len(rest) > 1 && rest[0] == '"' {
					if j := strings.IndexByte(rest[1:], '"'); j >= 0 {
						rest = rest[j+1:]
					}
				}
				if k := strings.Index(rest, `","`); k >= 0 {
					rest = rest[k+3:]
					if m := strings.IndexByte(rest, '"'); m >= 0 {
						cur.from = rest[:m]
					}
				}
			}
			got = true
			continue
		}
		if got && (l == "OK" || l == "ERROR") {
			flush()
			continue
		}
		if got && l != "" {
			cur.text = l
		}
	}
	flush()
	// Consume the whole unread inbox: read-once semantics without index
	// bookkeeping (indices shift on delete).
	if _, err := h.exchangeLocked(`AT+CMGD=1,4`, "OK"); err != nil {
		h.logger.Debug("gsm: inbox delete failed", "error", err)
	}
	return out
}
