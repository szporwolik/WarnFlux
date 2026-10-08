// Package gsm owns the optional cellular modem integration: a serial AT
// modem receives and sends SMS messages. The architecture mirrors the
// APRS/Meshtastic hubs — one Hub owns the transport session, the durable
// history rides the MessageRecorder, and incoming text funnels through a
// single handler hook so commands can be added later without touching
// the transport.
package gsm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf16"

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

// ErrModemRejected wraps AT commands the modem refused (+CMS ERROR /
// ERROR). Scans treat it as "nothing more to read", sends surface it.
var ErrModemRejected = errors.New("gsm: modem rejected")

// ErrTimeout is returned when the modem does not answer an AT exchange.
var ErrTimeout = errors.New("gsm: modem response timeout")

// pollInterval is the spacing of the unread-message scan.
const pollInterval = 10 * time.Second

// atTimeout bounds one AT exchange.
const atTimeout = 15 * time.Second

// sendAckTimeout bounds the wait for the network accept reply after a
// submitted SMS body (the modem answers when the network takes it).
const sendAckTimeout = 45 * time.Second

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
	connected atomic.Bool
	ready     atomic.Bool

	// operator and csq are the last modem status (network name and
	// signal quality); refreshed periodically by the poll loop.
	op  atomic.Pointer[string]
	csq atomic.Int64

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

// Operator returns the last reported network operator name ("" until
// the first status refresh succeeds).
func (h *Hub) Operator() string {
	if p := h.op.Load(); p != nil {
		return *p
	}
	return ""
}

// SignalCSQ returns the last reported signal quality (0..31, 99 =
// unknown; 0 until the first refresh).
func (h *Hub) SignalCSQ() int { return int(h.csq.Load()) }

// OperatorName maps the modem's operator report to a friendly display
// name (the network sends short aliases like "POL" for Play). Unknown
// names pass through unchanged.
func OperatorName(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "", "0":
		return ""
	case "POL", "PLAY", "PLAY (P4)":
		return "Play"
	case "PLUS", "PLUS PL":
		return "Plus"
	case "ORANGE PL":
		return "Orange"
	case "T-MOBILE.PL", "T-MOBILE", "TM PL":
		return "T-Mobile"
	}
	return strings.TrimSpace(raw)
}

// SignalLevel converts a CSQ value (0..31) into a 1..5 bar level
// (0 = unknown/no signal).
func SignalLevel(csq int) int {
	switch {
	case csq <= 0:
		return 0
	case csq <= 6: // ≈ ≤ -101 dBm
		return 1
	case csq <= 11: // ≈ ≤ -91 dBm
		return 2
	case csq <= 16: // ≈ ≤ -81 dBm
		return 3
	case csq <= 21: // ≈ ≤ -71 dBm
		return 4
	default:
		return 5
	}
}

// SetTransport injects an already-open transport (tests); nil clears it
// so Start opens the configured device.
func (h *Hub) SetTransport(rw io.ReadWriteCloser) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conn != nil {
		_ = h.conn.Close()
	}
	h.conn = rw
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

	// First operator/signal snapshot right away (errors are tolerated;
	// the poll loop refreshes every few minutes).
	h.mu.Lock()
	h.refreshStatusLocked()
	h.mu.Unlock()

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
	// Blocking mode: reads then honor VMIN/VTIME (an idle read returns
	// within ~0.1 s) so the hub's exchange deadlines actually fire.
	// In non-blocking mode the Go runtime poller waits indefinitely and
	// a silent modem would stall the session forever.
	if flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0); err == nil {
		_, _ = unix.FcntlInt(uintptr(fd), unix.F_SETFL, flags&^unix.O_NONBLOCK)
	}
	// A previous session (or a manual probe) may have left the modem
	// mid-text-entry with the prompt open: ESC aborts that entry and a
	// short drain eats the stale reply — otherwise the first AT command
	// is appended to the abandoned message body and the init times out.
	_, _ = unix.Write(fd, []byte("\x1B\r"))
	var buf [256]byte
	start := time.Now()
	last := start
	for time.Since(start) < time.Second && time.Since(last) < 200*time.Millisecond {
		if n, err := unix.Read(fd, buf[:]); err == nil && n > 0 {
			last = time.Now()
		} else {
			time.Sleep(20 * time.Millisecond)
		}
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
	return h.exchangeLockedTimeout(cmd, expect, atTimeout)
}

// exchangeLockedTimeout is exchangeLocked with an explicit response
// budget (the modem's SMS-accept reply can take tens of seconds).
func (h *Hub) exchangeLockedTimeout(cmd, expect string, timeout time.Duration) ([]string, error) {
	if h.conn == nil {
		return nil, ErrNoModem
	}
	if cmd != "" {
		if _, err := io.WriteString(h.conn, cmd+"\r"); err != nil {
			return nil, fmt.Errorf("gsm: write %q: %w", cmd, err)
		}
	}
	deadline := h.now().Add(timeout)
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
		case line == "ERROR":
			return lines, fmt.Errorf("gsm: modem error on %q", cmd)
		case strings.HasPrefix(line, "+CMS ERROR"):
			detail := strings.TrimSpace(strings.TrimPrefix(line, "+CMS ERROR"))
			detail = strings.TrimSpace(strings.TrimPrefix(detail, ":"))
			return lines, fmt.Errorf("%w %q (%s)", ErrModemRejected, cmd, detail)
		}
	}
}

// exchangePromptLocked sends one AT command and waits for the SMS text
// prompt ('> ') — Huawei-style modems answer it WITHOUT a trailing
// newline, so the line-based exchange would time out. The caller holds
// h.mu.
func (h *Hub) exchangePromptLocked(cmd string) ([]string, error) {
	if h.conn == nil {
		return nil, ErrNoModem
	}
	if _, err := io.WriteString(h.conn, cmd+"\r"); err != nil {
		return nil, fmt.Errorf("gsm: write %q: %w", cmd, err)
	}
	deadline := h.now().Add(atTimeout)
	var lines []string
	var partial strings.Builder
	for {
		if h.now().After(deadline) {
			return lines, ErrTimeout
		}
		b, err := h.readByte(deadline)
		if err != nil {
			return lines, err
		}
		if b == '\n' {
			line := strings.TrimRight(partial.String(), "\r")
			partial.Reset()
			if line == "" {
				continue
			}
			lines = append(lines, line)
			if line == "ERROR" || strings.HasPrefix(line, "+CMS ERROR") {
				detail := strings.TrimSpace(strings.TrimPrefix(line, "+CMS ERROR"))
				detail = strings.TrimSpace(strings.TrimPrefix(detail, ":"))
				return lines, fmt.Errorf("%w %q (%s)", ErrModemRejected, cmd, detail)
			}
			continue
		}
		partial.WriteByte(b)
		// The prompt may be the only content of the response — match it
		// as soon as the bytes spell '>' (with an optional space).
		s := strings.TrimRight(partial.String(), "\r")
		if s == ">" || s == "> " {
			lines = append(lines, s)
			return lines, nil
		}
	}
}

// readByte reads one byte with an absolute deadline. Idle reads — the
// VTIME tick of a blocking serial port, which Go surfaces as a 0-byte
// read (io.EOF) — retry until the deadline or a real failure; a
// silently dead modem can no longer stall the session forever.
func (h *Hub) readByte(deadline time.Time) (byte, error) {
	var one [1]byte
	for {
		if h.now().After(deadline) {
			return 0, ErrTimeout
		}
		n, err := h.conn.Read(one[:])
		if n > 0 {
			return one[0], nil
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, unix.EAGAIN) {
				// An empty VTIME read translates to io.EOF in Go — it
				// means "no data yet", not a disconnected modem.
				time.Sleep(20 * time.Millisecond)
				continue
			}
			return 0, err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// readLine reads one line (through '\n') with an absolute deadline; a
// partial line accumulated when the deadline hits is returned as-is.
func (h *Hub) readLine(deadline time.Time) (string, error) {
	var sb []byte
	for {
		b, err := h.readByte(deadline)
		if err != nil {
			if len(sb) > 0 {
				return string(sb), nil
			}
			return "", err
		}
		sb = append(sb, b)
		if b == '\n' || len(sb) >= 4096 {
			return string(sb), nil
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
	if h.conn == nil || !h.connected.Load() {
		return ErrNoModem
	}
	// Wait for the prompt, then submit the body with Ctrl-Z.
	if _, err := h.exchangePromptLocked(`AT+CMGS="` + number + `"`); err != nil {
		return fmt.Errorf("gsm: send to %s: %w", number, err)
	}
	if _, err := io.WriteString(h.conn, text+"\x1A"); err != nil {
		return fmt.Errorf("gsm: write body: %w", err)
	}
	// The network accept reply can take tens of seconds on a busy
	// channel — a wider budget than the regular exchange timeout.
	if _, err := h.exchangeLockedTimeout("", "OK", sendAckTimeout); err != nil {
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
	lastStatus := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.mu.Lock()
			if h.now().Sub(lastStatus) > 5*time.Minute {
				lastStatus = h.now()
				h.refreshStatusLocked()
			}
			// A USB modem may vanish mid-session (firmware reset): the
			// scan reports it and the session reopens itself so the
			// admin page never stays dead until a full restart.
			var msgs []received
			if h.conn != nil {
				var ok bool
				if msgs, ok = h.pollUnreadLocked(); !ok {
					h.reconnectLocked()
				}
			}
			h.mu.Unlock()
			// Handlers run outside the lock — they may call Send.
			if len(msgs) > 0 {
				h.mu.Lock()
				fn := h.handler
				h.mu.Unlock()
				for _, m := range msgs {
					if fn != nil {
						fn(m.from, m.text)
					}
				}
			}
		}
	}
}

// mccmncNames maps Polish network codes to operator names (numeric
// +COPS replies carry only the MCC-MNC).
var mccmncNames = map[string]string{
	"26001": "Plus",
	"26002": "T-Mobile",
	"26003": "Orange",
	"26006": "Play",
	"26010": "Aero2",
	"26012": "Cyfrowy Polsat",
}

// refreshStatusLocked queries the signal quality and the network
// operator and stores them for the admin page. Failures leave the
// previous values in place. The caller holds h.mu.
func (h *Hub) refreshStatusLocked() {
	if h.conn == nil {
		return
	}
	// Long-alphanumeric operator format (persisted on the modem); a
	// modem that rejects it keeps the numeric format and the code below
	// maps the MCC-MNC.
	_, _ = h.exchangeLocked(`AT+COPS=3,0`, "OK")
	if lines, err := h.exchangeLocked(`AT+COPS?`, "OK"); err == nil {
		for _, l := range lines {
			if !strings.HasPrefix(l, "+COPS:") {
				continue
			}
			if op := parseCOPS(l); op != "" {
				h.op.Store(&op)
				break
			}
		}
	}
	if lines, err := h.exchangeLocked(`AT+CSQ`, "OK"); err == nil {
		for _, l := range lines {
			if !strings.HasPrefix(l, "+CSQ:") {
				continue
			}
			if v := parseCSQ(l); v >= 0 {
				h.csq.Store(int64(v))
				break
			}
		}
	}
}

// DecodeSMSBody turns a modem-delivered SMS body into readable text.
// Messages containing non-GSM-7 characters (emoji, etc.) arrive as
// UCS-2 code units rendered as a plain hex string by the modem; anything
// else passes through unchanged (the check is idempotent, so an already
// decoded string stays as it is).
func DecodeSMSBody(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw)%4 != 0 {
		return raw
	}
	for _, c := range raw {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return raw
		}
	}
	units := make([]uint16, 0, len(raw)/4)
	for i := 0; i+4 <= len(raw); i += 4 {
		v, err := strconv.ParseUint(raw[i:i+4], 16, 16)
		if err != nil {
			return raw
		}
		units = append(units, uint16(v))
	}
	return string(utf16.Decode(units))
}

// parseCOPS extracts the operator field of a +COPS response
// ("+COPS: 0,0,\"PLAY\",0" → PLAY; numeric codes map to names).
func parseCOPS(line string) string {
	fields := strings.Split(line, ",")
	if len(fields) < 3 {
		return ""
	}
	op := strings.Trim(strings.TrimSpace(fields[2]), `"`)
	if op == "" {
		return ""
	}
	if _, err := strconv.Atoi(op); err == nil {
		if name := mccmncNames[op]; name != "" {
			return name
		}
	}
	return op
}

// parseCSQ extracts the rssi value of a +CSQ response ("+CSQ: 26,99" →
// 26; -1 when the line does not parse).
func parseCSQ(line string) int {
	rest := strings.TrimSpace(strings.TrimPrefix(line, "+CSQ:"))
	if i := strings.IndexByte(rest, ','); i >= 0 {
		rest = rest[:i]
	}
	v, err := strconv.Atoi(strings.TrimSpace(rest))
	if err != nil {
		return -1
	}
	return v
}

// pollUnreadLocked drains the stored inbox, records every message and
// returns the received set (handlers run outside the lock). ok=false
// means the transport died (the poll loop reconnects). The caller holds
// h.mu.
//
// The Huawei E173 firmware lists NOTHING via AT+CMGL even when the SIM
// holds unread messages, so the scan reads front-to-back by index
// (CMGR=1 + CMGD=1 — the remaining messages shift down after each
// delete).
func (h *Hub) pollUnreadLocked() (out []received, ok bool) {
	if h.conn == nil || !h.connected.Load() {
		return nil, false
	}
	_, maxSlots, err := h.storageStatsLocked()
	if err != nil {
		h.logger.Debug("gsm: unread scan failed", "error", err)
		return out, false
	}
	// The firmware leaves HOLES (a deleted slot stays empty instead of
	// compacting), so every slot 1..max must be probed individually.
	for idx := 1; idx <= maxSlots && idx <= 64; idx++ {
		m, status, err := h.readIndexLocked(idx)
		if errors.Is(err, ErrModemRejected) {
			// Indices beyond the firmware's real storage are refused —
			// the scan is simply done.
			return out, true
		}
		if err != nil {
			h.logger.Debug("gsm: unread scan failed", "error", err)
			return out, false
		}
		if status == "" {
			continue // empty slot (hole)
		}
		// Sent copies (STO SENT) also live in the storage: delete them
		// for hygiene but never import them as received messages.
		if strings.HasPrefix(status, "STO") {
			if _, err := h.exchangeLocked(fmt.Sprintf("AT+CMGD=%d", idx), "OK"); err != nil && !errors.Is(err, ErrModemRejected) {
				h.logger.Debug("gsm: inbox delete failed", "error", err)
			}
			continue
		}
		if h.rec != nil {
			recCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := h.rec.RecordGSMMessage(recCtx, "rx", m.from, "self", m.text, h.now())
			cancel()
			if err != nil {
				h.logger.Warn("gsm: rx history record failed", "from", m.from, "error", err)
			}
		}
		h.logger.Info("gsm: sms received", "from", m.from)
		out = append(out, m)
		if _, err := h.exchangeLocked(fmt.Sprintf("AT+CMGD=%d", idx), "OK"); err != nil && !errors.Is(err, ErrModemRejected) {
			h.logger.Debug("gsm: inbox delete failed", "error", err)
			return out, true
		}
	}
	return out, true
}

// storageStatsLocked reads the active storage status (+CPMS?): used
// slots and the storage capacity. The caller holds h.mu.
func (h *Hub) storageStatsLocked() (used, max int, err error) {
	lines, err := h.exchangeLocked(`AT+CPMS?`, "OK")
	if err != nil {
		return 0, 0, err
	}
	for _, l := range lines {
		if !strings.HasPrefix(l, "+CPMS:") {
			continue
		}
		// +CPMS: "SM",5,25,"SM",5,25,"SM",5,25
		fields := strings.Split(strings.TrimPrefix(l, "+CPMS:"), ",")
		if len(fields) >= 3 {
			u, err1 := strconv.Atoi(strings.Trim(strings.TrimSpace(fields[1]), `"`))
			m, err2 := strconv.Atoi(strings.Trim(strings.TrimSpace(fields[2]), `"`))
			if err1 == nil && err2 == nil {
				return u, m, nil
			}
		}
	}
	return 0, 0, nil
}

// readIndexLocked reads one stored message by index (+CMGR=<i>) and
// returns its content with the storage status field ("REC UNREAD",
// "STO SENT", ...; "" when the slot is empty). The caller holds h.mu.
func (h *Hub) readIndexLocked(idx int) (m received, status string, err error) {
	lines, err := h.exchangeLocked(fmt.Sprintf("AT+CMGR=%d", idx), "OK")
	if err != nil {
		return received{}, "", err
	}
	var body []string
	for _, l := range lines {
		if strings.HasPrefix(l, "+CMGR:") {
			// +CMGR: "REC UNREAD","+48509558155",,"26/10/08,18:51:52+08"
			rest := strings.TrimSpace(strings.TrimPrefix(l, "+CMGR:"))
			if len(rest) > 1 && rest[0] == '"' {
				if j := strings.IndexByte(rest[1:], '"'); j >= 0 {
					status = rest[1 : 1+j]
					rest = rest[j+2:]
					if i := strings.IndexByte(rest, ','); i >= 0 {
						rest = strings.TrimSpace(rest[i+1:])
						if len(rest) > 1 && rest[0] == '"' {
							if k := strings.IndexByte(rest[1:], '"'); k >= 0 {
								m.from = rest[1 : 1+k]
							}
						}
					}
				}
			}
			continue
		}
		if l == "OK" || l == "ERROR" {
			continue
		}
		if l != "" {
			body = append(body, l)
		}
	}
	if m.from == "" {
		return received{}, "", nil
	}
	text := strings.Join(body, "\n")
	if len(body) == 1 {
		text = DecodeSMSBody(body[0])
	}
	m.text = text
	return m, status, nil
}

// reconnectLocked drops the dead transport and reopens + reinitializes
// the modem (a firmware reset re-enumerates the USB device, so a fresh
// open is the only way back). The caller holds h.mu.
func (h *Hub) reconnectLocked() {
	h.closeTransport()
	dev, err := openSerial(h.cfg.Device)
	if err != nil {
		h.logger.Debug("gsm: reconnect open failed", "device", h.cfg.Device, "error", err)
		return
	}
	h.conn = dev
	h.connected.Store(true)
	if !h.initLocked() {
		h.logger.Warn("gsm: reconnect init failed", "device", h.cfg.Device)
		h.closeTransport()
		return
	}
	h.ready.Store(true)
	h.refreshStatusLocked()
	h.logger.Info("gsm: modem reconnected", "device", h.cfg.Device)
}
