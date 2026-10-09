// Package gsm owns the optional cellular modem integration: a serial AT
// modem receives and sends SMS messages. The architecture mirrors the
// APRS/Meshtastic hubs — one Hub owns the transport session, the durable
// history rides the MessageRecorder, and incoming text funnels through a
// single handler hook so commands can be added later without touching
// the transport.
package gsm

import (
	"context"
	"encoding/json"
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

	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/radiocli"
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

// NumberKey normalizes a phone number for identity comparison: digits
// only, so "+48 509 558 155" and "48509558155" compare equal.
func NumberKey(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

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

// received is one consumed inbox record (possibly one part of a
// concatenated message).
type received struct {
	from, text string
	// concat fields identify one part of a concatenated message
	// (from the PDU user-data header); zero = ordinary message.
	concatRef   uint16
	concatTotal int
	concatSeq   int
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

	// messageSink is the optional MQTT message-feed publisher: every
	// received and sent SMS is published as a non-retained document
	// under gsm/messages, mirroring the APRS and Meshtastic feeds.
	// Guarded by mu.
	messageSink func(ctx context.Context, topic string, retained bool, payload []byte) error

	// storage is the active SMS storage id ("SM" or "ME") — guarded
	// by mu, chosen by initLocked.
	storage string

	// operator and csq are the last modem status (network name and
	// signal quality); refreshed periodically by the poll loop.
	op  atomic.Pointer[string]
	csq atomic.Int64

	// cli is the shared radio-command interpreter (optional): an SMS
	// that parses as a command is answered in-band and (except /debug)
	// never becomes an alarm. Guarded by mu.
	cli *radiocli.Bot
	// senderGate resolves the sender's registered directory user by
	// phone number ("" = not registered); registered senders may run
	// restricted commands. Guarded by mu.
	senderGate func(from string) string
	// eventAcceptor accepts a marshalled COMMAND event (/alert and
	// /debug) and reports the local pipeline's acceptance. Guarded by
	// mu.
	eventAcceptor func(payload []byte) dispatch.Acceptance
	// eventTimes resolves the durable command registry for one command
	// key (lifecycle anchor + recorded result). Guarded by mu.
	eventTimes func(ctx context.Context, key string) (eff, exp time.Time, result string, ok bool)
	// cmds remembers executed commands by sender + content identity,
	// so a repeated text replays the previous result instead of
	// re-executing the command. Guarded by mu.
	cmds map[string]*cmdRecord
	// replyLast bounds ALL automatic replies per sender (command
	// answers, banners, denials); guarded by mu.
	replyLast map[string]replyWindow
	// bannerLast / bannerNext pace the plain-message banner replies
	// (per sender and globally); guarded by mu.
	bannerLast map[string]time.Time
	bannerNext time.Time
}

// NewHub builds the hub (nothing is opened yet).
func NewHub(cfg Config, rec MessageRecorder, logger *slog.Logger) *Hub {
	if logger == nil {
		logger = slog.Default()
	}
	return &Hub{
		cfg:        cfg,
		logger:     logger,
		rec:        rec,
		now:        time.Now,
		cmds:       map[string]*cmdRecord{},
		replyLast:  map[string]replyWindow{},
		bannerLast: map[string]time.Time{},
	}
}

// SetCLI attaches the shared radio-command interpreter (optional): an
// SMS that parses as a command is answered in-band and — except for
// /debug — never becomes an alarm; plain texts answer with the
// installation banner. The last registration wins; nil clears it.
func (h *Hub) SetCLI(b *radiocli.Bot) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cli = b
}

// SetMessageSink attaches the MQTT message feed publisher (optional):
// every received and sent SMS is published as a non-retained document
// under gsm/messages, mirroring the APRS and Meshtastic message feeds.
func (h *Hub) SetMessageSink(fn func(ctx context.Context, topic string, retained bool, payload []byte) error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messageSink = fn
}

// SetSenderGate installs the sender allow-list lookup: it resolves the
// sender's phone number to the directory username that registered it
// ("" = unknown sender). Without the gate no SMS can run restricted
// commands or raise alarms. The last registration wins; nil clears it.
func (h *Hub) SetSenderGate(fn func(from string) string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.senderGate = fn
}

// SetEventAcceptor installs the LOCAL acceptance path for command
// events (/debug, /alert): the acceptor receives the marshalled
// canonical /events payload and reports how the pipeline took it.
// The last registration wins; nil clears it.
func (h *Hub) SetEventAcceptor(fn func(payload []byte) dispatch.Acceptance) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.eventAcceptor = fn
}

// SetEventTimesResolver installs the durable command-registry lookup
// (the storage half of the restart-proof command dedup). The last
// registration wins; nil clears it.
func (h *Hub) SetEventTimesResolver(fn func(ctx context.Context, key string) (eff, exp time.Time, result string, ok bool)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.eventTimes = fn
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
	h.logger.Info("gsm: modem session closed")
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

// initLocked runs the modem setup: echo off, PDU-mode SMS, pinned TE
// character set and the preferred message storage. PDU mode is the only
// mode that carries the user-data header, so concatenated messages and
// non-GSM-7 alphabets work on receive. CSCS="IRA" is pinned because the
// text-mode send path needs it (a leftover UCS2 setting would make the
// modem reject ASCII AT+CMGS commands). The caller holds h.mu.
func (h *Hub) initLocked() bool {
	for _, cmd := range []string{"ATE0", "AT+CMGF=0"} {
		if _, err := h.exchangeLocked(cmd, "OK"); err != nil {
			return false
		}
	}
	// Tolerated if rejected — the pin is a convenience, not a feature.
	_, _ = h.exchangeLocked(`AT+CSCS="IRA"`, "OK")

	// Storage selection: prefer modem memory (ME — 23 slots) over the
	// SIM (SM). Drain whatever the SIM still holds first so pending
	// messages are not stranded when the active storage moves.
	h.storage = "SM"
	if lines, err := h.exchangeLocked("AT+CPMS=?", "OK"); err == nil && cpmsSupports(lines, "ME") {
		h.pollUnreadLocked() // best-effort drain of SIM leftovers
		if _, err := h.exchangeLocked(`AT+CPMS="ME","ME","ME"`, "OK"); err == nil {
			if lines, err := h.exchangeLocked("AT+CPMS?", "OK"); err == nil && cpmsActive(lines) == "ME" {
				h.storage = "ME"
			} else {
				_, _ = h.exchangeLocked(`AT+CPMS="SM","SM","SM"`, "OK")
			}
		}
	}
	return true
}

// cpmsSupports reports whether a +CPMS=? reply lists the given storage
// id ("SM", "ME", ...).
func cpmsSupports(lines []string, id string) bool {
	for _, l := range lines {
		if !strings.HasPrefix(l, "+CPMS:") {
			continue
		}
		for _, tok := range strings.FieldsFunc(l, func(r rune) bool {
			return r == '(' || r == ')' || r == ','
		}) {
			if strings.Trim(strings.TrimSpace(tok), `"`) == id {
				return true
			}
		}
	}
	return false
}

// cpmsActive returns the first storage id of a +CPMS? reply (the
// active read storage).
func cpmsActive(lines []string) string {
	for _, l := range lines {
		if !strings.HasPrefix(l, "+CPMS:") {
			continue
		}
		rest := strings.TrimSpace(strings.TrimPrefix(l, "+CPMS:"))
		if len(rest) > 1 && rest[0] == '"' {
			if j := strings.IndexByte(rest[1:], '"'); j >= 0 {
				return rest[1 : 1+j]
			}
		}
	}
	return ""
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

// Send transmits one SMS. The Huawei E173 firmware cannot send PDU-mode
// messages (CMS ERROR 500) and does not split long text-mode sends
// (CMS ERROR 305), so sending uses text mode with the single-segment
// limits: non-GSM-7 characters are transliterated and texts over 160
// characters are refused. The tx row lands in the durable history only
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
	text = TransliterateGSM7(text)
	if len([]rune(text)) > 160 {
		return ErrTooLong
	}
	// Receiving runs in PDU mode (CMGF=0) — sending switches to text
	// mode for this one message and always switches back.
	if _, err := h.exchangeLocked("AT+CMGF=1", "OK"); err != nil {
		return fmt.Errorf("gsm: send to %s: %w", number, err)
	}
	defer func() {
		if _, err := h.exchangeLocked("AT+CMGF=0", "OK"); err != nil {
			h.logger.Warn("gsm: text-mode restore failed", "to", number, "error", err)
		}
	}()
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
	h.logger.Info("gsm: sms sent", "to", number, "len", len([]rune(text)))
	h.publishMessageFeed("tx", "self", number, text, h.now())
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

// pollLoop scans unread inbox messages every pollInterval and runs the
// hourly storage maintenance.
func (h *Hub) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	lastStatus := time.Time{}
	lastDeep := time.Time{}
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
			// Hourly deep clean: drain the inactive storage and clear the
			// status-report mailbox so nothing clogs over years of
			// uptime (the zero-valued timers run both on the first tick).
			if h.now().Sub(lastDeep) > time.Hour {
				lastDeep = h.now()
				h.deepCleanLocked()
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
			// Command routing runs outside the lock — replies call Send.
			for _, m := range msgs {
				h.routeMessage(m.from, m.text)
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
	} else {
		h.logger.Debug("gsm: operator query failed", "error", err)
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
	} else {
		h.logger.Debug("gsm: signal query failed", "error", err)
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
	used, maxSlots, err := h.storageStatsLocked()
	if err != nil {
		h.logger.Debug("gsm: unread scan failed", "error", err)
		return out, false
	}
	// An empty storage needs no slot probing — this spares the modem
	// ~20 commands every poll tick for years of operation.
	if used == 0 {
		return nil, true
	}
	// The firmware leaves HOLES (a deleted slot stays empty instead of
	// compacting), so every slot 1..max must be probed individually.
	var raw []received
	for idx := 1; idx <= maxSlots && idx <= 64; idx++ {
		m, status, err := h.readIndexLocked(idx)
		if errors.Is(err, ErrModemRejected) {
			// Indices beyond the firmware's real storage are refused —
			// the scan is simply done.
			return h.recordBatch(raw), true
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
		raw = append(raw, m)
		if _, err := h.exchangeLocked(fmt.Sprintf("AT+CMGD=%d", idx), "OK"); err != nil && !errors.Is(err, ErrModemRejected) {
			h.logger.Debug("gsm: inbox delete failed", "error", err)
			return h.recordBatch(raw), true
		}
	}
	return h.recordBatch(raw), true
}

// deepCleanLocked drains the inactive message storage (SIM or modem
// memory) into the history and bulk-clears the status-report mailbox —
// over years of uptime nothing is allowed to accumulate in any of the
// modem's mailboxes. The receive storage (mem3) stays on the active one
// the whole time, so no delivery window opens. The caller holds h.mu.
func (h *Hub) deepCleanLocked() {
	if h.conn == nil || !h.connected.Load() {
		return
	}
	active := h.storage
	if active != "SM" && active != "ME" {
		active = "ME"
	}
	other := "SM"
	if active == "SM" {
		other = "ME"
	}
	// Only the READ storage moves — mem2/mem3 keep the active storage.
	set := func(mem1 string) bool {
		_, err := h.exchangeLocked(fmt.Sprintf(`AT+CPMS="%s","%s","%s"`, mem1, active, active), "OK")
		return err == nil
	}
	if set(other) {
		h.pollUnreadLocked() // import + delete everything pending there
		if !set(active) {
			h.logger.Warn("gsm: storage restore failed", "active", active)
		}
	}
	// Status reports are delivery receipts we never consume — clear the
	// mailbox wholesale so it cannot fill up. The firmware refuses the
	// delflag form on some builds; then it simply stays empty (we never
	// request reports).
	if set("SR") {
		if _, err := h.exchangeLocked("AT+CMGD=1,4", "OK"); err != nil {
			h.logger.Debug("gsm: status-report clear failed", "error", err)
		}
		if !set(active) {
			h.logger.Warn("gsm: storage restore failed", "active", active)
		}
	}
	h.logger.Info("gsm: storage maintenance done", "active", active)
}

// reassemble joins the parts of concatenated messages in their UDH
// sequence order. The E173 stores each segment of a long message as a
// separate slot (order not guaranteed), so the user-data header — the
// only standards-defined ordering — decides the final text.
func reassemble(raw []received) []received {
	type key struct {
		from string
		ref  uint16
	}
	type group struct{ parts []received }
	groups := map[key]*group{}
	var keys []key
	var out []received
	flush := func(k key) {
		g := groups[k]
		parts := append([]received(nil), g.parts...)
		// Sequence order; missing parts leave a gap but never scramble.
		for i := 1; i < len(parts); i++ {
			for j := i; j > 0 && parts[j].concatSeq < parts[j-1].concatSeq; j-- {
				parts[j], parts[j-1] = parts[j-1], parts[j]
			}
		}
		var sb strings.Builder
		for _, p := range parts {
			sb.WriteString(p.text)
		}
		out = append(out, received{from: parts[0].from, text: sb.String()})
		delete(groups, k)
	}
	for _, r := range raw {
		if r.concatTotal <= 0 {
			out = append(out, r)
			continue
		}
		k := key{r.from, r.concatRef}
		g, exists := groups[k]
		if !exists {
			g = &group{}
			groups[k] = g
			keys = append(keys, k)
		}
		g.parts = append(g.parts, r)
		if len(g.parts) == r.concatTotal {
			flush(k)
		}
	}
	// Incomplete groups (lost parts): flush what arrived, still in
	// sequence order.
	for _, k := range keys {
		if _, exists := groups[k]; exists {
			flush(k)
		}
	}
	return out
}

// recordBatch persists and reports one drained batch (after part
// reassembly). Empty shell messages (zero-length PDUs some SMSCs emit)
// are dropped — they carry nothing to display.
func (h *Hub) recordBatch(raw []received) []received {
	msgs := reassemble(raw)
	for _, m := range msgs {
		if strings.TrimSpace(m.from) == "" || strings.TrimSpace(m.text) == "" {
			h.logger.Debug("gsm: empty shell message dropped")
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
		h.logger.Info("gsm: sms received", "from", m.from, "len", len([]rune(m.text)))
		h.publishMessageFeed("rx", m.from, "self", m.text, h.now())
	}
	return msgs
}

// publishMessageFeed pushes one SMS document onto the MQTT message
// feed (gsm/messages, non-retained). Fire-and-forget like the automatic
// replies: the feed is auxiliary and must never stall the serial
// session. The caller holds h.mu (both call sites do: the tx path in
// Send and the rx batch drain).
func (h *Hub) publishMessageFeed(direction, from, to, text string, at time.Time) {
	sink := h.messageSink
	if sink == nil {
		return
	}
	payload, err := json.Marshal(map[string]any{
		"direction": direction,
		"from":      from,
		"to":        to,
		"text":      text,
		"at":        at.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := sink(ctx, "gsm/messages", false, payload); err != nil {
			h.logger.Warn("gsm: message feed publish failed", "direction", direction, "error", err)
		}
	}()
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
// returns the decoded PDU with the storage status field. PDU mode
// reports the status as a numeric field ("0"/"1" = received, "2"/"3" =
// sent copies) or a textual one; "" means the slot is empty. The
// caller holds h.mu.
func (h *Hub) readIndexLocked(idx int) (m received, status string, err error) {
	lines, err := h.exchangeLocked(fmt.Sprintf("AT+CMGR=%d", idx), "OK")
	if err != nil {
		return received{}, "", err
	}
	var pduHex string
	for _, l := range lines {
		if strings.HasPrefix(l, "+CMGR:") {
			// +CMGR: 0,,26 or +CMGR: "REC UNREAD",... — the first field
			// is the status, the body follows as a hex PDU.
			rest := strings.TrimSpace(strings.TrimPrefix(l, "+CMGR:"))
			first := rest
			if i := strings.IndexByte(rest, ','); i >= 0 {
				first = rest[:i]
			}
			first = strings.Trim(strings.TrimSpace(first), `"`)
			switch {
			case first == "":
				// header without a status — decide from the body below
			case first == "2" || first == "3" || strings.HasPrefix(first, "STO"):
				status = "STO"
			default:
				status = "REC"
			}
			continue
		}
		if l == "OK" || l == "ERROR" || l == "" {
			continue
		}
		if isHexLine(l) {
			pduHex += l
		}
	}
	if pduHex == "" {
		return received{}, "", nil // empty slot (hole)
	}
	d, err := ParseDeliverPDU(pduHex)
	if err != nil {
		// A stored SMS-SUBMIT (sent copy) has a different MTI — the
		// parse fails and the slot is deleted below as a sent copy.
		if status == "" {
			status = "STO"
		}
		return received{}, status, nil
	}
	if status == "" {
		status = "REC"
	}
	m.from = d.From
	m.text = d.Text
	m.concatRef = d.ConcatRef
	m.concatTotal = d.ConcatTotal
	m.concatSeq = d.ConcatSeq
	return m, status, nil
}

// isHexLine reports whether the line is a pure hex PDU body line.
func isHexLine(s string) bool {
	if len(s) == 0 || len(s)%2 != 0 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
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
