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

// CanonicalNumberKey normalizes a phone number across international
// dialing prefixes: digits only, with a leading "00" (the international
// access code) folded onto the same form as "+" — so "+48509558155"
// and "0048509558155" compare equal.
func CanonicalNumberKey(s string) string {
	return strings.TrimPrefix(NumberKey(s), "00")
}

// ErrNoModem is returned when the hub has no session (disabled or not
// started).
var ErrNoModem = errors.New("gsm: modem session not available")

// ErrModemRejected wraps AT commands the modem refused (+CMS ERROR /
// ERROR). Scans treat it as "nothing more to read", sends surface it.
var ErrModemRejected = errors.New("gsm: modem rejected")

// ErrTimeout is returned when the modem does not answer an AT exchange.
var ErrTimeout = errors.New("gsm: modem response timeout")

// ErrUnconfirmed reports a message the modem accepted — the body went out
// with Ctrl-Z — whose confirmation did not arrive inside the CALLER's
// budget. From Ctrl-Z on the modem owns the message, so this is a missing
// confirmation, not a missing transmission: a caller must not report
// "nothing was sent" (that invites a duplicate), and the channel is
// healthy — the exchange only needs a repair before the next command.
var ErrUnconfirmed = errors.New("gsm: sms committed, confirmation not received")

// pollInterval is the spacing of the unread-message scan.
const pollInterval = 10 * time.Second

// atTimeout bounds one AT exchange. A variable rather than a constant so
// tests can shorten it: a fake transport cannot reproduce the VTIME ticks
// a real serial port turns into idle reads.
var atTimeout = 15 * time.Second

// sendAckTimeout bounds the wait for the network accept reply after a
// submitted SMS body (the modem answers when the network takes it).
var sendAckTimeout = 45 * time.Second

// sendResyncTimeout bounds the repair probe that follows an exchange
// which ended without an answer (see resyncSendLocked).
var sendResyncTimeout = 2 * time.Second

// sendAckMargin is the slice of the CALLER's budget Send keeps for
// itself after the confirmation window: the worker disables an action
// whose call overruns its deadline, and that would take the whole channel
// down for a message that did go out (see Send).
var sendAckMargin = 250 * time.Millisecond

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

// sessionLock serializes the AT exchanges on the serial line (polling,
// sends and commands never interleave). Unlike a plain mutex it can be
// acquired WITH a context: a caller whose request was cancelled or timed
// out while the modem was busy leaves the queue instead of transmitting
// into a round nobody waits for (reported P1).
type sessionLock chan struct{}

func newSessionLock() sessionLock { return make(sessionLock, 1) }

// Lock acquires the session unconditionally: the internal callers (poll
// loop, status refresh, reconnection) have no request to honour.
func (l sessionLock) Lock() { l <- struct{}{} }

// Unlock releases the session.
func (l sessionLock) Unlock() { <-l }

// acquire takes the session, giving up with the context's error while it
// waits.
func (l sessionLock) acquire(ctx context.Context) error {
	select {
	case l <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Hub owns the AT session to one modem. All exchanges serialize under
// one lock, so polling, sends and future commands never interleave on
// the serial line.
type Hub struct {
	cfg    Config
	logger *slog.Logger
	rec    MessageRecorder
	now    func() time.Time

	// openRetry paces the background retry of a modem that is not there
	// yet (set at construction, read-only afterwards).
	openRetry time.Duration

	mu        sessionLock
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

	// sendDirty marks a session that owes a repair before the next
	// command: Send sets it when a confirmation window was cut short by
	// the caller's budget (repairing right there would make the call
	// overrun), and the next exchange runs the repair first (see
	// repairDirtyLocked). Guarded by mu.
	sendDirty bool

	// operator and csq are the last modem status (network name and
	// signal quality); refreshed periodically by the poll loop.
	op  atomic.Pointer[string]
	csq atomic.Int64

	// cli is the shared radio-command interpreter (optional): an SMS
	// that parses as a command is answered in-band and never
	// never becomes an alarm. Guarded by mu.
	cli *radiocli.Bot
	// senderGate resolves the sender's registered directory user by
	// phone number ("" = not registered); registered senders may run
	// restricted commands. Guarded by mu.
	senderGate func(from string) string
	// eventAcceptor accepts a marshalled COMMAND event (/alert and
	// and reports the local pipeline's acceptance. Guarded by
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
		openRetry:  modemOpenRetry,
		mu:         newSessionLock(),
		cmds:       map[string]*cmdRecord{},
		replyLast:  map[string]replyWindow{},
		bannerLast: map[string]time.Time{},
	}
}

// SetCLI attaches the shared radio-command interpreter (optional): an
// SMS that parses as a command is answered in-band and — except for
// — never becomes an alarm; plain texts answer with the
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
// events (/alert): the acceptor receives the marshalled
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
// modemOpenRetry is how long Start waits before another attempt at opening a
// modem that is not there yet. Plugging the stick in is a human action, so a
// minute is fast enough and keeps the log quiet.
const modemOpenRetry = time.Minute

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

// Start opens the device (unless a transport was injected), runs the AT init
// and launches the unread-message poll loop until ctx is done. A modem that is
// not plugged in yet must NOT stop the station: the open is retried in the
// background, so a device that appears after start is picked up without an
// operator restarting anything (the panel reports the missing device through
// internal/preflight meanwhile).
func (h *Hub) Start(ctx context.Context) {
	if !h.cfg.Enabled {
		return
	}
	if !h.openSession() {
		go h.awaitDevice(ctx)
		return
	}
	go h.pollLoop(ctx)
}

// retryInterval is the modem open retry pace (never zero, so a Hub built by
// hand still retries sanely).
func (h *Hub) retryInterval() time.Duration {
	if h.openRetry > 0 {
		return h.openRetry
	}
	return modemOpenRetry
}

// openSession opens the configured device (unless one was injected) and runs
// the AT init. It reports whether the session is ready for the poll loop.
func (h *Hub) openSession() bool {
	h.mu.Lock()
	conn := h.conn
	if conn == nil {
		dev, err := openSerial(h.cfg.Device)
		if err != nil {
			h.mu.Unlock()
			h.logger.Warn("gsm: cannot open modem", "device", h.cfg.Device,
				"error", err, "retry_in", h.retryInterval())
			return false
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
		h.logger.Warn("gsm: modem init failed — session closed", "device", h.cfg.Device,
			"retry_in", h.retryInterval())
		h.closeTransport()
		return false
	}
	h.ready.Store(true)
	h.logger.Info("gsm: modem ready", "device", h.cfg.Device)

	// First operator/signal snapshot right away (errors are tolerated;
	// the poll loop refreshes every few minutes).
	h.mu.Lock()
	h.refreshStatusLocked()
	h.mu.Unlock()
	return true
}

// awaitDevice retries openSession until the modem shows up (or ctx ends) and
// then hands the session to the poll loop.
func (h *Hub) awaitDevice(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(h.retryInterval()):
		}
		if h.openSession() {
			go h.pollLoop(ctx)
			return
		}
	}
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

// initLocked runs the modem setup: echo off, text-mode SMS, pinned TE
// character set and the preferred message storage. TEXT mode is the
// only mode this firmware family supports for reading stored messages
// (verified live on the E173: PDU-mode AT+CMGR returns nothing), so a
// long message arrives as separate stored parts with no user-data
// header — the parts carry no identity to group them by (reassemble
// joins what a PDU delivers) and each is stored on its own. Non-GSM-7
// bodies arrive as UCS-2 hex that DecodeSMSBody converts. CSCS="IRA" is pinned because the text-mode send path needs
// it (a leftover UCS2 setting would make the modem reject ASCII
// AT+CMGS commands). The caller holds h.mu.
func (h *Hub) initLocked() bool {
	for _, cmd := range []string{"ATE0", "AT+CMGF=1"} {
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
	// The repair runs first: it may drop a session that stayed silent,
	// and the command must not be written into that one.
	h.repairDirtyLocked()
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

// exchangePromptCtxLocked sends one AT command and waits for the SMS text
// prompt ('> ') — Huawei-style modems answer it WITHOUT a trailing
// newline, so the line-based exchange would time out. The context is
// honoured: a caller that gives up while the modem takes its time gets
// its own error, commits nothing and can abandon the pending message
// instead of sending into nobody's request. The caller holds h.mu.
func (h *Hub) exchangePromptCtxLocked(ctx context.Context, cmd string) ([]string, error) {
	// See exchangeLockedTimeout: the repair may drop the session.
	h.repairDirtyLocked()
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
		if err := ctx.Err(); err != nil {
			return lines, err
		}
		if h.now().After(deadline) {
			return lines, ErrTimeout
		}
		b, err := h.readByteCtx(ctx, deadline)
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
	return h.readByteCtx(context.Background(), deadline)
}

// readByteCtx is readByte for a caller that may give up while the modem
// stays silent: this loop is where the waiting happens, so the context
// has to end it here — a check in the caller would only run between
// reads (a modem that owes the SMS text prompt keeps them coming).
func (h *Hub) readByteCtx(ctx context.Context, deadline time.Time) (byte, error) {
	var one [1]byte
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
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
// SEPTETS are refused (an extension-table character costs two, so 81 '^'
// do not fit one message). The tx row lands in the durable history only
// after the modem accepted the message.
//
// ctx is honoured up to the moment the transmission is committed: an
// already cancelled request touches nothing, a caller waiting for a busy
// modem leaves the queue instead of transmitting into a round nobody
// waits for, and one waiting for the text prompt abandons the pending
// message (reported P1: a cancelled request still sent AT+CMGS, the body
// and Ctrl-Z, and answered success). After AT+CMGS the modem holds the
// body state and only Ctrl-Z sends it — ESC drops it — so the body is
// written only for a caller that is still there, and the caller that
// gives up before that point gets its own error.
//
// From Ctrl-Z on the modem owns the message, so the wait for the network
// accept reply runs to the END OF THE CALLER'S BUDGET at most: the worker
// disables an action whose call overruns its deadline, and a started
// transmission must never cost the channel. A window cut short by that
// budget is reported as ErrUnconfirmed (the message may well be on its
// way), and the repair the exchange needs is deferred to the next command
// instead of overrunning the call. An exchange that ends without an
// answer is repaired before the next command.
func (h *Hub) Send(ctx context.Context, number, text string) error {
	number = strings.TrimSpace(number)
	text = strings.TrimSpace(text)
	if !ValidNumber(number) {
		return fmt.Errorf("gsm: invalid addressee number %q", number)
	}
	if text == "" {
		return fmt.Errorf("gsm: message text must not be empty")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("gsm: send to %s: %w", number, err)
	}
	// Waiting for the modem is part of the request: a caller that gave up
	// while the session was busy must not send afterwards.
	if err := h.mu.acquire(ctx); err != nil {
		return fmt.Errorf("gsm: send to %s: %w", number, err)
	}
	defer h.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("gsm: send to %s: %w", number, err)
	}
	if h.conn == nil || !h.connected.Load() {
		return ErrNoModem
	}
	text = TransliterateGSM7(text)
	// The limit is septets, not characters: the extension-table symbols
	// (^ { } [ ] ~ | \ €) cost two each, so counting characters let over-
	// sized messages through to the modem (reported P2).
	if n := Septets(text); n < 0 || n > MaxSeptets {
		return ErrTooLong
	}
	// Both receive and send run in text mode (this firmware family
	// cannot read stored messages in PDU mode), so no mode switch is
	// needed around sends.
	// Wait for the prompt: nothing is committed yet, so a caller that
	// gives up here is honoured — the pending message is abandoned with
	// ESC and no body is written.
	if _, err := h.exchangePromptCtxLocked(ctx, `AT+CMGS="`+number+`"`); err != nil {
		if errors.Is(err, ErrTimeout) || ctx.Err() != nil {
			h.deferRepairLocked()
		}
		return fmt.Errorf("gsm: send to %s: %w", number, err)
	}
	if err := ctx.Err(); err != nil {
		// The modem is waiting for the body while the caller is gone:
		// ESC drops the pending message without sending anything.
		h.deferRepairLocked()
		return fmt.Errorf("gsm: send to %s: %w", number, err)
	}
	// Commit the body: from here the modem owns the message.
	if _, err := io.WriteString(h.conn, text+"\x1A"); err != nil {
		h.resyncSendLocked()
		return fmt.Errorf("gsm: write body: %w", err)
	}
	// The network accept reply can take tens of seconds on a busy
	// channel — a wider budget than the regular exchange timeout, but
	// never wider than what the caller still has: the window keeps room
	// for the caller's deadline (the worker disables an overrunning
	// action) and, when the MODEM's own budget ends the wait first, for
	// the inline repair that follows it.
	ack := sendAckTimeout
	cutShort := false
	if dl, ok := ctx.Deadline(); ok {
		if left := dl.Sub(h.now()) - sendAckMargin - sendResyncTimeout; left < ack {
			ack, cutShort = left, true
		}
	}
	if ack <= 0 {
		h.deferRepairLocked()
		return fmt.Errorf("gsm: send to %s: %w", number, ErrUnconfirmed)
	}
	if _, err := h.exchangeLockedTimeout("", "OK", ack); err != nil {
		if errors.Is(err, ErrTimeout) && cutShort {
			// The caller's budget ended the wait, not the modem: report
			// the committed message and repair before the next command,
			// so this call stays inside the deadline the worker enforces.
			h.deferRepairLocked()
			return fmt.Errorf("gsm: send to %s: %w", number, ErrUnconfirmed)
		}
		if errors.Is(err, ErrTimeout) {
			h.resyncSendLocked()
		}
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

// resyncSendLocked repairs the session after an exchange that ended
// without an answer (or lost its transport): the modem may still be
// holding a half-written body — it waits for Ctrl-Z — or be left
// mid-response. ESC abandons a pending body (Ctrl-Z would SEND it) and one
// AT probe with a short budget brings the command mode back; a session
// that does not answer either is dropped and reopened, so the next command
// starts from a known state. The caller holds h.mu.
func (h *Hub) resyncSendLocked() {
	if h.conn == nil {
		return
	}
	if _, err := io.WriteString(h.conn, "\x1b"); err != nil {
		h.logger.Warn("gsm: session repair failed, reconnecting", "error", err)
		h.reconnectLocked()
		return
	}
	if _, err := h.exchangeLockedTimeout("AT", "OK", sendResyncTimeout); err != nil {
		h.logger.Warn("gsm: session repair failed, reconnecting", "error", err)
		h.reconnectLocked()
	}
}

// deferRepairLocked hands the session repair a cut-short confirmation
// window owed to the next command: waiting for it here would push the
// call past its caller's deadline, which is the one thing a committed
// transmission must not do (the worker disables such an action).
func (h *Hub) deferRepairLocked() { h.sendDirty = true }

// repairDirtyLocked runs the deferred repair once, before a command
// writes into a session that may still owe (or hold) an answer. The
// repair gets the session's own budget: the caller that set the flag is
// already gone. The caller holds h.mu.
func (h *Hub) repairDirtyLocked() {
	if !h.sendDirty {
		return
	}
	h.sendDirty = false
	h.resyncSendLocked()
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

// decodeUCS2Sender turns an originator the modem reported as UCS2 hex back
// into text: some firmware answers +CMGR/+CMGL with the ADDRESS in UTF-16BE
// hex for numbers whose address type is not "international", so an ordinary
// short code arrives as "00380030003700360036003500380039" and the message
// history shows a hex blob instead of "80766589" (reported from the
// production station, where six carrier messages landed that way).
//
// The conversion is deliberately conservative, because a legitimately
// alphanumeric sender is made of characters that can LOOK like hex: only a
// pure-hex string of whole 4-digit units, at least two of them, whose every
// unit is an ASCII printable character, is accepted. "CAFE" (one unit,
// U+CAFE is not ASCII) and a plain number such as "80766589" (units 0x8076
// and 0x6589) therefore stay exactly as the modem sent them.
func decodeUCS2Sender(s string) string {
	if len(s) < 8 || len(s)%4 != 0 {
		return s
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return s
		}
	}
	text := make([]rune, 0, len(s)/4)
	for i := 0; i+4 <= len(s); i += 4 {
		v, err := strconv.ParseUint(s[i:i+4], 16, 32)
		if err != nil || v < 0x20 || v > 0x7e {
			return s
		}
		text = append(text, rune(v))
	}
	return strings.TrimSpace(string(text))
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

// recordBatch persists and reports one drained batch. Empty shell
// messages (some SMSCs emit them) are dropped — they carry nothing to
// display or route. Text mode stores each part of a long message as a
// separate entry with no user-data header, so the parts of a long
// message reach the history as separate rows. Nothing joins them by
// guesswork — not at display time and not on upgrade (see the v58
// migration note): the rows carry no identity to group them by, and a
// guess would also fuse independent messages that look alike.
func (h *Hub) recordBatch(raw []received) []received {
	msgs := reassemble(raw)
	kept := make([]received, 0, len(msgs))
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
		kept = append(kept, m)
	}
	return kept
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

// readIndexLocked reads one stored message by index (+CMGR=<i>) in
// TEXT mode — the only mode this firmware family supports for CMGR
// reads. It returns the decoded message with the storage status field;
// "" means the slot is empty. The caller holds h.mu.
//
// The live E173 header shape:
//
//	+CMGR: "REC UNREAD","+48509558155",,"26/10/09,02:51:43+08",145,...
//	Pong2
//
// Sent copies report "STO ..." as the first field. Bodies carrying
// non-GSM-7 characters arrive as UCS-2 hex (DecodeSMSBody converts
// them).
func (h *Hub) readIndexLocked(idx int) (m received, status string, err error) {
	lines, err := h.exchangeLocked(fmt.Sprintf("AT+CMGR=%d", idx), "OK")
	if err != nil {
		return received{}, "", err
	}
	var header string
	var body []string
	for _, l := range lines {
		if strings.HasPrefix(l, "+CMGR:") {
			header = l
			continue
		}
		if l == "OK" || l == "ERROR" || l == "" {
			continue
		}
		body = append(body, l)
	}
	if header == "" {
		return received{}, "", nil // empty slot (hole)
	}
	fields := splitCMGRFields(strings.TrimSpace(strings.TrimPrefix(header, "+CMGR:")))
	if len(fields) < 2 {
		return received{}, "", nil
	}
	first := strings.Trim(fields[0], `"`)
	if strings.HasPrefix(first, "STO") || first == "2" || first == "3" {
		return received{}, "STO", nil // sent copy: delete, never import
	}
	m.from = decodeUCS2Sender(strings.Trim(fields[1], `"`))
	m.text = DecodeSMSBody(strings.Join(body, "\n"))
	return m, "REC", nil
}

// splitCMGRFields splits a +CMGR header into comma-separated fields,
// honoring double quotes (the service-center timestamp contains a comma
// inside quotes).
func splitCMGRFields(s string) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	for _, r := range s {
		switch {
		case r == '"':
			inQuote = !inQuote
		case r == ',' && !inQuote:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	out = append(out, cur.String())
	return out
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
