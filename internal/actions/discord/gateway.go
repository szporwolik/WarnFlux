package discord

// The Discord gateway (WebSocket) client carries the INBOUND path of a
// token-carrying instance: an operator writes the SAME slash commands the
// radio channels take (/help, /hazard, /ping, /weather, /alert, /emcom)
// in a direct message to the bot and the station answers in that private
// conversation. The outbound paths (webhook posts, mass direct messages)
// are untouched and need no gateway.
//
// Direct messages only, and only from senders the station knows: the
// sender's Discord user id must sit in the directory (the same profile
// field the mass panel delivers to), otherwise the message is dropped
// without an answer. Channel posts never reach the command interpreter at
// all, so no command is answered on a channel.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/radiocli"
)

const (
	// Gateway opcodes (Discord API v10).
	opDispatch       = 0
	opHeartbeat      = 1
	opIdentify       = 2
	opResume         = 6
	opReconnect      = 7
	opInvalidSession = 9
	opHello          = 10
	opHeartbeatAck   = 11

	// intents is what the listener subscribes to: DIRECT_MESSAGES
	// delivers direct messages to the bot, MESSAGE_CONTENT (privileged)
	// makes their text visible — without it every message arrives with an
	// empty content and no command could ever be parsed.
	intents = (1 << 12) | (1 << 15)

	// Application flags marking the privileged MESSAGE_CONTENT intent as
	// enabled: bit 18 for small applications, bit 19 for verified ones.
	flagMessageContentLimited = 1 << 18
	flagMessageContent        = 1 << 19

	// Close codes that make a reconnect pointless: a rejected token and a
	// refused privileged intent are configuration problems, not outages.
	closeAuthenticationFailed = 4004
	closeDisallowedIntents    = 4014

	// gatewayHandshakeTimeout bounds one dial; gatewayReadTimeout bounds
	// one silent read (Discord heartbeats roughly every 41 seconds).
	gatewayHandshakeTimeout = 15 * time.Second
	gatewayReadTimeout      = 90 * time.Second
	gatewayReconnectMin     = time.Second
	gatewayReconnectMax     = 60 * time.Second
	// gatewayIntentRecheck is how often a disabled listener re-reads the
	// application flags: enabling the privileged intent in the Developer
	// Portal then needs no station restart.
	gatewayIntentRecheck = 5 * time.Minute
)

// gatewayURL is the Discord gateway endpoint. A variable so tests can
// point a listener at their own server.
var gatewayURL = "wss://gateway.discord.gg/?v=10&encoding=json"

// errGatewayFatal marks a condition no reconnect can fix.
var errGatewayFatal = errors.New("gateway session refused")

// errGatewayReconnect is Discord asking the client to rebuild the session.
// It is routine — the gateway rotates sessions — so the listener reports it
// at INFO instead of warning about an outage that is not happening.
var errGatewayReconnect = errors.New("gateway asked for a reconnect")

// gatewayFrame is one WebSocket frame of the gateway protocol.
type gatewayFrame struct {
	Op   int             `json:"op"`
	Type string          `json:"t"`
	Data json.RawMessage `json:"d"`
	Seq  int64           `json:"s"`
}

// gatewayConn serialises the writes of one connection: the read loop may
// answer a gateway-requested heartbeat while the heartbeat goroutine
// beats on its own schedule, and gorilla allows one writer at a time.
type gatewayConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (c *gatewayConn) write(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(v)
}

func (c *gatewayConn) read(timeout time.Duration) (gatewayFrame, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return gatewayFrame{}, err
	}
	var f gatewayFrame
	if err := c.conn.ReadJSON(&f); err != nil {
		return gatewayFrame{}, err
	}
	return f, nil
}

// gatewaySession is the resumable state of one gateway session: Discord
// hands the id and the resume endpoint out on READY, and accepts them
// again on RESUME, so a dropped connection does not lose the messages
// sent while the station was away.
type gatewaySession struct {
	id  string
	url string
	seq int64
}

// gatewayMessage is the part of a MESSAGE_CREATE event the station uses.
type gatewayMessage struct {
	ID        string `json:"id"`
	ChannelID string `json:"channel_id"`
	GuildID   string `json:"guild_id"`
	Content   string `json:"content"`
	Author    struct {
		ID       string `json:"id"`
		Username string `json:"username"`
		Bot      bool   `json:"bot"`
	} `json:"author"`
}

// applicationInfo is the part of the application document that decides
// whether direct messages can be read at all.
type applicationInfo struct {
	ID    string `json:"id"`
	Flags int64  `json:"flags"`
}

// messageContentEnabled reports whether the privileged MESSAGE_CONTENT
// intent is switched on for the application.
func (a applicationInfo) messageContentEnabled() bool {
	return a.Flags&(flagMessageContentLimited|flagMessageContent) != 0
}

// portalURL is the Developer Portal page carrying the privileged-intent
// switches of this application.
func (a applicationInfo) portalURL() string {
	return "https://discord.com/developers/applications/" + a.ID + "/bot"
}

// dmListener keeps one gateway session open and answers direct messages.
// It is created only for instances that carry a bot token.
type dmListener struct {
	action *discordAction
	logger *slog.Logger
	now    func() time.Time

	mu       sync.Mutex
	cli      *radiocli.Bot
	gate     func(userID string) string
	acceptor func(payload []byte) dispatch.Acceptance
	times    func(ctx context.Context, key string) (time.Time, time.Time, string, bool)
	seen     map[string]time.Time
	bursts   map[string]replyWindow
	banners  map[string]time.Time
}

func newDMListener(a *discordAction, logger *slog.Logger) *dmListener {
	if logger == nil {
		logger = slog.Default()
	}
	return &dmListener{
		action:  a,
		logger:  logger,
		now:     time.Now,
		seen:    map[string]time.Time{},
		bursts:  map[string]replyWindow{},
		banners: map[string]time.Time{},
	}
}

// ConfigureDMs installs the seams the inbound path needs: the shared
// radio CLI, the sender gate resolving a Discord user id to a directory
// username ("" for an unknown sender), the /events acceptor carrying
// command alerts and the durable-inbox time resolver.
func (a *discordAction) ConfigureDMs(
	cli *radiocli.Bot,
	gate func(userID string) string,
	acceptor func(payload []byte) dispatch.Acceptance,
	times func(ctx context.Context, key string) (time.Time, time.Time, string, bool),
) {
	if a.dm == nil {
		return
	}
	a.dm.mu.Lock()
	defer a.dm.mu.Unlock()
	a.dm.cli = cli
	a.dm.gate = gate
	if acceptor != nil {
		a.dm.acceptor = acceptor
	}
	a.dm.times = times
}

// RunDMs listens for direct messages until ctx is cancelled. Without the
// privileged Message Content intent there is nothing to read, so it
// waits for the flag instead of opening a useless session.
func (a *discordAction) RunDMs(ctx context.Context) {
	if a.dm == nil {
		return
	}
	a.dm.run(ctx)
}

// DMCommanderOf reports the direct-message COMMAND capability of one
// plugin instance: ok is false when the instance carries no bot token,
// because a webhook can only post on a channel — it can never read a
// direct message.
func DMCommanderOf(p action.Plugin) (DMCommander, bool) {
	a, ok := p.(*discordAction)
	if !ok || a.dm == nil {
		return nil, false
	}
	return a, true
}

// DMCommander is the inbound capability of a token-carrying instance.
type DMCommander interface {
	// ConfigureDMs installs the command interpreter, the sender gate, the
	// event acceptor and the time resolver; it must run before RunDMs.
	ConfigureDMs(cli *radiocli.Bot, gate func(userID string) string,
		acceptor func(payload []byte) dispatch.Acceptance,
		times func(ctx context.Context, key string) (time.Time, time.Time, string, bool))
	// RunDMs blocks until ctx is cancelled, answering direct messages.
	RunDMs(ctx context.Context)
}

// run keeps one session alive: connect, serve, reconnect with a growing
// backoff. A refused token or a refused intent stops the listener for
// good — retrying those would only repeat the same answer.
func (l *dmListener) run(ctx context.Context) {
	if !l.waitForIntent(ctx) {
		return
	}
	var session gatewaySession
	backoff := gatewayReconnectMin
	for ctx.Err() == nil {
		start := l.now()
		err := l.connect(ctx, &session)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errGatewayFatal) {
			l.logger.Error("discord: direct-message commands stopped", "error", err)
			return
		}
		if l.now().Sub(start) > time.Minute {
			// The session worked for a while: this is a fresh outage, not
			// a broken configuration, so start the backoff from scratch.
			backoff = gatewayReconnectMin
		}
		l.logSessionEnd(err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, gatewayReconnectMax)
	}
}

// logSessionEnd reports why the session ended. Discord rotates gateway
// sessions routinely and says so with op 7 — that is an expected reconnect,
// logged at INFO, not an outage. Warning about it put ~7 false alarms a day
// next to the real ones.
func (l *dmListener) logSessionEnd(err error, backoff time.Duration) {
	if errors.Is(err, errGatewayReconnect) {
		l.logger.Info("discord: gateway asked for a reconnect", "retry_in", backoff.String())
		return
	}
	l.logger.Warn("discord: direct-message listener disconnected", "error", err, "retry_in", backoff.String())
}

// waitForIntent blocks until the privileged Message Content intent is
// enabled for the application: without it Discord delivers the text of
// every direct message as an empty string and no command could be
// parsed, so questioning the gateway would be pointless. The check
// repeats, so switching the intent on needs no station restart.
func (l *dmListener) waitForIntent(ctx context.Context) bool {
	warned := false
	for ctx.Err() == nil {
		app, err := l.application(ctx)
		switch {
		case err != nil:
			l.logger.Warn("discord: application flags unreadable", "error", err)
		case app.messageContentEnabled():
			l.logger.Info("discord: direct-message commands enabled", "application", app.ID)
			return true
		default:
			msg := "discord: direct-message commands disabled — the privileged Message Content intent is off; " +
				"enable it in the Developer Portal (Privileged Gateway Intents) to let operators run commands in a DM"
			if warned {
				l.logger.Debug(msg, "application", app.ID, "portal", app.portalURL())
			} else {
				l.logger.Warn(msg, "application", app.ID, "portal", app.portalURL())
				warned = true
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(gatewayIntentRecheck):
		}
	}
	return false
}

// application reads the station's own application document (the bot
// token authenticates it).
func (l *dmListener) application(ctx context.Context) (applicationInfo, error) {
	var app applicationInfo
	if err := l.action.apiCall(ctx, http.MethodGet, "/applications/@me", nil, &app); err != nil {
		return app, err
	}
	return app, nil
}

// connect runs one gateway session until it drops. The session state is
// updated in place, so the next attempt can resume where this one ended.
func (l *dmListener) connect(ctx context.Context, session *gatewaySession) error {
	url := resumeEndpoint(session.url)
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = gatewayHandshakeTimeout
	raw, resp, err := dialer.DialContext(ctx, url, nil)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("dial gateway: %w", err)
	}
	conn := &gatewayConn{conn: raw}
	defer raw.Close()
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stop()

	hello, err := conn.read(gatewayHandshakeTimeout)
	if err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	if hello.Op != opHello {
		return fmt.Errorf("first gateway frame carries op %d, want HELLO", hello.Op)
	}
	var hb struct {
		Interval int `json:"heartbeat_interval"`
	}
	_ = json.Unmarshal(hello.Data, &hb)
	interval := time.Duration(hb.Interval) * time.Millisecond
	if interval <= 0 {
		interval = 41 * time.Second
	}

	var seq atomic.Int64
	seq.Store(session.seq)
	var acked atomic.Bool
	acked.Store(true) // nothing to acknowledge before the first beat

	hbCtx, stopHB := context.WithCancel(ctx)
	defer stopHB()
	go l.heartbeat(hbCtx, conn, interval, &seq, &acked)

	if session.id != "" && session.seq > 0 {
		if err := conn.write(resumePayload(l.action.token, session)); err != nil {
			return fmt.Errorf("send resume: %w", err)
		}
	} else if err := conn.write(identifyPayload(l.action.token)); err != nil {
		return fmt.Errorf("send identify: %w", err)
	}

	for {
		frame, err := conn.read(gatewayReadTimeout)
		if err != nil {
			return gatewayReadError(err)
		}
		if frame.Seq > 0 {
			seq.Store(frame.Seq)
			session.seq = frame.Seq
		}
		switch frame.Op {
		case opDispatch:
			l.dispatch(ctx, frame, session)
		case opHeartbeat:
			// The gateway wants a beat right now.
			if err := conn.write(heartbeatPayload(seq.Load())); err != nil {
				return fmt.Errorf("send heartbeat: %w", err)
			}
		case opHeartbeatAck:
			acked.Store(true)
		case opReconnect:
			return errGatewayReconnect
		case opInvalidSession:
			var resumable bool
			_ = json.Unmarshal(frame.Data, &resumable)
			if !resumable {
				// The session is gone for good: the next attempt
				// identifies from scratch.
				*session = gatewaySession{}
			}
			return errors.New("gateway invalidated the session")
		}
	}
}

// heartbeat keeps the session alive. Discord expects one beat per
// interval, and a beat that never gets its ACK means a dead connection
// ("zombie"): only closing the socket brings the listener back.
func (l *dmListener) heartbeat(ctx context.Context, conn *gatewayConn, interval time.Duration, seq *atomic.Int64, acked *atomic.Bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if !acked.Swap(false) {
			l.logger.Warn("discord: gateway heartbeat unacknowledged — reconnecting")
			_ = conn.conn.Close()
			return
		}
		if err := conn.write(heartbeatPayload(seq.Load())); err != nil {
			l.logger.Debug("discord: heartbeat write failed", "error", err)
			_ = conn.conn.Close()
			return
		}
	}
}

// dispatch routes one DISPATCH frame of interest.
func (l *dmListener) dispatch(ctx context.Context, frame gatewayFrame, session *gatewaySession) {
	switch frame.Type {
	case "READY":
		var ready struct {
			SessionID        string `json:"session_id"`
			ResumeGatewayURL string `json:"resume_gateway_url"`
		}
		if err := json.Unmarshal(frame.Data, &ready); err != nil {
			l.logger.Warn("discord: unreadable ready event", "error", err)
			return
		}
		session.id = ready.SessionID
		if ready.ResumeGatewayURL != "" {
			session.url = ready.ResumeGatewayURL
		}
		l.logger.Info("discord: gateway session ready", "resumable", ready.SessionID != "")
	case "RESUMED":
		l.logger.Info("discord: gateway session resumed")
	case "MESSAGE_CREATE":
		l.handleMessageCreate(ctx, frame.Data)
	}
}

// handleMessageCreate answers one incoming direct message.
func (l *dmListener) handleMessageCreate(ctx context.Context, data []byte) {
	var m gatewayMessage
	if err := json.Unmarshal(data, &m); err != nil {
		l.logger.Warn("discord: unreadable message event", "error", err)
		return
	}
	// Direct messages from people only: a channel post is not addressed
	// to the station, and a bot (ourselves included) is never answered.
	if m.GuildID != "" || m.Author.Bot || m.Author.ID == "" {
		return
	}
	if m.ID != "" && !l.remember(m.ID) {
		l.logger.Debug("discord: duplicate message event ignored", "id", m.ID)
		return
	}
	l.handleText(ctx, m.Author.ID, m.ChannelID, m.Content)
}

// remember records one message id and reports whether it is new: Discord
// redelivers the tail of a session after a resume, and a command must
// never run twice because of it.
func (l *dmListener) remember(id string) bool {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, dup := l.seen[id]; dup {
		return false
	}
	if len(l.seen) >= dmSeenCap {
		cutoff := now.Add(-dmSeenWindow)
		for k, at := range l.seen {
			if at.Before(cutoff) {
				delete(l.seen, k)
			}
		}
	}
	l.seen[id] = now
	return true
}

// identifyPayload is the IDENTIFY frame: the bot token plus the intents
// the listener needs.
func identifyPayload(token string) map[string]any {
	return map[string]any{
		"op": opIdentify,
		"d": map[string]any{
			"token":   token,
			"intents": intents,
			"properties": map[string]string{
				"os":      runtime.GOOS,
				"browser": "WarnFlux",
				"device":  "WarnFlux",
			},
		},
	}
}

// resumePayload is the RESUME frame: it replays the messages the station
// missed while the connection was down.
func resumePayload(token string, session *gatewaySession) map[string]any {
	return map[string]any{
		"op": opResume,
		"d": map[string]any{
			"token":      token,
			"session_id": session.id,
			"seq":        session.seq,
		},
	}
}

// resumeEndpoint completes the endpoint READY handed out with the
// gateway query parameters: dialled bare, the resume host answers
// INVALID_SESSION instead of resuming (measured against the live
// gateway), so the parameters are always carried along.
func resumeEndpoint(base string) string {
	base = strings.TrimSuffix(strings.TrimSpace(base), "/")
	if base == "" {
		return gatewayURL
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" {
		return gatewayURL
	}
	q := u.Query()
	if q.Get("v") == "" {
		q.Set("v", "10")
	}
	if q.Get("encoding") == "" {
		q.Set("encoding", "json")
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// heartbeatPayload is one heartbeat frame carrying the last sequence
// number seen (the gateway answers with the sequence to resume from).
func heartbeatPayload(seq int64) map[string]any {
	return map[string]any{"op": opHeartbeat, "d": seq}
}

// gatewayReadError classifies a closed connection: a refused token or a
// refused privileged intent is fatal, everything else is an outage the
// listener retries.
func gatewayReadError(err error) error {
	var cerr *websocket.CloseError
	if !errors.As(err, &cerr) {
		return err
	}
	switch cerr.Code {
	case closeAuthenticationFailed:
		return fmt.Errorf("%w: the bot token was rejected (close %d)", errGatewayFatal, cerr.Code)
	case closeDisallowedIntents:
		return fmt.Errorf("%w: the privileged Message Content intent is not enabled for this application (close %d)",
			errGatewayFatal, cerr.Code)
	}
	return fmt.Errorf("gateway closed the connection (%d %s)", cerr.Code, cerr.Text)
}
