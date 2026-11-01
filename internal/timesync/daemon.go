package timesync

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"go.bug.st/serial"
	"golang.org/x/sys/unix"
)

// Defaults for the configuration when the operator leaves a value out.
const (
	DefaultInterval      = 6 * time.Hour
	DefaultStepThreshold = 250 * time.Millisecond
	DefaultGPSBaud       = 9600
	// plausibilityFloor rejects a GPS clock that is obviously wrong (a
	// receiver without a fix reports its power-on date): the station must
	// never step its clock to 1999 because the antenna is unplugged.
	plausibilityFloor = "2020-01-01T00:00:00Z"
	// maxStep refuses a correction so large that it is far more likely a
	// broken source than a drifted clock.
	maxStep = 30 * 24 * time.Hour
	// gpsSamples GPS samples are median-filtered: one bad sentence must not
	// move the clock.
	gpsSamples = 3
	// gpsRetryDelay is how long the daemon waits before asking the receiver
	// again during the catch-up phase right after start-up. A receiver that was
	// just powered up needs tens of seconds for a fix, while the regular
	// interval is hours: without the retry the station would spend a whole
	// interval without a reference of its own even though the hardware was
	// there and answering, and a network outage in that window would leave it
	// free-running.
	gpsRetryDelay = 3 * time.Minute
	// gpsRetryTries bounds the catch-up attempts, so a station with no antenna
	// settles into the normal interval instead of polling the port.
	gpsRetryTries = 3
)

// Config is the time_sync section of the configuration.
type Config struct {
	// NTP servers asked while the network answers.
	NTPServers []string
	// Interval between two discipline passes (hours — the station clock
	// drifts slowly, so there is no reason to touch it more often).
	Interval time.Duration
	// GPSDevice is the serial port of the receiver ("" = no GPS).
	GPSDevice string
	GPSBaud   int
	// StepThreshold is the smallest offset worth a clock step: below it the
	// clock is left alone (a step for a few milliseconds of serial latency
	// would be noise).
	StepThreshold time.Duration
	// Serve enables the SNTP server, serving the LAN while off-grid.
	Serve       bool
	ServeListen string
	// StatusFile is rewritten after every pass so the panel and the operator
	// can see which source disciplined the clock ("" disables it).
	StatusFile string
}

// Status is what the daemon last did, mirrored to StatusFile.
type Status struct {
	Source    string    `json:"source"`     // "ntp", "gps" or "none"
	Detail    string    `json:"detail"`     // server or sentence
	OffsetMS  float64   `json:"offset_ms"`  // correction applied (or seen)
	Stepped   bool      `json:"stepped"`    // the clock was actually moved
	At        time.Time `json:"at"`         // when the pass ran
	NextAt    time.Time `json:"next_at"`    // when the next pass is due
	GPSSats   int       `json:"gps_sats"`   // satellites in view, 0 = no fix
	LastError string    `json:"last_error"` // why the pass failed, if it did
	Stratum   uint8     `json:"stratum"`    // what the SNTP server reports
}

// Clock is the wall clock the daemon reads and steps. Tests substitute it.
type Clock interface {
	Now() time.Time
	Set(time.Time) error
}

// SystemClock steps the host clock. Setting the clock needs CAP_SYS_TIME and a
// seccomp profile that lets clock_settime through — the compose file of the
// station ships both for the time-sync container only.
type SystemClock struct{}

// Now returns the system time.
func (SystemClock) Now() time.Time { return time.Now() }

// Set steps the system clock to t.
func (SystemClock) Set(t time.Time) error {
	ts := unix.NsecToTimespec(t.UnixNano())
	return unix.ClockSettime(unix.CLOCK_REALTIME, &ts)
}

// GPSReader opens the receiver; tests substitute it.
type GPSReader func(device string, baud int) (SampleSource, error)

// SampleSource yields NMEA lines from the receiver.
type SampleSource interface {
	ReadLine() (string, error)
	Close() error
}

// syncState is what the station tells the LAN about its own clock. It is kept
// as ONE value under the daemon mutex: the serving goroutine used to read a
// bare stratum field that the discipline loop wrote without a lock, which the
// race detector reported as a data race.
type syncState struct {
	// synced reports whether a reference disciplined the clock within the
	// freshness window. Before the first success — and after the window
	// lapses — the station must not claim to be a reference.
	synced bool
	// stratum is reported to clients: upstream+1 for NTP, 1 for GPS.
	stratum uint8
	source  string
	at      time.Time
}

// Daemon disciplines the station clock.
type Daemon struct {
	cfg     Config
	clock   Clock
	log     *slog.Logger
	openGPS GPSReader

	// mu guards the published status AND the sync state, so the panel and the
	// SNTP server always observe one consistent snapshot.
	mu     sync.Mutex
	status Status
	sync   syncState
	// fresh is how long a successful pass counts as current. Beyond it the
	// served time is marked unsynchronised instead of being advertised at a
	// stratum that is no longer true.
	fresh time.Duration
	// probeNTP is a seam for tests.
	probeNTP func(ctx context.Context, server string) (time.Duration, time.Duration, uint8, error)
	// gpsRetry is how long the catch-up phase waits between attempts.
	gpsRetry time.Duration
}

// NewDaemon builds the daemon over the given clock (nil = the system clock).
func NewDaemon(cfg Config, clock Clock, logger *slog.Logger) *Daemon {
	if clock == nil {
		clock = SystemClock{}
	}
	if logger == nil {
		// Never hold a typed-nil logger: the SNTP server keeps it in an
		// interface, where nil would only be discovered on the first log line.
		logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.StepThreshold <= 0 {
		cfg.StepThreshold = DefaultStepThreshold
	}
	if cfg.ServeListen == "" {
		cfg.ServeListen = ":123"
	}
	for i, srv := range cfg.NTPServers {
		cfg.NTPServers[i] = ntpAddress(srv)
	}
	d := &Daemon{
		cfg: cfg, clock: clock, log: logger, openGPS: openSerialGPS,
		// 16 is the RFC 5905 "unsynchronised" stratum: the honest answer
		// before the first successful pass.
		sync: syncState{stratum: 16},
	}
	// Two intervals: a pass may run late (GPS acquisition) without the LAN
	// being told the station lost its reference, while a genuinely stopped
	// discipline process shows up quickly.
	d.fresh = 2 * cfg.Interval
	if d.fresh < time.Minute {
		d.fresh = time.Minute
	}
	d.probeNTP = func(ctx context.Context, server string) (time.Duration, time.Duration, uint8, error) {
		return QueryNTP(ctx, server, 2*time.Second, nil)
	}
	d.gpsRetry = gpsRetryDelay
	return d
}

// ntpAddress gives a bare host the default NTP port, so "pool.ntp.org" in the
// configuration means the same thing to the client as to the validator (a
// missing port made every NTP probe fail with "missing port in address", and
// the station silently fell back to the GPS).
func ntpAddress(server string) string {
	server = strings.TrimSpace(server)
	if _, _, err := net.SplitHostPort(server); err == nil {
		return server
	}
	return net.JoinHostPort(server, "123")
}

// Run serves time and disciplines the clock until ctx is cancelled: one pass
// immediately, then one every Interval.
func (d *Daemon) Run(ctx context.Context) error {
	if d.cfg.Serve {
		server := &Server{
			Quality: d.quality,
			Now:     d.clock.Now,
			Logger:  d.log,
		}
		ready := make(chan struct{})
		errCh := make(chan error, 1)
		go func() { errCh <- server.Serve(ctx, d.cfg.ServeListen, ready) }()
		select {
		case <-ready:
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return nil
		}
	}

	// Catch-up phase: a receiver that was just powered up needs tens of seconds
	// before it reports a fix, and the pass runs immediately, so the first
	// attempt legitimately fails and NTP answers instead. Ask the receiver first
	// a few times, shortly after start-up, before settling into the interval —
	// otherwise the station advertises a borrowed reference for hours.
	gpsFirst := d.cfg.GPSDevice != ""
	retries := 0
	for {
		source := d.passWith(ctx, gpsFirst)
		wait := d.cfg.Interval
		switch {
		case source == "gps":
			gpsFirst = false
			retries = 0
		case gpsFirst && retries < gpsRetryTries:
			retries++
			wait = d.gpsRetry
			if d.log != nil {
				d.log.Info("time: gps has no fix yet, asking again",
					"in", wait.String(), "attempt", retries)
			}
		default:
			gpsFirst = false
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

// pass runs one discipline attempt in the normal order and reports which
// source answered.
func (d *Daemon) pass(ctx context.Context) string { return d.passWith(ctx, false) }

// passWith runs one discipline attempt: NTP first (cheap and precise), GPS when
// the network does not answer — the station is expected to work off-grid.
//
// gpsFirst reverses that order, which is what the catch-up phase needs: a
// receiver that has not acquired a fix yet would otherwise never be consulted
// while NTP answers, and the station would keep a borrowed reference instead of
// establishing its own. If the receiver still has nothing, the pass falls back
// to NTP so the clock is never left undisciplined.
func (d *Daemon) passWith(ctx context.Context, gpsFirst bool) string {
	start := d.clock.Now()
	st := Status{At: start, Source: "none", Stratum: d.currentStratum()}

	// The offset and its upstream stratum: NTP reports the stratum of its own
	// reference, GPS is a stratum 1 source by definition.
	var (
		offset   time.Duration
		source   string
		detail   string
		upstream uint8
		err      error
		gpsErr   error
	)
	if gpsFirst {
		offset, source, detail, upstream, err = d.measureGPS(ctx, &st)
		gpsErr = err
		if err != nil && d.log != nil {
			d.log.Debug("time: gps has no fix yet, falling back to ntp", "error", err)
		}
	}
	if !gpsFirst || err != nil {
		offset, source, detail, upstream, err = d.measureNTP(ctx)
		switch {
		case err == nil:
		case !gpsFirst:
			if d.log != nil && len(d.cfg.NTPServers) > 0 {
				d.log.Debug("time: ntp unavailable, trying the GPS receiver", "error", err)
			}
			offset, source, detail, upstream, err = d.measureGPS(ctx, &st)
		case gpsErr != nil:
			// Both were asked and both failed: report both, the panel shows it.
			err = fmt.Errorf("ntp: %v; gps: %v", err, gpsErr)
		}
	}
	if err != nil {
		st.LastError = err.Error()
		st.NextAt = d.clock.Now().Add(d.cfg.Interval)
		d.writeStatus(st)
		if d.log != nil {
			d.log.Warn("time: no usable source, clock left alone", "error", err)
		}
		return ""
	}

	st.Source, st.Detail = source, detail
	st.OffsetMS = float64(offset.Microseconds()) / 1000

	// The safety limit applies to EVERY source, not just the GPS: a broken or
	// spoofed NTP server reporting a year-off time moved the test clock by 365
	// days, and such a jump re-orders alarm expiry, retention and TLS validity.
	// A station booting without an RTC is hours off, never weeks.
	if absDuration(offset) > maxStep {
		st.LastError = fmt.Sprintf("offset %s exceeds the %d-day safety limit — check or reset the system clock",
			offset.Round(time.Second), int(maxStep.Hours()/24))
		st.NextAt = d.clock.Now().Add(d.cfg.Interval)
		d.writeStatus(st)
		if d.log != nil {
			d.log.Warn("time: correction refused, clock left alone",
				"source", source, "detail", detail, "offset", offset.Round(time.Second).String(),
				"limit", maxStep.String())
		}
		return source
	}

	if absDuration(offset) >= d.cfg.StepThreshold {
		target := d.clock.Now().Add(offset)
		if err := d.clock.Set(target); err != nil {
			st.LastError = "set clock: " + err.Error()
			st.NextAt = d.clock.Now().Add(d.cfg.Interval)
			d.writeStatus(st)
			if d.log != nil {
				d.log.Warn("time: cannot step the clock", "error", err,
					"offset", offset.Round(time.Millisecond).String(),
					"hint", "the container needs CAP_SYS_TIME and a seccomp profile allowing clock_settime")
			}
			return source
		}
		st.Stepped = true
		if d.log != nil {
			d.log.Info("time: clock stepped", "source", source, "detail", detail,
				"offset", offset.Round(time.Millisecond).String())
		}
	} else if d.log != nil {
		d.log.Info("time: clock is good", "source", source, "detail", detail,
			"offset", offset.Round(time.Millisecond).String(),
			"threshold", d.cfg.StepThreshold.String())
	}

	// Stamp the pass when it FINISHED, not when it started: after a step the
	// wall clock jumps by the whole correction — hours, when a station boots
	// without an RTC — so a start stamp would tell the panel and the digest
	// that the pass ran hours ago while the clock had just been corrected.
	st.At = d.clock.Now()
	st.NextAt = st.At.Add(d.cfg.Interval)
	// The reported distance from a reference: the GPS receiver IS a reference
	// (measureGPS returns 1), an NTP server leaves us one hop further away than
	// it is (measureNTP returns upstream+1).
	if upstream == 0 {
		upstream = 16 // unknown quality: never advertise a good stratum
	}
	st.Stratum = upstream
	d.markSynced(source, st.Stratum, st.At)
	d.writeStatus(st)
	return source
}

// markSynced records one successful pass as the current clock quality.
func (d *Daemon) markSynced(source string, stratum uint8, at time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sync = syncState{synced: true, stratum: stratum, source: source, at: at}
}

// currentStratum reports the stratum the server would advertise right now.
func (d *Daemon) currentStratum() uint8 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sync.stratum
}

// quality reports what the station tells the LAN about its clock: a reference
// only while a successful pass is recent. Past the window the answers are
// marked unsynchronised (LI=3, stratum 16) instead of advertising a stratum
// that stopped being true when the discipline began failing.
func (d *Daemon) quality() Quality {
	d.mu.Lock()
	state := d.sync
	d.mu.Unlock()
	if !state.synced || d.clock.Now().Sub(state.at) > d.fresh {
		return Quality{}
	}
	return Quality{Synced: true, Stratum: state.stratum}
}

// measureNTP asks every configured server and keeps the answer with the
// shortest round-trip (the least uncertain one).
func (d *Daemon) measureNTP(ctx context.Context) (time.Duration, string, string, uint8, error) {
	if len(d.cfg.NTPServers) == 0 {
		return 0, "", "", 0, fmt.Errorf("no ntp servers configured")
	}
	var (
		best     time.Duration
		bestRTT  time.Duration
		bestSrv  string
		bestStrt uint8
		lastErr  error
	)
	for _, server := range d.cfg.NTPServers {
		// A server that reports itself unsynchronised is skipped, not trusted:
		// the next one on the list may be healthy.
		offset, rtt, stratum, err := d.probeNTP(ctx, server)
		if err != nil {
			lastErr = err
			continue
		}
		if bestSrv == "" || rtt < bestRTT {
			// One hop further from a reference than the server we asked.
			best, bestRTT, bestSrv, bestStrt = offset, rtt, server, stratum+1
		}
	}
	if bestSrv == "" {
		if lastErr == nil {
			lastErr = fmt.Errorf("no ntp server answered")
		}
		return 0, "", "", 0, lastErr
	}
	return best, "ntp", bestSrv, bestStrt, nil
}

// measureGPS reads the receiver until it reports a plausible fix and returns
// the median of a few samples, so one bad sentence cannot move the clock.
func (d *Daemon) measureGPS(ctx context.Context, st *Status) (time.Duration, string, string, uint8, error) {
	if d.cfg.GPSDevice == "" {
		return 0, "", "", 0, fmt.Errorf("no GPS device configured")
	}
	floor, _ := time.Parse(time.RFC3339, plausibilityFloor)
	src, err := d.openGPS(d.cfg.GPSDevice, d.cfg.GPSBaud)
	if err != nil {
		return 0, "", "", 0, fmt.Errorf("open gps %s: %w", d.cfg.GPSDevice, err)
	}
	defer src.Close()

	deadline := time.Now().Add(30 * time.Second)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	var (
		samples  []time.Duration
		sentence string
		badTime  int
		readErr  error
	)
	for time.Now().Before(deadline) && len(samples) < gpsSamples {
		if ctx.Err() != nil {
			break
		}
		line, err := src.ReadLine()
		if err != nil {
			// A receiver that stops talking is reported below, together with
			// what it did say: "no fix" is the useful diagnosis, a bare EOF is
			// not.
			readErr = err
			break
		}
		fix, ok, err := ParseNMEA(line)
		if !ok || err != nil {
			continue
		}
		if fix.Satellites > 0 {
			st.GPSSats = fix.Satellites
		}
		if fix.Time.IsZero() || !fix.Valid {
			continue
		}
		if fix.Time.Before(floor) {
			badTime++
			continue
		}
		samples = append(samples, fix.Time.Sub(d.clock.Now()))
		sentence = fix.Sentence
	}
	if len(samples) < gpsSamples {
		if badTime > 0 {
			return 0, "", "", 0, fmt.Errorf("gps reports an implausible date (no satellite fix?)")
		}
		if readErr != nil {
			return 0, "", "", 0, fmt.Errorf("read gps %s: %w", d.cfg.GPSDevice, readErr)
		}
		return 0, "", "", 0, fmt.Errorf("gps delivered no time within the wait")
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return samples[len(samples)/2], "gps", sentence, 1, nil
}

// writeStatus mirrors the last pass into the status file (best effort: a
// read-only data volume must not stop the discipline).
func (d *Daemon) writeStatus(st Status) {
	d.mu.Lock()
	d.status = st
	d.mu.Unlock()
	if d.cfg.StatusFile == "" {
		return
	}
	body, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return
	}
	tmp := d.cfg.StatusFile + ".tmp"
	if err := os.WriteFile(tmp, append(body, '\n'), 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, d.cfg.StatusFile)
}

// Status returns the last pass result.
func (d *Daemon) Status() Status {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.status
}

// LoadStatus reads the status file the discipline process publishes, so the
// panel and the daily digest can show where the station's time comes from
// instead of assuming it is right because the container is up.
func LoadStatus(path string) (Status, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return Status{}, err
	}
	var st Status
	if err := json.Unmarshal(body, &st); err != nil {
		return Status{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return st, nil
}

// Abs is the absolute value of a duration.
func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// serialSource adapts the serial port to SampleSource.
type serialSource struct {
	port serial.Port
	r    *bufio.Reader
}

// ReadLine returns one NMEA line, ignoring the port's own buffering quirks.
func (s *serialSource) ReadLine() (string, error) {
	return s.r.ReadString('\n')
}

// Close closes the port.
func (s *serialSource) Close() error { return s.port.Close() }

// openSerialGPS opens the receiver at the configured baud rate.
func openSerialGPS(device string, baud int) (SampleSource, error) {
	if baud <= 0 {
		baud = DefaultGPSBaud
	}
	port, err := serial.Open(device, &serial.Mode{BaudRate: baud})
	if err != nil {
		return nil, err
	}
	// A silent receiver must not hang the pass: the read deadline is the
	// caller's wait, the port answers with what it has.
	if err := port.SetReadTimeout(2 * time.Second); err != nil {
		port.Close()
		return nil, err
	}
	return &serialSource{port: port, r: bufio.NewReader(port)}, nil
}
