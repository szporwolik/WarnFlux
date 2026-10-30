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

// Daemon disciplines the station clock.
type Daemon struct {
	cfg     Config
	clock   Clock
	log     *slog.Logger
	openGPS GPSReader

	// stratum what the local server reports to clients
	stratum uint8
	// statusMu guards status, which the discipline loop writes while the panel
	// (or a caller in another goroutine) may read it.
	statusMu sync.Mutex
	status   Status
	// probeNTP and nowNTP are seams for tests.
	probeNTP func(ctx context.Context, server string) (time.Duration, time.Duration, error)
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
	d := &Daemon{cfg: cfg, clock: clock, log: logger, stratum: 3, openGPS: openSerialGPS}
	d.probeNTP = func(ctx context.Context, server string) (time.Duration, time.Duration, error) {
		return QueryNTP(ctx, server, 2*time.Second, nil)
	}
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
			Stratum: func() uint8 { return d.stratum },
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

	for {
		d.pass(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(d.cfg.Interval):
		}
	}
}

// pass runs one discipline attempt: NTP first (cheap and precise), GPS when
// the network does not answer — the station is expected to work off-grid.
func (d *Daemon) pass(ctx context.Context) {
	start := d.clock.Now()
	st := Status{At: start, Source: "none", Stratum: d.stratum}

	offset, source, detail, err := d.measureNTP(ctx)
	if err != nil {
		if d.log != nil && len(d.cfg.NTPServers) > 0 {
			d.log.Debug("time: ntp unavailable, trying the GPS receiver", "error", err)
		}
		offset, source, detail, err = d.measureGPS(ctx, &st)
	}
	if err != nil {
		st.LastError = err.Error()
		st.NextAt = d.clock.Now().Add(d.cfg.Interval)
		d.writeStatus(st)
		if d.log != nil {
			d.log.Warn("time: no usable source, clock left alone", "error", err)
		}
		return
	}

	st.Source, st.Detail = source, detail
	st.OffsetMS = float64(offset.Microseconds()) / 1000
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
			return
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

	d.stratum = 1
	st.Stratum = d.stratum
	st.NextAt = d.clock.Now().Add(d.cfg.Interval)
	d.writeStatus(st)
}

// measureNTP asks every configured server and keeps the answer with the
// shortest round-trip (the least uncertain one).
func (d *Daemon) measureNTP(ctx context.Context) (time.Duration, string, string, error) {
	if len(d.cfg.NTPServers) == 0 {
		return 0, "", "", fmt.Errorf("no ntp servers configured")
	}
	var (
		best    time.Duration
		bestRTT time.Duration
		bestSrv string
		lastErr error
	)
	for _, server := range d.cfg.NTPServers {
		offset, rtt, err := d.probeNTP(ctx, server)
		if err != nil {
			lastErr = err
			continue
		}
		if bestSrv == "" || rtt < bestRTT {
			best, bestRTT, bestSrv = offset, rtt, server
		}
	}
	if bestSrv == "" {
		if lastErr == nil {
			lastErr = fmt.Errorf("no ntp server answered")
		}
		return 0, "", "", lastErr
	}
	return best, "ntp", bestSrv, nil
}

// measureGPS reads the receiver until it reports a plausible fix and returns
// the median of a few samples, so one bad sentence cannot move the clock.
func (d *Daemon) measureGPS(ctx context.Context, st *Status) (time.Duration, string, string, error) {
	if d.cfg.GPSDevice == "" {
		return 0, "", "", fmt.Errorf("no GPS device configured")
	}
	floor, _ := time.Parse(time.RFC3339, plausibilityFloor)
	src, err := d.openGPS(d.cfg.GPSDevice, d.cfg.GPSBaud)
	if err != nil {
		return 0, "", "", fmt.Errorf("open gps %s: %w", d.cfg.GPSDevice, err)
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
			return 0, "", "", fmt.Errorf("gps reports an implausible date (no satellite fix?)")
		}
		if readErr != nil {
			return 0, "", "", fmt.Errorf("read gps %s: %w", d.cfg.GPSDevice, readErr)
		}
		return 0, "", "", fmt.Errorf("gps delivered no time within the wait")
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	median := samples[len(samples)/2]
	if absDuration(median) > maxStep {
		return 0, "", "", fmt.Errorf("gps offset %s exceeds the safety limit", median)
	}
	return median, "gps", sentence, nil
}

// writeStatus mirrors the last pass into the status file (best effort: a
// read-only data volume must not stop the discipline).
func (d *Daemon) writeStatus(st Status) {
	d.statusMu.Lock()
	d.status = st
	d.statusMu.Unlock()
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
	d.statusMu.Lock()
	defer d.statusMu.Unlock()
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
