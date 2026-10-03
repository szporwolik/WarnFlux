// Package aprspresence announces mobile APRS stations entering or
// leaving the operational range on the Meshtastic group channel.
//
// Design notes (EMCOM):
//   - only MOBILE operator stations qualify — a station that announced a
//     speed, a course or a movement track (the hub already suppresses
//     sub-30 m GPS noise); fixed homes, weather stations, digipeaters,
//     repeaters and other infrastructure never trigger announcements;
//   - the first scan is a silent baseline: stations already in range at
//     startup never spam the channel, only transitions afterwards do;
//   - repeated announcements of the same event for one station are
//     debounced (edge flapping), while the opposite event always goes
//     through — a vehicle passing the range reports both entry and exit.
package aprspresence

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/aprs"
)

// announcementMaxRunes bounds one announcement line to the mesh text
// limit (133 characters): the station comment truncates at the end.
const announcementMaxRunes = 133

// Options wires the watcher.
type Options struct {
	// Channel is the Meshtastic group channel index (1-7) for the
	// announcements; 0 disables the watcher.
	Channel int
	// Send broadcasts one line on the group channel.
	Send func(ctx context.Context, text string) error
	// Stations snapshots the current APRS station state.
	Stations func() []aprs.StationDocument
	// Lat/Lon/RadiusKM bounds the operational range: stations outside
	// it are ignored. RadiusKM <= 0 disables the distance check.
	Lat, Lon, RadiusKM float64
	// Interval is the scan cadence (default 30 seconds).
	Interval time.Duration
	// Debounce suppresses repeated announcements of the same event for
	// one station within this window (default 10 minutes).
	Debounce time.Duration
	Logger   *slog.Logger
}

// Watcher scans the APRS station state and announces range transitions
// on the group channel.
type Watcher struct {
	opts      Options
	inRange   map[string]bool
	baselined bool
	last      map[string]announcement
}

// announcement remembers the last announced event per station (for the
// debounce).
type announcement struct {
	event string
	at    time.Time
}

// New builds a watcher. A watcher without a configured channel and a
// sender is disabled and scans are no-ops.
func New(opts Options) *Watcher {
	if opts.Interval <= 0 {
		opts.Interval = 30 * time.Second
	}
	if opts.Debounce <= 0 {
		opts.Debounce = 10 * time.Minute
	}
	return &Watcher{
		opts:    opts,
		inRange: make(map[string]bool),
		last:    make(map[string]announcement),
	}
}

// Enabled reports whether the watcher can announce: a group channel is
// configured and a sender is installed.
func (w *Watcher) Enabled() bool {
	return w != nil && w.opts.Channel > 0 && w.opts.Send != nil && w.opts.Stations != nil
}

// Run scans until ctx ends. The first scan establishes the silent
// baseline: only transitions observed afterwards are announced.
func (w *Watcher) Run(ctx context.Context) {
	if !w.Enabled() {
		return
	}
	w.scan(ctx, time.Now()) // baseline
	ticker := time.NewTicker(w.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			w.scan(ctx, now)
		}
	}
}

// scan compares the current mobile-station set against the previous
// scan and announces entries and exits. The first scan only establishes
// the silent baseline.
func (w *Watcher) scan(ctx context.Context, now time.Time) {
	if !w.Enabled() {
		return
	}
	seen := make(map[string]string) // callsign -> sanitized comment
	for _, d := range w.opts.Stations() {
		if d.Self || d.IsInfrastructure() || !d.IsMobile() {
			continue
		}
		if !withinRange(d, w.opts) {
			continue
		}
		seen[d.Callsign] = strings.Join(strings.Fields(d.Comment), " ")
	}
	if !w.baselined {
		w.inRange = make(map[string]bool, len(seen))
		for call := range seen {
			w.inRange[call] = true
		}
		w.baselined = true
		return
	}
	for call := range w.inRange {
		if _, ok := seen[call]; !ok {
			w.announce(ctx, now, call, "out of range", "")
		}
	}
	for call, comment := range seen {
		if !w.inRange[call] {
			w.announce(ctx, now, call, "in range", comment)
		}
	}
	w.inRange = make(map[string]bool, len(seen))
	for call := range seen {
		w.inRange[call] = true
	}
}

// withinRange reports whether a station sits inside the operational
// ring. Positions are required; RadiusKM <= 0 disables the check.
func withinRange(d aprs.StationDocument, o Options) bool {
	if o.RadiusKM <= 0 {
		return true
	}
	if d.Position == nil {
		return false
	}
	return aprs.DistanceKM(o.Lat, o.Lon, d.Position.Latitude, d.Position.Longitude) <= o.RadiusKM
}

// announce sends one transition line, debounced per station and event.
// The entry announcement carries the station's APRS comment (sanitized
// by the caller, truncated at the mesh limit here).
func (w *Watcher) announce(ctx context.Context, now time.Time, callsign, event, comment string) {
	if last, ok := w.last[callsign]; ok && last.event == event && now.Sub(last.at) < w.opts.Debounce {
		return // edge flapping: the same event was announced moments ago
	}
	text := fmt.Sprintf("WarnFlux APRS %s %s", callsign, event)
	if comment != "" {
		text += ": " + comment
	}
	// Blunt cut at the mesh limit: whatever does not fit is dropped,
	// marked with an ellipsis so the reader knows the comment was cut.
	if r := []rune(text); len(r) > announcementMaxRunes {
		text = string(r[:announcementMaxRunes-1]) + "…"
	}
	sctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := w.opts.Send(sctx, text); err != nil {
		if w.opts.Logger != nil {
			w.opts.Logger.Warn("aprspresence: announce failed", "callsign", callsign, "error", err)
		}
		return
	}
	w.last[callsign] = announcement{event: event, at: now}
	if w.opts.Logger != nil {
		w.opts.Logger.Info("aprspresence: announced", "callsign", callsign, "event", event)
	}
}
