// Package preflight keeps the station's unmet hardware dependencies visible.
//
// The station must start even when a device it is configured to use is not
// plugged in: a missing GSM modem or AIOC cable used to take the whole process
// down (or, in a container, refuse to be created at all), which also killed
// the panel that would have explained why. Instead every dependency is probed,
// a missing one is logged once and listed on the panel's health card, and the
// probe keeps running so a device that appears later clears its own finding.
package preflight

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Condition severities. "bad" means something the configuration promises
// cannot work at all; "warn" means it works with a limitation.
const (
	SeverityBad  = "bad"
	SeverityWarn = "warn"
)

// Condition is one unmet dependency as the panel reports it.
type Condition struct {
	// Key is the stable identifier ("gsm.modem"); the panel translates it
	// into a human name and uses it to clear the finding.
	Key string
	// Severity is SeverityBad or SeverityWarn.
	Severity string
	// Detail is the technical "what exactly is missing" text (device path
	// and the probe's error), shown next to the name.
	Detail string
	// Since is when the dependency was first found missing.
	Since time.Time
}

// Probe reports nil while the dependency is satisfied.
type Probe func() error

type check struct {
	key      string
	severity string
	probe    Probe
}

// Monitor probes a set of dependencies and remembers which ones are missing.
type Monitor struct {
	logger   *slog.Logger
	interval time.Duration
	now      func() time.Time

	mu     sync.Mutex
	checks []check
	active map[string]Condition
}

// New returns a monitor that logs dependency transitions to logger. The probe
// interval is deliberately short: a device plugged in after start must clear
// its own finding without an operator restarting anything.
func New(logger *slog.Logger) *Monitor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Monitor{
		logger:   logger,
		interval: 15 * time.Second,
		now:      time.Now,
		active:   map[string]Condition{},
	}
}

// Add registers one dependency. Severity is the class its failure gets on the
// health card. A probe that is nil is ignored (the dependency is not
// configured, so there is nothing to report).
func (m *Monitor) Add(key, severity string, p Probe) {
	if p == nil {
		return
	}
	if severity == "" {
		severity = SeverityBad
	}
	m.mu.Lock()
	m.checks = append(m.checks, check{key: key, severity: severity, probe: p})
	m.mu.Unlock()
}

// Run probes immediately and then on every interval until ctx is done.
func (m *Monitor) Run(ctx context.Context) {
	m.Probe()
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.Probe()
		}
	}
}

// Probe runs every registered check once and records the outcome. A finding
// appears in the log once (WARN) and disappears once (INFO) — the same episode
// policy the sources follow, so an unplugged modem does not flood the log.
func (m *Monitor) Probe() {
	m.mu.Lock()
	checks := append([]check(nil), m.checks...)
	m.mu.Unlock()
	for _, c := range checks {
		m.record(c, c.probe())
	}
}

func (m *Monitor) record(c check, err error) {
	m.mu.Lock()
	previous, had := m.active[c.key]
	if err == nil {
		if had {
			delete(m.active, c.key)
		}
		m.mu.Unlock()
		if had {
			m.logger.Info("preflight: dependency available again",
				"key", c.key, "unavailable_for", m.now().Sub(previous.Since).Round(time.Second))
		}
		return
	}
	detail := err.Error()
	if had && previous.Detail == detail {
		m.mu.Unlock()
		return
	}
	cond := Condition{Key: c.key, Severity: c.severity, Detail: detail, Since: m.now()}
	if had {
		// Still missing with a different wording: keep the original start so
		// the "since" column keeps telling the truth, and stay quiet.
		cond.Since = previous.Since
	}
	m.active[c.key] = cond
	m.mu.Unlock()
	if had {
		m.logger.Debug("preflight: dependency still unavailable", "key", c.key, "detail", detail)
		return
	}
	m.logger.Warn("preflight: dependency unavailable",
		"key", c.key, "severity", c.severity, "detail", detail)
}

// Active returns the current findings, sorted by key so the card stays stable
// between refreshes. A nil monitor reports nothing (the panel then leaves the
// hardware section out).
func (m *Monitor) Active() []Condition {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Condition, 0, len(m.active))
	for _, c := range m.active {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// Len reports how many dependencies are registered. The panel uses it to tell
// "nothing was probed" (no section) from "probed and all present" (green row).
func (m *Monitor) Len() int {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.checks)
}

// DevicePath is the probe for one device node: the common case of a serial
// modem or radio that is simply not plugged in.
func DevicePath(path string) Probe {
	path = strings.TrimSpace(path)
	return func() error {
		if path == "" {
			return errors.New("no device configured")
		}
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		return nil
	}
}

// DevicePaths is DevicePath for a comma-separated candidate list: the
// Meshtastic hub tries the paths in order, so any one of them is enough.
func DevicePaths(list string) Probe {
	paths := make([]string, 0, 4)
	for _, p := range strings.Split(list, ",") {
		if p = strings.TrimSpace(p); p != "" {
			paths = append(paths, p)
		}
	}
	return func() error {
		if len(paths) == 0 {
			return errors.New("no device configured")
		}
		missing := make([]string, 0, len(paths))
		for _, p := range paths {
			if _, err := os.Stat(p); err == nil {
				return nil
			} else {
				missing = append(missing, fmt.Sprintf("%s: %v", p, err))
			}
		}
		return errors.New(strings.Join(missing, "; "))
	}
}
