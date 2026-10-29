// Package health holds the station's shared system-health assessment. The
// dashboard card, the daily digest and any other consumer must answer "is the
// station healthy?" the same way, so the classification lives here once
// instead of being re-derived per view — a digest that looked only at source
// plugins could mail "OK" while a receiver was disconnected, an action was
// degraded or the modem was gone (reported P2).
package health

import (
	"strings"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/mqttreceiver"
	"github.com/szporwolik/WarnFlux/internal/plugin"
	"github.com/szporwolik/WarnFlux/internal/preflight"
)

// Level is a verdict, ordered: OK < Warn < Bad. The overall verdict is the
// WORST finding, never an average.
type Level int

const (
	LevelOK Level = iota
	// LevelWarn means something is limited or held (a degraded or suspended
	// plugin, a degraded action, dispatch drops, missing data).
	LevelWarn
	// LevelBad means something that should run does not (a stopped plugin, a
	// disconnected receiver, a broken database, a full queue, missing
	// hardware the station was configured with).
	LevelBad
)

func (l Level) String() string {
	switch l {
	case LevelBad:
		return "bad"
	case LevelWarn:
		return "warn"
	default:
		return "ok"
	}
}

// Worse returns the more severe of two levels.
func Worse(a, b Level) Level {
	if b > a {
		return b
	}
	return a
}

// Subsystem identifiers carried by Findings.
const (
	SubsystemPlugins   = "plugins"
	SubsystemReceivers = "receivers"
	SubsystemActions   = "actions"
	SubsystemHardware  = "hardware"
	SubsystemDatabase  = "database"
	SubsystemQueue     = "queue"
)

// Finding is one subsystem problem: the stable subsystem identifier, the
// concrete component and the operator-facing detail.
type Finding struct {
	Subsystem string
	// Subject names the component (plugin ID, receiver ID, action ID,
	// hardware key); empty for station-wide findings.
	Subject string
	Level   Level
	Detail  string
}

// QueueState is the dispatch intake pressure.
type QueueState struct {
	Depth     int
	Capacity  int
	Dropped   int64
	Emergency int64
}

// Database is the storage probe result ("" = the probe passed).
type Database struct {
	Err string
}

// Inputs is everything the assessment looks at. Every field is optional: a
// caller that cannot read one names it in Missing, and the report then says so
// instead of pretending the subsystem is fine.
type Inputs struct {
	Plugins   []plugin.PluginStatus
	Receivers []mqttreceiver.Status
	Actions   []action.Status
	Hardware  []preflight.Condition
	// Database is nil when the storage was not probed.
	Database *Database
	Queue    *QueueState
	// Missing names the inputs the caller could not read ("counters",
	// "settings", ...).
	Missing []string
}

// PendingOf sums the queued action requests.
func (in Inputs) PendingOf() int {
	pending := 0
	for _, as := range in.Actions {
		pending += as.QueueDepth
	}
	return pending
}

// Report is the shared assessment.
type Report struct {
	Findings []Finding
	// Pending counts queued notification jobs: busy, not broken, so it never
	// colours the verdict.
	Pending int
	// Missing names the inputs that could not be read. A report with missing
	// inputs is INCOMPLETE and can never be a plain OK.
	Missing []string
}

// FindingsLevel is the worst finding alone, ignoring the missing inputs: what
// the station actually reported as wrong.
func (r Report) FindingsLevel() Level {
	level := LevelOK
	for _, f := range r.Findings {
		level = Worse(level, f.Level)
	}
	return level
}

// Level is the worst finding, and at least a warning when the assessment is
// incomplete: "I could not look" is never "all good".
func (r Report) Level() Level {
	level := r.FindingsLevel()
	if len(r.Missing) > 0 {
		level = Worse(level, LevelWarn)
	}
	return level
}

// Incomplete reports whether some input could not be read.
func (r Report) Incomplete() bool { return len(r.Missing) > 0 }

// Of returns the findings of one subsystem.
func (r Report) Of(subsystem string) []Finding {
	var out []Finding
	for _, f := range r.Findings {
		if f.Subsystem == subsystem {
			out = append(out, f)
		}
	}
	return out
}

// Assess classifies every subsystem. Switched-off components (a disabled
// receiver or action, a disabled plugin) never colour the verdict: the
// operator turned them off on purpose.
func Assess(in Inputs) Report {
	r := Report{Pending: in.PendingOf(), Missing: append([]string(nil), in.Missing...)}

	for _, p := range in.Plugins {
		switch p.State {
		case plugin.StateDegraded, plugin.StateSuspended:
			r.Findings = append(r.Findings, Finding{SubsystemPlugins, p.ID, LevelWarn, p.LastError})
		case plugin.StateStopped:
			r.Findings = append(r.Findings, Finding{SubsystemPlugins, p.ID, LevelBad, p.LastError})
		}
	}

	for _, rs := range in.Receivers {
		if rs.Enabled && !rs.Connected {
			r.Findings = append(r.Findings, Finding{SubsystemReceivers, rs.ID, LevelBad, rs.LastError})
		}
	}

	for _, as := range in.Actions {
		if as.Enabled && as.State == action.StateDegraded {
			r.Findings = append(r.Findings, Finding{SubsystemActions, as.ID, LevelWarn, as.Reason + " " + as.LastErrorText})
		}
	}

	for _, cond := range in.Hardware {
		level := LevelWarn
		if cond.Severity == preflight.SeverityBad {
			level = LevelBad
		}
		r.Findings = append(r.Findings, Finding{SubsystemHardware, cond.Key, level, cond.Detail})
	}

	if in.Database != nil && in.Database.Err != "" {
		r.Findings = append(r.Findings, Finding{SubsystemDatabase, "", LevelBad, in.Database.Err})
	}

	if q := in.Queue; q != nil {
		switch {
		case q.Capacity > 0 && q.Depth >= q.Capacity:
			r.Findings = append(r.Findings, Finding{SubsystemQueue, "", LevelBad, "intake full"})
		case q.Emergency > 0:
			r.Findings = append(r.Findings, Finding{SubsystemQueue, "", LevelWarn, "emergency acceptance"})
		case q.Dropped > 0:
			r.Findings = append(r.Findings, Finding{SubsystemQueue, "", LevelWarn, "drops"})
		}
	}

	for i := range r.Findings {
		r.Findings[i].Detail = strings.Join(strings.Fields(r.Findings[i].Detail), " ")
	}
	return r
}
