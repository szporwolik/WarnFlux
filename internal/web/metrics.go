package web

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// storageProbe is the optional storage surface the metrics endpoint needs
// (satisfied by *sqlite.Store).
type storageProbe interface {
	PendingStats(ctx context.Context) (pending int, oldest time.Duration, err error)
	CountActive(ctx context.Context) (int, error)
}

// handleMetrics renders the Prometheus exposition text. It is intentionally
// unauthenticated (Prometheus has no session support); it leaks only
// counters, never content or secrets.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")

	var b strings.Builder

	// Source counters (live from plugin statuses).
	fmt.Fprintf(&b, "# HELP warnflux_source_polls_total Provider polls per source.\n")
	fmt.Fprintf(&b, "# TYPE warnflux_source_polls_total counter\n")
	fmt.Fprintf(&b, "# HELP warnflux_source_errors_total Provider poll failures per source.\n")
	fmt.Fprintf(&b, "# TYPE warnflux_source_errors_total counter\n")
	fmt.Fprintf(&b, "# HELP warnflux_events_filtered_total Events filtered out by source plugins (geography, severity, duplicates).\n")
	fmt.Fprintf(&b, "# TYPE warnflux_events_filtered_total counter\n")
	for _, st := range s.router.Statuses() {
		if st.Kind == plugin.KindSource {
			fmt.Fprintf(&b, "warnflux_source_polls_total{source=%q} %d\n", st.ID, st.Polls)
			fmt.Fprintf(&b, "warnflux_source_errors_total{source=%q} %d\n", st.ID, st.Errors)
			fmt.Fprintf(&b, "warnflux_events_filtered_total{source=%q} %d\n", st.ID, st.Filtered)
		}
	}

	// MQTT connection gauges.
	fmt.Fprintf(&b, "# HELP warnflux_mqtt_connected Whether an MQTT receiver is connected (1) or not (0).\n")
	fmt.Fprintf(&b, "# TYPE warnflux_mqtt_connected gauge\n")
	for _, rs := range s.receivers.Statuses() {
		v := 0
		if rs.Connected {
			v = 1
		}
		fmt.Fprintf(&b, "warnflux_mqtt_connected{receiver=%q} %d\n", rs.ID, v)
	}

	// Dispatch queue depth.
	fmt.Fprintf(&b, "# HELP warnflux_dispatch_queue_depth Current dispatch ingress queue depth.\n")
	fmt.Fprintf(&b, "# TYPE warnflux_dispatch_queue_depth gauge\n")
	_, _, _, depth, _ := s.ingress.Stats()
	fmt.Fprintf(&b, "warnflux_dispatch_queue_depth %d\n", depth)

	// Storage-derived gauges.
	fmt.Fprintf(&b, "# HELP warnflux_pending_changes Unacknowledged journal changes awaiting output delivery.\n")
	fmt.Fprintf(&b, "# TYPE warnflux_pending_changes gauge\n")
	fmt.Fprintf(&b, "# HELP warnflux_events_active Currently active hazard events.\n")
	fmt.Fprintf(&b, "# TYPE warnflux_events_active gauge\n")
	if probe, ok := s.users.(storageProbe); ok {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		if pending, _, err := probe.PendingStats(ctx); err == nil {
			fmt.Fprintf(&b, "warnflux_pending_changes %d\n", pending)
		}
		if active, err := probe.CountActive(ctx); err == nil {
			fmt.Fprintf(&b, "warnflux_events_active %d\n", active)
		}
		cancel()
	}

	// Public ingest HTTP request counters.
	fmt.Fprintf(&b, "# HELP warnflux_ingest_http_requests_total Public ingest requests per endpoint and result.\n")
	fmt.Fprintf(&b, "# TYPE warnflux_ingest_http_requests_total counter\n")
	var ingestIDs []string
	for id := range s.ingest {
		ingestIDs = append(ingestIDs, id)
	}
	sort.Strings(ingestIDs)
	for _, id := range ingestIDs {
		probe, ok := s.ingest[id].(ingestProbe)
		if !ok {
			continue
		}
		c := probe.Counters()
		emit := func(result string, n int64) {
			fmt.Fprintf(&b, "warnflux_ingest_http_requests_total{instance=%q,result=%q} %d\n", id, result, n)
		}
		emit("accepted", c.Accepted)
		emit("rejected", c.Rejected)
		emit("auth_failed", c.AuthFailed)
		emit("rate_limited", c.RateLimited)
		emit("forbidden", c.Forbidden)
	}

	// Registry-held counters (ingested/duplicates, notifications, retries).
	b.WriteString(s.metrics.Render())

	_, _ = w.Write([]byte(b.String()))
}
