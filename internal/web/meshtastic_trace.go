package web

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// handleMeshtasticTraceroute runs one route probe to a heard node and
// returns the discovered hops as JSON (admin only; CSRF-guarded because
// it transmits on the radio).
//
// The reply shape mirrors meshtastic.TracerouteResult: hops from our
// node towards the destination (id, name, snr) plus reached — false
// when only intermediate nodes answered.
func (s *Server) handleMeshtasticTraceroute(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		http.Error(w, "invalid csrf token", http.StatusForbidden)
		return
	}
	if s.meshtastic == nil || !s.meshtastic.Enabled() {
		http.Error(w, "meshtastic disabled", http.StatusNotFound)
		return
	}
	to := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(r.PostFormValue("to")), "!"))
	if len(to) != 8 || strings.Trim(to, "0123456789abcdef") != "" {
		http.Error(w, "node id must be 8 hex characters", http.StatusBadRequest)
		return
	}
	// Stale nodes are not probed: the radio answer is physically
	// impossible, and every such probe burns airtime on a busy channel.
	lang := s.langFor(r)
	if ls := s.meshtastic.NodeLastSeen(to); !ls.IsZero() && time.Since(ls) > meshtasticTraceStaleAfter {
		http.Error(w, meshInactiveText(lang, time.Since(ls)), http.StatusConflict)
		return
	}
	// Far nodes (or unknown distance) get a longer window: three probes
	// ride a 60 s budget instead of the usual 35 s.
	timeout := 35 * time.Second
	if hops := s.meshtastic.NodeHops(to); hops < 0 || hops > 3 {
		timeout = 60 * time.Second
	}
	// The far-node budget matches the server's global 60 s WriteTimeout:
	// the result would be written right at the deadline and the proxy
	// would cut the connection (visible as a 502). Extend THIS response's
	// write deadline past the probe window instead.
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Now().Add(timeout + 15*time.Second))
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout+10*time.Second)
	defer cancel()
	res, err := s.meshtastic.Traceroute(ctx, to, timeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	s.audit(sess.username, "meshtastic-traceroute", to)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(res); err != nil {
		s.logger.Warn("web: encode meshtastic traceroute failed", "error", err)
	}
}
