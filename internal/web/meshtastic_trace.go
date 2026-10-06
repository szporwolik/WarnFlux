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
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	res, err := s.meshtastic.Traceroute(ctx, to, 15*time.Second)
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
