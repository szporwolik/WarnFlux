package web

import (
	"net/http"
	"sync"
	"time"
)

// publicTTLBody shares one rendered public-API response between the visitors
// arriving inside the same short window. The map layers poll every few
// seconds, and each of those requests used to rebuild the whole response from
// the mirrored MQTT state; with several open dashboards the same work was
// repeated per visitor and per layer.
//
// The cache is keyed on a revision supplied by the caller, so it can only ever
// serve a body built from the state the caller sees now: a changed mirror (or a
// changed APRS hub) invalidates it immediately. The TTL is just an upper bound
// for the parts of a layer that depend on the wall clock.
type publicTTLBody struct {
	mu   sync.Mutex
	ttl  time.Duration
	rev  uint64
	body []byte
	at   time.Time
}

// publicLayerTTL bounds how long a rendered layer may outlive a wall-clock
// dependent input (weather observations age out on their own).
const publicLayerTTL = 5 * time.Second

func newPublicTTLBody() *publicTTLBody {
	return &publicTTLBody{ttl: publicLayerTTL}
}

// render returns the cached body when it was built from the same revision and
// is still inside the freshness window, otherwise rebuilds it. The build runs
// under the lock so concurrent pollers wait for one build instead of each
// starting their own.
func (c *publicTTLBody) render(rev uint64, build func() ([]byte, error)) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.body != nil && c.rev == rev && time.Since(c.at) <= c.ttl {
		return c.body, nil
	}
	out, err := build()
	if err != nil {
		return nil, err
	}
	c.body, c.rev, c.at = out, rev, time.Now()
	return out, nil
}

// weatherRevision mixes the mirror revision with the APRS hub activity, so a
// weather observation received over RF invalidates the cached layer even though
// it never entered the mirror.
func (s *Server) weatherRevision() uint64 {
	rev := s.st.Version()
	if s.aprs == nil {
		return rev
	}
	st := s.aprs.Stats()
	hub := uint64(st.Accepted) + uint64(st.Dropped)<<7 + uint64(st.Filtered)<<13 + uint64(st.Expired)<<19
	return rev*1099511628211 + hub
}

// servePublicLayer writes a public JSON layer through its shared short cache.
func (s *Server) servePublicLayer(w http.ResponseWriter, cache *publicTTLBody, rev uint64, build func() ([]byte, error)) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	body, err := cache.render(rev, build)
	if err != nil {
		s.logger.Warn("web: public layer build failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if _, err := w.Write(body); err != nil {
		s.logger.Warn("web: public layer write failed", "error", err)
	}
}
