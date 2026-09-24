package web

import "net/http"

// Handler returns the root HTTP handler. Test seam used by httptest
// servers; production binds via Bind/Serve and never exposes the mux.
func (s *Server) Handler() http.Handler { return s.mux }
