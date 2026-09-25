package web

import "net/http"

// Handler returns the root HTTP handler. Test seam used by httptest
// servers; mirrors the production wrapping (security headers).
func (s *Server) Handler() http.Handler { return securityHeaders(s.mux) }
