package web

import (
	"fmt"
	"net"
	"net/http"
	"strings"
)

// parseTrustedProxies validates the configured trusted reverse proxies:
// entries are single IPs or CIDRs. An empty list means the service is
// exposed directly and proxy headers are never trusted.
func parseTrustedProxies(entries []string) ([]*net.IPNet, error) {
	var out []*net.IPNet
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if ip := net.ParseIP(e); ip != nil {
			bits := 8 * len(ip)
			out = append(out, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
			continue
		}
		_, cidr, err := net.ParseCIDR(e)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted proxy %q: must be an IP or CIDR", e)
		}
		out = append(out, cidr)
	}
	return out, nil
}

// trustedProxy reports whether the direct peer (host, no port) belongs
// to the configured trusted proxy set.
func (s *Server) trustedProxy(host string) bool {
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, n := range s.trustedProxies {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// clientIP returns the normalized client identity used by the rate
// limiters: the remote host WITHOUT the port — a new TCP connection must
// never buy a fresh counter. X-Forwarded-For is honored only when the
// direct peer is a configured trusted proxy.
//
// The chain is analyzed from the RIGHT (each proxy appends the client
// address it saw, so the rightmost entries are the hops closest to us):
// trusted-proxy addresses are skipped and the first non-trusted address
// from the right is the client. A client that pre-populates the header
// with its own addresses can only control entries LEFT of the address
// the proxy appended — those are never consulted. When every entry is a
// trusted proxy (or unparseable), no reliable client identity exists in
// the header and the direct peer is used.
func (s *Server) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if !s.trustedProxy(host) {
		return host
	}
	entries := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	for i := len(entries) - 1; i >= 0; i-- {
		e := strings.TrimSpace(entries[i])
		if e == "" {
			continue
		}
		ip := net.ParseIP(e)
		if ip == nil || s.trustedProxy(e) {
			continue // malformed hop or trusted proxy: keep walking left
		}
		return ip.String()
	}
	return host
}
