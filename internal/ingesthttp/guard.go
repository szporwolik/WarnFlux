package ingesthttp

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// defaultRateLimitPerMinute is applied when the configuration omits the
// rate limit (0). A negative configured value disables limiting entirely.
const defaultRateLimitPerMinute = 60

// Counters is an immutable snapshot of one endpoint's request metrics.
type Counters struct {
	Accepted    int64
	Rejected    int64
	AuthFailed  int64
	RateLimited int64
	Forbidden   int64
}

// counters stores the atomic metric cells.
type counters struct {
	accepted    atomic.Int64
	rejected    atomic.Int64
	authFailed  atomic.Int64
	rateLimited atomic.Int64
	forbidden   atomic.Int64
}

func (c *counters) snapshot() Counters {
	return Counters{
		Accepted:    c.accepted.Load(),
		Rejected:    c.rejected.Load(),
		AuthFailed:  c.authFailed.Load(),
		RateLimited: c.rateLimited.Load(),
		Forbidden:   c.forbidden.Load(),
	}
}

// rateLimiter is a simple token bucket: rate tokens per minute, bursting
// up to one minute worth of tokens. One limiter per endpoint keeps the
// semantics obvious (a per-key bucket would just double-count the same
// scraper during key rotation).
type rateLimiter struct {
	mu         sync.Mutex
	ratePerSec float64
	burst      float64
	tokens     float64
	last       time.Time
}

func newRateLimiter(perMinute int) *rateLimiter {
	if perMinute <= 0 {
		return nil // rate limiting disabled
	}
	return &rateLimiter{
		ratePerSec: float64(perMinute) / 60,
		burst:      float64(perMinute),
		tokens:     float64(perMinute),
		last:       time.Now(),
	}
}

// allow consumes one token. When denied, it returns the retry delay.
func (l *rateLimiter) allow(now time.Time) (bool, time.Duration) {
	if l == nil {
		return true, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	elapsed := now.Sub(l.last).Seconds()
	if elapsed > 0 {
		l.tokens += elapsed * l.ratePerSec
		if l.tokens > l.burst {
			l.tokens = l.burst
		}
		l.last = now
	}
	if l.tokens >= 1 {
		l.tokens--
		return true, 0
	}
	wait := time.Duration((1 - l.tokens) / l.ratePerSec * float64(time.Second))
	if wait < time.Second {
		wait = time.Second
	}
	return false, wait
}

// parseAllowedCIDRs accepts CIDR blocks and plain addresses (which become
// a full-length prefix). An invalid entry is a configuration error.
func parseAllowedCIDRs(cidrs []string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for _, raw := range cidrs {
		c := strings.TrimSpace(raw)
		if c == "" {
			continue
		}
		if p, err := netip.ParsePrefix(c); err == nil {
			out = append(out, p.Masked())
			continue
		}
		if a, err := netip.ParseAddr(c); err == nil {
			out = append(out, netip.PrefixFrom(a, a.BitLen()))
			continue
		}
		return nil, fmt.Errorf("allowed_cidrs: %q is neither a CIDR block nor an IP address", raw)
	}
	return out, nil
}

// addrAllowed reports whether the address matches any allowed prefix.
// An empty allowlist allows everything.
func addrAllowed(allowed []netip.Prefix, addr netip.Addr) bool {
	if len(allowed) == 0 {
		return true
	}
	if !addr.IsValid() {
		return false
	}
	for _, p := range allowed {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// remoteIP extracts the peer address from RemoteAddr ("1.2.3.4:5678").
// Unparseable values are rejected (fail closed).
func remoteIP(rAddr string) netip.Addr {
	ap, err := netip.ParseAddrPort(rAddr)
	if err == nil {
		return ap.Addr()
	}
	a, err := netip.ParseAddr(rAddr)
	if err != nil {
		return netip.Addr{}
	}
	return a
}

// requestID returns the client-supplied request id (bounded) or mints a
// fresh random one. It is echoed in the response and in every audit line.
func requestID(rHeader string) string {
	if v := strings.TrimSpace(rHeader); v != "" {
		if len(v) > 64 {
			v = v[:64]
		}
		return v
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
