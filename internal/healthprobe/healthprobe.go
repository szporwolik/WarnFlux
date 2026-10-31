// Package healthprobe implements the container liveness probe.
//
// The runtime image is distroless: no shell, no curl, no wget. A Docker
// HEALTHCHECK therefore has to be the binary itself, so `warnflux -healthcheck`
// runs one probe and exits non-zero when the station stops answering. Docker
// then reports the container as unhealthy, and the station's own boot-up pass
// restarts it (deploy/warnflux-boot-up.sh) — that is the only thing that brings
// back a process which is alive but wedged, because a restart policy covers
// exits, not hangs.
package healthprobe

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DefaultURL is the panel's liveness endpoint on the container-local address.
// Operators who move the listener override it with -healthcheck-url (or the
// WARNFLUX_HEALTHCHECK_URL environment variable).
const DefaultURL = "http://127.0.0.1:8080/healthz"

// Timeout bounds one probe. It has to stay below the HEALTHCHECK --timeout in
// the Dockerfile, otherwise Docker kills the probe and reports a code we cannot
// distinguish from a real answer.
const Timeout = 4 * time.Second

// Probe asks the running station whether it is alive. A nil error means a
// healthy answer: HTTP 200 from the unauthenticated liveness handler.
func Probe(ctx context.Context, url string) error {
	if url == "" {
		url = DefaultURL
	}
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("probe %s: %w", url, err)
	}
	client := &http.Client{
		// The probe runs inside the container: a proxy inherited from the
		// environment must not swallow a loopback request.
		Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true},
		// A redirect means the request never reached the health handler (for
		// example a listener answering with a login page). Treat it as
		// unhealthy instead of following it to an unrelated status.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("probe %s: %w", url, err)
	}
	defer resp.Body.Close()
	// Drain the tiny body so the connection closes cleanly rather than resetting.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<10))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("probe %s: HTTP %d", url, resp.StatusCode)
	}
	return nil
}
