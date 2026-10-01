package web_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestStaticAssetETag proves embedded assets revalidate instead of
// re-downloading: every asset carries a strong ETag and a conditional
// request answers 304. app.js/style.css/fonts/Leaflet add up to a few
// hundred KB per page load, so this is the main UI-latency win on a
// low-power station.
func TestStaticAssetETag(t *testing.T) {
	env := newTestEnv(t)

	req, _ := http.NewRequest(http.MethodGet, env.srv.URL+"/static/app.js", nil)
	resp, err := env.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /static/app.js = %d", resp.StatusCode)
	}
	if len(body) < 1000 {
		t.Fatalf("app.js body suspiciously small: %d bytes", len(body))
	}
	et := resp.Header.Get("ETag")
	if !strings.HasPrefix(et, `"`) || !strings.HasSuffix(et, `"`) {
		t.Fatalf("ETag missing or malformed: %q", et)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("Cache-Control = %q, want no-cache (must revalidate)", cc)
	}

	// Conditional request: 304, no body.
	req2, _ := http.NewRequest(http.MethodGet, env.srv.URL+"/static/app.js", nil)
	req2.Header.Set("If-None-Match", et)
	resp2, err := env.client.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotModified {
		t.Fatalf("conditional GET = %d, want 304", resp2.StatusCode)
	}
	if len(body2) != 0 {
		t.Errorf("304 carries a body: %d bytes", len(body2))
	}
	if got := resp2.Header.Get("ETag"); got != et {
		t.Errorf("304 ETag = %q, want %q", got, et)
	}

	// A stale ETag (old binary) forces a fresh copy.
	req3, _ := http.NewRequest(http.MethodGet, env.srv.URL+"/static/app.js", nil)
	req3.Header.Set("If-None-Match", `"deadbeefdeadbeefdeadbeefdeadbeef"`)
	resp3, err := env.client.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("stale-ETag GET = %d, want 200", resp3.StatusCode)
	}

	// Unknown assets keep 404ing.
	req4, _ := http.NewRequest(http.MethodGet, env.srv.URL+"/static/nope.js", nil)
	resp4, err := env.client.Do(req4)
	if err != nil {
		t.Fatal(err)
	}
	resp4.Body.Close()
	if resp4.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /static/nope.js = %d, want 404", resp4.StatusCode)
	}
}
