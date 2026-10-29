package web

import (
	"errors"
	"testing"
	"time"
)

// TestPublicLayerCacheSharesOneBuild is the regression test for public map
// layers that rebuilt the whole mirrored state for every visitor: within the
// freshness window the layer must serve the first build to every poller.
func TestPublicLayerCacheSharesOneBuild(t *testing.T) {
	c := newPublicTTLBody()
	builds := 0
	build := func() ([]byte, error) {
		builds++
		return []byte("layer"), nil
	}

	for i := 0; i < 5; i++ {
		body, err := c.render(7, build)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != "layer" {
			t.Fatalf("render = %q, want %q", body, "layer")
		}
	}
	if builds != 1 {
		t.Fatalf("build ran %d times inside the freshness window, want 1", builds)
	}

	// Once the window passes the layer is rebuilt from fresh state.
	c.mu.Lock()
	c.at = time.Now().Add(-2 * publicLayerTTL)
	c.mu.Unlock()
	if _, err := c.render(7, build); err != nil {
		t.Fatal(err)
	}
	if builds != 2 {
		t.Fatalf("build ran %d times after the window expired, want 2", builds)
	}
}

// TestPublicLayerCacheSkipsFailedBuild documents that a failed build is not
// cached: the next poller must retry instead of receiving the error for a whole
// freshness window.
func TestPublicLayerCacheSkipsFailedBuild(t *testing.T) {
	c := newPublicTTLBody()
	fail := func() ([]byte, error) { return nil, errors.New("boom") }
	if _, err := c.render(1, fail); err == nil {
		t.Fatal("render swallowed the build error")
	}
	body, err := c.render(1, func() ([]byte, error) { return []byte("ok"), nil })
	if err != nil {
		t.Fatalf("render after a failed build = %v, want a successful retry", err)
	}
	if string(body) != "ok" {
		t.Fatalf("render = %q, want %q", body, "ok")
	}
}

// TestPublicLayerCacheFollowsRevision documents the freshness rule: a cache
// entry dies the moment the state it was built from changes, so a new station
// or aircraft appears immediately instead of after the TTL.
func TestPublicLayerCacheFollowsRevision(t *testing.T) {
	c := newPublicTTLBody()
	builds := 0
	build := func() ([]byte, error) {
		builds++
		return []byte("layer"), nil
	}
	if _, err := c.render(1, build); err != nil {
		t.Fatal(err)
	}
	if _, err := c.render(2, build); err != nil {
		t.Fatal(err)
	}
	if builds != 2 {
		t.Fatalf("build ran %d times after the revision changed, want 2", builds)
	}
	// Same revision again: shared.
	if _, err := c.render(2, build); err != nil {
		t.Fatal(err)
	}
	if builds != 2 {
		t.Fatalf("build ran %d times for an unchanged revision, want 2", builds)
	}
}
