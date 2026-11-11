package websafe

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

const (
	secretID    = "123456789012345678"
	secretToken = "REVIEW_SECRET_TOKEN_VALUE"
)

// TestRedactedURLKeepsTheDiagnosablePart pins what an operator still needs:
// which host and which endpoint kind failed — and nothing that lets them post.
func TestRedactedURLKeepsTheDiagnosablePart(t *testing.T) {
	raw := "https://discord.com/api/webhooks/" + secretID + "/" + secretToken + "?wait=true"
	got := RedactedURL(raw)
	if strings.Contains(got, secretToken) || strings.Contains(got, secretID) {
		t.Fatalf("redacted URL still carries the credential: %q", got)
	}
	if !strings.Contains(got, "discord.com") || !strings.Contains(got, "/api/") {
		t.Errorf("redacted URL lost the diagnosable part: %q", got)
	}
	if !strings.Contains(got, "?[redacted]") {
		t.Errorf("query not masked: %q", got)
	}
}

// TestErrorMasksNestedTransportFailures is WF-REV-02: the transport error quotes
// the request URL (that is the leak), and the reader must still learn the kind
// of failure.
func TestErrorMasksNestedTransportFailures(t *testing.T) {
	raw := "https://discord.com/api/webhooks/" + secretID + "/" + secretToken
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"eof", &url.Error{Op: "Post", URL: raw, Err: errors.New("EOF")}, "EOF"},
		{"timeout", &url.Error{Op: "Post", URL: raw, Err: errors.New("context deadline exceeded")}, "deadline"},
		{"wrapped", fmt.Errorf("post mirror: %w", &url.Error{Op: "Post", URL: raw, Err: errors.New("connection reset by peer")}), "connection reset"},
	} {
		got := Error(tc.err, raw)
		if strings.Contains(got, secretToken) || strings.Contains(got, secretID) {
			t.Errorf("%s: secret survived in %q", tc.name, got)
		}
		if !strings.Contains(got, tc.want) {
			t.Errorf("%s: diagnostic lost in %q (want %q)", tc.name, got, tc.want)
		}
	}

	// A malformed URL must not turn the log line into a panic or a leak of the
	// raw text it came with.
	if got := Error(errors.New("boom"), "://not a url"); strings.Contains(got, "boom") == false {
		t.Errorf("unparseable endpoint lost the error text: %q", got)
	}
}

// TestErrorsWithUserinfo covers the credential forms that are not path tokens.
func TestErrorsWithUserinfo(t *testing.T) {
	raw := "https://user:pass@hooks.example.test/services/T0KEN/B0T"
	got := Error(fmt.Errorf("Post %q: %w", raw, errors.New("dial tcp: refused")), raw)
	for _, secret := range []string{"user:pass", "T0KEN", "B0T"} {
		if strings.Contains(got, secret) {
			t.Errorf("secret %q survived in %q", secret, got)
		}
	}
	if !strings.Contains(got, "refused") {
		t.Errorf("diagnostic lost: %q", got)
	}
}
