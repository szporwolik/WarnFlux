package smtp

import (
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/action"
)

// TestFooterBrand verifies the mail footer is branded by the configured
// header1 and falls back to the project name when it is not populated.
func TestFooterBrand(t *testing.T) {
	req := action.ActionRequest{} // zero App: no header1
	if txt := footerText(req); !strings.Contains(txt, "Sent by WarnFlux") {
		t.Errorf("footerText = %q, want WarnFlux fallback", txt)
	}
	if html := footerHTML(req); !strings.Contains(html, ">WarnFlux</strong>") {
		t.Errorf("footerHTML = %q, want WarnFlux fallback", html)
	}

	req.App.Header1 = "SOSNA"
	if txt := footerText(req); !strings.Contains(txt, "Sent by SOSNA") {
		t.Errorf("footerText = %q, want SOSNA brand", txt)
	}
	if html := footerHTML(req); !strings.Contains(html, ">SOSNA</strong>") {
		t.Errorf("footerHTML = %q, want SOSNA brand", html)
	}
}
