package smtp

import (
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/action"
)

// TestFooterBrand verifies the mail footer is branded by the configured
// header1 alongside the project name, and falls back to the project name
// alone when header1 is not populated.
func TestFooterBrand(t *testing.T) {
	req := action.ActionRequest{} // zero App: no header1
	if txt := footerText(req); !strings.Contains(txt, "Sent by WarnFlux") {
		t.Errorf("footerText = %q, want WarnFlux fallback", txt)
	}
	if html := footerHTML(req); !strings.Contains(html, ">WarnFlux</strong>") {
		t.Errorf("footerHTML = %q, want WarnFlux fallback", html)
	}

	req.App.Header1 = "SOSNA"
	if txt := footerText(req); !strings.Contains(txt, "Sent by SOSNA · WarnFlux") {
		t.Errorf("footerText = %q, want SOSNA + WarnFlux brand", txt)
	}
	if html := footerHTML(req); !strings.Contains(html, ">SOSNA · WarnFlux</strong>") {
		t.Errorf("footerHTML = %q, want SOSNA + WarnFlux brand", html)
	}

	// A header1 equal to the project name must not duplicate it.
	req.App.Header1 = "WarnFlux"
	if txt := footerText(req); strings.Contains(txt, "WarnFlux · WarnFlux") {
		t.Errorf("footerText = %q, want no duplicated brand", txt)
	}
}
