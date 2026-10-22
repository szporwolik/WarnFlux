package web

import (
	"strings"
	"testing"
)

// ruleBodies returns the declaration block of every rule whose selector is
// exactly selector. Several rules may share a selector (a base rule plus a
// narrow-screen override), so callers must look across all of them.
func ruleBodies(css, selector string) []string {
	var bodies []string
	needle := selector + " {"
	for offset := 0; ; {
		idx := strings.Index(css[offset:], needle)
		if idx < 0 {
			return bodies
		}
		start := offset + idx + len(needle)
		end := strings.Index(css[start:], "}")
		if end < 0 {
			return bodies
		}
		bodies = append(bodies, css[start:start+end])
		offset = start + end
	}
}

// TestStylesheetKeepsNarrowScreensInsideTheViewport pins the rules that stop a
// phone from scrolling the whole page sideways: a flex row that cannot wrap
// pushes its last item (a tab label, a switch, a status badge) past the
// viewport, which is invisible to a desktop browser and awkward on a phone.
func TestStylesheetKeepsNarrowScreensInsideTheViewport(t *testing.T) {
	raw, err := staticFS.ReadFile("static/style.css")
	if err != nil {
		t.Fatal(err)
	}
	css := string(raw)

	wrappable := []string{
		".page-tabs",                       // tab strips: logs, traffic, meshtastic, nodes filter
		".msgs-filter",                     // device/node badge rows
		".config-offline-form .config-row", // title + state badge + switch
		".emcom-add-row",                   // network name input + add button
		"#cookie-bar",                      // consent text + buttons
	}
	for _, selector := range wrappable {
		bodies := ruleBodies(css, selector)
		if len(bodies) == 0 {
			t.Errorf("style.css has no rule for %s", selector)
			continue
		}
		wrapped := false
		for _, body := range bodies {
			if strings.Contains(body, "flex-wrap: wrap") {
				wrapped = true
				break
			}
		}
		if !wrapped {
			t.Errorf("%s must set flex-wrap: wrap so its last item cannot widen the page", selector)
		}
	}

	// A long card (a warning headline with one very long word) must be allowed
	// to shrink inside its grid column instead of forcing the column wider.
	if bodies := ruleBodies(css, ".hazard"); len(bodies) == 0 || !strings.Contains(strings.Join(bodies, " "), "min-width: 0") {
		t.Error(".hazard must set min-width: 0 so a long headline cannot widen the page")
	}
	if !strings.Contains(css, "@media (max-width: 420px)") {
		t.Error("style.css is missing the narrow top bar media query")
	}
}
