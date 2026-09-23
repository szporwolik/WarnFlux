package metrics

import (
	"strings"
	"testing"
)

// TestCounterGaugeRender pins the text-format output: increments, label
// quoting, gauge set and stable ordering.
func TestCounterGaugeRender(t *testing.T) {
	r := New()
	inc := r.Counter("warnflux_x_total", "X things.", "action", "smtp")
	inc(2)
	inc(3)
	set := r.Gauge("warnflux_y", "Y thing.")
	set(7)

	out := r.Render()
	for _, want := range []string{
		"# HELP warnflux_x_total X things.",
		"# TYPE warnflux_x_total counter",
		`warnflux_x_total{action="smtp"} 5`,
		"# TYPE warnflux_y gauge",
		"warnflux_y 7",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("render missing %q:\n%s", want, out)
		}
	}

	// Same name with two label sets stays independent.
	r.Counter("warnflux_x_total", "X things.", "action", "sms")(1)
	out = r.Render()
	if !strings.Contains(out, `warnflux_x_total{action="sms"} 1`) ||
		!strings.Contains(out, `warnflux_x_total{action="smtp"} 5`) {
		t.Errorf("labeled families not independent:\n%s", out)
	}

	// Output is sorted by name+labels.
	firstSmtp := strings.Index(out, `{action="sms"}`)
	firstSmtpAlerts := strings.Index(out, `{action="smtp"}`)
	if firstSmtp > firstSmtpAlerts {
		t.Errorf("entries not sorted:\n%s", out)
	}
}
