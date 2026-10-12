package markdown

import (
	"strings"
	"testing"
)

func TestHTMLEscapesBeforeRenderingMarkers(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{"plain", "Zwykły komunikat.", "<p>Zwykły komunikat.</p>"},
		{"script is literal", "<script>alert(1)</script>", "<p>&lt;script&gt;alert(1)&lt;/script&gt;</p>"},
		{"attribute injection stays text", `<img src=x onerror="alert(1)">`, "<p>&lt;img src=x onerror=&#34;alert(1)&#34;&gt;</p>"},
		{"bold", "To **ważne**.", "<p>To <strong>ważne</strong>.</p>"},
		{"italic", "To *ważne*.", "<p>To <em>ważne</em>.</p>"},
		{"inline code", "Użyj `wf --test`.", "<p>Użyj <code>wf --test</code>.</p>"},
		{"code keeps markup literal", "`<b>x</b>`", "<p><code>&lt;b&gt;x&lt;/b&gt;</code></p>"},
		{"heading", "## Radar", "<h4>Radar</h4>"},
		{"paragraph breaks on blank line", "raz\n\ndwa", "<p>raz</p>\n<p>dwa</p>"},
		{"soft break", "raz\ndwa", "<p>raz<br>dwa</p>"},
		{"bullets", "- jeden\n- dwa", "<ul><li>jeden</li><li>dwa</li></ul>"},
		{"stars are bullets too", "* jeden", "<ul><li>jeden</li></ul>"},
		{"fenced code", "```\na <b>\n```", "<pre><code>a &lt;b&gt;</code></pre>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := HTML(tc.src); got != tc.want {
				t.Fatalf("HTML(%q) = %q, want %q", tc.src, got, tc.want)
			}
		})
	}
}

func TestHTMLOnlyLinksHttp(t *testing.T) {
	got := HTML("Zobacz [mapę](https://sosna.sp9moa.pl/dashboard).")
	want := `<p>Zobacz <a href="https://sosna.sp9moa.pl/dashboard" target="_blank" rel="noopener noreferrer">mapę</a>.</p>`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	// A javascript: URL is not markdown at all: it stays visible text.
	got = HTML("[klik](javascript:alert(1))")
	if strings.Contains(got, "<a ") || strings.Contains(got, "href") {
		t.Fatalf("javascript: URL became a link: %q", got)
	}
	if !strings.Contains(got, "javascript:alert(1)") {
		t.Fatalf("javascript: URL should stay visible, got %q", got)
	}
}

func TestHTMLNeverEmitsUnescapedInput(t *testing.T) {
	src := "<a href=\"x\">y</a> & **bold** <b>\n<script>x</script>"
	got := HTML(src)
	for _, bad := range []string{"<a href=", "<b>", "<script>"} {
		if strings.Contains(got, bad) {
			t.Fatalf("HTML() leaked %q in %q", bad, got)
		}
	}
	if !strings.Contains(got, "&lt;script&gt;") {
		t.Fatalf("expected escaped script tag, got %q", got)
	}
}

func TestDiscordTurnsHeadingsBold(t *testing.T) {
	got := Discord("# Alarm\nRadar **włączony**\n\n```\n# nie nagłówek\n```")
	want := "**Alarm**\nRadar **włączony**\n\n```\n# nie nagłówek\n```"
	if got != want {
		t.Fatalf("Discord() = %q, want %q", got, want)
	}
}

func TestDiscordLeavesSupportedMarkupAlone(t *testing.T) {
	src := "Sprawdź [mapę](https://sosna.sp9moa.pl) — `wf --test`\n- jeden\n- dwa"
	if got := Discord(src); got != src {
		t.Fatalf("Discord() changed supported markup: %q", got)
	}
}

func TestPlainStripsMarkersAndKeepsLinksReadable(t *testing.T) {
	got := Plain("# Alarm\nZobacz **mapę** [tutaj](https://sosna.sp9moa.pl/x) i `kod`.\n\n- jeden\n- dwa\n\n```\nlinia <b>\n```")
	want := "Alarm\nZobacz mapę tutaj (https://sosna.sp9moa.pl/x) i kod.\n\n- jeden\n- dwa\n\nlinia <b>"
	if got != want {
		t.Fatalf("Plain() = %q, want %q", got, want)
	}
	if strings.Contains(got, "```") {
		t.Fatalf("Plain() kept a fence: %q", got)
	}
}

func TestPlainOfPlainTextIsItself(t *testing.T) {
	src := "Krótki komunikat bez znaczników."
	if got := Plain(src); got != src {
		t.Fatalf("Plain(%q) = %q", src, got)
	}
}
