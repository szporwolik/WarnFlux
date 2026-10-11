package web

import (
	"encoding/base64"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"

	qrcode "github.com/skip2/go-qrcode"

	"github.com/szporwolik/WarnFlux/internal/actions/discord"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	mesh "github.com/szporwolik/WarnFlux/internal/meshtastic"
)

// helpCommandView is one radio command in the manual. The list itself
// comes from the shared interpreter (never stale); the usage line and
// the explanation are translated, with the registry description as the
// fallback so a freshly registered command still shows up.
type helpCommandView struct {
	Usage       string // e.g. "/alert <text>"
	Description string
	Restricted  bool
}

// helpSection is one block of the Help manual: a title, an icon and a
// short list of instructions (i18n keys, resolved in the page language).
type helpSection struct {
	Title string // i18n key
	Icon  string // sprite id (without the i- prefix? kept full: "i-house")
	Items []string
}

// helpView is the Help page model — a very short manual for users and
// operators, rendered in the signed-in user's UI language.
type helpView struct {
	Lang     string
	AppTitle string
	Name     string
	Header1  string
	Header2  string
	Tagline  string
	Version  string
	Commit   string
	RepoURL  string
	CSRF     string
	Username string
	Role     string

	Sections []helpSection
	// Commands is the radio-command reference (empty without an
	// interpreter).
	Commands []helpCommandView
	// MeshChannel is the installation's own Meshtastic channel (name,
	// key, share link and scan code) — nil when the station has none.
	MeshChannel *helpMeshChannelView
	// Discord is the Discord card (what the station posts there and what
	// a user must do to be reachable) — nil when the station has no
	// Discord instance.
	Discord *helpDiscordView

	NavDashboard     bool
	NavUsers         bool
	NavGroups        bool
	NavCompose       bool
	NavEmcom         bool
	NavAccount       bool
	NavLogs          bool
	NavAudit         bool
	NavAPRS          bool
	NavMessages      bool
	NavGSM           bool
	NavMeshtastic    bool
	NavMeshMap       bool
	NavTraffic       bool
	NavWebsite       bool
	NavNotifications bool
	NavHealth        bool
	NavConfig        bool
	NavHelp          bool
	NavMass          bool
}

// helpMeshChannelView is the "join our channel" card: what users need
// to add the installation's group channel to their own radio.
type helpMeshChannelView struct {
	// Index is the device channel slot the station transmits on.
	Index int
	Name  string
	Key   string
	// Link is the share link the apps import (may be empty when the
	// channel cannot be encoded).
	Link string
	// QR is a PNG data URI of the share link, rendered locally so the
	// card works offline.
	QR template.URL
}

// helpDiscordView is the Discord card: the surfaces the station uses
// there, the cadence of its periodic notices and the few steps a user
// has to take to be reachable on Discord.
type helpDiscordView struct {
	Facts []helpFact
	Items []string // instruction lines (i18n keys, resolved by the template)
}

// helpFact is one labelled row of a card ("Channel alerts: enabled").
type helpFact struct {
	Label string
	Value string
	// On marks an enabled surface (rendered as a positive badge).
	On bool
}

// buildHelpView assembles the manual sections (content lives in i18n).
func (s *Server) buildHelpView(lang string) helpView {
	return helpView{
		Lang:     lang,
		AppTitle: s.cfg.Title,
		Name:     s.displayName(),
		Header1:  s.displayHeader1(),
		Header2:  s.DisplayHeader2(),
		Tagline:  s.DisplayTagline(),
		Version:  s.version,
		Commit:   s.commit,
		RepoURL:  repoURL,
		NavHelp:  true,
		Commands: s.helpCommands(lang),

		MeshChannel: s.helpMeshChannel(),
		Discord:     s.helpDiscord(lang),
		Sections: []helpSection{
			{Title: "help.start.title", Icon: "i-lightning-charge", Items: []string{
				"help.start.1", "help.start.2", "help.start.3", "help.start.4",
			}},
			{Title: "help.general.title", Icon: "i-info-circle", Items: []string{
				"help.general.1", "help.general.2",
			}},
			{Title: "help.dashboard.title", Icon: "i-house", Items: []string{
				"help.dashboard.1", "help.dashboard.2",
			}},
			{Title: "help.messages.title", Icon: "i-pencil-square", Items: []string{
				"help.messages.1", "help.messages.2",
			}},
			{Title: "help.emcom.title", Icon: "i-megaphone", Items: []string{
				"help.emcom.1", "help.emcom.2", "help.emcom.3",
			}},
			{Title: "help.aprs.title", Icon: "i-broadcast-pin", Items: []string{
				"help.aprs.1",
			}},
			{Title: "help.gsm.title", Icon: "i-telephone", Items: []string{
				"help.gsm.1", "help.gsm.2",
			}},
			{Title: "help.meshtastic.title", Icon: "i-broadcast", Items: []string{
				"help.meshtastic.1", "help.meshtastic.2",
			}},
			{Title: "help.discord.title", Icon: "i-chat-text", Items: []string{
				"help.discord.section.1", "help.discord.section.2",
			}},
			{Title: "help.account.title", Icon: "i-person", Items: []string{
				"help.account.1",
			}},
			{Title: "help.access.title", Icon: "i-people", Items: []string{
				"help.access.1",
			}},
			{Title: "help.config.title", Icon: "i-gear", Items: []string{
				"help.config.1",
			}},
		},
	}
}

// helpMeshChannel describes the installation's own Meshtastic channel for
// the Help page: name, key, share link and a locally rendered QR code, so
// a user can join the group channel by scanning it. Nil when the station
// has no channel of its own (no radio, no key) — the card then stays off
// the page entirely.
func (s *Server) helpMeshChannel() *helpMeshChannelView {
	if s.meshtastic == nil {
		return nil
	}
	idx, name, key, ok := s.meshtastic.OwnChannel()
	if !ok {
		return nil
	}
	view := &helpMeshChannelView{Index: idx, Name: name, Key: base64.StdEncoding.EncodeToString(key)}
	view.Link = mesh.ChannelShareURL(name, key)
	if view.Link != "" {
		if png, err := qrcode.Encode(view.Link, qrcode.Medium, 256); err == nil {
			view.QR = template.URL("data:image/png;base64," + base64.StdEncoding.EncodeToString(png))
		}
	}
	return view
}

// helpDiscord describes the station's Discord surface for the Help page:
// which notices go out, how often, and what a user must register to be
// messaged privately. Nil when the station runs no Discord instance.
func (s *Server) helpDiscord(lang string) *helpDiscordView {
	if s.actions == nil {
		return nil
	}
	var info discord.Info
	found := false
	for _, st := range s.actions.Statuses() {
		if st.Type != discord.Type || !st.Enabled {
			continue
		}
		p, ok := s.actions.Plugin(st.ID)
		if !ok {
			continue
		}
		if got, ok := discord.InfoOf(p); ok {
			info, found = got, true
			if got.DirectMessages {
				break
			}
		}
	}
	if !found {
		return nil
	}
	onOff := func(on bool) string {
		if on {
			return i18n.T(lang, "help.discord.on")
		}
		return i18n.T(lang, "help.discord.off")
	}
	every := func(d time.Duration, note string) string {
		if d <= 0 {
			return i18n.T(lang, "help.discord.off")
		}
		value := strings.ReplaceAll(i18n.T(lang, "help.discord.every"), "%s", helpDuration(d))
		if note != "" {
			value += " " + note
		}
		return value
	}
	view := &helpDiscordView{
		Facts: []helpFact{
			// A configured instance always posts routed alerts on its
			// webhook channel.
			{Label: i18n.T(lang, "help.discord.channel"), Value: onOff(true), On: true},
			{Label: i18n.T(lang, "help.discord.dm"), Value: onOff(info.DirectMessages), On: info.DirectMessages},
			{Label: i18n.T(lang, "help.discord.status"),
				Value: every(info.HazardsInterval, i18n.T(lang, "help.discord.status_note")), On: info.HazardsInterval > 0},
			{Label: i18n.T(lang, "help.discord.uptime"),
				Value: every(info.UptimeInterval, ""), On: info.UptimeInterval > 0},
		},
		Items: []string{
			"help.discord.item.1", "help.discord.item.2", "help.discord.item.3",
		},
	}
	if !info.DirectMessages {
		view.Items = view.Items[:1]
	}
	return view
}

// helpDuration renders a notice interval the way an operator reads it
// ("1h", "30 min", "4h", "2d").
func helpDuration(d time.Duration) string {
	switch {
	case d <= 0:
		return ""
	case d < time.Hour:
		return fmt.Sprintf("%d min", int(d.Minutes()))
	case d < 24*time.Hour:
		if d%time.Hour == 0 {
			return fmt.Sprintf("%dh", int(d.Hours()))
		}
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		if d%(24*time.Hour) == 0 {
			return fmt.Sprintf("%dd", int(d.Hours())/24)
		}
		return fmt.Sprintf("%dd%dh", int(d.Hours())/24, int(d.Hours())%24)
	}
}

// handleHelpPage renders the Help manual for any signed-in user.
func (s *Server) handleHelpPage(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	view := s.buildHelpView(s.langFor(r))
	view.CSRF = sess.csrf
	view.Username = sess.username
	view.Role = sess.role
	w.Header().Set("Cache-Control", "no-store")
	s.renderL(w, r, "help", view)
}

// helpCommands renders the radio-command reference from the shared
// interpreter: every command the station answers, in registration order.
func (s *Server) helpCommands(lang string) []helpCommandView {
	if s.radioCLI == nil {
		return nil
	}
	cmds := s.radioCLI.Commands()
	out := make([]helpCommandView, 0, len(cmds))
	for _, c := range cmds {
		usageKey := "help.commands." + c.Name + ".usage"
		usage := i18n.T(lang, usageKey)
		if usage == usageKey {
			usage = "/" + c.Name
		}
		descKey := "help.commands." + c.Name + ".desc"
		desc := i18n.T(lang, descKey)
		if desc == descKey {
			desc = c.Description
		}
		out = append(out, helpCommandView{Usage: usage, Description: desc, Restricted: c.Restricted})
	}
	return out
}
