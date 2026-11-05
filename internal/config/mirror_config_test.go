package config

import (
	"os"
	"path/filepath"
	"testing"
)

// The mirror's "channel: 0" means the announcement channel, and it has to be
// resolved while loading: a consumer that only sees MeshtasticConfig must never
// end up mirroring the public primary channel.
func TestTrafficMirrorDefaultsToTheAnnouncementChannel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := `
meshtastic:
  enabled: true
  device: /dev/ttyACM0
  emcom_channel: 3
  traffic_mirror:
    webhook_url: "https://discord.com/api/webhooks/1/token"
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Meshtastic.TrafficMirror.Channel; got != 3 {
		t.Fatalf("mirror channel = %d, want the announcement channel 3", got)
	}
	if !cfg.Meshtastic.TrafficMirror.IncludeSystem {
		t.Fatal("include_system = false, want the station sends mirrored by default")
	}
}

// An explicit channel wins over the announcement channel.
func TestTrafficMirrorKeepsExplicitChannel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := `
meshtastic:
  enabled: true
  device: /dev/ttyACM0
  emcom_channel: 3
  traffic_mirror:
    webhook_url: "https://discord.com/api/webhooks/1/token"
    channel: 5
    include_system: false
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Meshtastic.TrafficMirror.Channel; got != 5 {
		t.Fatalf("mirror channel = %d, want the explicit 5", got)
	}
	if cfg.Meshtastic.TrafficMirror.IncludeSystem {
		t.Fatal("include_system = true, want the explicit opt-out honoured")
	}
}

// A list entry names the channel it watches: channel 0 there is the public
// PRIMARY channel, not the announcement channel the single block defaults to.
func TestTrafficMirrorsWatchTheChannelTheyName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := `
meshtastic:
  enabled: true
  device: /dev/ttyACM0
  emcom_channel: 1
  traffic_mirror:
    webhook_url: "https://discord.com/api/webhooks/1/club"
  traffic_mirrors:
    - channel: 0
      webhook_url: "https://discord.com/api/webhooks/2/primary"
      username: "SOSNA Mesh ch0"
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Meshtastic.TrafficMirror.Channel; got != 1 {
		t.Fatalf("club mirror channel = %d, want the announcement channel 1", got)
	}
	if len(cfg.Meshtastic.TrafficMirrors) != 1 {
		t.Fatalf("additional mirrors = %d, want 1", len(cfg.Meshtastic.TrafficMirrors))
	}
	extra := cfg.Meshtastic.TrafficMirrors[0]
	if extra.Channel != 0 {
		t.Errorf("list entry channel = %d, want the PRIMARY channel 0", extra.Channel)
	}
	if extra.WebhookURL != "https://discord.com/api/webhooks/2/primary" {
		t.Errorf("list entry webhook = %q", extra.WebhookURL)
	}
	if extra.Username != "SOSNA Mesh ch0" {
		t.Errorf("list entry username = %q", extra.Username)
	}
	if !extra.IncludeSystem {
		t.Error("list entry include_system = false, want the default true")
	}
}

// A broken extra mirror is refused at load, not silently dropped.
func TestTrafficMirrorsValidateWebhook(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := `
meshtastic:
  enabled: true
  device: /dev/ttyACM0
  traffic_mirrors:
    - channel: 0
      webhook_url: "nie-url"
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("a malformed traffic_mirrors webhook was accepted")
	}
}
