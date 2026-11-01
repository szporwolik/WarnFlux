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
