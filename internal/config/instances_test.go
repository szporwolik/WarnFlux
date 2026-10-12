package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInstancesReadsSources(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "c.yaml")
	os.WriteFile(p, []byte(`sources:
  - id: weather-home
    type: openmeteo
    config:
      poll_interval: 2h
  - id: planes
    type: adsb
    config:
      request_timeout: 5s
actions:
  - id: sosna
    type: discord
    config:
      hazards_interval: 1h
`), 0o600)
	got, err := Instances(p, "sources")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Index != 0 || got[0].ID != "weather-home" || got[0].Type != "openmeteo" ||
		got[0].PollInterval != 2*time.Hour || !got[0].HasPoll {
		t.Fatalf("first instance: %+v", got[0])
	}
	if got[1].HasPoll || got[1].ConfigPath("sources") != "sources[1].config" {
		t.Fatalf("second instance: %+v", got[1])
	}
	acts, err := Instances(p, "actions")
	if err != nil || len(acts) != 1 || acts[0].Type != "discord" {
		t.Fatalf("actions: %+v err=%v", acts, err)
	}
	if missing, err := Instances(p, "outputs"); err != nil || len(missing) != 0 {
		t.Fatalf("absent section: %+v err=%v", missing, err)
	}
}
