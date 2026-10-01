package config

import (
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/mqttpolicy"
)

// TestMQTTPublishDefaults: an omitted mqtt_publish block enables every
// category (backwards compatible).
func TestMQTTPublishDefaults(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, "app:\n  log_level: info\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MQTTPublish.Mask() != uint32(mqttpolicy.CatAll) {
		t.Errorf("default mask = %#x, want all categories", cfg.MQTTPublish.Mask())
	}
}

// TestMQTTPublishExplicit: an explicit block is honored, defaults fill
// the omitted keys.
func TestMQTTPublishExplicit(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, `
app:
  log_level: info
mqtt_publish:
  events: true
  status: false
  meshcore_messages: false
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	mask := cfg.MQTTPublish.Mask()
	if mask&uint32(mqttpolicy.CatEvents) == 0 {
		t.Error("events must stay enabled")
	}
	if mask&uint32(mqttpolicy.CatStatus) != 0 {
		t.Error("status must be disabled")
	}
	if mask&uint32(mqttpolicy.CatMeshcoreMessages) != 0 {
		t.Error("meshcore_messages must be disabled")
	}
	if mask&uint32(mqttpolicy.CatActive) == 0 {
		t.Error("omitted active must default to enabled")
	}
}

// TestMQTTPublishUnknownKey: strict decoding rejects typos.
func TestMQTTPublishUnknownKey(t *testing.T) {
	_, err := Load(writeTempConfig(t, `
app:
  log_level: info
mqtt_publish:
  aprs_stations: true
  aprs_stationz: true
`))
	if err == nil || !strings.Contains(err.Error(), "aprs_stationz") {
		t.Fatalf("unknown key must fail decoding, got %v", err)
	}
}
