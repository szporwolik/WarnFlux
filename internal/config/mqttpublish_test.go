package config

import (
	"strings"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/mqttpolicy"
)

// TestMQTTPublishDefaults: an omitted mqtt_publish block enables the
// canonical stream and the retained current-state documents; the noisy
// per-packet/per-message mirrors stay off.
func TestMQTTPublishDefaults(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, "app:\n  log_level: info\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := uint32(mqttpolicy.CatAll) &^
		uint32(mqttpolicy.CatAPRSPackets|mqttpolicy.CatAPRSMessages|mqttpolicy.CatMeshtasticMessages)
	if cfg.MQTTPublish.Mask() != want {
		t.Errorf("default mask = %#x, want quiet mask %#x", cfg.MQTTPublish.Mask(), want)
	}
	if cfg.MQTTPublish.Mask()&uint32(mqttpolicy.CatEvents) == 0 {
		t.Error("events must stay enabled by default (canonical integration stream)")
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
  meshtastic_messages: false
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
	if mask&uint32(mqttpolicy.CatMeshtasticMessages) != 0 {
		t.Error("meshtastic_messages must be disabled")
	}
	if mask&uint32(mqttpolicy.CatActive) == 0 {
		t.Error("omitted active must default to enabled")
	}
	if mask&uint32(mqttpolicy.CatAPRSPackets) != 0 {
		t.Error("omitted aprs_packets must default to disabled")
	}
	if mask&uint32(mqttpolicy.CatAPRSMessages) != 0 {
		t.Error("omitted aprs_messages must default to disabled")
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
