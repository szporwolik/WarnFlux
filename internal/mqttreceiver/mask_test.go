package mqttreceiver

import (
	"testing"

	"github.com/szporwolik/WarnFlux/internal/dispatch/state"
	"github.com/szporwolik/WarnFlux/internal/mqttpolicy"
)

// TestRawCategoryClassification pins the suffix → publish-mask mapping:
// every raw publisher's traffic is attributed to the right checkbox.
func TestRawCategoryClassification(t *testing.T) {
	cases := []struct {
		suffix string
		cat    mqttpolicy.Category
	}{
		{"aprs/stations/SP9MOA-10", mqttpolicy.CatAPRSStations},
		{"aprs/bulletins/SP9MOA-1700000000", mqttpolicy.CatAPRSBulletins},
		{"aprs/packets", mqttpolicy.CatAPRSPackets},
		{"aprs/messages", mqttpolicy.CatAPRSMessages},
		{"events", mqttpolicy.CatEvents},
		{"meshtastic/stations/d1e51b043a9c", mqttpolicy.CatMeshtasticStations},
		{"meshtastic/messages", mqttpolicy.CatMeshtasticMessages},
		{"info/emcom/emcom/sp9moa/emcom", mqttpolicy.CatInfo},
		{"anything/else", 0}, // unclassified = always allowed
	}
	for _, c := range cases {
		if got := rawCategory(c.suffix); got != c.cat {
			t.Errorf("rawCategory(%q) = %#x, want %#x", c.suffix, uint32(got), uint32(c.cat))
		}
	}
}

// TestPublishMaskShortCircuits: a masked category is skipped BEFORE any
// receiver lookup, so even a manager with no connected receiver returns
// nil (the policy, not the transport, decides). An unmasked category
// still reports the transport error.
func TestPublishMaskShortCircuits(t *testing.T) {
	t.Cleanup(func() { mqttpolicy.Set(uint32(mqttpolicy.CatAll)) })

	m := mgrEnv(t, nil) // no receivers at all

	mqttpolicy.Set(uint32(mqttpolicy.CatAll) &^ uint32(mqttpolicy.CatAPRSStations|mqttpolicy.CatActive))
	if err := m.PublishRaw("aprs/stations/SP9XYZ-1", true, []byte("{}")); err != nil {
		t.Errorf("masked PublishRaw returned %v, want nil (skipped)", err)
	}
	if err := m.PublishActive("imgw", state.Hazard{EventKey: "imgw:1"}); err != nil {
		t.Errorf("masked PublishActive returned %v, want nil (skipped)", err)
	}
	if err := m.ExpireActive("imgw", "imgw:1"); err != nil {
		t.Errorf("masked ExpireActive returned %v, want nil (skipped)", err)
	}

	// Unmasked category still tries the transport and reports the error.
	if err := m.PublishRaw("meshtastic/messages", false, []byte("{}")); err == nil {
		t.Error("unmasked PublishRaw with no receivers must fail, got nil")
	}
}
