package config

import (
	"testing"
	"time"
)

// TestPinUserNodesDefaultsAndValidation pins the two knobs the radio pinning
// uses: favorite pinning is ON by default (an operator who registers a node
// expects the station to keep being able to message it) and the interval is
// bounded, so a typo cannot make the station hammer the radio's admin API.
func TestPinUserNodesDefaultsAndValidation(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, "app:\n  log_level: info\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Meshtastic.PinUserNodes {
		t.Error("pin_user_nodes defaults to false, want true")
	}
	if cfg.Meshtastic.PinUserNodesInterval != 6*time.Hour {
		t.Errorf("pin_user_nodes_interval defaults to %s, want 6h", cfg.Meshtastic.PinUserNodesInterval)
	}

	off, err := Load(writeTempConfig(t, "meshtastic:\n  enabled: false\n  pin_user_nodes: false\n  pin_user_nodes_interval: 90m\n"))
	if err != nil {
		t.Fatalf("Load with explicit values: %v", err)
	}
	if off.Meshtastic.PinUserNodes || off.Meshtastic.PinUserNodesInterval != 90*time.Minute {
		t.Errorf("explicit values not applied: %+v", off.Meshtastic)
	}

	if _, err := Load(writeTempConfig(t, "meshtastic:\n  enabled: false\n  pin_user_nodes_interval: 30s\n")); err == nil {
		t.Error("an interval below 1m was accepted")
	}
	if _, err := Load(writeTempConfig(t, "meshtastic:\n  enabled: false\n  pin_user_nodes_interval: 48h\n")); err == nil {
		t.Error("an interval above 24h was accepted")
	}
}
