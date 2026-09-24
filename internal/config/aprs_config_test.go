package config

import (
	"strings"
	"testing"
	"time"
)

func TestAPRSDefaults(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, ""))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.APRS.Enabled {
		t.Error("aprs must default to disabled")
	}
	if cfg.APRS.RadiusKM != 60 {
		t.Errorf("radius default = %v, want 60", cfg.APRS.RadiusKM)
	}
	if cfg.APRS.StationTTL != 30*time.Minute {
		t.Errorf("station_ttl default = %s, want 30m", cfg.APRS.StationTTL)
	}
	if !cfg.APRS.ExcludeInfrastructure {
		t.Error("exclude_infrastructure must default to true")
	}
}

func TestAPRSConfigAccepted(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, `
aprs:
  enabled: true
  callsign: sp9moa-10
  icon: /j
  gridsquare: jo90ww
  radius_km: 25
  station_ttl: 15m
  exclude_infrastructure: false
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	a := cfg.APRS
	if !a.Enabled || a.Callsign != "SP9MOA-10" || a.Icon != "/j" {
		t.Errorf("aprs = %+v", a)
	}
	if a.GridSquare != "JO90WW" || a.RadiusKM != 25 || a.StationTTL != 15*time.Minute {
		t.Errorf("aprs = %+v", a)
	}
	if a.ExcludeInfrastructure {
		t.Error("explicit exclude_infrastructure: false must be honoured")
	}
}

func TestAPRSPositionConfig(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, `
aprs:
  enabled: true
  callsign: "SP9MOA-10"
  gridsquare: "JO90WW"
  latitude: 50.0212
  longitude: 20.2075
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.APRS.Latitude == nil || cfg.APRS.Longitude == nil {
		t.Fatalf("position not parsed: %+v", cfg.APRS)
	}
	if *cfg.APRS.Latitude != 50.0212 || *cfg.APRS.Longitude != 20.2075 {
		t.Errorf("position = (%v, %v), want (50.0212, 20.2075)", *cfg.APRS.Latitude, *cfg.APRS.Longitude)
	}
}

func TestAPRSConfigRejected(t *testing.T) {
	cases := map[string]string{
		"bad callsign":      `aprs: {enabled: true, callsign: "BAD CALL", gridsquare: "JO90WW"}`,
		"bad gridsquare":    `aprs: {enabled: true, callsign: "SP9MOA-10", gridsquare: "JO9X"}`,
		"radius too small":  `aprs: {enabled: true, callsign: "SP9MOA-10", gridsquare: "JO90WW", radius_km: 0}`,
		"radius too large":  `aprs: {enabled: true, callsign: "SP9MOA-10", gridsquare: "JO90WW", radius_km: 5000}`,
		"ttl too small":     `aprs: {enabled: true, callsign: "SP9MOA-10", gridsquare: "JO90WW", station_ttl: 10s}`,
		"ttl too large":     `aprs: {enabled: true, callsign: "SP9MOA-10", gridsquare: "JO90WW", station_ttl: 48h}`,
		"icon too long":     `aprs: {enabled: true, callsign: "SP9MOA-10", gridsquare: "JO90WW", icon: "/j1"}`,
		"only latitude":     `aprs: {enabled: true, callsign: "SP9MOA-10", gridsquare: "JO90WW", latitude: 50.0}`,
		"only longitude":    `aprs: {enabled: true, callsign: "SP9MOA-10", gridsquare: "JO90WW", longitude: 20.0}`,
		"latitude too far":  `aprs: {enabled: true, callsign: "SP9MOA-10", gridsquare: "JO90WW", latitude: 91.0, longitude: 20.0}`,
		"longitude too far": `aprs: {enabled: true, callsign: "SP9MOA-10", gridsquare: "JO90WW", latitude: 50.0, longitude: 181.0}`,
	}
	for name, yaml := range cases {
		if _, err := Load(writeTempConfig(t, yaml)); err == nil {
			t.Errorf("%s: invalid aprs config accepted", name)
		}
	}
}

func TestAPRSDisabledSkipsValidation(t *testing.T) {
	// A disabled hub must not validate its identity.
	cfg, err := Load(writeTempConfig(t, `
aprs:
  enabled: false
  callsign: ""
  gridsquare: ""
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.APRS.Enabled {
		t.Error("aprs must be disabled")
	}
}

func TestAPRSUnknownFieldRejected(t *testing.T) {
	_, err := Load(writeTempConfig(t, `
aprs:
  enabled: true
  callsign: "SP9MOA-10"
  gridsquare: "JO90WW"
  bogus_field: 1
`))
	if err == nil || !strings.Contains(err.Error(), "bogus_field") {
		t.Errorf("unknown aprs field must fail validation, got %v", err)
	}
}
