package core

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestWeatherSnapshotValidateDailyDates: daily dates must be real calendar
// dates in YYYY-MM-DD form, strictly ascending.
func TestWeatherSnapshotValidateDailyDates(t *testing.T) {
	valid := []string{"2026-09-22", "2026-09-23"}
	reject := []string{
		"2026-02-29", // 2026 is not a leap year
		"2026-13-01", // month 13
		"2026-09-31", // day 31 in September
		"2026/09/22", // wrong format
		"22-09-2026", // wrong format
		"banana",     // arbitrary text
		"zebra",      // arbitrary text
		"",           // empty
	}

	s := baseWeatherSnapshot()
	s.Current = nil
	s.Daily = []WeatherDaily{
		{Date: "2028-02-29", Condition: ConditionClear}, // leap year: valid
	}
	if err := s.Validate(); err != nil {
		t.Errorf("valid leap-day date rejected: %v", err)
	}

	s.Daily = []WeatherDaily{
		{Date: valid[0], Condition: ConditionClear},
		{Date: valid[1], Condition: ConditionClear},
	}
	if err := s.Validate(); err != nil {
		t.Errorf("valid ascending dates rejected: %v", err)
	}

	for _, bad := range reject {
		s := baseWeatherSnapshot()
		s.Current = nil
		s.Daily = []WeatherDaily{{Date: bad, Condition: ConditionClear}}
		if err := s.Validate(); err == nil {
			t.Errorf("date %q: expected rejection, got nil", bad)
		}
	}

	// Duplicate and descending valid dates are still rejected.
	for _, dates := range [][]string{{"2026-09-22", "2026-09-22"}, {"2026-09-23", "2026-09-22"}} {
		s := baseWeatherSnapshot()
		s.Current = nil
		s.Daily = []WeatherDaily{
			{Date: dates[0], Condition: ConditionClear},
			{Date: dates[1], Condition: ConditionClear},
		}
		if err := s.Validate(); err == nil {
			t.Errorf("dates %v: expected rejection, got nil", dates)
		}
	}
}

// TestWeatherSnapshotValidateTimezone: the public contract promises an IANA
// timezone, so canonical validation must reject unknown zones.
func TestWeatherSnapshotValidateTimezone(t *testing.T) {
	for _, tz := range []string{"Europe/Warsaw", "UTC", "America/New_York"} {
		s := baseWeatherSnapshot()
		s.Location.Timezone = tz
		if err := s.Validate(); err != nil {
			t.Errorf("timezone %q: expected acceptance, got %v", tz, err)
		}
	}
	for _, tz := range []string{"Mars/Olympus", "garbage", "Europe/Warsaw "} {
		s := baseWeatherSnapshot()
		s.Location.Timezone = tz
		if err := s.Validate(); err == nil {
			t.Errorf("timezone %q: expected rejection, got nil", tz)
		}
	}
}

// TestWeatherSnapshotValidateBounds: the canonical arrays are bounded
// before serialization so a buggy adapter cannot build a giant snapshot.
func TestWeatherSnapshotValidateBounds(t *testing.T) {
	hourly := make([]WeatherHourly, MaxWeatherHourlyEntries)
	base := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	for i := range hourly {
		hourly[i] = WeatherHourly{Time: base.Add(time.Duration(i) * time.Hour), Condition: ConditionClear}
	}
	daily := make([]WeatherDaily, MaxWeatherDailyEntries)
	for i := range daily {
		daily[i] = WeatherDaily{Date: base.AddDate(0, 0, i).Format("2006-01-02"), Condition: ConditionClear}
	}

	s := baseWeatherSnapshot()
	s.Current = nil
	s.Hourly = hourly
	if err := s.Validate(); err != nil {
		t.Errorf("hourly at exactly the maximum rejected: %v", err)
	}
	s.Hourly = append(s.Hourly, WeatherHourly{Time: base.AddDate(0, 0, 2000), Condition: ConditionClear})
	if err := s.Validate(); err == nil {
		t.Error("hourly above the maximum was accepted")
	}

	s = baseWeatherSnapshot()
	s.Current = nil
	s.Daily = daily
	if err := s.Validate(); err != nil {
		t.Errorf("daily at exactly the maximum rejected: %v", err)
	}
	s.Daily = append(s.Daily, WeatherDaily{Date: "2028-01-01", Condition: ConditionClear})
	if err := s.Validate(); err == nil {
		t.Error("daily above the maximum was accepted")
	}
}

// TestWeatherSnapshotValidateStringBounds: public-model strings carry
// byte caps and oversized values are rejected, never truncated.
func TestWeatherSnapshotValidateStringBounds(t *testing.T) {
	cases := []struct {
		name    string
		max     int
		mut     func(*WeatherSnapshot, string)
		exceeds func(*WeatherSnapshot)
	}{
		{
			"provider name", MaxProviderNameBytes,
			func(s *WeatherSnapshot, v string) { s.Provider.Name = v },
			func(s *WeatherSnapshot) { s.Provider.Name = strings.Repeat("x", MaxProviderNameBytes+1) },
		},
		{
			"provider attribution", MaxProviderAttributionBytes,
			func(s *WeatherSnapshot, v string) { s.Provider.Attribution = v },
			func(s *WeatherSnapshot) { s.Provider.Attribution = strings.Repeat("x", MaxProviderAttributionBytes+1) },
		},
		{
			"location name", MaxLocationNameBytes,
			func(s *WeatherSnapshot, v string) { s.Location.Name = v },
			func(s *WeatherSnapshot) { s.Location.Name = strings.Repeat("x", MaxLocationNameBytes+1) },
		},
	}
	for _, c := range cases {
		s := baseWeatherSnapshot()
		c.mut(&s, strings.Repeat("x", c.max))
		if err := s.Validate(); err != nil {
			t.Errorf("%s at exactly the maximum rejected: %v", c.name, err)
		}
		s = baseWeatherSnapshot()
		c.exceeds(&s)
		if err := s.Validate(); err == nil {
			t.Errorf("%s above the maximum was accepted", c.name)
		}
	}

	// Timezone is byte-capped before the IANA lookup.
	s := baseWeatherSnapshot()
	s.Location.Timezone = strings.Repeat("x", MaxTimezoneBytes+1)
	if err := s.Validate(); err == nil {
		t.Error("oversized timezone was accepted")
	}
}

// TestWeatherSnapshotValidateUTF8: canonical public text must be valid
// UTF-8 before JSON serialization.
func TestWeatherSnapshotValidateUTF8(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*WeatherSnapshot)
	}{
		{"provider name", func(s *WeatherSnapshot) { s.Provider.Name = "Open\xffMeteo" }},
		{"provider attribution", func(s *WeatherSnapshot) { s.Provider.Attribution = "data\xffby" }},
		{"location name", func(s *WeatherSnapshot) { s.Location.Name = "Ho\xffme" }},
		{"timezone", func(s *WeatherSnapshot) { s.Location.Timezone = "Europe/Warsaw\xff" }},
		{"current condition code", func(s *WeatherSnapshot) {
			v := "co\xffde"
			s.Current.ProviderConditionCode = &v
		}},
	}
	for _, c := range cases {
		s := baseWeatherSnapshot()
		c.mut(&s)
		if err := s.Validate(); err == nil {
			t.Errorf("%s: invalid UTF-8 was accepted", c.name)
		}
	}
}

// TestWeatherSnapshotConditionCodeSemantics: nil means the provider had no
// raw code; a non-nil value must be non-empty, valid UTF-8 and bounded.
func TestWeatherSnapshotConditionCodeSemantics(t *testing.T) {
	// nil is accepted (unavailable).
	s := baseWeatherSnapshot()
	s.Current.ProviderConditionCode = nil
	if err := s.Validate(); err != nil {
		t.Errorf("nil condition code rejected: %v", err)
	}
	// A non-nil empty code is rejected: empty metadata is not useful.
	empty := ""
	s = baseWeatherSnapshot()
	s.Current.ProviderConditionCode = &empty
	if err := s.Validate(); err == nil {
		t.Error("empty condition code was accepted")
	}
	// Exact maximum accepted; one over rejected.
	maxed := strings.Repeat("9", MaxProviderConditionCodeBytes)
	s = baseWeatherSnapshot()
	s.Current.ProviderConditionCode = &maxed
	if err := s.Validate(); err != nil {
		t.Errorf("condition code at exactly the maximum rejected: %v", err)
	}
	s = baseWeatherSnapshot()
	over := strings.Repeat("9", MaxProviderConditionCodeBytes+1)
	s.Current.ProviderConditionCode = &over
	if err := s.Validate(); err == nil {
		t.Error("oversized condition code was accepted")
	}
}

// TestWeatherGeneratedAtAnyZoneSerializesUTC pins the wire rule:
// generated_at/valid_until are normalized to UTC while provider forecast
// timestamps keep their location offset.
func TestWeatherGeneratedAtAnyZoneSerializesUTC(t *testing.T) {
	s := baseWeatherSnapshot()
	s.GeneratedAt = time.Date(2026, 9, 22, 14, 0, 0, 0, time.FixedZone("CEST", 2*3600))
	data, err := MarshalWeatherSnapshot(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out struct {
		GeneratedAt string `json:"generated_at"`
		Current     struct {
			Time string `json:"time"`
		} `json:"current"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.GeneratedAt != "2026-09-22T12:00:00Z" {
		t.Errorf("generated_at = %q, want UTC-normalized 2026-09-22T12:00:00Z", out.GeneratedAt)
	}
	if out.Current.Time != "2026-09-22T14:00:00+02:00" {
		t.Errorf("current.time = %q, want location offset preserved", out.Current.Time)
	}
}
