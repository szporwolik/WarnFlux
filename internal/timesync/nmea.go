// Package timesync keeps the station's clock good without the internet: NTP
// while the network answers, the GPS receiver when it does not, and a small
// SNTP server so the rest of the LAN can take its time from this station while
// it runs off-grid.
package timesync

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Fix is the UTC time carried by one NMEA sentence.
type Fix struct {
	// Time is the UTC instant the receiver reported.
	Time time.Time
	// Valid is false when the receiver itself says the time is not usable
	// (RMC status "V"): a receiver without a fix still prints its own clock,
	// which must never discipline a station.
	Valid bool
	// Satellites is the number of satellites in use (GGA), 0 when the
	// sentence does not say.
	Satellites int
	// Sentence names what the fix came from ("$GNRMC").
	Sentence string
}

// ParseNMEA parses one NMEA 0183 line into a Fix. ok reports whether the line
// was a time-bearing sentence this package understands; a malformed sentence
// returns an error so the reader can count it.
func ParseNMEA(line string) (fix Fix, ok bool, err error) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "$") {
		return Fix{}, false, nil
	}
	body := line[1:]
	if i := strings.IndexByte(body, '*'); i >= 0 {
		if !checksumOK(body) {
			return Fix{}, false, fmt.Errorf("nmea: checksum mismatch in %q", line)
		}
		body = body[:i]
	}
	fields := strings.Split(body, ",")
	if len(fields) < 2 {
		return Fix{}, false, nil
	}

	switch strings.ToUpper(fields[0]) {
	case "GPRMC", "GNRMC", "GLRMC", "GARMC", "BDRMC":
		fix, err = parseRMC(fields)
		return fix, true, err
	case "GPZDA", "GNZDA", "GLZDA", "GAZDA", "BDZDA":
		fix, err = parseZDA(fields)
		return fix, true, err
	case "GPGGA", "GNGGA", "GLGGA", "GAGGA", "BDGGA":
		// GGA carries a time but no date: it is used for the satellite count
		// only, never to set the clock (a wrong date is worse than a few
		// milliseconds of serial latency).
		if len(fields) > 7 {
			if sats, convErr := strconv.Atoi(strings.TrimSpace(fields[7])); convErr == nil {
				fix.Satellites = sats
			}
		}
		fix.Sentence = "$" + strings.ToUpper(fields[0])
		return fix, true, nil
	}
	return Fix{}, false, nil
}

// parseRMC reads the recommended minimum sentence:
// $--RMC,hhmmss.ss,A,ddmm.mmmm,N,dddmm.mmmm,E,kn,deg,ddmmyy,...*hh
func parseRMC(fields []string) (Fix, error) {
	if len(fields) < 10 {
		return Fix{}, fmt.Errorf("nmea: short RMC (%d fields)", len(fields))
	}
	fix := Fix{Sentence: "$" + strings.ToUpper(fields[0])}
	fix.Valid = strings.EqualFold(strings.TrimSpace(fields[2]), "A")
	t, err := parseTimeAndDate(fields[1], fields[9])
	if err != nil {
		return fix, err
	}
	fix.Time = t
	return fix, nil
}

// parseZDA reads the date/time sentence:
// $--ZDA,hhmmss.ss,dd,mm,yyyy,zh,zm*hh
func parseZDA(fields []string) (Fix, error) {
	if len(fields) < 5 {
		return Fix{}, fmt.Errorf("nmea: short ZDA (%d fields)", len(fields))
	}
	fix := Fix{Sentence: "$" + strings.ToUpper(fields[0]), Valid: true}
	clock := strings.TrimSpace(fields[1])
	frac, err := fraction(clock)
	if err != nil {
		return fix, err
	}
	if dot := strings.IndexByte(clock, '.'); dot >= 0 {
		clock = clock[:dot]
	}
	h, m, s, err := parseClock(clock)
	if err != nil {
		return fix, err
	}
	day, err := strconv.Atoi(strings.TrimSpace(fields[2]))
	if err != nil {
		return fix, fmt.Errorf("nmea: bad ZDA day %q", fields[2])
	}
	month, err := strconv.Atoi(strings.TrimSpace(fields[3]))
	if err != nil {
		return fix, fmt.Errorf("nmea: bad ZDA month %q", fields[3])
	}
	year, err := strconv.Atoi(strings.TrimSpace(fields[4]))
	if err != nil {
		return fix, fmt.Errorf("nmea: bad ZDA year %q", fields[4])
	}
	fix.Time = time.Date(year, time.Month(month), day, h, m, s, int(frac), time.UTC)
	return fix, nil
}

// parseTimeAndDate combines the RMC clock (hhmmss.ss) with its date (ddmmyy).
func parseTimeAndDate(clock, date string) (time.Time, error) {
	clock = strings.TrimSpace(clock)
	date = strings.TrimSpace(date)
	if clock == "" || date == "" {
		return time.Time{}, fmt.Errorf("nmea: empty RMC time or date")
	}
	frac, err := fraction(clock)
	if err != nil {
		return time.Time{}, err
	}
	if dot := strings.IndexByte(clock, '.'); dot >= 0 {
		clock = clock[:dot]
	}
	h, m, s, err := parseClock(clock)
	if err != nil {
		return time.Time{}, err
	}
	if len(date) != 6 {
		return time.Time{}, fmt.Errorf("nmea: bad date %q", date)
	}
	day, err1 := strconv.Atoi(date[0:2])
	month, err2 := strconv.Atoi(date[2:4])
	year, err3 := strconv.Atoi(date[4:6])
	if err1 != nil || err2 != nil || err3 != nil {
		return time.Time{}, fmt.Errorf("nmea: bad date %q", date)
	}
	// NMEA carries two digits of year; the GNSS era starts in 1980.
	full := 2000 + year
	if full > time.Now().UTC().Year()+1 {
		full = 1900 + year
	}
	return time.Date(full, time.Month(month), day, h, m, s, int(frac), time.UTC), nil
}

// fraction parses the sub-second part of an NMEA clock ("hhmmss.sss").
func fraction(clock string) (time.Duration, error) {
	dot := strings.IndexByte(clock, '.')
	if dot < 0 {
		return 0, nil
	}
	digits := clock[dot+1:]
	if len(digits) > 9 {
		digits = digits[:9]
	}
	nanos, err := strconv.Atoi(digits)
	if err != nil {
		return 0, fmt.Errorf("nmea: bad time fraction %q", clock)
	}
	for i := len(digits); i < 9; i++ {
		nanos *= 10
	}
	return time.Duration(nanos), nil
}

// parseClock splits hhmmss into its parts.
func parseClock(hhmmss string) (h, m, s int, err error) {
	if len(hhmmss) < 6 {
		return 0, 0, 0, fmt.Errorf("nmea: bad clock %q", hhmmss)
	}
	if h, err = strconv.Atoi(hhmmss[0:2]); err != nil {
		return 0, 0, 0, fmt.Errorf("nmea: bad clock %q", hhmmss)
	}
	if m, err = strconv.Atoi(hhmmss[2:4]); err != nil {
		return 0, 0, 0, fmt.Errorf("nmea: bad clock %q", hhmmss)
	}
	if s, err = strconv.Atoi(hhmmss[4:6]); err != nil {
		return 0, 0, 0, fmt.Errorf("nmea: bad clock %q", hhmmss)
	}
	return h, m, s, nil
}

// checksumOK verifies the NMEA XOR checksum after the trailing '*'.
func checksumOK(body string) bool {
	star := strings.IndexByte(body, '*')
	if star < 0 || star+3 > len(body) {
		return false
	}
	want, err := strconv.ParseUint(body[star+1:star+3], 16, 8)
	if err != nil {
		return false
	}
	var sum byte
	for i := 0; i < star; i++ {
		sum ^= body[i]
	}
	return sum == byte(want)
}
