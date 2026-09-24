package aprs

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// maxLineBytes bounds one APRS-IS frame (position + comment). Longer lines
// are truncated defensively; APRS frames are ~100 bytes in practice.
const maxLineBytes = 1024

// micEDstRE is the destination-field shape of a Mic-E frame.
var micEDstRE = regexp.MustCompile(`^[0-9A-Z]{3}[0-9L-Z]{3}$`)

// ParseFeedLine parses one APRS-IS / TNC2 frame into a Packet. Malformed
// frames never fail the caller: they come back with KindOther and whatever
// header fields could be extracted.
func ParseFeedLine(line string, now time.Time) Packet {
	p := Packet{
		Raw:        strings.TrimRight(line, "\r\n"),
		Kind:       KindOther,
		ReceivedAt: now.Unix(),
	}
	if len(p.Raw) > maxLineBytes {
		p.Raw = p.Raw[:maxLineBytes]
	}

	gt := strings.IndexByte(p.Raw, '>')
	colon := strings.IndexByte(p.Raw, ':')
	if gt <= 0 || colon < gt {
		return p
	}
	p.Src = NormalizeCallsign(p.Raw[:gt])
	header := p.Raw[gt+1 : colon]
	if parts := strings.Split(header, ","); len(parts) > 0 {
		p.Dst = parts[0]
		p.Path = parts[1:]
	}

	info := p.Raw[colon+1:]
	if info == "" {
		return p
	}
	switch info[0] {
	case '!':
		parsePositionBody(&p, info[1:], false)
	case '=':
		parsePositionBody(&p, info[1:], true)
	case '/', '\\':
		// Both compressed and uncompressed positions start with the symbol
		// table; the uncompressed form carries hemisphere letters at fixed
		// offsets (body[8] and body[17]).
		if len(info) > 18 && (info[9] == 'N' || info[9] == 'S') && (info[18] == 'E' || info[18] == 'W') {
			parsePositionBody(&p, info[1:], false)
		} else {
			parseCompressed(&p, info)
		}
	case '@':
		parseTimestamped(&p, info, now)
	case ':':
		parseMessage(&p, info)
	case '>':
		p.Kind = KindStatus
		p.Status = strings.TrimSpace(info[1:])
	case ';', ')':
		parseObject(&p, info)
	case '_':
		// Uncompressed weather reports: full decoding is out of scope;
		// the raw block is preserved in the comment.
		p.Kind = KindWeather
		p.Comment = strings.TrimSpace(info[1:])
	case '`', '\'':
		parseMicE(&p, info)
	case 'T':
		p.Kind = KindTelemetry
		p.Comment = strings.TrimSpace(info[1:])
	case '?':
		p.Kind = KindQuery
		p.Comment = strings.TrimSpace(info[1:])
	default:
		p.Comment = strings.TrimSpace(info)
	}
	return p
}

// parsePositionBody parses an uncompressed position body:
//
//	4903.50N/01934.56E-065/070/A=001234 comment
//
// The separator between latitude and longitude doubles as the APRS
// symbol-table indicator: '/' for the primary table, '\' for the
// alternate one (e.g. the "\?" information-kiosk icon Direwolf sends).
func parsePositionBody(p *Packet, body string, capable bool) {
	p.Kind = KindPosition
	p.MessageCapable = capable
	if len(body) < 19 {
		p.Comment = strings.TrimSpace(body)
		return
	}
	lat, okLat := parseCoord(body[0:8], 2, 90)
	lon, okLon := parseCoord(body[9:18], 3, 180)
	sep := body[8]
	if !okLat || !okLon || (sep != '/' && sep != '\\') {
		p.Comment = strings.TrimSpace(body)
		return
	}
	p.SymbolTable = sep
	p.Position = &Position{Latitude: lat, Longitude: lon}
	p.Symbol = body[18]
	parseExtensions(p, body[19:])
}

// parseCoord parses one coordinate field: degrees + "MM.mm" + hemisphere
// (e.g. "4903.50N", "01934.56E"). limit is the maximum absolute value.
func parseCoord(field string, degDigits, limit int) (float64, bool) {
	if len(field) != degDigits+6 {
		return 0, false
	}
	deg, err1 := strconv.Atoi(field[:degDigits])
	min, err2 := strconv.ParseFloat(field[degDigits:degDigits+5], 64)
	hemi := field[degDigits+5]
	if err1 != nil || err2 != nil || (hemi != 'N' && hemi != 'S' && hemi != 'E' && hemi != 'W') {
		return 0, false
	}
	if min < 0 || min >= 60 || deg > limit {
		return 0, false
	}
	val := float64(deg) + min/60
	if hemi == 'S' || hemi == 'W' {
		val = -val
	}
	return val, true
}

// parseExtensions consumes the optional course/speed and altitude blocks
// that follow an uncompressed position; the remainder is the comment.
func parseExtensions(p *Packet, ext string) {
	// Course/speed: "065/070".
	if len(ext) >= 7 && isDigits(ext[0:3]) && ext[3] == '/' && isDigits(ext[4:7]) {
		p.CourseDeg, _ = strconv.Atoi(ext[0:3])
		speed, _ := strconv.ParseFloat(ext[4:7], 64)
		p.SpeedKMH = speed * 1.852 // knots
		ext = ext[7:]
	}
	// Altitude: "/A=001234" (feet).
	if len(ext) >= 7 && ext[0] == '/' && ext[1] == 'A' && ext[2] == '=' {
		if feet, err := strconv.ParseFloat(strings.TrimSpace(ext[3:9]), 64); err == nil {
			meters := feet * 0.3048
			p.AltitudeM = &meters
		}
		ext = ext[9:]
	}
	p.Comment = strings.TrimSpace(ext)
}

// parseTimestamped parses "@092345z..." positions.
func parseTimestamped(p *Packet, info string, now time.Time) {
	if len(info) < 8 {
		p.Comment = strings.TrimSpace(info[1:])
		return
	}
	if ts, ok := parseAPRSTime(info[1:8], now); ok {
		unix := ts.Unix()
		p.Timestamp = &unix
	}
	parsePositionBody(p, info[8:], false)
}

// parseAPRSTime parses "HHMMSS" + zone ('z' UTC, '/' local, 'h' = zulu in
// practice for fixed HHMMSS fields).
func parseAPRSTime(field string, now time.Time) (time.Time, bool) {
	if len(field) != 7 {
		return time.Time{}, false
	}
	hh, err1 := strconv.Atoi(field[0:2])
	mm, err2 := strconv.Atoi(field[2:4])
	ss, err3 := strconv.Atoi(field[4:6])
	if err1 != nil || err2 != nil || err3 != nil || hh > 23 || mm > 59 || ss > 59 {
		return time.Time{}, false
	}
	loc := time.UTC
	if field[6] == '/' {
		loc = time.Local
	}
	t := time.Date(now.Year(), now.Month(), now.Day(), hh, mm, ss, 0, loc)
	// A timestamp far in the future relative to "now" means the frame
	// crossed midnight UTC; step it back one day.
	if t.Sub(now) > 12*time.Hour {
		t = t.AddDate(0, 0, -1)
	}
	return t, true
}

// parseCompressed parses the base-91 compressed position form:
//
//	/YYYYXXXX$c...  (table, 4 lat chars, 4 lon chars, symbol code)
func parseCompressed(p *Packet, info string) {
	p.Kind = KindPosition
	if len(info) < 10 {
		p.Comment = strings.TrimSpace(info[1:])
		return
	}
	lat, okLat := base91Value(info[1:5])
	lon, okLon := base91Value(info[5:9])
	if !okLat || !okLon {
		p.Comment = strings.TrimSpace(info[1:])
		return
	}
	p.SymbolTable = info[0]
	p.Symbol = info[9]
	p.Position = &Position{
		Latitude:  90 - float64(lat)/380926,
		Longitude: -180 + float64(lon)/190463,
	}

	ext := info[10:]
	// Compressed course/speed: "c<base91>".
	if len(ext) >= 2 && ext[0] == 'c' {
		d := int(ext[1]) - 33
		if d >= 0 && d <= 89 {
			p.SpeedKMH = (math.Pow(1.08, float64(d)) - 1) * 1.60934 // mph → km/h
			p.CourseDeg = (d * 4) % 360
		}
		ext = ext[2:]
	}
	// Compressed altitude: "<base91>{<base91>...}" — meters + 10000.
	if len(ext) >= 4 && ext[3] == '}' {
		if alt, ok := base91Value(ext[0:3]); ok {
			meters := float64(alt) - 10000
			p.AltitudeM = &meters
		}
		ext = ext[4:]
	}
	p.Comment = strings.TrimSpace(ext)
}

// parseMessage parses ":TO       :text{001".
func parseMessage(p *Packet, info string) {
	p.Kind = KindMessage
	if len(info) < 11 {
		p.Comment = strings.TrimSpace(info[1:])
		return
	}
	msg := &Message{
		To:   NormalizeCallsign(info[1:10]),
		Text: info[11:],
	}
	// Ack request: trailing "{<id>".
	if i := strings.LastIndexByte(msg.Text, '{'); i > 0 {
		if id := msg.Text[i+1:]; id != "" && isDigits(id) {
			msg.ID = id
			msg.Text = msg.Text[:i]
		}
	}
	// Ack/reject replies arrive as ordinary messages with "ack<id>" text.
	if len(msg.Text) >= 3 && (strings.HasPrefix(msg.Text, "ack") || strings.HasPrefix(msg.Text, "rej")) {
		if id := msg.Text[3:]; id != "" && isDigits(id) {
			msg.ID = id
		}
	}
	p.Message = msg
}

// parseObject parses ";NAME*092345z..." objects and ")NAME!..." items.
func parseObject(p *Packet, info string) {
	p.Kind = KindObject
	body := info[1:]
	p.Name = body
	if i := strings.IndexByte(body, '*'); i >= 0 && i <= 9 {
		p.Name = strings.TrimSpace(body[:i])
		body = body[i+1:]
		if len(body) >= 7 {
			if ts, ok := parseAPRSTime(body[0:7], time.Unix(p.ReceivedAt, 0)); ok {
				unix := ts.Unix()
				p.Timestamp = &unix
				body = body[7:]
			}
		}
	}
	if body == "" {
		return
	}
	// Dead ("_") or killed objects.
	if body[0] == '_' {
		p.Comment = body
		return
	}
	if body[0] >= '0' && body[0] <= '9' {
		parsePositionBody(p, body, false)
		p.Kind = KindObject
		return
	}
	p.Comment = strings.TrimSpace(body)
}

// parseMicE decodes a Mic-E position (the “ ` “ / `'` forms).
func parseMicE(p *Packet, info string) {
	p.Kind = KindPosition
	p.MessageCapable = info[0] == '`'
	body := info[1:] // skip the '`' / '\'' marker
	if len(body) < 8 {
		return
	}
	dst := strings.ToUpper(p.Dst)
	if len(dst) != 6 || !micEDstRE.MatchString(dst) {
		return
	}

	// Latitude digits live encoded in the destination field.
	var lat [6]byte
	for i := 0; i < 6; i++ {
		c := dst[i]
		switch {
		case c == 'K' || c == 'L' || c == 'Z':
			lat[i] = ' '
		case c > 'L': // P-Y → 0-9
			lat[i] = c - 32
		case c > '9': // A-J → 0-9
			lat[i] = c - 17
		default:
			lat[i] = c
		}
	}
	s := strings.ReplaceAll(string(lat[:]), " ", "0")
	deg, err1 := strconv.Atoi(s[0:2])
	min, err2 := strconv.ParseFloat(s[2:4]+"."+s[4:6], 64)
	if err1 == nil && err2 == nil {
		latitude := float64(deg) + min/60
		if dst[3] <= 'L' {
			latitude = -latitude
		}
		p.Position = &Position{Latitude: latitude}
	}

	// Longitude.
	lng := float64(body[0]) - 28
	if dst[4] >= 'P' {
		lng += 100
	}
	if lng >= 180 && lng <= 189 {
		lng -= 80
	}
	if lng >= 190 && lng <= 199 {
		lng -= 190
	}
	minL := float64(body[1]) - 28
	if minL >= 60 {
		minL -= 60
	}
	minL += (float64(body[2]) - 28) / 100
	lng += minL / 60
	if dst[5] >= 'P' {
		lng = -lng
	}
	if p.Position == nil {
		p.Position = &Position{Longitude: lng}
	} else {
		p.Position.Longitude = lng
	}

	// Speed and course.
	speed := (float64(body[3]) - 28) * 10
	course := float64(body[4]) - 28
	q := int(course) / 10
	course -= float64(q * 10)
	course = course*100 + float64(body[5]) - 28
	speed += float64(q)
	if speed >= 800 {
		speed -= 800
	}
	if course >= 400 {
		course -= 400
	}
	p.SpeedKMH = speed * 1.852 // knots → km/h
	p.CourseDeg = int(course)

	// Symbol table and code.
	p.Symbol = body[6]
	p.SymbolTable = body[7]

	// Message type bits (destination chars 0-2).
	mbits := dst[0:3]
	mbits = strings.Map(func(r rune) rune {
		switch {
		case r >= '0' && r <= '9' || r == 'L':
			return '0'
		case r >= 'P' && r <= 'Z':
			return '1'
		default: // A-K
			return '2'
		}
	}, mbits)
	if strings.ContainsRune(mbits, '2') {
		p.Comment = micETypeCustom[strings.ReplaceAll(mbits, "2", "1")]
	} else {
		p.Comment = micETypeStd[mbits]
	}

	// Optional trailing blocks: altitude and comment.
	rest := body[8:]
	if len(rest) >= 4 && rest[3] == '}' {
		if alt, ok := base91Value(rest[0:3]); ok {
			meters := float64(alt) - 10000
			p.AltitudeM = &meters
		}
		rest = rest[4:]
	}
	if extra := strings.TrimSpace(rest); extra != "" {
		if p.Comment != "" {
			p.Comment += " " + extra
		} else {
			p.Comment = extra
		}
	}
}

// micE standard and custom message-type tables (Mic-E destination bits).
var (
	micETypeStd = map[string]string{
		"111": "M0: Off Duty",
		"110": "M1: En Route",
		"101": "M2: In Service",
		"100": "M3: Returning",
		"011": "M4: Committed",
		"010": "M5: Special",
		"001": "M6: Priority",
		"000": "Emergency",
	}
	micETypeCustom = map[string]string{
		"111": "C0: Custom-0",
		"110": "C1: Custom-1",
		"101": "C2: Custom-2",
		"100": "C3: Custom-3",
		"011": "C4: Custom-4",
		"010": "C5: Custom-5",
		"001": "C6: Custom-6",
		"000": "Emergency",
	}
)

// base91Value decodes n base-91 characters (!-{) into an integer.
func base91Value(s string) (int, bool) {
	v := 0
	for i := 0; i < len(s); i++ {
		c := int(s[i]) - 33
		if c < 0 || c > 90 {
			return 0, false
		}
		v = v*91 + c
	}
	return v, true
}

// isDigits reports whether s is non-empty and all ASCII digits.
func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
