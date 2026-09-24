package aprs

import (
	"strings"
	"testing"
	"time"
)

func parseLine(t *testing.T, line string) Packet {
	t.Helper()
	p := ParseFeedLine(line, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC))
	if p.Src == "" {
		t.Fatalf("ParseFeedLine(%q) failed to parse the header", line)
	}
	return p
}

// TestParseAlternateTablePosition covers the alternate symbol table: the
// separator between latitude and longitude is '\' instead of '/'
// (e.g. Direwolf PBEACON symbol="\?" — the information kiosk).
func TestParseAlternateTablePosition(t *testing.T) {
	p := parseLine(t, "SP9SPM-10>APDW17,TCPIP*:!5001.27N\\02012.45E?Temp/dev work")
	if p.Kind != KindPosition {
		t.Fatalf("kind = %s, want position", p.Kind)
	}
	if p.Position == nil {
		t.Fatal("position missing")
	}
	if mathAbs(p.Position.Latitude-50.021167) > 1e-5 {
		t.Errorf("lat = %v, want 50.021167", p.Position.Latitude)
	}
	if mathAbs(p.Position.Longitude-20.2075) > 1e-5 {
		t.Errorf("lon = %v, want 20.2075", p.Position.Longitude)
	}
	if p.SymbolTable != '\\' || p.Symbol != '?' {
		t.Errorf("symbol = %c%c, want \\?", p.SymbolTable, p.Symbol)
	}
	if p.Comment != "Temp/dev work" {
		t.Errorf("comment = %q, want %q", p.Comment, "Temp/dev work")
	}
}

func TestParsePosition(t *testing.T) {
	// Classic position without timestamp.
	p := parseLine(t, "SP9MOA-7>APRS,TCPIP*,qAC,T2POLAND:!5003.08N/01956.44E-")
	if p.Kind != KindPosition {
		t.Fatalf("kind = %s, want position", p.Kind)
	}
	if p.Src != "SP9MOA-7" || p.Dst != "APRS" {
		t.Fatalf("header = %s>%s", p.Src, p.Dst)
	}
	if p.Position == nil {
		t.Fatal("position missing")
	}
	if mathAbs(p.Position.Latitude-50.051333) > 1e-5 {
		t.Errorf("lat = %v", p.Position.Latitude)
	}
	if mathAbs(p.Position.Longitude-19.940667) > 1e-5 {
		t.Errorf("lon = %v", p.Position.Longitude)
	}
	if p.Symbol != '-' || p.SymbolTable != '/' {
		t.Errorf("symbol = %c/%c", p.SymbolTable, p.Symbol)
	}
	if len(p.Path) == 0 || p.Path[0] != "TCPIP*" {
		t.Errorf("path = %v", p.Path)
	}
}

func TestParsePositionWithExtensions(t *testing.T) {
	// '=' prefix announces messaging capability.
	p := parseLine(t, "SP9XYZ>APRS:!5003.08N/01956.44E-065/070/A=001234 hello")
	if p.CourseDeg != 65 || mathAbs(p.SpeedKMH-129.64) > 0.01 {
		t.Errorf("course/speed = %d / %v, want 65 / 129.64", p.CourseDeg, p.SpeedKMH)
	}
	if p.AltitudeM == nil || mathAbs(*p.AltitudeM-376.1) > 0.1 {
		t.Errorf("altitude = %v, want ~376.1 m", p.AltitudeM)
	}
	if p.Comment != "hello" {
		t.Errorf("comment = %q", p.Comment)
	}

	p2 := parseLine(t, "SP9XYZ>APRS:=5003.08N/01956.44E-")
	if !p2.MessageCapable {
		t.Error("'=' position must announce messaging capability")
	}
}

func TestParseTimestamped(t *testing.T) {
	p := parseLine(t, "SP9XYZ>APRS:@092345z5003.08N/01956.44E-")
	if p.Timestamp == nil {
		t.Fatal("timestamp missing")
	}
	ts := time.Unix(*p.Timestamp, 0).UTC()
	if ts.Hour() != 9 || ts.Minute() != 23 || ts.Second() != 45 {
		t.Errorf("timestamp = %v, want 09:23:45", ts)
	}
	if p.Position == nil {
		t.Fatal("position missing")
	}
}

func TestParseMessage(t *testing.T) {
	p := parseLine(t, "SP9XYZ>APRS,TCPIP*::SP9MOA   :czesc, tu SP9XYZ{001")
	if p.Kind != KindMessage || p.Message == nil {
		t.Fatalf("kind = %s message = %v, want message", p.Kind, p.Message)
	}
	if p.Message.To != "SP9MOA" {
		t.Errorf("to = %q, want SP9MOA", p.Message.To)
	}
	if p.Message.Text != "czesc, tu SP9XYZ" {
		t.Errorf("text = %q", p.Message.Text)
	}
	if p.Message.ID != "001" {
		t.Errorf("id = %q, want 001", p.Message.ID)
	}

	// Ack reply.
	p = parseLine(t, "SP9MOA>APRS::SP9XYZ   :ack001")
	if p.Message == nil || p.Message.Text != "ack001" || p.Message.ID != "001" {
		t.Errorf("ack message = %+v", p.Message)
	}
}

func TestParseStatus(t *testing.T) {
	p := parseLine(t, "SP9XYZ>APRS:>on duty 145.500 MHz")
	if p.Kind != KindStatus || p.Status != "on duty 145.500 MHz" {
		t.Errorf("status packet = %+v", p)
	}
}

func TestParseCompressed(t *testing.T) {
	// Compressed position, hand-encoded for Warsaw (~52.2N 21.0E).
	p := parseLine(t, "SP9XYZ>APRS:/4*ijSj!!>/")
	if p.Kind != KindPosition {
		t.Fatalf("kind = %s", p.Kind)
	}
	if p.Position == nil {
		t.Fatal("position missing")
	}
	if p.SymbolTable != '/' || p.Symbol != '>' {
		t.Errorf("symbol = %c/%c", p.SymbolTable, p.Symbol)
	}
	if mathAbs(p.Position.Latitude-52.2) > 1e-3 || mathAbs(p.Position.Longitude-21.0) > 1e-3 {
		t.Errorf("position = %+v, want ~52.2N 21.0E", p.Position)
	}
}

func TestParseMicE(t *testing.T) {
	// Mic-E current-position packet, hand-encoded for Warsaw (52.333N 21.0E):
	// dstcall digits "522000", northern hemisphere via dstcall[3]='P'.
	line := "SP9XYZ-9>522P00,SP9ZAP*,WIDE1-1:`" +
		string([]byte{0x31, 0x1c, 0x1c, 0x1c, 0x1c, 0x1c, '>', '/'}) + "test"
	p := parseLine(t, line)
	if p.Kind != KindPosition {
		t.Fatalf("kind = %s", p.Kind)
	}
	if p.Position == nil {
		t.Fatal("position missing")
	}
	if !p.MessageCapable {
		t.Error("mic-e '`' must announce messaging capability")
	}
	// Warsaw area.
	if mathAbs(p.Position.Latitude-52.333) > 1e-2 || mathAbs(p.Position.Longitude-21.0) > 1e-3 {
		t.Errorf("position = %+v, want ~52.333N 21.0E", p.Position)
	}
	if p.Symbol == 0 {
		t.Error("mic-e symbol missing")
	}
	if p.Comment != "M1: En Route test" && p.Comment != "test" && !strings.Contains(p.Comment, "test") {
		t.Errorf("comment = %q, want to include the mic-e type and trailing comment", p.Comment)
	}
}

func TestParseObject(t *testing.T) {
	p := parseLine(t, "SP9XYZ>APRS:;SP9REP *111111z5003.08N/01956.44Er145.500MHz")
	if p.Kind != KindObject || p.Name != "SP9REP" {
		t.Fatalf("object = %+v", p)
	}
	if p.Position == nil {
		t.Fatal("object position missing")
	}
}

func TestParseWeatherAndOther(t *testing.T) {
	p := parseLine(t, "SP9XYZ>APRS:_100405c270s004g005t080")
	if p.Kind != KindWeather {
		t.Errorf("kind = %s, want weather", p.Kind)
	}
	p = parseLine(t, "SP9XYZ>APRS:T#123,456,789")
	if p.Kind != KindTelemetry {
		t.Errorf("kind = %s, want telemetry", p.Kind)
	}
}

func TestEncodeMessageLine(t *testing.T) {
	line := EncodeMessageLine("sp9moa-10", "SP9XYZ", "test 123")
	want := "SP9MOA-10>APRS,TCPIP*::SP9XYZ   :test 123"
	if line != want {
		t.Errorf("line = %q, want %q", line, want)
	}
}

func TestTrimMessageText(t *testing.T) {
	if got := TrimMessageText("  hello   world  "); got != "hello world" {
		t.Errorf("trim = %q", got)
	}
	long := make([]byte, 100)
	for i := range long {
		long[i] = 'x'
	}
	if got := TrimMessageText(string(long)); len(got) != MaxMessageText {
		t.Errorf("trim length = %d, want %d", len(got), MaxMessageText)
	}
}

// mathAbs avoids importing math in the test file.
func mathAbs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
