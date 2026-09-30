package aprs

import (
	"fmt"
	"math"
)

// FormatPosition renders a latitude/longitude pair in the APRS
// uncompressed position format: ddmm.mmN/dddmm.mmE (hemisphere letters
// included). It powers the manual "send beacon now" packet.
func FormatPosition(lat, lon float64) string {
	ns, ew := "N", "E"
	if lat < 0 {
		ns = "S"
	}
	if lon < 0 {
		ew = "W"
	}
	la := math.Abs(lat)
	lo := math.Abs(lon)
	laDeg := int(la)
	loDeg := int(lo)
	laMin := (la - float64(laDeg)) * 60
	loMin := (lo - float64(loDeg)) * 60
	return fmt.Sprintf("%02d%05.2f%s/%03d%05.2f%s", laDeg, laMin, ns, loDeg, loMin, ew)
}

// BuildPositionPacket renders the AX.25 info field of our own position
// beacon: the "!" data type, the uncompressed position, the station
// symbol and the comment. The symbol falls back to "/-" (no symbol) when
// the configuration carries none.
func BuildPositionPacket(lat, lon float64, symbol, comment string) ([]byte, error) {
	if lat == 0 && lon == 0 {
		return nil, fmt.Errorf("aprs: no position for beacon")
	}
	if symbol == "" {
		symbol = "/-"
	}
	return []byte("!" + FormatPosition(lat, lon) + symbol + comment), nil
}
