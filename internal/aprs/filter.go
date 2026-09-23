package aprs

// infrastructureSymbols are the APRS symbol codes used by digipeaters,
// gateways, internet-only nodes, repeaters and similar infrastructure —
// not actual ham stations. The neighbourhood map excludes them so it
// shows real operators (houses, cars, bikes, hikers, ...).
//
// Codes per the WA8LMF "Updated APRS Symbol Set" (primary and alternate
// tables):
//
//	'#'  digipeater (both tables)
//	'&'  gateway (alternate) / HF gateway (primary)
//	'I'  TCP/IP network station (internet-only node)
//	'r'  repeater tower
//	'm'  Mic-E repeater
//	'n'  node
//	'B'  BBS
//	'$'  telephone
//	'W'  weather service site (NWS)
//	'8'  802.11 / WiFi network node
//	'0'  VOIP repeater base
var infrastructureSymbols = map[byte]bool{
	'#': true,
	'&': true,
	'I': true,
	'r': true,
	'm': true,
	'n': true,
	'B': true,
	'$': true,
	'W': true,
	'8': true,
	'0': true,
}

// IsInfrastructure reports whether a packet belongs to APRS infrastructure
// rather than an actual ham station: objects/items (repeater or event
// announcements), queries, and stations using infrastructure symbols.
func IsInfrastructure(p Packet) bool {
	switch p.Kind {
	case KindObject, KindQuery:
		return true
	}
	return p.Symbol != 0 && infrastructureSymbols[p.Symbol]
}
