package aprs

// primaryInfrastructureSymbols are the PRIMARY table (/) codes of APRS
// infrastructure: digipeaters, gateways, network nodes, repeaters and
// similar fixed services — not actual ham operators. The neighbourhood
// map excludes them so it shows real people (houses, cars, bikes, ...).
// Meanings per the official WA8LMF symbol tables.
var primaryInfrastructureSymbols = map[byte]bool{
	'#': true, // digipeater
	'&': true, // HF gateway
	'I': true, // TCP/IP on-air network station
	'r': true, // repeater
	'm': true, // Mic-E repeater
	'n': true, // node
	'B': true, // BBS / PBBS
	'$': true, // telephone
	'W': true, // national weather service site
	'8': true, // 802.11 / WiFi node
	'0': true, // circle (obsolete)
}

// alternateInfrastructureSymbols are the ALTERNATE table (\) codes of
// non-operator markers: digipeaters, i-gates, service points of interest
// and network nodes. Person symbols (car, jeep, bike, ...) are absent by
// design: no ham operator is ever excluded.
var alternateInfrastructureSymbols = map[byte]bool{
	'#': true, // digipeater (green star)
	'&': true, // gateway / i-gate
	'I': true, // rain shower (weather marker, not a station)
	'r': true, // restrooms
	'm': true, // value sign (3-digit display)
	'n': true, // overlay triangle
	'B': true, // availability marker
	'$': true, // bank / ATM
	'W': true, // NWS site
	'8': true, // 802.11 / WiFi node
	'0': true, // IRLP / Echolink / WIRES circle
}

// IsInfrastructure reports whether a packet belongs to APRS infrastructure
// rather than an actual ham station: objects/items (repeater or event
// announcements), queries, and stations using infrastructure symbols in
// their own symbol table.
func IsInfrastructure(p Packet) bool {
	switch p.Kind {
	case KindObject, KindQuery:
		return true
	}
	if p.Symbol == 0 {
		return false
	}
	if p.SymbolTable == '\\' {
		return alternateInfrastructureSymbols[p.Symbol]
	}
	return primaryInfrastructureSymbols[p.Symbol]
}
