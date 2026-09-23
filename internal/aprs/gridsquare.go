package aprs

import (
	"math"
	"regexp"
	"strings"
)

// gridRE accepts 2-, 4-, 6- and 8-character Maidenhead locators.
var gridRE = regexp.MustCompile(`^([A-Ra-r])([A-Ra-r])(\d)(\d)(?:([A-Xa-x])([A-Xa-x]))?(?:(\d)(\d))?$`)

// ParseGridSquare returns the CENTER of the Maidenhead locator cell.
// 2-character locators resolve to the middle of the 10°x20° field,
// 4-character to the middle of the 1°x2° square and so on. ok is false
// when the locator is malformed.
func ParseGridSquare(locator string) (lat, lon float64, ok bool) {
	m := gridRE.FindStringSubmatch(strings.ToUpper(strings.TrimSpace(locator)))
	if m == nil {
		return 0, 0, false
	}

	lon = float64(m[1][0]-'A')*20 - 180
	lat = float64(m[2][0]-'A')*10 - 90

	lon += float64(m[3][0]-'0') * 2
	lat += float64(m[4][0] - '0')

	// The locator was uppercased, so subsquares use the 'A' base.
	switch len(m[0]) {
	case 2:
		lon += 10
		lat += 5
	case 4:
		lon += 1
		lat += 0.5
	case 6:
		lon += float64(m[5][0]-'A')*2/24 + 1.0/24
		lat += float64(m[6][0]-'A')/24 + 0.5/24
	case 8:
		lon += float64(m[5][0]-'A')*2/24 + float64(m[7][0]-'0')*2/240 + 1.0/240
		lat += float64(m[6][0]-'A')/24 + float64(m[8][0]-'0')/240 + 0.5/240
	default:
		return 0, 0, false
	}
	return lat, lon, true
}

// DistanceKM returns the great-circle distance between two WGS84
// coordinates in kilometers (haversine).
func DistanceKM(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusKM = 6371.0088

	toRad := math.Pi / 180
	dLat := (lat2 - lat1) * toRad
	dLon := (lon2 - lon1) * toRad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*toRad)*math.Cos(lat2*toRad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusKM * math.Asin(math.Sqrt(a))
}
