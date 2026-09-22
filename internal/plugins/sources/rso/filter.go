package rso

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// filterConfig is the RSO-specific policy. Defaults are applied by New
// and validated there; this struct carries the runtime-ready values.
type filterConfig struct {
	highSignalOnly      bool
	excludeRCB          bool
	excludeAirQuality   bool
	suppressIMGWDupes   bool
	localMinSeverity    string
	regionalMinSeverity string
	corridor            corridorConfig
}

// corridorConfig bounds the relevant A4 Balice–Tarnów kilometre window.
type corridorConfig struct {
	enabled bool
	kmFrom  int
	kmTo    int
}

// decision is the outcome of applying the policy to one RSO item.
type decision struct {
	emit     bool
	reason   string // why the item was suppressed (debug logs)
	severity string
	category string
	areas    []string // extra normalized area tokens
}

// ---- text normalization -------------------------------------------------

// foldDiacritics maps Polish letters to ASCII equivalents so both
// "Niepołomice" and "Niepolomice" match.
var foldDiacritics = strings.NewReplacer(
	"ą", "a", "ć", "c", "ę", "e", "ł", "l", "ń", "n", "ó", "o",
	"ś", "s", "ź", "z", "ż", "z",
	"Ą", "A", "Ć", "C", "Ę", "E", "Ł", "L", "Ń", "N", "Ó", "O",
	"Ś", "S", "Ź", "Z", "Ż", "Z",
)

// normalizeMatchText lowercases, folds diacritics and collapses whitespace.
func normalizeMatchText(s string) string {
	return strings.Join(strings.Fields(foldDiacritics.Replace(strings.ToLower(s))), " ")
}

// ---- absolute exclusions ------------------------------------------------

var (
	// RCB appears with flexible separators ("ALERT RCB", "ALERT-RCB",
	// "ALERT -RCB", plain "RCB").
	rcbPattern = regexp.MustCompile(`\balert[\s._-]*rcb\b|\brcb\b`)
	// Clear air-quality notices: smog, particulate matter, air quality.
	airQualityExplicit = regexp.MustCompile(`jakosc powietrza|zanieczyszczenie powietrza|smog|pm\s*10|pm\s*2[.,]?5|pyl zawieszony|pylem zawieszonym|zapylenie`)
	// "poziom informowania/alarmowy" only counts when the context is
	// particulate pollution (hydrology uses the same wording for rivers).
	airQualityLevel = regexp.MustCompile(`(poziom informowania|poziom alarmowy)[^.]{0,80}(pm\s*10|pm\s*2[.,]?5|pyl|smog)`)
	// A civil-protection event mentioning smoke/chemistry is NOT an
	// air-quality notice.
	civilFireChemical = regexp.MustCompile(`pozar|ewakuacj|substancj|chemiczn|amoniak|chlor|toksyczn`)
	// IMGW replication: "IMGW-PIB wydał ostrzeżenie..." with no added
	// civil-protection consequence.
	imgwMention = regexp.MustCompile(`\bimgw`)
	// Consequences that make an IMGW-mentioning communication NOT a plain
	// duplicate: evacuation, road closure, water trouble, infrastructure
	// failure, emergency instructions.
	imgwCivilConsequence = regexp.MustCompile(`ewakuacj|zamkni[eę]c|zablokowan|woda (nie)?nadaje|nieprzydatn|niezdatn|ska[zż]eni|awari|zakaz|nakaz|nie spo[zż]ywac|przegotow`)
)

// isRCB reports whether the combined text is clearly an RCB message.
func isRCB(text string) bool {
	return rcbPattern.MatchString(text)
}

// isAirQuality reports whether the combined text is a routine air-quality
// / smog / particulate notice. Toxic smoke from a fire or a chemical
// release is civil protection, not an air-quality notice.
func isAirQuality(text string) bool {
	if airQualityLevel.MatchString(text) {
		return true
	}
	if !airQualityExplicit.MatchString(text) {
		return false
	}
	return !civilFireChemical.MatchString(text)
}

// isIMGWDuplicate reports whether the text is an obvious RSO copy of an
// IMGW meteorological/hydrological warning without an added local
// civil-protection consequence.
func isIMGWDuplicate(text string) bool {
	if !imgwMention.MatchString(text) {
		return false
	}
	return !imgwCivilConsequence.MatchString(text)
}

// ---- severity classification -------------------------------------------

var (
	// "ostrzeżenie 1/2/3 stopnia" and word forms.
	degreeAfterWord = regexp.MustCompile(`ostrzezenie[\s.:_-]*(pierwszego|drugiego|trzeciego|1|2|3)\s+stopnia\b`)
	// "1 stopień zagrożenia", "2 stopnia", "3 stopień" adjacent to the digit.
	degreeBeforeWord = regexp.MustCompile(`\b(1|2|3)[\s.:_-]*(stopien|stopnia|stopniu)\s+zagrozenia\b`)
	// "stopień: 1", "stopień zagrożenia: 2".
	degreeColon = regexp.MustCompile(`stop(ien|nia|niu)(\s+zagrozenia)?[\s.:_-]*(1|2|3)\b`)

	extremePatterns = regexp.MustCompile(`katastrofaln|natychmiastowa ewakuacja|ekstremalne zagrozenie|zagrozenie zycia na duza skale`)

	severeWater = regexp.MustCompile(`brak przydatnosci wody do spozycia|woda nieprzydatna do spozycia|woda niezdatna do spozycia|zanieczyszczenie mikrobiologiczne|woda skazona|nie spozywac wody|nie nadaje sie do spozycia|zakaz spozywania wody|wylacznie po przegotowaniu|przegotowania wody|awaria (wodociagu|sieci wodociagowej|magistrali wodociagowej)`)
	severeCivil = regexp.MustCompile(`ewakuacj|eksplozj|wybuch|ulotnieni[ea] gazu|wyciek gazu|rozszczelnien|ska[zż]eni[ea]|awaria sieci gazowej|awaria chemiczn|uwolnieni[ea] substancji|zagrozenie chemiczne|duzy po[zż]ar|rozlegly po[zż]ar|po[zż]ar skladowiska|wstrzymanie ruchu|ruch wstrzymany|brak przejazdu`)
	severeFlood = regexp.MustCompile(`powodz|stan alarmowy|przekroczony stan alarmowy|zagrozenie powodziowe|wezbrani[ea] z przelaniem`)

	// One-lane / one-carriageway / alternating traffic stays moderate.
	laneBlocked = regexp.MustCompile(`zablokowan\w*\s+(jeden|1)\s*pas|jeden pas ruchu|zaj[eę]ty jeden pas|zw[eę]zenie do jednego pasa|ruch wahadlowy|jedna jezdnia|zablokowana jedna jezdnia`)
	// Any complete-closure wording (subject-independent).
	closure    = regexp.MustCompile(`zablokowan\w*|calkowicie zamkni[eę]t|zamkni[eę]t[ay]\s*(w obu kierunkach|dla ruchu)|nieprzejezdn|zamkni[eę]ty jeden kierunek`)
	targetRoad = regexp.MustCompile(`\b(a4|dk\s*75|dk\s*94|dw\s*964)\b`)

	moderateWater = regexp.MustCompile(`warunkowa przydatnosc|warunkowo przydatn|jakosc wody nie odpowiada|przekroczenie parametrow`)
	moderateRoad  = regexp.MustCompile(`kolizj|wypadek|utrudnienia w ruchu|znaczne utrudnienia|spowolnien|kork[oi]`)
	moderateHydro = regexp.MustCompile(`stan ostrzegawczy|przekroczony stan ostrzegawczy`)

	// Routine low-value items. They classify minor only when no complete
	// closure is announced — a real closure always wins (see
	// classifySeverity ordering).
	routinePatterns = regexp.MustCompile(`koszenie|koszenia traw|remont|przebudow|roboty drogowe|prace (drogowe|budowlane|konserwacyjne|remontowe)|roboty utrzymaniowe|utrzymani[ea] (drog|most)|malowanie|sprzatanie|czyszczeni|przycinka|wymiana nawierzchni`)
	minorPatterns   = regexp.MustCompile(`uwaga halas|test syren|proba syren|proby systemu alarmowania|cwiczen|trening|szczepienie lisow|zrzut szczepionki|informacyjny|informacja prasowa|ogloszenie|obwieszczenie|warsztat|spotkani[ea]`)
)

// degreeFromText extracts an explicit Polish warning degree (1/2/3) when
// the context is unambiguous. Returns 0 when no degree is stated.
func degreeFromText(text string) int {
	if m := degreeAfterWord.FindStringSubmatch(text); m != nil {
		return degreeValue(m[1])
	}
	if m := degreeBeforeWord.FindStringSubmatch(text); m != nil {
		return degreeValue(m[1])
	}
	if m := degreeColon.FindStringSubmatch(text); m != nil {
		return degreeValue(m[3])
	}
	return 0
}

func degreeValue(s string) int {
	switch s {
	case "pierwszego", "1":
		return 1
	case "drugiego", "2":
		return 2
	case "trzeciego", "3":
		return 3
	}
	return 0
}

// classifySeverity infers the WarnFlux severity from explicit semantics in
// the title/shortcut/content. rso_alarm is deliberately never consulted:
// the provider does not document it as a severity scale.
func classifySeverity(text string) string {
	// Explicit official wording wins over heuristics.
	switch degreeFromText(text) {
	case 1:
		return "moderate"
	case 2:
		return "severe"
	case 3:
		return "extreme"
	}

	if extremePatterns.MatchString(text) {
		return "extreme"
	}

	switch {
	case severeWater.MatchString(text):
		return "severe"
	case severeCivil.MatchString(text):
		return "severe"
	case severeFlood.MatchString(text):
		return "severe"
	}

	// Routine works: minor — unless the message announces a COMPLETE
	// closure, in which case the closure rules below win.
	if routinePatterns.MatchString(text) && !closure.MatchString(text) {
		return "minor"
	}

	// One blocked lane/carriageway: moderate.
	if laneBlocked.MatchString(text) {
		return "moderate"
	}

	// Complete closure: a target road (A4, DK75, DK94, DW964) is severe;
	// any other road is moderate.
	if closure.MatchString(text) {
		if targetRoad.MatchString(text) {
			return "severe"
		}
		return "moderate"
	}

	switch {
	case moderateWater.MatchString(text):
		return "moderate"
	case moderateHydro.MatchString(text):
		return "moderate"
	case moderateRoad.MatchString(text):
		return "moderate"
	}

	if minorPatterns.MatchString(text) {
		return "minor"
	}
	return "unknown"
}

// ---- category classification -------------------------------------------

var (
	catWater     = regexp.MustCompile(`wodociag|woda|uj[eę]cie wody|przydatnosc|spo[zż]ycia|zanieczyszczenie mikrobiologiczne|wodociagow`)
	catHydrology = regexp.MustCompile(`rzek|potok|stan alarmowy|stan ostrzegawczy|wezbrani|powodz|zlewni|hydrologiczn|opadowej na rzekach`)
	catWeather   = regexp.MustCompile(`burz|grad|wiatr|upal|mroz|opady|snieg|mgla|oblodzen|burza|pogod|imgw|ostrzezenie meteorologiczn`)
	catRoad      = regexp.MustCompile(`droga|autostrada|ulica|jezdnia|pas ruchu|wezel|skrzyzowanie|objazd|\bkm\b|wypadek|kolizja|remont|przebudow|ruch`)
	catCivil     = regexp.MustCompile(`ewakuacj|pozar|wybuch|gaz|chemiczn|ska[zż]eni|zagrozenie|awaria|epidemi|zakaz|nakaz|ratownic`)
)

// classifyCategory assigns one of road/water/weather/hydrology/
// civil-protection only when the evidence is clear; otherwise empty.
func classifyCategory(text string) string {
	score := map[string]int{}
	for cat, re := range map[string]*regexp.Regexp{
		"water":            catWater,
		"hydrology":        catHydrology,
		"weather":          catWeather,
		"road":             catRoad,
		"civil-protection": catCivil,
	} {
		score[cat] = len(re.FindAllString(text, -1))
	}
	best, bestScore := "", 0
	for _, cat := range []string{"road", "water", "hydrology", "weather", "civil-protection"} {
		if score[cat] > bestScore {
			best, bestScore = cat, score[cat]
		}
	}
	if bestScore == 0 {
		return ""
	}
	return best
}

// ---- geography ----------------------------------------------------------

// geoMatch summarizes where an RSO communication applies.
type geoMatch struct {
	core     bool // gmina Niepołomice / powiat wielicki / villages
	city     map[string]bool
	nearby   bool
	corridor bool
	roads    map[string]bool
	regional bool
}

var (
	corePattern = regexp.MustCompile(`\b(niepolomic\w*|gmina niepolomic\w*|podlez\w*|staniatk\w*|wola batorsk\w*|wola zabierzowsk\w*|zabierzow bochensk\w*|chobot|ochmanow|slomirog|suchorab|zagorz\w*|zakrzow\w*)\b`)
	// powiat wielicki reasonably covers the Niepołomice area.
	powiatWielickiPattern = regexp.MustCompile(`\bpowiat wielicki\b|\bwielickim\b`)

	cityPatterns = map[string]*regexp.Regexp{
		"krakow":    regexp.MustCompile(`\bkrakow\w*\b`),
		"wieliczka": regexp.MustCompile(`\bwieliczk\w*\b|\bwieliczc\w*\b|\bwielick\w*\b`),
		"bochnia":   regexp.MustCompile(`\bbochn\w*\b`),
	}
	nearbyPattern = regexp.MustCompile(`\b(klaj\w*|targowisko|szarow\w*|brzezie|kokotow\w*)\b`)

	// A4 corridor location tokens (Balice through Tarnów).
	corridorLocations = regexp.MustCompile(`\b(balice|krakow\w*|biezanow|wieliczk\w*|podleze|niepolomic\w*|targowisko|szarow\w*|klaj\w*|bochn\w*|brzesk\w*|wierzchoslawic\w*|tarnow\w*|moscice)\b`)

	roadPatterns = map[string]*regexp.Regexp{
		"a4":    regexp.MustCompile(`\ba4\b|\bautostrad[ay] a4\b`),
		"dk75":  regexp.MustCompile(`\bdk\s*75\b|\bdroga krajowa nr 75\b`),
		"dk94":  regexp.MustCompile(`\bdk\s*94\b|\bdroga krajowa nr 94\b`),
		"dw964": regexp.MustCompile(`\bdw\s*964\b`),
		"dw965": regexp.MustCompile(`\bdw\s*965\b`),
		"dw966": regexp.MustCompile(`\bdw\s*966\b`),
		"dw967": regexp.MustCompile(`\bdw\s*967\b`),
		"s7":    regexp.MustCompile(`\bs7\b|\bdroga ekspresowa s7\b`),
	}

	kmAfterKM    = regexp.MustCompile(`\bkm\s*(\d{1,4})(?:[.,]\d+)?\b`)
	kmBeforeKM   = regexp.MustCompile(`\b(\d{1,4})(?:[.,]\d+)?\s*km\b`)
	kmHectometre = regexp.MustCompile(`\b(\d{1,4})(?:[.,]\d+)?\+\d{3}\b`)
)

// classifyGeography scans the folded text for local relevance using the
// configured A4 corridor window.
func (p filterConfig) classifyGeography(text string) geoMatch {
	g := geoMatch{
		city:  map[string]bool{},
		roads: map[string]bool{},
	}
	if corePattern.MatchString(text) || powiatWielickiPattern.MatchString(text) {
		g.core = true
	}
	for slug, re := range cityPatterns {
		if re.MatchString(text) {
			g.city[slug] = true
		}
	}
	g.nearby = nearbyPattern.MatchString(text)

	hasA4 := roadPatterns["a4"].MatchString(text)
	for slug, re := range roadPatterns {
		if re.MatchString(text) {
			g.roads[slug] = true
		}
	}

	// Corridor relevance: A4 with an in-range kilometre or an A4 event
	// naming a corridor location (only while the corridor is enabled).
	if hasA4 && p.corridor.enabled {
		g.corridor = corridorLocations.MatchString(text)
		if !g.corridor {
			for _, km := range extractKilometres(text) {
				if km >= p.corridor.kmFrom && km <= p.corridor.kmTo {
					g.corridor = true
					break
				}
			}
		}
	}

	g.regional = !g.core && len(g.city) == 0 && !g.nearby && !g.corridor
	return g
}

// extractKilometres returns the integer kilometre values mentioned in the
// text (tolerant of "435 km", "435,6 km", "435+600", "km 435+600").
func extractKilometres(text string) []int {
	var out []int
	for _, re := range []*regexp.Regexp{kmAfterKM, kmBeforeKM, kmHectometre} {
		for _, m := range re.FindAllStringSubmatch(text, -1) {
			var v int
			if _, err := fmt.Sscanf(m[1], "%d", &v); err == nil {
				out = append(out, v)
			}
		}
	}
	sort.Ints(out)
	dedup := out[:0]
	var prev int
	for i, v := range out {
		if i > 0 && v == prev {
			continue
		}
		dedup = append(dedup, v)
		prev = v
	}
	return dedup
}

// ---- emit decision ------------------------------------------------------

// decide applies the full policy to one item. Suppression decisions never
// make the provider snapshot incomplete.
func (p filterConfig) decide(item newsItem) decision {
	title := normalizeMatchText(item.Title)
	shortcut := normalizeMatchText(item.Shortcut)
	content := normalizeMatchText(item.Content)
	text := strings.Join([]string{title, shortcut, content}, " ")

	if p.excludeRCB && isRCB(text) {
		return decision{reason: "rcb message suppressed (handled by the dedicated RCB path)"}
	}
	if p.excludeAirQuality && isAirQuality(text) {
		return decision{reason: "air-quality communication suppressed (dedicated source)"}
	}
	if p.suppressIMGWDupes && isIMGWDuplicate(text) {
		return decision{reason: "plain IMGW duplicate suppressed"}
	}

	sev := classifySeverity(text)
	geo := p.classifyGeography(text)

	d := decision{
		severity: sev,
		category: classifyCategory(text),
		areas:    enrichAreas(geo),
	}
	d.emit = p.shouldEmit(sev, geo)
	if !d.emit {
		local := geo.core || len(geo.city) > 0 || geo.corridor
		d.reason = fmt.Sprintf("severity %s below threshold for scope (local=%v corridor=%v)", sev, local, geo.corridor)
	}
	return d
}

// shouldEmit applies the geographic severity thresholds.
func (p filterConfig) shouldEmit(sev string, geo geoMatch) bool {
	if !p.highSignalOnly {
		return true
	}
	rank, _ := storage.SeverityRank(sev)
	if rank == 0 {
		// unknown/unranked never passes the high-signal filter.
		return false
	}

	local := geo.core || len(geo.city) > 0 || geo.nearby || geo.corridor

	// A road-specific event outside the configured local area is never
	// relevant, regardless of severity: an A4 closure at km 97 towards
	// Wrocław or an S7 incident in the far end of the voivodeship must not
	// reach Niepołomice just because a road number matched.
	if len(geo.roads) > 0 && !local {
		return false
	}

	if local {
		threshold, _ := storage.SeverityRank(p.localMinSeverity)
		return rank >= threshold
	}
	threshold, _ := storage.SeverityRank(p.regionalMinSeverity)
	return rank >= threshold
}

// enrichAreas builds the additional normalized area tokens for a
// confidently classified event.
func enrichAreas(geo geoMatch) []string {
	var out []string
	if geo.core {
		out = append(out, "gmina:niepolomice", "powiat:wielicki")
	}
	for city := range geo.city {
		switch city {
		case "krakow":
			out = append(out, "miasto:krakow")
		case "wieliczka":
			out = append(out, "miasto:wieliczka")
		case "bochnia":
			out = append(out, "miasto:bochnia")
		}
	}
	for road := range geo.roads {
		out = append(out, "droga:"+road)
	}
	if geo.corridor {
		out = append(out, "corridor:a4-balice-tarnow")
	}
	return out
}
