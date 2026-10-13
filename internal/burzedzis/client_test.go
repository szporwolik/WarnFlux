package burzedzis

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The fixtures below are the provider's own answers, trimmed to the
// elements the client reads (the envelopes are prefixed exactly as the
// service prefixes them).

const cityUnresolved = `<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="https://burze.dzis.net/soap.php">
<SOAP-ENV:Body><ns1:miejscowoscResponse><return xsi:type="ns1:MyComplexTypeMiejscowosc" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><y xsi:type="xsd:float">0</y><x xsi:type="xsd:float">0</x></return></ns1:miejscowoscResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`

const cityNiepolomice = `<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="https://burze.dzis.net/soap.php">
<SOAP-ENV:Body><ns1:miejscowoscResponse><return xsi:type="ns1:MyComplexTypeMiejscowosc" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><y xsi:type="xsd:float">50.02</y><x xsi:type="xsd:float">20.13</x></return></ns1:miejscowoscResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`

// warningsQuiet is a real "nothing in force" answer: every degree 0, every
// window the placeholder "0".
const warningsQuiet = `<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="https://burze.dzis.net/soap.php">
<SOAP-ENV:Body><ns1:ostrzezenia_pogodoweResponse><return xsi:type="ns1:MyComplexTypeOstrzezenia">
<od_dnia xsi:type="xsd:string"></od_dnia><do_dnia xsi:type="xsd:string"></do_dnia>
<mroz xsi:type="xsd:int">0</mroz><mroz_od_dnia xsi:type="xsd:string">0</mroz_od_dnia><mroz_do_dnia xsi:type="xsd:string">0</mroz_do_dnia>
<upal xsi:type="xsd:int">0</upal><upal_od_dnia xsi:type="xsd:string">0</upal_od_dnia><upal_do_dnia xsi:type="xsd:string">0</upal_do_dnia>
<wiatr xsi:type="xsd:int">0</wiatr><wiatr_od_dnia xsi:type="xsd:string">0</wiatr_od_dnia><wiatr_do_dnia xsi:type="xsd:string">0</wiatr_do_dnia>
<opad xsi:type="xsd:int">0</opad><opad_od_dnia xsi:type="xsd:string">0</opad_od_dnia><opad_do_dnia xsi:type="xsd:string">0</opad_do_dnia>
<burza xsi:type="xsd:int">0</burza><burza_od_dnia xsi:type="xsd:string">0</burza_od_dnia><burza_do_dnia xsi:type="xsd:string">0</burza_do_dnia>
<traba xsi:type="xsd:int">0</traba><traba_od_dnia xsi:type="xsd:string">0</traba_od_dnia><traba_do_dnia xsi:type="xsd:string">0</traba_do_dnia>
</return></ns1:ostrzezenia_pogodoweResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`

// stormQuiet is a real "no discharges" answer.
const stormQuiet = `<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="https://burze.dzis.net/soap.php">
<SOAP-ENV:Body><ns1:szukaj_burzyResponse><return xsi:type="ns1:MyComplexTypeBurza">
<liczba xsi:type="xsd:int">0</liczba><odleglosc xsi:type="xsd:float">0</odleglosc><kierunek xsi:type="xsd:string"></kierunek><okres xsi:type="xsd:int">20</okres>
</return></ns1:szukaj_burzyResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`

// warningsStorm is the shape the provider uses for a warning in force
// (degrees set, real UTC windows). Only the storm family is non-zero.
const warningsStorm = `<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="https://burze.dzis.net/soap.php">
<SOAP-ENV:Body><ns1:ostrzezenia_pogodoweResponse><return xsi:type="ns1:MyComplexTypeOstrzezenia">
<mroz xsi:type="xsd:int">0</mroz><mroz_od_dnia xsi:type="xsd:string">0</mroz_od_dnia><mroz_do_dnia xsi:type="xsd:string">0</mroz_do_dnia>
<upal xsi:type="xsd:int">0</upal><upal_od_dnia xsi:type="xsd:string">0</upal_od_dnia><upal_do_dnia xsi:type="xsd:string">0</upal_do_dnia>
<wiatr xsi:type="xsd:int">1</wiatr><wiatr_od_dnia xsi:type="xsd:string">2026-10-13 13:00:00</wiatr_od_dnia><wiatr_do_dnia xsi:type="xsd:string">2026-10-13 19:00:00</wiatr_do_dnia>
<opad xsi:type="xsd:int">0</opad><opad_od_dnia xsi:type="xsd:string">0</opad_od_dnia><opad_do_dnia xsi:type="xsd:string">0</opad_do_dnia>
<burza xsi:type="xsd:int">3</burza><burza_od_dnia xsi:type="xsd:string">2026-10-13 14:00:00</burza_od_dnia><burza_do_dnia xsi:type="xsd:string">2026-10-13 20:00:00</burza_do_dnia>
<traba xsi:type="xsd:int">0</traba><traba_od_dnia xsi:type="xsd:string">0</traba_od_dnia><traba_do_dnia xsi:type="xsd:string">0</traba_do_dnia>
</return></ns1:ostrzezenia_pogodoweResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`

// warningsUnreadable is a COMPLETE envelope (all six phenomena, as the
// provider always ships them) whose wind and rain windows cannot be read.
const warningsUnreadable = `<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="https://burze.dzis.net/soap.php">
<SOAP-ENV:Body><ns1:ostrzezenia_pogodoweResponse><return xsi:type="ns1:MyComplexTypeOstrzezenia">
<mroz xsi:type="xsd:int">0</mroz><mroz_od_dnia xsi:type="xsd:string">0</mroz_od_dnia><mroz_do_dnia xsi:type="xsd:string">0</mroz_do_dnia>
<upal xsi:type="xsd:int">0</upal><upal_od_dnia xsi:type="xsd:string">0</upal_od_dnia><upal_do_dnia xsi:type="xsd:string">0</upal_do_dnia>
<wiatr xsi:type="xsd:int">2</wiatr><wiatr_od_dnia xsi:type="xsd:string">brak</wiatr_od_dnia><wiatr_do_dnia xsi:type="xsd:string">0</wiatr_do_dnia>
<opad xsi:type="xsd:int">2</opad><opad_od_dnia xsi:type="xsd:string">2026-10-13 18:00:00</opad_od_dnia><opad_do_dnia xsi:type="xsd:string">2026-10-13 12:00:00</opad_do_dnia>
<burza xsi:type="xsd:int">0</burza><burza_od_dnia xsi:type="xsd:string">0</burza_od_dnia><burza_do_dnia xsi:type="xsd:string">0</burza_do_dnia>
<traba xsi:type="xsd:int">0</traba><traba_od_dnia xsi:type="xsd:string">0</traba_od_dnia><traba_do_dnia xsi:type="xsd:string">0</traba_do_dnia>
</return></ns1:ostrzezenia_pogodoweResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`

// warningsIncomplete is missing a whole phenomenon (tornado): the provider
// never does that, so the document is not the operation's payload.
const warningsIncomplete = `<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="https://burze.dzis.net/soap.php">
<SOAP-ENV:Body><ns1:ostrzezenia_pogodoweResponse><return xsi:type="ns1:MyComplexTypeOstrzezenia">
<mroz xsi:type="xsd:int">0</mroz><mroz_od_dnia xsi:type="xsd:string">0</mroz_od_dnia><mroz_do_dnia xsi:type="xsd:string">0</mroz_do_dnia>
<upal xsi:type="xsd:int">0</upal><upal_od_dnia xsi:type="xsd:string">0</upal_od_dnia><upal_do_dnia xsi:type="xsd:string">0</upal_do_dnia>
<wiatr xsi:type="xsd:int">0</wiatr><wiatr_od_dnia xsi:type="xsd:string">0</wiatr_od_dnia><wiatr_do_dnia xsi:type="xsd:string">0</wiatr_do_dnia>
<opad xsi:type="xsd:int">0</opad><opad_od_dnia xsi:type="xsd:string">0</opad_od_dnia><opad_do_dnia xsi:type="xsd:string">0</opad_do_dnia>
<burza xsi:type="xsd:int">0</burza><burza_od_dnia xsi:type="xsd:string">0</burza_od_dnia><burza_do_dnia xsi:type="xsd:string">0</burza_do_dnia>
</return></ns1:ostrzezenia_pogodoweResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`

// maintenancePage is what a proxy or the provider itself answers with HTTP
// 200 when it is being worked on: valid XML, no SOAP at all.
const maintenancePage = `<!DOCTYPE html><html><body><h1>maintenance</h1></body></html>`

// emptySOAPBody is a well-formed envelope with nothing inside.
const emptySOAPBody = `<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Body/></SOAP-ENV:Envelope>`

// stormDetected is the provider's own test-data shape: a count, a distance
// and a compass direction.
const stormDetected = `<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="https://burze.dzis.net/soap.php">
<SOAP-ENV:Body><ns1:szukaj_burzyResponse><return xsi:type="ns1:MyComplexTypeBurza">
<liczba xsi:type="xsd:int">999</liczba><odleglosc xsi:type="xsd:float">4.99</odleglosc><kierunek xsi:type="xsd:string">NW</kierunek><okres xsi:type="xsd:int">15</okres>
</return></ns1:szukaj_burzyResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`

// faultNoAuth is the provider refusing an unauthenticated call.
const faultNoAuth = `<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Body><SOAP-ENV:Fault><faultcode>SOAP-ENV:Server</faultcode><faultstring>W celu skorzystania z funkcji, wymagane jest uwierzytelnienie lub przekroczono limit zapytań / To use the function, authentication is required or the query limit has been exceeded</faultstring></SOAP-ENV:Fault></SOAP-ENV:Body></SOAP-ENV:Envelope>`

// soapServer answers every operation by looking at the element the client
// sent, so the request shape itself is part of the test.
func soapServer(t *testing.T, replies map[string]string, seen *[]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/soap.php" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/xml") {
			t.Errorf("content type = %q", ct)
		}
		body, _ := io.ReadAll(r.Body)
		text := string(body)
		if seen != nil {
			*seen = append(*seen, text)
		}
		for op, reply := range replies {
			if strings.Contains(text, "<"+op+" ") {
				w.Header().Set("Content-Type", "text/xml; charset=utf-8")
				_, _ = io.WriteString(w, reply)
				return
			}
		}
		t.Errorf("unknown operation in %s", text)
		_, _ = io.WriteString(w, faultNoAuth)
	}))
}

func testClient(t *testing.T, url string) *Client {
	t.Helper()
	return NewClient(url, "test-key", 5*time.Second)
}

func TestLocalityResolvesAndRejectsZeroCoordinates(t *testing.T) {
	var seen []string
	srv := soapServer(t, map[string]string{
		"miejscowosc": cityNiepolomice,
	}, &seen)
	defer srv.Close()
	c := testClient(t, srv.URL)

	loc, err := c.Locality(context.Background(), "Niepołomice")
	if err != nil {
		t.Fatalf("Locality: %v", err)
	}
	if loc.Name != "Niepołomice" || loc.Lat != 50.02 || loc.Lon != 20.13 {
		t.Errorf("locality = %+v", loc)
	}
	if len(seen) != 1 || !strings.Contains(seen[0], "<nazwa>Niepołomice</nazwa>") || !strings.Contains(seen[0], "<klucz>test-key</klucz>") {
		t.Errorf("request body = %q", seen)
	}
	if !strings.Contains(seen[0], `xmlns="https://burze.dzis.net/soap.php"`) {
		t.Errorf("request lacks the operation namespace: %q", seen[0])
	}

	// A mistyped locality answers 0/0 — the same coordinates that make the
	// warnings endpoint return its test data, so it must be an error.
	srvZero := soapServer(t, map[string]string{"miejscowosc": cityUnresolved}, nil)
	defer srvZero.Close()
	if _, err := testClient(t, srvZero.URL).Locality(context.Background(), "Niepolomice"); !errors.Is(err, ErrUnknownLocality) {
		t.Errorf("unknown locality error = %v, want ErrUnknownLocality", err)
	}
}

func TestWarningsSnapshot(t *testing.T) {
	cases := []struct {
		name   string
		reply  string
		inF    int
		reject []string
	}{
		{name: "quiet", reply: warningsQuiet},
		{name: "storm and wind in force", reply: warningsStorm, inF: 2},
		{name: "unreadable windows are rejected", reply: warningsUnreadable, reject: []string{"wind", "rain"}},
	}
	for _, c := range cases {
		srv := soapServer(t, map[string]string{"ostrzezenia_pogodowe": c.reply}, nil)
		got, err := testClient(t, srv.URL).Warnings(context.Background(), Locality{Name: "Niepołomice", Lat: 50.02, Lon: 20.13})
		srv.Close()
		if err != nil {
			t.Fatalf("%s: Warnings: %v", c.name, err)
		}
		if len(got.InForce) != c.inF {
			t.Errorf("%s: in force = %d, want %d (%+v)", c.name, len(got.InForce), c.inF, got.InForce)
		}
		if len(got.Rejected) != len(c.reject) {
			t.Errorf("%s: rejected = %v, want %v", c.name, got.Rejected, c.reject)
		}
		for i, want := range c.reject {
			if i < len(got.Rejected) && !strings.Contains(got.Rejected[i], want) {
				t.Errorf("%s: rejected[%d] = %q, want it to name %q", c.name, i, got.Rejected[i], want)
			}
		}
	}
}

func TestWarningsDecodesDegreeWindowAndPoint(t *testing.T) {
	srv := soapServer(t, map[string]string{"ostrzezenia_pogodowe": warningsStorm}, nil)
	defer srv.Close()
	at := Locality{Name: "Niepołomice", Lat: 50.02, Lon: 20.13}
	got, err := testClient(t, srv.URL).Warnings(context.Background(), at)
	if err != nil {
		t.Fatalf("Warnings: %v", err)
	}
	for _, in := range got.InForce {
		if in.Point != at {
			t.Errorf("warning point = %+v, want %+v", in.Point, at)
		}
		switch in.Phenomenon.Key {
		case "storm":
			if in.Degree != 3 {
				t.Errorf("storm degree = %d, want 3", in.Degree)
			}
			// The provider publishes UTC; 14:00 UTC must stay 14:00.
			if in.From.Format(time.RFC3339) != "2026-10-13T14:00:00Z" || in.To.Format(time.RFC3339) != "2026-10-13T20:00:00Z" {
				t.Errorf("storm window = %s – %s", in.From.Format(time.RFC3339), in.To.Format(time.RFC3339))
			}
		case "wind":
			if in.Degree != 1 {
				t.Errorf("wind degree = %d, want 1", in.Degree)
			}
		}
	}
}

func TestStormSearch(t *testing.T) {
	srv := soapServer(t, map[string]string{"szukaj_burzy": stormDetected}, nil)
	defer srv.Close()
	got, err := testClient(t, srv.URL).Storm(context.Background(), Locality{Name: "Niepołomice", Lat: 50.02, Lon: 20.13}, 25)
	if err != nil {
		t.Fatalf("Storm: %v", err)
	}
	if got.Count != 999 || got.DistanceKM != 4.99 || got.Direction != "NW" || got.WindowMins != 15 {
		t.Errorf("storm = %+v", got)
	}
	if !got.Detected() || got.Window() != 15*time.Minute {
		t.Errorf("detected/window = %v/%s", got.Detected(), got.Window())
	}

	quiet := soapServer(t, map[string]string{"szukaj_burzy": stormQuiet}, nil)
	defer quiet.Close()
	empty, err := testClient(t, quiet.URL).Storm(context.Background(), Locality{Name: "Niepołomice"}, 25)
	if err != nil {
		t.Fatalf("Storm: %v", err)
	}
	if empty.Detected() {
		t.Errorf("quiet search reported a detection: %+v", empty)
	}
}

func TestFaultIsReportedAsRefusal(t *testing.T) {
	srv := soapServer(t, map[string]string{"ostrzezenia_pogodowe": faultNoAuth}, nil)
	defer srv.Close()
	_, err := testClient(t, srv.URL).Warnings(context.Background(), Locality{Name: "Niepołomice", Lat: 50.02, Lon: 20.13})
	if err == nil {
		t.Fatal("a SOAP fault must not be swallowed")
	}
	if !IsFault(err) {
		t.Errorf("IsFault(%v) = false", err)
	}
	if !strings.Contains(err.Error(), "uwierzytelnienie") {
		t.Errorf("error does not carry the provider's Polish text: %v", err)
	}
	if strings.Contains(err.Error(), "To use the function") {
		t.Errorf("error kept the English duplicate: %v", err)
	}
}

func TestDirectionNames(t *testing.T) {
	for code, want := range map[string]string{
		"N": "północ", "NE": "północny wschód", "E": "wschód", "SE": "południowy wschód",
		"S": "południe", "SW": "południowy zachód", "W": "zachód", "NW": "północny zachód",
		"": "", "?": "?",
	} {
		if got := DirectionName(code); got != want {
			t.Errorf("DirectionName(%q) = %q, want %q", code, got, want)
		}
	}
}

// TestIncompleteAnswersAreErrors pins the reported P1: a successful
// xml.Unmarshal does NOT prove the SOAP answer is there. A maintenance page
// parses into an empty struct, and reading that as "no warnings are in force"
// would reconcile every live warning of the source away.
func TestIncompleteAnswersAreErrors(t *testing.T) {
	noReturn := `<?xml version="1.0" encoding="UTF-8"?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Body><ns1:ostrzezenia_pogodoweResponse xmlns:ns1="https://burze.dzis.net/soap.php"/></SOAP-ENV:Body></SOAP-ENV:Envelope>`

	cases := []struct {
		name  string
		reply string
	}{
		{"maintenance page", maintenancePage},
		{"empty SOAP body", emptySOAPBody},
		{"operation without return", noReturn},
		{"missing phenomenon", warningsIncomplete},
	}
	for _, c := range cases {
		srv := soapServer(t, map[string]string{"ostrzezenia_pogodowe": c.reply}, nil)
		got, err := testClient(t, srv.URL).Warnings(context.Background(), Locality{Name: "Niepołomice", Lat: 50.02, Lon: 20.13})
		srv.Close()
		if err == nil {
			t.Errorf("%s: Warnings = %+v, want an error (an unreadable answer must never look like a quiet point)", c.name, got)
			continue
		}
		if !strings.Contains(err.Error(), "payload") && !strings.Contains(err.Error(), "incomplete warning answer") {
			t.Errorf("%s: error = %v, want it to name the unreadable answer", c.name, err)
		}
		if len(got.InForce) != 0 {
			t.Errorf("%s: returned %d warnings with an error", c.name, len(got.InForce))
		}
	}

	// The same rule for the other two operations: a maintenance page must
	// not look like a locality at 0/0 (which would be a "typo") or like a
	// quiet sky.
	for _, op := range []string{"miejscowosc", "szukaj_burzy"} {
		srv := soapServer(t, map[string]string{op: maintenancePage}, nil)
		client := testClient(t, srv.URL)
		switch op {
		case "miejscowosc":
			loc, err := client.Locality(context.Background(), "Niepołomice")
			if err == nil || errors.Is(err, ErrUnknownLocality) {
				t.Errorf("%s: maintenance page gave %+v, %v (it must fail, and never look like a typo)", op, loc, err)
			}
		case "szukaj_burzy":
			st, err := client.Storm(context.Background(), Locality{Name: "Niepołomice", Lat: 50.02, Lon: 20.13}, 25)
			if err == nil {
				t.Errorf("%s: maintenance page gave %+v, want an error", op, st)
			}
		}
		srv.Close()
	}

	// A complete envelope whose windows are unreadable still yields the
	// per-phenomenon rejection (that is a provider data defect, not a wrong
	// document), so the two rules stay distinguishable.
	srv := soapServer(t, map[string]string{"ostrzezenia_pogodowe": warningsUnreadable}, nil)
	defer srv.Close()
	got, err := testClient(t, srv.URL).Warnings(context.Background(), Locality{Name: "Niepołomice", Lat: 50.02, Lon: 20.13})
	if err != nil {
		t.Fatalf("a complete envelope with bad windows must decode: %v", err)
	}
	if len(got.Rejected) != 2 || len(got.InForce) != 0 {
		t.Errorf("rejected/in force = %v/%d, want the two unreadable windows rejected", got.Rejected, len(got.InForce))
	}
}
