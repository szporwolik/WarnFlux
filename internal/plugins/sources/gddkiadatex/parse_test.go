package gddkiadatex

import (
	"os"
	"strings"
	"testing"
)

// The fixture is a real KPD response trimmed to three situations: the parser has
// to survive the prefixed namespaces, the nested value lists and the point
// geometry exactly as the service writes them.
func TestParseResponseFromTheService(t *testing.T) {
	raw, err := os.ReadFile("testdata/snapshot.xml")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := parseResponse(raw)
	if err != nil {
		t.Fatalf("parseResponse: %v", err)
	}
	if got := len(doc.Payload.Situations); got != 3 {
		t.Fatalf("situations = %d, want 3", got)
	}
	rec := doc.Payload.Situations[0].Records[0]
	if rec.ID == "" {
		t.Fatal("record has no id")
	}
	if localType(rec.Type) != "MaintenanceWorks" {
		t.Fatalf("type = %q, want MaintenanceWorks", rec.Type)
	}
	if _, _, ok := rec.coordinates(); !ok {
		t.Fatalf("record %s has no coordinates", rec.ID)
	}
	if rec.text() == "" {
		t.Fatalf("record %s has no description text", rec.ID)
	}
	if got := rec.Validity.Status; got != "active" {
		t.Fatalf("validity = %q, want active", got)
	}
}

// An empty publication is legitimate (nothing is happening on the roads), but a
// payload that is not a SituationPublication means the shape changed and must
// be reported instead of silently cancelling every warning.
func TestParseResponseRejectsForeignPayload(t *testing.T) {
	_, err := parseResponse([]byte(`<messageContainer><payload xsi:type="ns5:MeasuredDataPublication" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"/></messageContainer>`))
	if err == nil {
		t.Fatal("parseResponse accepted a non-situation publication")
	}
}

// A publication with no situations at all is a valid, empty result.
func TestParseResponseAcceptsEmptySituationPublication(t *testing.T) {
	doc, err := parseResponse([]byte(`<messageContainer><payload xsi:type="ns5:SituationPublication" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"/></messageContainer>`))
	if err != nil {
		t.Fatalf("parseResponse: %v", err)
	}
	if len(doc.Payload.Situations) != 0 {
		t.Fatalf("situations = %d, want none", len(doc.Payload.Situations))
	}
}

// The service answers with a SOAP envelope, not a bare publication: the parser
// must unwrap Body and still see the situations (regression: it once expected
// messageContainer at the root and rejected every real response).
func TestParseResponseUnwrapsSOAPEnvelope(t *testing.T) {
	inner, err := os.ReadFile("testdata/snapshot.xml")
	if err != nil {
		t.Fatal(err)
	}
	wrapped := append([]byte(`<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Header/><SOAP-ENV:Body>`), inner...)
	wrapped = append(wrapped, []byte(`</SOAP-ENV:Body></SOAP-ENV:Envelope>`)...)
	doc, err := parseResponse(wrapped)
	if err != nil {
		t.Fatalf("parseResponse on a SOAP envelope: %v", err)
	}
	if len(doc.Payload.Situations) != 3 {
		t.Fatalf("situations = %d, want 3", len(doc.Payload.Situations))
	}
}

// A rejected call comes back as a SOAP fault; the message must survive into the
// error so the log says why.
func TestParseResponseReportsSOAPFault(t *testing.T) {
	fault := `<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Body><SOAP-ENV:Fault><faultcode>SOAP-ENV:Client</faultcode><faultstring xml:lang="en">The signature or decryption was invalid</faultstring></SOAP-ENV:Fault></SOAP-ENV:Body></SOAP-ENV:Envelope>`
	_, err := parseResponse([]byte(fault))
	if err == nil {
		t.Fatal("parseResponse accepted a SOAP fault")
	}
	if !strings.Contains(err.Error(), "signature or decryption") {
		t.Fatalf("error = %v, want the fault text", err)
	}
}
