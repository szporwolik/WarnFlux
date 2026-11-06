package gsm

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestValidUSSD(t *testing.T) {
	for _, ok := range []string{"*100#", "*#06#", "#111#", "*101*1#", " *100# "} {
		if !ValidUSSD(ok) {
			t.Errorf("ValidUSSD(%q) = false, want true", ok)
		}
	}
	for _, bad := range []string{"", "*100", "100#", "*10 0#", `*100#"`, "*100#\r\nAT+CMGS", "*#", "abc#", "*100#" + string(make([]byte, 40))} {
		if ValidUSSD(bad) {
			t.Errorf("ValidUSSD(%q) = true, want false", bad)
		}
	}
}

func TestParseCUSDAndDecoding(t *testing.T) {
	// Plain text, the usual shape on a Polish SIM.
	if got, fail := parseCUSD(`+CUSD: 0,"Stan konta: 12,34 zl",15`); fail || got != "Stan konta: 12,34 zl" {
		t.Errorf("plain = %q fail=%v", got, fail)
	}
	// UCS2 hex, exactly what the modem sends for Polish characters.
	if got, _ := parseCUSD(`+CUSD: 0,"005300740061006E0020006B006F006E00740061003A002000310032002C003300340020007A0142",15`); got != "Stan konta: 12,34 zł" {
		t.Errorf("ucs2 = %q", got)
	}
	// A menu (status 1) is still an answer the operator wants to read.
	if got, fail := parseCUSD(`+CUSD: 1,"Wybierz: 1 konto, 2 pakiet",15`); fail || got == "" {
		t.Errorf("menu = %q fail=%v", got, fail)
	}
	// A failure status is not a reply.
	if _, fail := parseCUSD(`+CUSD: 4,"",15`); !fail {
		t.Error("status 4 must be reported as a failure")
	}
}

// TestSendUSSDTalksToTheModem pins the whole round trip: the command carries
// the code and charset 15, the network's answer is returned, and a refused
// code is reported as a rejection instead of a silent empty reply.
func TestSendUSSDTalksToTheModem(t *testing.T) {
	h, f, _ := newTestHub(t)
	// The hub cancels a possibly stale session first, then sends the code.
	f.feed("AT+CUSD=2\r\r\nOK\r\n")
	f.feed("AT+CUSD=1,\"*100#\",15\r\r\nOK\r\n+CUSD: 0,\"005300740061006E0020006B006F006E00740061003A002000350020007A0142\",15\r\n")
	got, err := h.SendUSSD(context.Background(), "*100#")
	if err != nil {
		t.Fatalf("SendUSSD: %v", err)
	}
	if got != "Stan konta: 5 zł" {
		t.Errorf("reply = %q", got)
	}
	if !strings.Contains(f.written(), "AT+CUSD=1,\"*100#\",15") {
		t.Errorf("modem never saw the code: %q", f.written())
	}

	h2, f2, _ := newTestHub(t)
	f2.feed("AT+CUSD=2\r\r\nOK\r\n")
	f2.feed("AT+CUSD=1,\"*999#\",15\r\r\n+CUSD: 4,\"\",15\r\n")
	if _, err := h2.SendUSSD(context.Background(), "*999#"); !errors.Is(err, ErrUSSDRejected) {
		t.Errorf("refused code = %v, want ErrUSSDRejected", err)
	}

	// A code that is not a short code never reaches the modem.
	h3, f3, _ := newTestHub(t)
	if _, err := h3.SendUSSD(context.Background(), "AT+CMGS=\"+48111222333\""); err == nil {
		t.Fatal("an AT command was accepted as a ussd code")
	}
	if f3.written() != "" {
		t.Errorf("the modem was written to with a non-code: %q", f3.written())
	}
}

// TestSendUSSDCancelsAStaleSession pins the recovery measured on the production
// modem: a session left open makes every new code fail with a bare ERROR, so
// the station cancels the old one first and then sends.
func TestSendUSSDCancelsAStaleSession(t *testing.T) {
	h, f, _ := newTestHub(t)
	f.feed("AT+CUSD=2\r\r\nOK\r\n")
	f.feed("AT+CUSD=1,\"*100#\",15\r\r\nOK\r\n+CUSD: 0,\"Saldo: 12 zl\",15\r\n")
	got, err := h.SendUSSD(context.Background(), "*100#")
	if err != nil {
		t.Fatalf("SendUSSD: %v", err)
	}
	if got != "Saldo: 12 zl" {
		t.Errorf("reply = %q", got)
	}
	written := f.written()
	if !strings.Contains(written, "AT+CUSD=2") {
		t.Errorf("the stale session was not cancelled first: %q", written)
	}
	if strings.Index(written, "AT+CUSD=2") > strings.Index(written, "AT+CUSD=1,") {
		t.Errorf("cancel came after the code: %q", written)
	}
}
