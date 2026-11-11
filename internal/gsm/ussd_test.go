package gsm

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
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
	if got, _, fail := parseCUSD(`+CUSD: 0,"Stan konta: 12,34 zl",15`); fail || got != "Stan konta: 12,34 zl" {
		t.Errorf("plain = %q fail=%v", got, fail)
	}
	// UCS2 hex, exactly what the modem sends for Polish characters.
	if got, _, _ := parseCUSD(`+CUSD: 0,"005300740061006E0020006B006F006E00740061003A002000310032002C003300340020007A0142",15`); got != "Stan konta: 12,34 zł" {
		t.Errorf("ucs2 = %q", got)
	}
	// A menu (status 1) is still an answer the operator wants to read.
	if got, _, fail := parseCUSD(`+CUSD: 1,"Wybierz: 1 konto, 2 pakiet",15`); fail || got == "" {
		t.Errorf("menu = %q fail=%v", got, fail)
	}
	// A failure status is not a reply.
	if _, _, fail := parseCUSD(`+CUSD: 4,"",15`); !fail {
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

// TestBareCUSDStatusesAreFailures is WF-REV-09: the network may answer with the
// status alone ("+CUSD: 4"), and a parser that needs the text field reported
// that as a successful empty reply — a refused code looked like a reply nobody
// wrote.
func TestBareCUSDStatusesAreFailures(t *testing.T) {
	for _, line := range []string{
		`+CUSD: 2`, `+CUSD: 4`, `+CUSD: 5`,
		`+CUSD: 2,"",15`, `+CUSD: 4,"",15`, `+CUSD: 5,"",15`,
		`+CUSD: 2,"abc",15`,
	} {
		if _, _, fail := parseCUSD(line); !fail {
			t.Errorf("parseCUSD(%q) reported success, want a failure", line)
		}
	}
	// The successful shapes keep working, with and without text.
	for _, line := range []string{`+CUSD: 0,"Saldo: 12 zl",15`, `+CUSD: 0`, `+CUSD: 1,"Menu",15`, `+CUSD: 1`} {
		if _, _, fail := parseCUSD(line); fail {
			t.Errorf("parseCUSD(%q) reported a failure, want success", line)
		}
	}
	// A line that is not a +CUSD answer at all is never a success.
	if _, _, fail := parseCUSD(`+CUSD: nonsense`); !fail {
		t.Error("an unparseable status was taken as success")
	}
}

// TestSendUSSDReportsBareStatusFailure pins the same rule through the call:
// "+CUSD: 5" with no payload must surface as ErrUSSDRejected, not as an empty
// answer with a nil error.
func TestSendUSSDReportsBareStatusFailure(t *testing.T) {
	h, f, _ := newTestHub(t)
	f.feed("AT+CUSD=2\r\r\nOK\r\n")
	f.feed("AT+CUSD=1,\"*100#\",15\r\r\nOK\r\n+CUSD: 5\r\n")
	text, err := h.SendUSSD(context.Background(), "*100#")
	if !errors.Is(err, ErrUSSDRejected) {
		t.Fatalf("SendUSSD = %q, %v; want ErrUSSDRejected", text, err)
	}
}

// TestSendUSSDModemLostDuringRepairCannotPanic is WF-REV-03: the session repair
// may reconnect, which zeroes h.conn while the request is already under way.
// Writing into that nil handle used to panic; the call must return "no modem"
// instead and leave the lock free.
func TestSendUSSDModemLostDuringRepairCannotPanic(t *testing.T) {
	h, f, _ := newTestHub(t)
	// The dirty flag makes the next exchange repair the session, and the closed
	// port makes that repair give up and zero h.conn — a modem unplugged between
	// the lock and the write, which is exactly what the review reproduced.
	h.sendDirty = true
	if err := f.Close(); err != nil {
		t.Fatalf("closing the fake port: %v", err)
	}

	text, err := h.SendUSSD(context.Background(), "*100#")
	if err == nil {
		t.Fatalf("SendUSSD on a lost modem = %q, nil; want an error", text)
	}
	if !errors.Is(err, ErrNoModem) {
		t.Errorf("SendUSSD = %v, want ErrNoModem", err)
	}
	// The lock must be free again, or every later send would wait for nothing.
	probe, cancelProbe := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelProbe()
	if err := h.mu.acquire(probe); err != nil {
		t.Fatalf("the modem lock was left held after the failure: %v", err)
	}
	h.mu.Unlock()
}

// TestSendUSSDCancellationReleasesTheModem is WF-REV-04: the network goes
// silent after the code was sent and the operator cancels. The shared modem lock
// must come back at once — not after the 40-second USSD budget — and the session
// must be marked for repair so a late +CUSD cannot be read as the answer to the
// next command.
func TestSendUSSDCancellationReleasesTheModem(t *testing.T) {
	h, f, _ := newTestHub(t)
	f.feed("AT+CUSD=2\r\r\nOK\r\n")
	f.feed("AT+CUSD=1,\"*100#\",15\r\r\nOK\r\n")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := h.SendUSSD(ctx, "*100#")
		done <- err
	}()
	// Let the code go out, then cancel while the modem says nothing.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(f.written(), `AT+CUSD=1,"*100#"`) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("SendUSSD = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		buf := make([]byte, 1<<15)
		n := runtime.Stack(buf, true)
		t.Fatalf("cancelling a silent USSD left the modem locked\n%s", buf[:n])
	}
	if !h.sendDirty {
		t.Error("a cancelled USSD did not mark the session for repair")
	}
}
