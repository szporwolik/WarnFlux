package sms

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/dispatch"
	"github.com/szporwolik/WarnFlux/internal/gsm"
)

// fakeSender is an in-memory sender seam.
type fakeSender struct {
	enabled bool

	mu   sync.Mutex
	sent [][2]string // number, text
}

func (f *fakeSender) Enabled() bool { return f.enabled }

func (f *fakeSender) Send(_ context.Context, number, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, [2]string{number, text})
	return nil
}

func (f *fakeSender) list() [][2]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][2]string, len(f.sent))
	copy(out, f.sent)
	return out
}

func configNode(t *testing.T, m map[string]any) *yaml.Node {
	t.Helper()
	var node yaml.Node
	if err := node.Encode(m); err != nil {
		t.Fatal(err)
	}
	return &node
}

func hazardReq() action.ActionRequest {
	return action.ActionRequest{
		App: action.AppInfo{Header1: "SOSNA"},
		Event: dispatch.Event{
			Kind: dispatch.EventHazardTransition,
			Hazard: &dispatch.HazardTransition{
				Hazard: dispatch.Hazard{
					EventKey:  "imgw-meteo:1",
					Severity:  "severe",
					Event:     "Burza z gradem",
					Headline:  "Ostrzeżenie dla powiatu krakowskiego",
					Latitude:  ptr(50.06),
					Longitude: ptr(19.94),
				},
			},
		},
	}
}

func ptr(f float64) *float64 { return &f }

func TestNewValidation(t *testing.T) {
	hub := &fakeSender{enabled: true}

	if _, err := New(configNode(t, map[string]any{"phones": []string{"+48600111222", "abc"}}), hub); err == nil {
		t.Error("invalid phone accepted")
	}
	if _, err := New(configNode(t, map[string]any{"phones": []string{"+48600111222"}}), nil); err == nil {
		t.Error("nil hub accepted")
	}
	if _, err := New(configNode(t, map[string]any{"phones": []string{"+48600111222"}}), &fakeSender{enabled: false}); err == nil {
		t.Error("disabled hub accepted")
	}
	// No static numbers is valid: the routing rule may rely entirely on
	// the matched group members' registered phones.
	if _, err := New(configNode(t, map[string]any{}), hub); err != nil {
		t.Errorf("config without phones rejected: %v", err)
	}
	if _, err := New(nil, hub); err != nil {
		t.Errorf("empty node rejected: %v", err)
	}
	p, err := New(configNode(t, map[string]any{
		"phones": []string{"+48600111222", "509558155"},
		"prefix": "WarnFlux",
	}), hub)
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if p.Name() != Type {
		t.Errorf("Name() = %q, want %q", p.Name(), Type)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestExecuteGroupPhonesOnly pins the routing-rule shape the operator
// picks when the numbers live in the directory: no static list at all,
// every recipient coming from the matched group's members — and a no-op
// (not a failure) when none of them registered one.
func TestExecuteGroupPhonesOnly(t *testing.T) {
	hub := &fakeSender{enabled: true}
	p, err := New(configNode(t, map[string]any{"prefix": "SOSNA"}), hub)
	if err != nil {
		t.Fatal(err)
	}
	req := hazardReq()
	req.Phones = []string{"+48509558155"}
	req.PhoneLangs = []string{"pl"}
	if err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sent := hub.list(); len(sent) != 1 || sent[0][0] != "+48509558155" {
		t.Fatalf("sent = %v, want the group member's number only", sent)
	}

	if err := p.Execute(context.Background(), hazardReq()); err != nil {
		t.Fatalf("Execute without recipients: %v", err)
	}
	if sent := hub.list(); len(sent) != 1 {
		t.Fatalf("sent = %v, want nothing sent without recipients", sent)
	}
}

func TestExecuteSendsToAllRecipients(t *testing.T) {
	hub := &fakeSender{enabled: true}
	p, err := New(configNode(t, map[string]any{
		"phones": []string{"+48600111222"},
		"prefix": "WarnFlux",
	}), hub)
	if err != nil {
		t.Fatal(err)
	}
	req := hazardReq()
	req.Phones = []string{"509558155"}
	req.PhoneLangs = []string{"pl"}
	if err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	sent := hub.list()
	if len(sent) != 2 {
		t.Fatalf("sent = %d messages, want 2: %v", len(sent), sent)
	}
	if sent[0][0] != "+48600111222" || sent[1][0] != "509558155" {
		t.Errorf("recipients = %v", sent)
	}
	if !strings.HasPrefix(sent[0][1], "WarnFlux SEVERE") {
		t.Errorf("message missing prefix/severity: %q", sent[0][1])
	}
}

func TestExecuteDedupsByNumberKey(t *testing.T) {
	hub := &fakeSender{enabled: true}
	p, err := New(configNode(t, map[string]any{
		"phones": []string{"+48600111222"},
	}), hub)
	if err != nil {
		t.Fatal(err)
	}
	req := hazardReq()
	req.Phones = []string{"+48 600 111 222", "0048600111222"} // same number, different formats
	if err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sent := hub.list(); len(sent) != 1 {
		t.Fatalf("sent = %d messages, want 1 (dedup by digits): %v", len(sent), sent)
	}
}

func TestTextBoundedAndTransliterated(t *testing.T) {
	hub := &fakeSender{enabled: true}
	p, err := New(configNode(t, map[string]any{
		"phones": []string{"+48600111222"},
		"prefix": "WarnFlux",
	}), hub)
	if err != nil {
		t.Fatal(err)
	}
	a := p.(*Action)
	req := hazardReq()
	req.Event.Hazard.Hazard.Headline = "Żółć ąę " + strings.Repeat("x", 200)
	text := a.textFor(context.Background(), req, "pl")
	// The budget is in septets: the formatter's own separators (" | ",
	// "[WF-…]") already contain extension-table characters.
	if n := gsm.Septets(text); n < 0 || n > gsm.MaxSeptets {
		t.Fatalf("text = %d septets, want <= %d (%q)", n, gsm.MaxSeptets, text)
	}
	if strings.ContainsAny(text, "Żółćąę") {
		t.Fatalf("text not transliterated: %q", text)
	}
	if !strings.HasPrefix(text, "WarnFlux ") {
		t.Fatalf("text missing the prefix: %q", text)
	}
	if !strings.HasPrefix(text, "WarnFlux SEVERE") && !strings.HasPrefix(text, "WarnFlux POWAZNE") {
		t.Fatalf("text missing the severity: %q", text)
	}
}

// failingSender refuses the numbers it is told to, like a modem that has
// no route to one of them.
type failingSender struct {
	fakeSender
	refuse map[string]bool
}

func (f *failingSender) Send(_ context.Context, number, text string) error {
	if f.refuse[number] {
		return errors.New("gsm: no network")
	}
	return f.fakeSender.Send(context.Background(), number, text)
}

// TestPartialBatchNarrowsTheRetry pins the reported P2: when the message
// to A goes out and the one to B fails, the action used to report the
// WHOLE batch as failed, so the retry charged A with a second SMS (and a
// second slice of modem time). The numbers that went out now ride with
// the result and the retry covers only the rest.
func TestPartialBatchNarrowsTheRetry(t *testing.T) {
	hub := &failingSender{refuse: map[string]bool{"509558155": true}}
	hub.enabled = true
	p, err := New(configNode(t, map[string]any{
		"phones": []string{"+48600111222"},
	}), hub)
	if err != nil {
		t.Fatal(err)
	}
	req := hazardReq()
	req.Phones = []string{"509558155"}

	err = p.Execute(context.Background(), req)
	if err == nil {
		t.Fatal("Execute reported success although one message failed")
	}
	var partial action.PartialDelivery
	if !errors.As(err, &partial) {
		t.Fatalf("error %v does not carry the delivered numbers", err)
	}
	if got := partial.Delivered(); len(got) != 1 || got[0] != "+48600111222" {
		t.Fatalf("delivered = %v, want [+48600111222]", got)
	}

	// The retry carries what went out: only B is sent again.
	hub.refuse = map[string]bool{}
	retry := req
	retry.DoneRecipients = partial.Delivered()
	if err := p.Execute(context.Background(), retry); err != nil {
		t.Fatalf("retry: %v", err)
	}
	var toA, toB int
	for _, msg := range hub.list() {
		switch msg[0] {
		case "+48600111222":
			toA++
		case "509558155":
			toB++
		}
	}
	if toA != 1 {
		t.Fatalf("the number that already got the message received it %d times, want 1", toA)
	}
	if toB != 1 {
		t.Fatalf("the retried number received it %d times, want 1", toB)
	}
}

// TestDoneRecipientsAreMatchedByNumberKey pins the normalization: the
// progress recorded by an earlier attempt must match the recipient list
// even when the same number is written in another format (international
// prefix, spacing), which is how operators and directories write them.
func TestDoneRecipientsAreMatchedByNumberKey(t *testing.T) {
	hub := &fakeSender{enabled: true}
	p, err := New(configNode(t, map[string]any{
		"phones": []string{"+48600111222"},
	}), hub)
	if err != nil {
		t.Fatal(err)
	}
	req := hazardReq()
	req.Phones = []string{"0048 600 111 222"}
	req.DoneRecipients = []string{"+48600111222"}
	if err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sent := hub.list(); len(sent) != 0 {
		t.Fatalf("sent = %v, want none (the number was already done)", sent)
	}
}

// TestEveryRecipientDoneIsASuccess pins the idempotent retry: nothing
// left to do is a success, never a "no recipients" failure.
func TestEveryRecipientDoneIsASuccess(t *testing.T) {
	hub := &fakeSender{enabled: true}
	p, err := New(configNode(t, map[string]any{
		"phones": []string{"+48600111222"},
	}), hub)
	if err != nil {
		t.Fatal(err)
	}
	req := hazardReq()
	req.DoneRecipients = []string{"+48600111222"}
	if err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sent := hub.list(); len(sent) != 0 {
		t.Fatalf("sent = %v, want none", sent)
	}
}

// TestTextFitsTheSeptetBudget pins the formatter against both halves of the
// production failure. A headline full of GSM-7 EXTENSION symbols must come out
// as text the modem actually takes: the symbols are FOLDED (this modem answers
// +CMS ERROR 305 for a body carrying them, so a text that keeps them is a text
// that is never delivered) and the whole message fits ONE segment, cut on the
// septet budget rather than a character count.
func TestTextFitsTheSeptetBudget(t *testing.T) {
	hub := &fakeSender{enabled: true}
	p, err := New(configNode(t, map[string]any{
		"phones": []string{"+48600111222"},
		"prefix": "WarnFlux",
	}), hub)
	if err != nil {
		t.Fatal(err)
	}
	a := p.(*Action)
	req := hazardReq()
	req.Event.Hazard.Hazard.Headline = strings.Repeat("[^]", 60)
	req.Event.Hazard.Hazard.Latitude = nil
	req.Event.Hazard.Hazard.Longitude = nil

	text := a.textFor(context.Background(), req, "pl")
	if n := gsm.Septets(text); n < 0 || n > gsm.MaxSeptets {
		t.Fatalf("text = %d septets, want <= %d (%q)", n, gsm.MaxSeptets, text)
	}
	if strings.ContainsAny(text, "[]{}|\\~^€") {
		t.Fatalf("the text still carries a character this modem refuses: %q", text)
	}
	if !strings.Contains(text, "(") {
		t.Fatalf("text lost the headline content: %q", text)
	}
	if err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("Execute with an extended headline = %v, want the fitted message", err)
	}

	// A long PLAIN headline is still cut to one message: the fold must not
	// have taken the budget check away.
	req.Event.Hazard.Hazard.Headline = strings.Repeat("A", 400)
	text = a.textFor(context.Background(), req, "pl")
	if n := gsm.Septets(text); n < 0 || n > gsm.MaxSeptets {
		t.Fatalf("long plain text = %d septets, want <= %d", n, gsm.MaxSeptets)
	}
}

// unconfirmedSender answers like a modem that took the body and never
// confirmed it (gsm.ErrUnconfirmed); refuse, when set, fails a number
// before transmission.
type unconfirmedSender struct {
	fakeSender
	silent map[string]bool
	refuse map[string]bool
}

func (s *unconfirmedSender) Send(_ context.Context, number, text string) error {
	if s.refuse[number] {
		return errors.New("gsm: no network")
	}
	if s.silent[number] {
		s.mu.Lock()
		s.sent = append(s.sent, [2]string{number, text})
		s.mu.Unlock()
		return fmt.Errorf("gsm: send to %s: %w", number, gsm.ErrUnconfirmed)
	}
	return s.fakeSender.Send(context.Background(), number, text)
}

// TestUnconfirmedSendIsAStageNotAFailure pins the reported P2: a message
// the modem took without confirming used to come back as a plain failure,
// so the worker retried it and the number got a SECOND SMS. The call now
// reports the weaker stage with no error — the worker settles it as
// "unconfirmed" and schedules no resend — and a retry caused by ANOTHER
// recipient skips the number that was handed over.
func TestUnconfirmedSendIsAStageNotAFailure(t *testing.T) {
	const silent = "509558155"
	hub := &unconfirmedSender{silent: map[string]bool{silent: true}}
	hub.enabled = true
	p, err := New(configNode(t, map[string]any{"phones": []string{"+48600111222"}}), hub)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := p.(action.ConfirmingPlugin).ExecuteStage(context.Background(), hazardReqWithPhones(silent))
	if err != nil {
		t.Fatalf("ExecuteStage = %v, want success (the stage carries the uncertainty)", err)
	}
	if stage != action.StageUnconfirmed {
		t.Fatalf("stage = %v, want StageUnconfirmed", stage)
	}
	counts := map[string]int{}
	for _, msg := range hub.list() {
		counts[msg[0]]++
	}
	if counts[silent] != 1 || counts["+48600111222"] != 1 {
		t.Fatalf("sends = %v, want exactly one message per number", counts)
	}
}

// TestDefiniteFailureNarrowsPastTheUnconfirmedNumber pins the mixed case:
// one number is handed over without confirmation and another fails before
// transmission, so the call stays a retryable error — but the retry must
// cover ONLY the number that truly failed.
func TestDefiniteFailureNarrowsPastTheUnconfirmedNumber(t *testing.T) {
	const silent, refused = "509558155", "509558156"
	hub := &unconfirmedSender{
		silent: map[string]bool{silent: true},
		refuse: map[string]bool{refused: true},
	}
	hub.enabled = true
	p, err := New(configNode(t, map[string]any{"phones": []string{"+48600111222"}}), hub)
	if err != nil {
		t.Fatal(err)
	}
	req := hazardReqWithPhones(silent, refused)
	err = p.Execute(context.Background(), req)
	if err == nil {
		t.Fatal("a message that failed before transmission reported success")
	}
	var partial action.PartialDelivery
	if !errors.As(err, &partial) {
		t.Fatalf("error %v does not carry the numbers that are already settled", err)
	}
	done := map[string]bool{}
	for _, n := range partial.Delivered() {
		done[n] = true
	}
	if !done["+48600111222"] {
		t.Fatalf("delivered numbers = %v, want the confirmed one", partial.Delivered())
	}
	if done[silent] {
		t.Fatalf("the unconfirmed number is reported as DELIVERED: %v", partial.Delivered())
	}
	if done[refused] {
		t.Fatalf("the number that failed before transmission is marked settled: %v", partial.Delivered())
	}
	// The unconfirmed number travels in its own list, so the job never
	// presents it as a delivery.
	var reported action.PartialUnconfirmed
	if !errors.As(err, &reported) {
		t.Fatalf("error %v does not report the numbers handed over without confirmation", err)
	}
	if got := reported.Unconfirmed(); len(got) != 1 || got[0] != silent {
		t.Fatalf("unconfirmed = %v, want [%s]", got, silent)
	}

	// The retry therefore mails only the refused number (the worker
	// narrows the payload with what the failed attempt reported).
	hub.refuse = map[string]bool{}
	retry := req
	retry.DoneRecipients = partial.Delivered()
	retry.UnconfirmedRecipients = reported.Unconfirmed()
	before := len(hub.list())
	if err := p.Execute(context.Background(), retry); err != nil {
		t.Fatalf("retry: %v", err)
	}
	retried := hub.list()[before:]
	if len(retried) != 1 || retried[0][0] != refused {
		t.Fatalf("retry sent %v, want only the refused number", retried)
	}
}

// hazardReqWithPhones builds a request routed to one extra phone number.
func hazardReqWithPhones(phones ...string) action.ActionRequest {
	req := hazardReq()
	req.Phones = phones
	return req
}

// TestUnconfirmedSurvivesTheRetryAndTheRestart pins the reported P2: the
// first attempt handed A to the modem without a confirmation and failed on
// B. The retry runs from the durable payload (as it would after a restart)
// and delivers B — the job must STILL report the weaker stage, because A
// was never confirmed, and A must not receive a second SMS.
func TestUnconfirmedSurvivesTheRetryAndTheRestart(t *testing.T) {
	const silent, refused = "509558155", "509558156"
	hub := &unconfirmedSender{
		silent: map[string]bool{silent: true},
		refuse: map[string]bool{refused: true},
	}
	hub.enabled = true
	p, err := New(configNode(t, map[string]any{"phones": []string{"+48600111222"}}), hub)
	if err != nil {
		t.Fatal(err)
	}
	req := hazardReqWithPhones(silent, refused)
	err = p.Execute(context.Background(), req)
	if err == nil {
		t.Fatal("the refused number did not fail the attempt")
	}
	var partial action.PartialDelivery
	if !errors.As(err, &partial) {
		t.Fatalf("error %v carries no progress", err)
	}
	var reported action.PartialUnconfirmed
	if !errors.As(err, &reported) {
		t.Fatalf("error %v does not report the unconfirmed number", err)
	}

	// The worker persists the narrowed payload; a restart rebuilds the
	// request from it and a FRESH plugin runs the retry.
	retry := req
	retry.DoneRecipients = partial.Delivered()
	retry.UnconfirmedRecipients = reported.Unconfirmed()
	payload, err := json.Marshal(retry)
	if err != nil {
		t.Fatal(err)
	}
	var restored action.ActionRequest
	if err := json.Unmarshal(payload, &restored); err != nil {
		t.Fatal(err)
	}

	healthy := &unconfirmedSender{silent: map[string]bool{}}
	healthy.enabled = true
	p2, err := New(configNode(t, map[string]any{"phones": []string{"+48600111222"}}), healthy)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := p2.(action.ConfirmingPlugin).ExecuteStage(context.Background(), restored)
	if err != nil {
		t.Fatalf("retry = %v, want success (only the refused number was left)", err)
	}
	if stage != action.StageUnconfirmed {
		t.Fatalf("retry stage = %v, want StageUnconfirmed (A was never confirmed)", stage)
	}
	for _, msg := range healthy.list() {
		if msg[0] == silent {
			t.Fatalf("the retry sent a second SMS to the unconfirmed number: %v", healthy.list())
		}
	}
}

// TestTextCarriesTheDescriptionNotTheAreaSlugs pins the production message
// that read "SOSNA SEVERE Utrudnienia na A4 | corridor:a4-balice-tarnow,
// droga:a4, ..." — a title repeated as the event and a machine slug list, with
// nothing about what happened. The description sentence must come first and
// the area list may only fill what is left.
func TestTextCarriesTheDescriptionNotTheAreaSlugs(t *testing.T) {
	hub := &fakeSender{enabled: true}
	p, err := New(configNode(t, map[string]any{
		"phones": []string{"+48600111222"},
		"prefix": "SOSNA",
	}), hub)
	if err != nil {
		t.Fatal(err)
	}
	a := p.(*Action)
	req := hazardReq()
	h := &req.Event.Hazard.Hazard
	h.Event = "Utrudnienia na A4"
	h.Headline = "Utrudnienia na A4"
	h.Description = "Jerzmanowice ul. Jurajska, pow. krakowski wypadek autobusu MPK i samochodu osobowego, DK 94 zablokowana (314km)"
	h.Areas = []string{"corridor:a4-balice-tarnow", "droga:a4", "droga:dk94", "miasto:krakow", "wojewodztwo:malopolskie"}
	h.Latitude = nil
	h.Longitude = nil

	text := a.textFor(context.Background(), req, "pl")
	if n := gsm.Septets(text); n < 0 || n > gsm.MaxSeptets {
		t.Fatalf("text = %d septets, want <= %d (%q)", n, gsm.MaxSeptets, text)
	}
	if !strings.Contains(text, "Jerzmanowice") && !strings.Contains(text, "wypadek") {
		t.Fatalf("text carries no description: %q", text)
	}
	if strings.Count(text, "Utrudnienia") > 1 {
		t.Fatalf("the headline repeats the event label: %q", text)
	}
	// The modem refuses the GSM-7 extension symbols, so the formatter folds
	// "[" and "]" into parentheses — the ID itself must survive.
	if !strings.Contains(text, "WF-") {
		t.Fatalf("text lost the message ID: %q", text)
	}
}
