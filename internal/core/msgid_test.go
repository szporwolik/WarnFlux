package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeMsgIDStore is an in-memory MessageIDStore used to exercise the
// persistence seam without a database.
type fakeMsgIDStore struct {
	mu      sync.Mutex
	byKey   map[string]MessageIDAssignment
	seq     map[string]int
	saveErr error
}

func newFakeMsgIDStore() *fakeMsgIDStore {
	return &fakeMsgIDStore{byKey: make(map[string]MessageIDAssignment), seq: make(map[string]int)}
}

func (f *fakeMsgIDStore) LoadMessageID(_ context.Context, eventKey string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.byKey[eventKey]
	if !ok {
		return "", false, nil
	}
	return a.MsgID, true, nil
}

func (f *fakeMsgIDStore) HighestSeqInMinute(_ context.Context, minute string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seq[minute], nil
}

func (f *fakeMsgIDStore) SaveMessageID(_ context.Context, a MessageIDAssignment) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return f.saveErr
	}
	if _, ok := f.byKey[a.EventKey]; ok {
		return nil // first writer wins, like the durable store
	}
	f.byKey[a.EventKey] = a
	if a.Seq > f.seq[a.Minute] {
		f.seq[a.Minute] = a.Seq
	}
	return nil
}

// cleanMsgIDState isolates the package-global id state and the store seam
// for one test, restoring both afterwards.
func cleanMsgIDState(t *testing.T) {
	t.Helper()
	msgIDMu.Lock()
	prevByKey, prevMinute, prevSeq := msgIDByKey, msgIDMinute, msgIDSeq
	prevStore, prevLogf := msgIDStore, msgIDLogf
	msgIDByKey = make(map[string]string)
	msgIDMinute, msgIDSeq = "", 0
	msgIDStore, msgIDLogf = nil, nil
	msgIDMu.Unlock()
	t.Cleanup(func() {
		msgIDMu.Lock()
		msgIDByKey, msgIDMinute, msgIDSeq = prevByKey, prevMinute, prevSeq
		msgIDStore, msgIDLogf = prevStore, prevLogf
		msgIDMu.Unlock()
	})
}

// TestMessageIDPersistedWinsOverMinting pins the restart guarantee: a key
// already present in the durable store is reused verbatim instead of being
// renumbered.
func TestMessageIDPersistedWinsOverMinting(t *testing.T) {
	cleanMsgIDState(t)
	store := newFakeMsgIDStore()
	if err := store.SaveMessageID(context.Background(), MessageIDAssignment{
		EventKey: "imgw-meteo:1", MsgID: "WF-1007195807",
		Minute: "20261007-1958", Seq: 7, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	SetMessageIDStore(store, nil)

	at := time.Date(2026, 10, 7, 19, 58, 12, 0, time.UTC)
	if got := MessageIDAt("imgw-meteo:1", at); got != "WF-1007195807" {
		t.Fatalf("MessageIDAt = %q, want the persisted WF-1007195807", got)
	}
}

// TestMessageIDSurvivesRestart pins the end-to-end restart flow: after the
// in-memory state is dropped (as on a fresh process) a new key continues
// the minute sequence from the store and an old key keeps its id.
func TestMessageIDSurvivesRestart(t *testing.T) {
	cleanMsgIDState(t)
	store := newFakeMsgIDStore()
	SetMessageIDStore(store, nil)
	at := time.Date(2026, 10, 7, 19, 58, 12, 0, time.UTC)

	if got := MessageIDAt("a", at); got != "WF-1007195801" {
		t.Fatalf("first id = %q, want WF-1007195801", got)
	}
	if got := MessageIDAt("b", at); got != "WF-1007195802" {
		t.Fatalf("second id = %q, want WF-1007195802", got)
	}

	// Simulate a process restart: the in-memory cache and sequence are
	// gone, the store remains.
	msgIDMu.Lock()
	msgIDByKey = make(map[string]string)
	msgIDMinute, msgIDSeq = "", 0
	msgIDMu.Unlock()

	if got := MessageIDAt("a", at); got != "WF-1007195801" {
		t.Fatalf("old key renumbered after restart: %q, want WF-1007195801", got)
	}
	if got := MessageIDAt("c", at); got != "WF-1007195803" {
		t.Fatalf("new key after restart = %q, want WF-1007195803 (resumed sequence)", got)
	}
}

// TestMessageIDSaveFailureIsNonFatal pins graceful degradation: a failing
// store is logged but never blocks id assignment.
func TestMessageIDSaveFailureIsNonFatal(t *testing.T) {
	cleanMsgIDState(t)
	store := newFakeMsgIDStore()
	store.saveErr = errors.New("disk full")
	var logged []string
	SetMessageIDStore(store, func(msg string, _ ...any) { logged = append(logged, msg) })

	at := time.Date(2026, 10, 7, 19, 58, 12, 0, time.UTC)
	if got := MessageIDAt("a", at); got != "WF-1007195801" {
		t.Fatalf("id = %q, want an id despite the save failure", got)
	}
	if len(logged) == 0 {
		t.Fatal("save failure was not logged")
	}
}

// TestMessageIDDoesNotSaturateAt99 pins the reported P2: the per-minute
// sequence stopped at 99, so every further message of that minute reused
// the id of the 99th and operators could not name one alarm unambiguously.
func TestMessageIDDoesNotSaturateAt99(t *testing.T) {
	cleanMsgIDState(t)
	at := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)

	seen := make(map[string]string, 100)
	var ids []string
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("storm:%d", i)
		id := MessageIDAt(key, at)
		if prev, ok := seen[id]; ok {
			t.Fatalf("keys %q and %q share the id %q", prev, key, id)
		}
		seen[id] = key
		ids = append(ids, id)
	}
	if len(seen) != 100 {
		t.Fatalf("100 keys produced %d different ids", len(seen))
	}
	// The first 99 keep exactly the decimal form operators already cite ...
	if ids[98] != "WF-1012000099" {
		t.Fatalf("99th id = %q, want WF-1012000099", ids[98])
	}
	// ... and the 100th is a different id — the reported collision was
	// both messages carrying WF-1012000099.
	if ids[99] != "WF-10120000A0" {
		t.Fatalf("100th id = %q, want WF-10120000A0 (distinct from the 99th)", ids[99])
	}
}

// TestMessageIDScalesPastTheDecimalSequence is the acceptance test: 1000
// new keys inside one minute must all get different ids, and a restart
// must keep every assignment (a cited id never changes) while continuing
// with fresh, still-unique numbers.
func TestMessageIDScalesPastTheDecimalSequence(t *testing.T) {
	cleanMsgIDState(t)
	store := newFakeMsgIDStore()
	SetMessageIDStore(store, nil)
	at := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)

	ids := make(map[string]string, 1000)
	byKey := make(map[string]string, 1000)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("storm:%d", i)
		id := MessageIDAt(key, at)
		if len(id) != 13 || !strings.HasPrefix(id, "WF-") {
			t.Fatalf("id %q is not a 13-character WF- identifier", id)
		}
		if prev, ok := ids[id]; ok {
			t.Fatalf("keys %q and %q share the id %q", prev, key, id)
		}
		ids[id], byKey[key] = key, id
	}
	if len(ids) != 1000 {
		t.Fatalf("1000 keys produced %d different ids", len(ids))
	}

	// Restart: the in-memory state is gone, the store remains.
	msgIDMu.Lock()
	msgIDByKey = make(map[string]string)
	msgIDMinute, msgIDSeq = "", 0
	msgIDMu.Unlock()

	// Every cited id is still the one the key had ...
	if got := MessageIDAt("storm:0", at); got != byKey["storm:0"] {
		t.Fatalf("a cited id changed after the restart: %q, want %q", got, byKey["storm:0"])
	}
	// ... and new keys continue with fresh, still-unique numbers.
	for i := 1000; i < 1100; i++ {
		key := fmt.Sprintf("storm:%d", i)
		id := MessageIDAt(key, at)
		if prev, ok := ids[id]; ok {
			t.Fatalf("a post-restart id %q collides with the id of %q", id, prev)
		}
		ids[id] = key
	}
}

// TestMessageIDOverflowsIntoTheNextBucket pins the explicitly handled
// overflow: a bucket whose two characters are spent continues in the NEXT
// one instead of reusing a number, so ids stay unique past 1295 messages
// in a single minute.
func TestMessageIDOverflowsIntoTheNextBucket(t *testing.T) {
	cleanMsgIDState(t)
	at := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)

	seen := make(map[string]bool, msgIDSeqCap+8)
	for i := 0; i <= msgIDSeqCap+4; i++ {
		key := fmt.Sprintf("flood:%d", i)
		id := MessageIDAt(key, at)
		if seen[id] {
			t.Fatalf("id %q handed out twice at key %q", id, key)
		}
		seen[id] = true
	}
	// The first 1035 stay in the minute of the assignment ...
	if !seen["WF-10120000ZZ"] {
		t.Fatal("the last number of the minute is missing")
	}
	// ... and the overflow continues in the next one, with a fresh 01.
	if !seen["WF-1012000101"] {
		t.Fatal("the overflow did not continue in the next minute bucket")
	}
}

// TestMessageIDWalksPastSpentBucketsAfterARestart pins the reported P2: the
// overflow skipped ONE bucket, so when two minutes were already full (a
// restart keeps them) the next assignment landed above the capacity, the
// formatter clamped it and the id repeated — the reproduction got
// WF-10122001ZZ twice.
func TestMessageIDWalksPastSpentBucketsAfterARestart(t *testing.T) {
	cleanMsgIDState(t)
	store := newFakeMsgIDStore()
	SetMessageIDStore(store, nil)
	at := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)

	seen := make(map[string]int, 2*msgIDSeqCap)
	for i := 0; i < 2*msgIDSeqCap; i++ {
		key := fmt.Sprintf("flood:%d", i)
		id := MessageIDAt(key, at)
		if prev, ok := seen[id]; ok {
			t.Fatalf("keys %q and %q share the id %q", fmt.Sprintf("flood:%d", prev), key, id)
		}
		seen[id] = i
	}
	if _, ok := seen["WF-10120001ZZ"]; !ok {
		t.Fatal("the second minute was not filled: the test does not cover the reported case")
	}

	// Restart: the in-memory state is gone, the store keeps both spent
	// minutes.
	msgIDMu.Lock()
	msgIDByKey = make(map[string]string)
	msgIDMinute, msgIDSeq = "", 0
	msgIDMu.Unlock()

	id := MessageIDAt("flood:after-restart", at)
	if prev, ok := seen[id]; ok {
		t.Fatalf("after the restart a new key reused %q (key %q)", id, fmt.Sprintf("flood:%d", prev))
	}
	// It lands in the next bucket that still has room, at its first number.
	if want := "WF-1012000201"; id != want {
		t.Fatalf("id after the restart = %q, want %q", id, want)
	}
}
