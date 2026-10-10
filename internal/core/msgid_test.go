package core

import (
	"context"
	"errors"
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
