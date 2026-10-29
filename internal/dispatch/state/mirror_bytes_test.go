package state

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// payloadEntry builds an info entry whose raw payload is exactly n bytes.
func payloadEntry(n int) InfoEntry {
	payload := make([]byte, n)
	for i := range payload {
		payload[i] = 'x'
	}
	return InfoEntry{
		Source:  "adsb",
		Key:     "area",
		Kind:    "aircraft",
		Payload: json.RawMessage(`"` + string(payload) + `"`),
	}
}

// TestInfoMirrorIsBoundedByBytes is the regression test for a memory bound that
// only counted ENTRIES: ten thousand info entries of up to a megabyte each is
// ~9.8 GiB of payload, more than a small station has in total. The byte budget
// has to stop the mirror long before the count does.
func TestInfoMirrorIsBoundedByBytes(t *testing.T) {
	s := New()
	const chunk = 1 << 20 // one megabyte, the receiver's frame limit
	accepted := 0
	for i := 0; i < MaxInfoEntries && accepted <= MaxInfoBytes/chunk+2; i++ {
		err := s.AddOrUpdateInfo("local", fmt.Sprintf("warnflux/info/adsb/area-%d", i), payloadEntry(chunk))
		if err != nil {
			break
		}
		accepted++
	}
	if accepted > MaxInfoBytes/chunk {
		t.Fatalf("accepted %d megabyte entries (about %d MiB), the byte budget is %d bytes",
			accepted, accepted, MaxInfoBytes)
	}
	c := s.MirrorCounters()
	if c.InfoBytes > MaxInfoBytes {
		t.Errorf("info bytes = %d, want at most %d", c.InfoBytes, MaxInfoBytes)
	}
	if c.DroppedInfoBytes == 0 {
		t.Error("the byte-budget rejection is not counted")
	}
	if c.InfoBytes+c.ActiveBytes > MaxMirrorBytes {
		t.Errorf("mirror holds %d bytes, want at most %d", c.InfoBytes+c.ActiveBytes, MaxMirrorBytes)
	}
}

// TestReplacementLargerThanBudgetIsRejected pins the accounting subtlety: an
// update that REPLACES an existing entry must release the old size first, and
// a replacement that would grow past the budget is refused rather than charged
// silently.
func TestReplacementLargerThanBudgetIsRejected(t *testing.T) {
	s := New()
	const big = MaxInfoBytes - infoEntryOverhead - 64
	key := "warnflux/info/adsb/area"
	if err := s.AddOrUpdateInfo("local", key, payloadEntry(big)); err != nil {
		t.Fatalf("first entry: %v", err)
	}
	held := s.MirrorCounters().InfoBytes
	if held <= 0 {
		t.Fatal("an accepted entry was not accounted")
	}

	// Growing past the budget must fail and leave the old entry in place.
	if err := s.AddOrUpdateInfo("local", key, payloadEntry(MaxInfoBytes)); err == nil {
		t.Fatal("a replacement larger than the budget was accepted")
	}
	if got := s.MirrorCounters().InfoBytes; got != held {
		t.Errorf("bytes after a rejected replacement = %d, want %d (the old entry stays)", got, held)
	}
	if s.MirrorCounters().DroppedInfoBytes == 0 {
		t.Error("the rejected replacement is not counted")
	}

	// A same-size replacement is fine: the old size is released first.
	if err := s.AddOrUpdateInfo("local", key, payloadEntry(big)); err != nil {
		t.Fatalf("same-size replacement: %v", err)
	}
	if got := s.MirrorCounters().BytesReplaced; got < 2 {
		t.Errorf("replacements counted = %d, want the accounting to notice them", got)
	}
}

// TestActiveMirrorIsBoundedByBytes covers the other kind: a flood of hazards
// with long descriptions must hit its own budget.
func TestActiveMirrorIsBoundedByBytes(t *testing.T) {
	s := New()
	big := Hazard{Description: strings.Repeat("d", 64<<10)}
	accepted := 0
	for i := 0; i < 1000; i++ {
		if err := s.AddOrUpdateActive("local", fmt.Sprintf("warnflux/active/test/%d", i), big); err != nil {
			break
		}
		accepted++
	}
	if accepted > MaxActiveBytes/(64<<10)+2 {
		t.Fatalf("accepted %d entries of 64 KiB, the active budget is %d bytes", accepted, MaxActiveBytes)
	}
	c := s.MirrorCounters()
	if c.ActiveBytes > MaxActiveBytes {
		t.Errorf("active bytes = %d, want at most %d", c.ActiveBytes, MaxActiveBytes)
	}
	if c.DroppedActiveBytes == 0 {
		t.Error("the active byte-budget rejection is not counted")
	}
}

// TestDeleteReleasesBytes keeps the accounting honest: a deleted entry must
// give its bytes back, or the budget would drain over a day of deletions.
func TestDeleteReleasesBytes(t *testing.T) {
	s := New()
	topic := "warnflux/active/test/one"
	if err := s.AddOrUpdateActive("local", topic, Hazard{Headline: strings.Repeat("h", 1024)}); err != nil {
		t.Fatal(err)
	}
	held := s.MirrorCounters().ActiveBytes
	entry := int64(activeEntryOverhead + 1024 + len("local") + len(topic))
	if held != entry {
		t.Fatalf("accounted bytes = %d, want %d", held, entry)
	}
	s.DeleteActive("local", topic)
	if got := s.MirrorCounters().ActiveBytes; got != 0 {
		t.Errorf("active bytes after delete = %d, want 0 (the entry's own size released)", got)
	}
}
