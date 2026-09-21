package core

import "testing"

// FuzzNormalizeValidate checks that normalization and validation never
// panic and leave the event in a consistent state.
func FuzzNormalizeValidate(f *testing.F) {
	f.Add("src", "id", "cat", "evt", "sev", "head", "desc", "instr", "url")
	f.Add("", "", "", "", "", "", "", "", "")
	f.Add(" UPPER SRC ", "  x  ", "c", "e", "s", "h", "d", "i", "u")
	f.Fuzz(func(t *testing.T, source, sourceID, category, event, severity, headline, description, instruction, sourceURL string) {
		e := HazardEvent{
			Source:      source,
			SourceID:    sourceID,
			Category:    category,
			Event:       event,
			Severity:    severity,
			Headline:    headline,
			Description: description,
			Instruction: instruction,
			SourceURL:   sourceURL,
		}
		e.Normalize()
		// Normalize must be idempotent.
		e2 := e.Clone()
		e2.Normalize()
		if e2.Key() != e.Key() {
			t.Fatalf("normalize not idempotent for key: %q vs %q", e.Key(), e2.Key())
		}
		_ = e.Validate() // result may be an error; must not panic
	})
}

// FuzzEventKey checks that key construction never panics and is stable.
func FuzzEventKey(f *testing.F) {
	f.Add("meteoalarm", "2.49.0.1.616.0.DEU")
	f.Add("", "")
	f.Add("a", "b")
	f.Fuzz(func(t *testing.T, source, sourceID string) {
		k1 := EventKey(source, sourceID)
		k2 := EventKey(source, sourceID)
		if k1 != k2 {
			t.Fatalf("EventKey not deterministic: %q vs %q", k1, k2)
		}
	})
}

// FuzzFingerprint checks that fingerprinting never panics, is
// deterministic, and ignores lifecycle metadata (status).
func FuzzFingerprint(f *testing.F) {
	f.Add("src", "id", "cat", "evt", "sev", "head", "desc", "instr", "url")
	f.Fuzz(func(t *testing.T, source, sourceID, category, event, severity, headline, description, instruction, sourceURL string) {
		e := HazardEvent{
			Source:      source,
			SourceID:    sourceID,
			Category:    category,
			Event:       event,
			Severity:    severity,
			Headline:    headline,
			Description: description,
			Instruction: instruction,
			SourceURL:   sourceURL,
			Status:      StatusActive,
		}
		fp1 := Fingerprint(e)
		fp2 := Fingerprint(e)
		if fp1 != fp2 {
			t.Fatalf("Fingerprint not deterministic: %q vs %q", fp1, fp2)
		}
		cancelled := e
		cancelled.Status = StatusCancelled
		if Fingerprint(cancelled) != fp1 {
			t.Fatalf("fingerprint must ignore lifecycle status")
		}
	})
}
