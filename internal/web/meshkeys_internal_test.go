package web

import (
	"strings"
	"testing"
)

// TestMeshKeyValidation pins the MeshCore public key parsing/validation.
func TestMeshKeyValidation(t *testing.T) {
	good := "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"

	if keys, msg := validateMeshKeys("0x" + good + " " + good + " " + strings.ToUpper(good)); msg != "" || len(keys) != 1 {
		t.Fatalf("normalize = %v, %q; want one deduped key", keys, msg)
	}
	if keys, msg := validateMeshKeys(good); msg != "" || len(keys) != 1 {
		t.Fatalf("valid key rejected: %v, %q", keys, msg)
	}
	if _, msg := validateMeshKeys("zzzz"); msg == "" {
		t.Fatal("non-hex key accepted")
	}
	if _, msg := validateMeshKeys("abcd"); msg == "" {
		t.Fatal("short key accepted")
	}
	// Five distinct valid keys exceed the per-user bound.
	var five []string
	for i := 0; i < 5; i++ {
		key := []byte(strings.Repeat("a", 64))
		key[0] = byte('0' + i)
		five = append(five, string(key))
	}
	if _, msg := validateMeshKeys(strings.Join(five, " ")); msg == "" {
		t.Fatal("five keys accepted, want at most 4")
	}
}
