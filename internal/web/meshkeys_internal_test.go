package web

import (
	"strings"
	"testing"
)

// TestMeshKeyValidation pins the MeshCore public key parsing/validation:
// both the full 64-hex key and its 12-hex short prefix are accepted.
func TestMeshKeyValidation(t *testing.T) {
	good := "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"
	short := good[:12]

	if keys, msg := validateMeshKeys("0x" + good + " " + good + " " + strings.ToUpper(good)); msg != "" || len(keys) != 1 {
		t.Fatalf("normalize = %v, %q; want one deduped key", keys, msg)
	}
	if keys, msg := validateMeshKeys(good); msg != "" || len(keys) != 1 {
		t.Fatalf("valid key rejected: %v, %q", keys, msg)
	}
	if keys, msg := validateMeshKeys("0x" + short); msg != "" || len(keys) != 1 || keys[0] != short {
		t.Fatalf("short prefix rejected: %v, %q", keys, msg)
	}
	if _, msg := validateMeshKeys(good + " " + short); msg != "" {
		t.Fatalf("mixed short and long keys rejected: %q", msg)
	}
	if _, msg := validateMeshKeys("zzzz"); msg == "" {
		t.Fatal("non-hex key accepted")
	}
	if _, msg := validateMeshKeys("abcd"); msg == "" {
		t.Fatal("4-char key accepted, want 12 or 64 hex")
	}
	if _, msg := validateMeshKeys("zzzzzzzzzzzz"); msg == "" {
		t.Fatal("non-hex 12-char key accepted")
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

// TestMeshOwnerFor pins the directory resolution across both key forms.
func TestMeshOwnerFor(t *testing.T) {
	full := "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"
	owners := map[string]string{full[:12]: "sp9spm"}
	if got := meshOwnerFor(owners, full); got != "sp9spm" {
		t.Fatalf("short-registered owner = %q, want sp9spm", got)
	}
	owners = map[string]string{full: "sp9moa"}
	if got := meshOwnerFor(owners, full); got != "sp9moa" {
		t.Fatalf("full-registered owner = %q, want sp9moa", got)
	}
	if got := meshOwnerFor(owners, full[:12]); got != "sp9moa" {
		t.Fatalf("prefix lookup against full-registered key = %q, want sp9moa", got)
	}
}
