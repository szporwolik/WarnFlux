package web

import (
	"strings"
	"testing"
)

// TestMeshIDValidation pins the Meshtastic node id parsing/validation:
// 8-hex ids are accepted (with or without the '!' / 0x prefix), anything
// else is rejected.
func TestMeshIDValidation(t *testing.T) {
	good := "abcd1234"

	if keys, msg := validateMeshtasticIDs("0x" + good + " " + good + " " + strings.ToUpper(good)); msg != "" || len(keys) != 1 {
		t.Fatalf("normalize = %v, %q; want one deduped id", keys, msg)
	}
	if keys, msg := validateMeshtasticIDs(good); msg != "" || len(keys) != 1 {
		t.Fatalf("valid id rejected: %v, %q", keys, msg)
	}
	if keys, msg := validateMeshtasticIDs("!" + good); msg != "" || len(keys) != 1 || keys[0] != good {
		t.Fatalf("bang-prefixed id rejected: %v, %q", keys, msg)
	}
	if _, msg := validateMeshtasticIDs("zzzz"); msg == "" {
		t.Fatal("non-hex id accepted")
	}
	if _, msg := validateMeshtasticIDs("abcd"); msg == "" {
		t.Fatal("4-char id accepted, want 8 hex")
	}
	if _, msg := validateMeshtasticIDs("zzzzzzzz"); msg == "" {
		t.Fatal("non-hex 8-char id accepted")
	}
	// Five distinct valid ids exceed the per-user bound.
	var five []string
	for i := 0; i < 5; i++ {
		id := []byte(strings.Repeat("a", 8))
		id[0] = byte('0' + i)
		five = append(five, string(id))
	}
	if _, msg := validateMeshtasticIDs(strings.Join(five, " ")); msg == "" {
		t.Fatal("five ids accepted, want at most 4")
	}
}

// TestMeshOwnerFor pins the directory resolution for node ids.
func TestMeshOwnerFor(t *testing.T) {
	id := "abcd1234"
	owners := map[string]string{id: "sp9spm"}
	if got := meshtasticOwnerFor(owners, "!"+id); got != "sp9spm" {
		t.Fatalf("bang-form owner lookup = %q, want sp9spm", got)
	}
	owners = map[string]string{"!" + id: "sp9moa"}
	if got := meshtasticOwnerFor(owners, id); got != "sp9moa" {
		t.Fatalf("registered-bang owner lookup = %q, want sp9moa", got)
	}
	if got := meshtasticOwnerFor(owners, "0x"+strings.ToUpper(id)); got != "sp9moa" {
		t.Fatalf("0x-prefixed owner lookup = %q, want sp9moa", got)
	}
	if got := meshtasticOwnerFor(owners, "00000000"); got != "" {
		t.Fatalf("unknown id owner = %q, want empty", got)
	}
}
