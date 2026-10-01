package sqlite

import (
	"path/filepath"
	"testing"
)

// hexNodeID returns an 8-char lowercase hex string derived from the seed.
func hexNodeID(seed byte) string {
	out := make([]byte, 8)
	const alphabet = "0123456789abcdef"
	for i := range out {
		out[i] = alphabet[(int(seed)+i)%16]
	}
	return string(out)
}

func TestUserMeshtasticIDs(t *testing.T) {
	s, _, err := Open(filepath.Join(t.TempDir(), "meshtastic_ids.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	u, err := s.CreateUser("ham1", "", "ham1@example.com", "", "", "")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Set with duplicates and the '!' form: stored lowercase, de-duplicated.
	if err := s.SetUserMeshtasticIDs(u.ID, []string{
		hexNodeID(1),
		"!" + hexNodeID(2),
		hexNodeID(1),
	}); err != nil {
		t.Fatalf("set meshtastic ids: %v", err)
	}

	got, err := s.GetUser(u.ID)
	if err != nil {
		t.Fatalf("get user: %v", err)
	}
	if len(got.MeshtasticIDs) != 2 {
		t.Fatalf("meshtastic ids = %v, want 2 (deduplicated)", got.MeshtasticIDs)
	}
	if got.MeshtasticIDs[0] != hexNodeID(1) || got.MeshtasticIDs[1] != hexNodeID(2) {
		t.Fatalf("meshtastic ids = %v, want normalized sorted [%s %s]", got.MeshtasticIDs, hexNodeID(1), hexNodeID(2))
	}

	// Replace clears previous ids.
	if err := s.SetUserMeshtasticIDs(u.ID, []string{hexNodeID(3)}); err != nil {
		t.Fatalf("replace meshtastic ids: %v", err)
	}
	got, err = s.GetUser(u.ID)
	if err != nil {
		t.Fatalf("get user after replace: %v", err)
	}
	if len(got.MeshtasticIDs) != 1 || got.MeshtasticIDs[0] != hexNodeID(3) {
		t.Fatalf("meshtastic ids after replace = %v, want [%s]", got.MeshtasticIDs, hexNodeID(3))
	}

	// Deleting the user cascades the ids away.
	if err := s.DeleteUser(u.ID); err != nil {
		t.Fatalf("delete user: %v", err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM user_meshtastic_ids WHERE user_id = ?`, u.ID).Scan(&n); err != nil {
		t.Fatalf("count orphan ids: %v", err)
	}
	if n != 0 {
		t.Fatalf("orphan meshtastic ids = %d, want 0", n)
	}
}
