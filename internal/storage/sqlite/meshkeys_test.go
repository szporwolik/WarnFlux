package sqlite

import (
	"path/filepath"
	"testing"
)

// hexKey returns a 64-char lowercase hex string derived from the seed.
func hexKey(seed byte) string {
	out := make([]byte, 64)
	const alphabet = "0123456789abcdef"
	for i := range out {
		out[i] = alphabet[(int(seed)+i)%16]
	}
	return string(out)
}

func TestUserMeshKeys(t *testing.T) {
	s, _, err := Open(filepath.Join(t.TempDir(), "meshkeys.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	u, err := s.CreateUser("ham1", "", "ham1@example.com", "", "", "")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Set with duplicates and mixed case: stored lowercase, de-duplicated.
	if err := s.SetUserMeshKeys(u.ID, []string{
		hexKey(1),
		"0x" + hexKey(2),
		hexKey(1),
	}); err != nil {
		t.Fatalf("set mesh keys: %v", err)
	}

	got, err := s.GetUser(u.ID)
	if err != nil {
		t.Fatalf("get user: %v", err)
	}
	if len(got.MeshKeys) != 2 {
		t.Fatalf("mesh keys = %v, want 2 (deduplicated)", got.MeshKeys)
	}
	if got.MeshKeys[0] != hexKey(1) || got.MeshKeys[1] != hexKey(2) {
		t.Fatalf("mesh keys = %v, want normalized sorted [%s %s]", got.MeshKeys, hexKey(1), hexKey(2))
	}

	// Replace clears previous keys.
	if err := s.SetUserMeshKeys(u.ID, []string{hexKey(3)}); err != nil {
		t.Fatalf("replace mesh keys: %v", err)
	}
	got, err = s.GetUser(u.ID)
	if err != nil {
		t.Fatalf("get user after replace: %v", err)
	}
	if len(got.MeshKeys) != 1 || got.MeshKeys[0] != hexKey(3) {
		t.Fatalf("mesh keys after replace = %v, want [%s]", got.MeshKeys, hexKey(3))
	}

	// Deleting the user cascades the keys away.
	if err := s.DeleteUser(u.ID); err != nil {
		t.Fatalf("delete user: %v", err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM user_meshkeys WHERE user_id = ?`, u.ID).Scan(&n); err != nil {
		t.Fatalf("count orphan keys: %v", err)
	}
	if n != 0 {
		t.Fatalf("orphan mesh keys = %d, want 0", n)
	}
}
