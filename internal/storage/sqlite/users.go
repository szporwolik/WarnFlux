package sqlite

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/storage"
)

// userColumns is the canonical user column list for SELECTs.
const userColumns = `id, username, phone, email, discord, is_admin, role, created_at_ms, updated_at_ms`

// passwordIterations is the PBKDF2-HMAC-SHA256 iteration count used for
// directory-user passwords (local, single-tenant scope).
const passwordIterations = 120_000

// passwordSaltBytes is the random per-user salt length.
const passwordSaltBytes = 16

// EnsureAdminUser makes the read-only admin row exist. It never changes an
// existing row (the web auth account stays authoritative).
func (s *Store) EnsureAdminUser(username string) error {
	now := s.now().UnixMilli()
	_, err := s.db.Exec(`
		INSERT INTO users (username, is_admin, created_at_ms, updated_at_ms)
		VALUES (?, 1, ?, ?)
		ON CONFLICT(username) DO NOTHING`,
		username, now, now)
	if err != nil {
		return fmt.Errorf("ensure admin user %q: %w", username, err)
	}
	return nil
}

// ListUsers returns the users on the given 1-based page plus the total
// count. Pages beyond the last valid one are clamped. The admin row is
// always first.
func (s *Store) ListUsers(page, perPage int) ([]storage.User, int, error) {
	if perPage < 1 {
		perPage = 1
	}
	var total int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count users: %w", err)
	}
	pages := (total + perPage - 1) / perPage
	if pages < 1 {
		pages = 1
	}
	if page < 1 {
		page = 1
	}
	if page > pages {
		page = pages
	}
	rows, err := s.db.Query(`
		SELECT `+userColumns+`
		FROM users
		ORDER BY is_admin DESC, username COLLATE NOCASE ASC
		LIMIT ? OFFSET ?`, perPage, (page-1)*perPage)
	if err != nil {
		return nil, 0, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()
	out := make([]storage.User, 0, perPage)
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan user: %w", err)
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate users: %w", err)
	}
	return out, total, nil
}

// GetUser returns one user by ID.
func (s *Store) GetUser(id int64) (storage.User, error) {
	u, err := s.userByID(id)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.User{}, storage.ErrUserNotFound
	}
	if err != nil {
		return storage.User{}, err
	}
	return u, nil
}

// CreateUser inserts a new regular user. A duplicate username (case
// insensitive) reports storage.ErrUsernameTaken.
func (s *Store) CreateUser(username, phone, email, discord, role, password string) (storage.User, error) {
	taken, err := s.usernameTaken(username, 0)
	if err != nil {
		return storage.User{}, err
	}
	if taken {
		return storage.User{}, storage.ErrUsernameTaken
	}
	salt, hash, err := s.passwordFields(password)
	if err != nil {
		return storage.User{}, err
	}
	now := s.now().UnixMilli()
	res, err := s.db.Exec(`
		INSERT INTO users (username, phone, email, discord, is_admin, role, password_salt, password_hash, created_at_ms, updated_at_ms)
		VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?)`,
		username, phone, email, discord, role, salt, hash, now, now)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return storage.User{}, storage.ErrUsernameTaken
		}
		return storage.User{}, fmt.Errorf("insert user %q: %w", username, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return storage.User{}, fmt.Errorf("insert user %q: %w", username, err)
	}
	return s.userByID(id)
}

// UpdateUser replaces the contact fields, role and (when password is
// non-empty) the password of a regular user. The admin row reports
// storage.ErrUserProtected and never changes.
func (s *Store) UpdateUser(id int64, username, phone, email, discord, role, password string) (storage.User, error) {
	taken, err := s.usernameTaken(username, id)
	if err != nil {
		return storage.User{}, err
	}
	if taken {
		return storage.User{}, storage.ErrUsernameTaken
	}
	now := s.now().UnixMilli()

	if password != "" {
		salt, hash, err := s.passwordFields(password)
		if err != nil {
			return storage.User{}, err
		}
		res, err := s.db.Exec(`
			UPDATE users SET username = ?, phone = ?, email = ?, discord = ?, role = ?,
				password_salt = ?, password_hash = ?, updated_at_ms = ?
			WHERE id = ? AND is_admin = 0`,
			username, phone, email, discord, role, salt, hash, now, id)
		if err != nil {
			if strings.Contains(err.Error(), "UNIQUE") {
				return storage.User{}, storage.ErrUsernameTaken
			}
			return storage.User{}, fmt.Errorf("update user %d: %w", id, err)
		}
		return s.afterUpdate(res, id)
	}

	res, err := s.db.Exec(`
		UPDATE users SET username = ?, phone = ?, email = ?, discord = ?, role = ?, updated_at_ms = ?
		WHERE id = ? AND is_admin = 0`,
		username, phone, email, discord, role, now, id)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return storage.User{}, storage.ErrUsernameTaken
		}
		return storage.User{}, fmt.Errorf("update user %d: %w", id, err)
	}
	return s.afterUpdate(res, id)
}

// afterUpdate maps an UPDATE result to the refreshed user or the precise
// not-found / protected error.
func (s *Store) afterUpdate(res sql.Result, id int64) (storage.User, error) {
	n, err := res.RowsAffected()
	if err != nil {
		return storage.User{}, fmt.Errorf("update user %d: %w", id, err)
	}
	if n == 0 {
		return storage.User{}, s.missingOrProtected(id)
	}
	return s.userByID(id)
}

// Authenticate verifies a directory user's credentials. Users without a
// role or without a stored password can never sign in; the admin row
// signs in through the configured auth account only.
func (s *Store) Authenticate(username, password string) (storage.User, error) {
	var id, isAdmin int64
	var role, saltHex, hashHex string
	err := s.db.QueryRow(`
		SELECT id, is_admin, role, password_salt, password_hash
		FROM users WHERE username = ? COLLATE NOCASE`, username).
		Scan(&id, &isAdmin, &role, &saltHex, &hashHex)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.User{}, storage.ErrBadCredentials
	}
	if err != nil {
		return storage.User{}, fmt.Errorf("authenticate %q: %w", username, err)
	}
	if isAdmin != 0 || role == "" || saltHex == "" || hashHex == "" || !verifyPassword(password, saltHex, hashHex) {
		return storage.User{}, storage.ErrBadCredentials
	}
	return s.userByID(id)
}

// passwordFields derives a fresh salt and the password hash for it.
func (s *Store) passwordFields(password string) (salt, hash string, err error) {
	saltBytes := make([]byte, passwordSaltBytes)
	if _, err := rand.Read(saltBytes); err != nil {
		return "", "", fmt.Errorf("generate password salt: %w", err)
	}
	salt = hex.EncodeToString(saltBytes)
	hash, err = hashPassword(password, salt)
	if err != nil {
		return "", "", err
	}
	return salt, hash, nil
}

// hashPassword derives the stored hex PBKDF2-HMAC-SHA256 key.
func hashPassword(password, saltHex string) (string, error) {
	salt, err := hex.DecodeString(saltHex)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(pbkdf2Key([]byte(password), salt, passwordIterations, 32)), nil
}

// verifyPassword compares a candidate password against the stored hash in
// constant time.
func verifyPassword(password, saltHex, wantHex string) bool {
	got, err := hashPassword(password, saltHex)
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(wantHex)) == 1
}

// pbkdf2Key implements PBKDF2 (RFC 8018) with HMAC-SHA256 as the PRF.
func pbkdf2Key(password, salt []byte, iterations, keyLen int) []byte {
	prf := func(p, s []byte) []byte {
		h := hmac.New(sha256.New, p)
		h.Write(s)
		return h.Sum(nil)
	}
	hashLen := sha256.Size
	numBlocks := (keyLen + hashLen - 1) / hashLen
	var block [4]byte
	out := make([]byte, 0, numBlocks*hashLen)
	for i := 1; i <= numBlocks; i++ {
		block[0] = byte(i >> 24)
		block[1] = byte(i >> 16)
		block[2] = byte(i >> 8)
		block[3] = byte(i)
		u := prf(password, append(append([]byte(nil), salt...), block[:]...))
		t := append([]byte(nil), u...)
		for j := 1; j < iterations; j++ {
			u = prf(password, u)
			for k := range t {
				t[k] ^= u[k]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}

// DeleteUser removes a regular user. The admin row reports
// storage.ErrUserProtected and is never deleted.
func (s *Store) DeleteUser(id int64) error {
	res, err := s.db.Exec(`DELETE FROM users WHERE id = ? AND is_admin = 0`, id)
	if err != nil {
		return fmt.Errorf("delete user %d: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete user %d: %w", id, err)
	}
	if n == 0 {
		return s.missingOrProtected(id)
	}
	return nil
}

// missingOrProtected distinguishes a missing row from the protected admin
// row for precise API errors.
func (s *Store) missingOrProtected(id int64) error {
	var isAdmin int
	err := s.db.QueryRow(`SELECT is_admin FROM users WHERE id = ?`, id).Scan(&isAdmin)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrUserNotFound
	}
	if err != nil {
		return fmt.Errorf("inspect user %d: %w", id, err)
	}
	return storage.ErrUserProtected
}

// usernameTaken reports whether another row already uses the username.
func (s *Store) usernameTaken(username string, excludeID int64) (bool, error) {
	var exists int
	err := s.db.QueryRow(`
		SELECT 1 FROM users WHERE username = ? COLLATE NOCASE AND id != ?`,
		username, excludeID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check username %q: %w", username, err)
	}
	return true, nil
}

// userByID loads one user by ID (used after inserts/updates).
func (s *Store) userByID(id int64) (storage.User, error) {
	row := s.db.QueryRow(`SELECT `+userColumns+` FROM users WHERE id = ?`, id)
	u, err := scanUser(row)
	if err != nil {
		return storage.User{}, err
	}
	return u, nil
}

// scanUser reads one row via the given scanner (Row or Rows).
type userScanner interface {
	Scan(dest ...any) error
}

func scanUser(sc userScanner) (storage.User, error) {
	var (
		u      storage.User
		admin  int
		ca, ua int64
	)
	if err := sc.Scan(&u.ID, &u.Username, &u.Phone, &u.Email, &u.Discord, &admin, &u.Role, &ca, &ua); err != nil {
		return storage.User{}, err
	}
	u.IsAdmin = admin != 0
	u.CreatedAt = time.UnixMilli(ca).UTC()
	u.UpdatedAt = time.UnixMilli(ua).UTC()
	return u, nil
}
