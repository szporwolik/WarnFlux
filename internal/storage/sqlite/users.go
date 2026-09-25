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

// EnsureAdminUser makes the read-only admin row exist and keeps its
// stored password in sync with the configured web auth account (the YAML
// password is authoritative). Idempotent: when the stored hash already
// verifies the password, nothing changes.
func (s *Store) EnsureAdminUser(username, password string) error {
	now := s.now().UnixMilli()
	if _, err := s.db.Exec(`
		INSERT INTO users (username, is_admin, created_at_ms, updated_at_ms)
		VALUES (?, 1, ?, ?)
		ON CONFLICT(username) DO NOTHING`,
		username, now, now); err != nil {
		return fmt.Errorf("ensure admin user %q: %w", username, err)
	}

	// The configured password is authoritative: sync the stored hash
	// unless it already verifies (no churn on every restart).
	var saltHex, hashHex string
	err := s.db.QueryRow(`SELECT password_salt, password_hash FROM users WHERE username = ? COLLATE NOCASE`, username).
		Scan(&saltHex, &hashHex)
	if err != nil {
		return fmt.Errorf("ensure admin user %q: %w", username, err)
	}
	if password != "" && !verifyPassword(password, saltHex, hashHex) {
		salt, hash, err := s.passwordFields(password)
		if err != nil {
			return fmt.Errorf("ensure admin user %q: %w", username, err)
		}
		if _, err := s.db.Exec(`
			UPDATE users SET password_salt = ?, password_hash = ?, updated_at_ms = ?
			WHERE username = ? COLLATE NOCASE AND is_admin = 1`,
			salt, hash, s.now().UnixMilli(), username); err != nil {
			return fmt.Errorf("ensure admin user %q: %w", username, err)
		}
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
	out, err = s.attachAPRS(out)
	if err != nil {
		return nil, 0, err
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

// GetUserByUsername returns one user by username (case insensitive);
// unknown names report storage.ErrUserNotFound.
func (s *Store) GetUserByUsername(username string) (storage.User, error) {
	var id int64
	err := s.db.QueryRow(`SELECT id FROM users WHERE username = ? COLLATE NOCASE`, username).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.User{}, storage.ErrUserNotFound
	}
	if err != nil {
		return storage.User{}, fmt.Errorf("lookup user %q: %w", username, err)
	}
	return s.userByID(id)
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
	// Every new user is subscribed to all channels by default; users can
	// unsubscribe themselves on the /account page.
	if _, err := s.db.Exec(`
		INSERT INTO user_groups (user_id, group_id)
		SELECT ?, id FROM groups`, id); err != nil {
		return storage.User{}, fmt.Errorf("subscribe new user %q to all groups: %w", username, err)
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

// SetUserAPRS replaces the user's registered APRS callsigns (uppercase,
// de-duplicated). The admin row reports storage.ErrUserProtected.
func (s *Store) SetUserAPRS(userID int64, callsigns []string) error {
	var isAdmin int
	err := s.db.QueryRow(`SELECT is_admin FROM users WHERE id = ?`, userID).Scan(&isAdmin)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrUserNotFound
	}
	if err != nil {
		return fmt.Errorf("inspect user %d: %w", userID, err)
	}
	if isAdmin != 0 {
		return storage.ErrUserProtected
	}
	seen := make(map[string]bool, len(callsigns))
	var clean []string
	for _, c := range callsigns {
		c = strings.ToUpper(strings.TrimSpace(c))
		if c == "" || seen[c] {
			continue
		}
		seen[c] = true
		clean = append(clean, c)
	}
	now := s.now().UnixMilli()
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin aprs update: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM user_aprs WHERE user_id = ?`, userID); err != nil {
		return fmt.Errorf("clear user %d aprs callsigns: %w", userID, err)
	}
	for _, c := range clean {
		if _, err := tx.Exec(`INSERT INTO user_aprs (user_id, callsign, created_at_ms) VALUES (?, ?, ?)`,
			userID, c, now); err != nil {
			return fmt.Errorf("insert user %d aprs callsign %q: %w", userID, c, err)
		}
	}
	if _, err := tx.Exec(`UPDATE users SET updated_at_ms = ? WHERE id = ?`, now, userID); err != nil {
		return fmt.Errorf("touch user %d: %w", userID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit aprs update: %w", err)
	}
	return nil
}

// UserChannelOptOuts returns the delivery channels this user has disabled,
// keyed by channel kind. An empty set means every channel is enabled.
func (s *Store) UserChannelOptOuts(userID int64) (map[string]bool, error) {
	rows, err := s.db.Query(`SELECT channel FROM user_channel_opts WHERE user_id = ?`, userID)
	if err != nil {
		return nil, fmt.Errorf("list user %d channel opt-outs: %w", userID, err)
	}
	defer rows.Close()
	out := make(map[string]bool)
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			return nil, fmt.Errorf("scan user %d channel opt-out: %w", userID, err)
		}
		out[kind] = true
	}
	return out, rows.Err()
}

// SetUserChannelOptOuts replaces the user's delivery-channel opt-outs in
// one transaction: listed kinds are disabled, every other channel stays
// on. Unknown kinds are stored as-is so future channels degrade
// gracefully. The admin row reports storage.ErrUserProtected.
func (s *Store) SetUserChannelOptOuts(userID int64, kinds []string) error {
	var isAdmin int
	err := s.db.QueryRow(`SELECT is_admin FROM users WHERE id = ?`, userID).Scan(&isAdmin)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrUserNotFound
	}
	if err != nil {
		return fmt.Errorf("inspect user %d: %w", userID, err)
	}
	if isAdmin != 0 {
		return storage.ErrUserProtected
	}

	seen := make(map[string]bool, len(kinds))
	var clean []string
	for _, k := range kinds {
		k = strings.ToLower(strings.TrimSpace(k))
		if k == "" || seen[k] {
			continue
		}
		seen[k] = true
		clean = append(clean, k)
	}
	now := s.now().UnixMilli()
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin channel opt-out update: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM user_channel_opts WHERE user_id = ?`, userID); err != nil {
		return fmt.Errorf("clear user %d channel opt-outs: %w", userID, err)
	}
	for _, k := range clean {
		if _, err := tx.Exec(`INSERT INTO user_channel_opts (user_id, channel) VALUES (?, ?)`, userID, k); err != nil {
			return fmt.Errorf("insert user %d channel opt-out %q: %w", userID, k, err)
		}
	}
	if _, err := tx.Exec(`UPDATE users SET updated_at_ms = ? WHERE id = ?`, now, userID); err != nil {
		return fmt.Errorf("touch user %d: %w", userID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit channel opt-out update: %w", err)
	}
	return nil
}

// AllAPRSCallsigns returns the distinct base callsigns (SSID stripped,
// uppercase, de-duplicated) registered for any user, sorted. It backs the
// APRS message-routing sender allow-list.
func (s *Store) AllAPRSCallsigns() ([]string, error) {
	rows, err := s.db.Query(`SELECT DISTINCT callsign FROM user_aprs ORDER BY callsign COLLATE NOCASE ASC`)
	if err != nil {
		return nil, fmt.Errorf("list aprs callsigns: %w", err)
	}
	defer rows.Close()
	seen := make(map[string]bool)
	var out []string
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan aprs callsign: %w", err)
		}
		base := raw
		if i := strings.IndexByte(base, '-'); i >= 0 {
			base = base[:i]
		}
		base = strings.ToUpper(base)
		if base == "" || seen[base] {
			continue
		}
		seen[base] = true
		out = append(out, base)
	}
	return out, rows.Err()
}

// SetUserPassword replaces a regular user's password (fresh salt + PBKDF2
// hash). The admin row reports storage.ErrUserProtected.
func (s *Store) SetUserPassword(userID int64, password string) error {
	var isAdmin int
	err := s.db.QueryRow(`SELECT is_admin FROM users WHERE id = ?`, userID).Scan(&isAdmin)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrUserNotFound
	}
	if err != nil {
		return fmt.Errorf("inspect user %d: %w", userID, err)
	}
	if isAdmin != 0 {
		return storage.ErrUserProtected
	}
	salt, hash, err := s.passwordFields(password)
	if err != nil {
		return err
	}
	res, err := s.db.Exec(`
		UPDATE users SET password_salt = ?, password_hash = ?, updated_at_ms = ?
		WHERE id = ? AND is_admin = 0`,
		salt, hash, s.now().UnixMilli(), userID)
	if err != nil {
		return fmt.Errorf("set user %d password: %w", userID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("set user %d password: %w", userID, err)
	} else if n == 0 {
		return storage.ErrUserNotFound
	}
	return nil
}

// CreatePasswordReset issues a one-time reset token (64 hex chars) for a
// regular user, stores only its SHA-256 hash and returns the plaintext
// once. Expired tokens are pruned opportunistically.
func (s *Store) CreatePasswordReset(userID int64) (string, error) {
	var isAdmin int
	err := s.db.QueryRow(`SELECT is_admin FROM users WHERE id = ?`, userID).Scan(&isAdmin)
	if errors.Is(err, sql.ErrNoRows) {
		return "", storage.ErrUserNotFound
	}
	if err != nil {
		return "", fmt.Errorf("inspect user %d: %w", userID, err)
	}
	if isAdmin != 0 {
		return "", storage.ErrUserProtected
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate reset token: %w", err)
	}
	token := hex.EncodeToString(raw)
	sum := sha256.Sum256([]byte(token))
	now := s.now().UnixMilli()
	expires := now + int64((time.Hour).Milliseconds())

	// Opportunistic prune: old rows for the user + globally expired ones.
	if _, err := s.db.Exec(`DELETE FROM password_resets WHERE user_id = ? OR expires_at_ms < ?`, userID, now); err != nil {
		return "", fmt.Errorf("prune reset tokens: %w", err)
	}
	if _, err := s.db.Exec(`
		INSERT INTO password_resets (user_id, token_hash, expires_at_ms, created_at_ms)
		VALUES (?, ?, ?, ?)`,
		userID, hex.EncodeToString(sum[:]), expires, now); err != nil {
		return "", fmt.Errorf("store reset token: %w", err)
	}
	return token, nil
}

// ConsumePasswordReset validates a plaintext reset token in constant
// time, marks it used and returns the owning user ID. Unknown, expired or
// already-used tokens report storage.ErrPasswordResetInvalid.
func (s *Store) ConsumePasswordReset(token string) (int64, error) {
	now := s.now().UnixMilli()
	var userID, expires, used int64
	var hashHex string
	err := s.db.QueryRow(`
		SELECT user_id, token_hash, expires_at_ms, used_at_ms
		FROM password_resets
		WHERE token_hash = ?`, tokenHashHex(token)).
		Scan(&userID, &hashHex, &expires, &used)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, storage.ErrPasswordResetInvalid
	}
	if err != nil {
		return 0, fmt.Errorf("lookup reset token: %w", err)
	}
	if used != 0 || expires < now {
		return 0, storage.ErrPasswordResetInvalid
	}
	if _, err := s.db.Exec(`UPDATE password_resets SET used_at_ms = ? WHERE token_hash = ?`, now, hashHex); err != nil {
		return 0, fmt.Errorf("consume reset token: %w", err)
	}
	return userID, nil
}

// PeekPasswordReset validates a token without consuming it.
func (s *Store) PeekPasswordReset(token string) error {
	now := s.now().UnixMilli()
	var expires, used int64
	err := s.db.QueryRow(`
		SELECT expires_at_ms, used_at_ms FROM password_resets
		WHERE token_hash = ?`, tokenHashHex(token)).
		Scan(&expires, &used)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrPasswordResetInvalid
	}
	if err != nil {
		return fmt.Errorf("lookup reset token: %w", err)
	}
	if used != 0 || expires < now {
		return storage.ErrPasswordResetInvalid
	}
	return nil
}

// tokenHashHex returns the SHA-256 hex of a plaintext token for the
// constant-shape index lookup.
func tokenHashHex(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// attachAPRS fills APRSCallsigns on the given users with one grouped query
// and returns the updated slice (the input elements are copies).
func (s *Store) attachAPRS(users []storage.User) ([]storage.User, error) {
	if len(users) == 0 {
		return users, nil
	}
	ids := make([]any, 0, len(users))
	byID := make(map[int64]int, len(users))
	for i, u := range users {
		ids = append(ids, u.ID)
		byID[u.ID] = i
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	rows, err := s.db.Query(`SELECT user_id, callsign FROM user_aprs WHERE user_id IN (`+ph+`) ORDER BY callsign COLLATE NOCASE ASC`, ids...)
	if err != nil {
		return nil, fmt.Errorf("list user aprs callsigns: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var userID int64
		var callsign string
		if err := rows.Scan(&userID, &callsign); err != nil {
			return nil, fmt.Errorf("scan user aprs callsign: %w", err)
		}
		if i, ok := byID[userID]; ok {
			users[i].APRSCallsigns = append(users[i].APRSCallsigns, callsign)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return users, nil
}

// userByID loads one user by ID (used after inserts/updates).
func (s *Store) userByID(id int64) (storage.User, error) {
	row := s.db.QueryRow(`SELECT `+userColumns+` FROM users WHERE id = ?`, id)
	u, err := scanUser(row)
	if err != nil {
		return storage.User{}, err
	}
	users, err := s.attachAPRS([]storage.User{u})
	if err != nil {
		return storage.User{}, err
	}
	return users[0], nil
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
