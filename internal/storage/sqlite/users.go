package sqlite

import (
	"context"
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
const userColumns = `id, username, phone, email, discord, is_admin, role, lang, created_at_ms, updated_at_ms, auth_version`

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
	if err == nil {
		out, err = s.attachMeshtasticIDs(out)
	}
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

// GetUserByEmail returns the one user carrying that e-mail address (case
// insensitive). An address shared by more than one account is ambiguous and
// reports storage.ErrUserNotFound, exactly like an unknown one: nothing may
// decide which of them a password reset, or a login, refers to.
func (s *Store) GetUserByEmail(email string) (storage.User, error) {
	id, err := s.uniqueUserBy(`WHERE email = ? COLLATE NOCASE AND email <> ''`, email)
	if err != nil {
		if errors.Is(err, storage.ErrBadCredentials) {
			return storage.User{}, storage.ErrUserNotFound
		}
		return storage.User{}, err
	}
	return s.userByID(id)
}

// uniqueUserBy resolves a selector that must name exactly one row: no row and
// several rows both report storage.ErrBadCredentials (an ambiguous identifier
// is no identifier).
func (s *Store) uniqueUserBy(where string, arg any) (int64, error) {
	rows, err := s.db.Query(`SELECT id FROM users `+where, arg)
	if err != nil {
		return 0, fmt.Errorf("lookup user: %w", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, fmt.Errorf("lookup user: %w", err)
		}
		ids = append(ids, id)
		if len(ids) > 1 {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("lookup user: %w", err)
	}
	if len(ids) != 1 {
		return 0, storage.ErrBadCredentials
	}
	return ids[0], nil
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
	// Membership is admin-assigned only: every new user starts with NO
	// groups — an administrator adds them on the Access page.
	return s.userByID(id)
}

// UpdateUser replaces the contact fields, role and (when password is
// non-empty) the password of a regular user. The admin row's identity is
// config-owned (username, role and password never change here), but its
// contact fields (phone, email, discord) still update.
//
// A password change and a change of the recovery address both end the
// outstanding password-reset links of the account, in the same
// transaction as the edit itself. An edit that re-passwords the account or
// changes its role or username also raises its credential version, which
// retires every session opened from an older one.
func (s *Store) UpdateUser(id int64, username, phone, email, discord, role, password string) (storage.User, error) {
	var isAdmin int
	err := s.db.QueryRow(`SELECT is_admin FROM users WHERE id = ?`, id).Scan(&isAdmin)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.User{}, storage.ErrUserNotFound
	}
	if err != nil {
		return storage.User{}, fmt.Errorf("inspect user %d: %w", id, err)
	}
	if isAdmin != 0 {
		// The admin row is config-owned and can never own a reset link
		// (CreatePasswordReset refuses it), so editing its contact fields
		// cannot leave a live token behind.
		res, err := s.db.Exec(`
			UPDATE users SET phone = ?, email = ?, discord = ?, updated_at_ms = ?
			WHERE id = ? AND is_admin = 1`,
			phone, email, discord, s.now().UnixMilli(), id)
		if err != nil {
			return storage.User{}, fmt.Errorf("update admin contact fields: %w", err)
		}
		return s.afterUpdate(res, id)
	}

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
		// The new hash and the invalidation of every outstanding reset
		// link commit together: an edit that re-passwords the account must
		// not leave a token issued before it usable (reported P1).
		ctx := context.Background()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return storage.User{}, fmt.Errorf("begin update user %d: %w", id, err)
		}
		defer tx.Rollback()
		res, err := tx.ExecContext(ctx, `
			UPDATE users SET username = ?, phone = ?, email = ?, discord = ?, role = ?,
				password_salt = ?, password_hash = ?, updated_at_ms = ?,
				auth_version = auth_version + 1
			WHERE id = ? AND is_admin = 0`,
			username, phone, email, discord, role, salt, hash, now, id)
		if err != nil {
			if strings.Contains(err.Error(), "UNIQUE") {
				return storage.User{}, storage.ErrUsernameTaken
			}
			return storage.User{}, fmt.Errorf("update user %d: %w", id, err)
		}
		if err := purgePasswordResetsTx(ctx, tx, id); err != nil {
			return storage.User{}, err
		}
		if err := tx.Commit(); err != nil {
			return storage.User{}, fmt.Errorf("commit update user %d: %w", id, err)
		}
		return s.afterUpdate(res, id)
	}

	// An edit that leaves the password alone still closes the recovery
	// path when it moves the account to another mailbox: a link mailed to
	// the previous address must not set a password on an account that
	// address no longer belongs to. The edit and the purge commit
	// together, exactly like a password change. A new username or role
	// retires the account's sessions through the credential version.
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storage.User{}, fmt.Errorf("begin update user %d: %w", id, err)
	}
	defer tx.Rollback()
	prev, err := previousIdentityTx(ctx, tx, id)
	if err != nil {
		return storage.User{}, err
	}
	addressChanged := !sameAddress(prev.email, email)
	bumpVersion := prev.username != username || prev.role != role
	res, err := tx.ExecContext(ctx, `
		UPDATE users SET username = ?, phone = ?, email = ?, discord = ?, role = ?, updated_at_ms = ?`+versionBump(bumpVersion)+`
		WHERE id = ? AND is_admin = 0`,
		username, phone, email, discord, role, now, id)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return storage.User{}, storage.ErrUsernameTaken
		}
		return storage.User{}, fmt.Errorf("update user %d: %w", id, err)
	}
	if addressChanged {
		if err := purgePasswordResetsTx(ctx, tx, id); err != nil {
			return storage.User{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return storage.User{}, fmt.Errorf("commit update user %d: %w", id, err)
	}
	return s.afterUpdate(res, id)
}

// identityEdit is the account state an edit is about to replace: the recovery
// address decides whether outstanding reset links die, the username and role
// decide whether the account's sessions do.
type identityEdit struct {
	username string
	email    string
	role     string
}

// previousIdentityTx reads the state an edit replaces, inside the caller's
// transaction.
func previousIdentityTx(ctx context.Context, q queryRower, id int64) (identityEdit, error) {
	var prev identityEdit
	err := q.QueryRowContext(ctx, `SELECT username, email, role FROM users WHERE id = ?`, id).
		Scan(&prev.username, &prev.email, &prev.role)
	if errors.Is(err, sql.ErrNoRows) {
		return identityEdit{}, storage.ErrUserNotFound
	}
	if err != nil {
		return identityEdit{}, fmt.Errorf("inspect user %d: %w", id, err)
	}
	return prev, nil
}

// sameAddress compares two recovery addresses the way the store matches them
// elsewhere: case-insensitively and ignoring surrounding blanks.
func sameAddress(a, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}

// versionBump is the SQL fragment that retires the account's sessions, or an
// empty string when the edit is not an identity change.
func versionBump(bump bool) string {
	if !bump {
		return ""
	}
	return ", auth_version = auth_version + 1"
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
	return s.authenticate(`WHERE username = ? COLLATE NOCASE`, username, password)
}

// AuthenticateByEmail signs in the account an operator addressed by its e-mail
// address. The address must identify exactly ONE account: two accounts sharing
// an address (nothing in the schema forbids it) make the identifier ambiguous,
// and an ambiguous identifier must never sign anybody in.
func (s *Store) AuthenticateByEmail(email, password string) (storage.User, error) {
	id, err := s.uniqueUserBy(`WHERE email = ? COLLATE NOCASE AND email <> ''`, email)
	if err != nil {
		return storage.User{}, err
	}
	return s.authenticate(`WHERE id = ?`, id, password)
}

// authenticate verifies the password of the row one selector names. The
// returned user carries the credential version read in the SAME statement as
// the hash it was verified against: the caller binds the session to that
// version, so a password change landing after the verification (but before
// the session exists) leaves the session unusable instead of valid.
func (s *Store) authenticate(where string, arg any, password string) (storage.User, error) {
	var id, isAdmin, authVersion int64
	var role, saltHex, hashHex string
	err := s.db.QueryRow(`
		SELECT id, is_admin, role, password_salt, password_hash, auth_version
		FROM users `+where, arg).
		Scan(&id, &isAdmin, &role, &saltHex, &hashHex, &authVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.User{}, storage.ErrBadCredentials
	}
	if err != nil {
		return storage.User{}, fmt.Errorf("authenticate: %w", err)
	}
	if isAdmin != 0 || role == "" || saltHex == "" || hashHex == "" || !verifyPassword(password, saltHex, hashHex) {
		return storage.User{}, storage.ErrBadCredentials
	}
	u, err := s.userByID(id)
	if err != nil {
		return storage.User{}, err
	}
	// The row is read again for the profile fields; its version may already
	// be newer than the verified one, and the OLDER of the two is the honest
	// answer: this session proved the credentials of that version.
	u.AuthVersion = authVersion
	return u, nil
}

// AuthVersion reports the account's current credential version. Sessions
// carry the version their password was verified against; a mismatch means
// the credentials or the role changed after that verification.
func (s *Store) AuthVersion(userID int64) (int64, error) {
	var v int64
	err := s.db.QueryRow(`SELECT auth_version FROM users WHERE id = ?`, userID).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, storage.ErrUserNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("auth version of user %d: %w", userID, err)
	}
	return v, nil
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
// de-duplicated). The admin row may register its own callsigns like any
// other user.
func (s *Store) SetUserAPRS(userID int64, callsigns []string) error {
	var one int
	err := s.db.QueryRow(`SELECT 1 FROM users WHERE id = ?`, userID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrUserNotFound
	}
	if err != nil {
		return fmt.Errorf("inspect user %d: %w", userID, err)
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

// SetUserMeshtasticIDs replaces the user's registered Meshtastic node ids
// (lowercase 64-hex, de-duplicated). The admin row may register its own
// node ids like any other user.
func (s *Store) SetUserMeshtasticIDs(userID int64, keys []string) error {
	var one int
	err := s.db.QueryRow(`SELECT 1 FROM users WHERE id = ?`, userID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrUserNotFound
	}
	if err != nil {
		return fmt.Errorf("inspect user %d: %w", userID, err)
	}
	seen := make(map[string]bool, len(keys))
	var clean []string
	for _, k := range keys {
		k = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(k), "0x"))
		k = strings.TrimPrefix(k, "!")
		if k == "" || seen[k] {
			continue
		}
		seen[k] = true
		clean = append(clean, k)
	}
	now := s.now().UnixMilli()
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin meshtastic ids update: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM user_meshtastic_ids WHERE user_id = ?`, userID); err != nil {
		return fmt.Errorf("clear user %d meshtastic ids: %w", userID, err)
	}
	for _, k := range clean {
		if _, err := tx.Exec(`INSERT INTO user_meshtastic_ids (user_id, node_id, created_at_ms) VALUES (?, ?, ?)`,
			userID, k, now); err != nil {
			return fmt.Errorf("insert user %d meshtastic id: %w", userID, err)
		}
	}
	if _, err := tx.Exec(`UPDATE users SET updated_at_ms = ? WHERE id = ?`, now, userID); err != nil {
		return fmt.Errorf("touch user %d: %w", userID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit meshtastic ids update: %w", err)
	}
	return nil
}

// MeshtasticOwners returns every registered Meshtastic node id mapped to
// the username that registered it.
func (s *Store) MeshtasticOwners() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT m.node_id, u.username FROM user_meshtastic_ids m
		JOIN users u ON u.id = m.user_id`)
	if err != nil {
		return nil, fmt.Errorf("query meshtastic id owners: %w", err)
	}
	defer rows.Close()
	owners := make(map[string]string)
	for rows.Next() {
		var key, username string
		if err := rows.Scan(&key, &username); err != nil {
			return nil, fmt.Errorf("scan meshtastic id owner: %w", err)
		}
		owners[key] = username
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate meshtastic id owners: %w", err)
	}
	return owners, nil
}

// PhoneOwners returns every registered phone number mapped to the
// username that registered it; the keys are digits-only with the
// international "00" prefix folded onto "+", so formatting differences
// (spaces, + or 00 prefix, dashes) collapse onto one number.
func (s *Store) PhoneOwners() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT phone, username FROM users WHERE phone IS NOT NULL AND phone != ''`)
	if err != nil {
		return nil, fmt.Errorf("query phone owners: %w", err)
	}
	defer rows.Close()
	owners := make(map[string]string)
	for rows.Next() {
		var phone, username string
		if err := rows.Scan(&phone, &username); err != nil {
			return nil, fmt.Errorf("scan phone owner: %w", err)
		}
		key := strings.Map(func(r rune) rune {
			if r >= '0' && r <= '9' {
				return r
			}
			return -1
		}, phone)
		if key == "" {
			continue
		}
		owners[strings.TrimPrefix(key, "00")] = username
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate phone owners: %w", err)
	}
	return owners, nil
}

// DiscordOwners returns every registered Discord user id mapped to the
// username that registered it. The inbound Discord listener uses it as
// the sender allow-list: the gateway reports the author's numeric user
// id, which is what a profile must carry (a mention like "<@123>" is
// accepted, a username is not — a handle can never authorize a sender).
func (s *Store) DiscordOwners() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT discord, username FROM users WHERE discord IS NOT NULL AND discord != ''`)
	if err != nil {
		return nil, fmt.Errorf("query discord owners: %w", err)
	}
	defer rows.Close()
	owners := make(map[string]string)
	for rows.Next() {
		var discord, username string
		if err := rows.Scan(&discord, &username); err != nil {
			return nil, fmt.Errorf("scan discord owner: %w", err)
		}
		key := discordUserID(discord)
		if key == "" {
			continue
		}
		owners[key] = username
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate discord owners: %w", err)
	}
	return owners, nil
}

// discordUserID reduces a profile Discord value to the numeric user id
// the gateway reports; "" means the value cannot identify a sender.
func discordUserID(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "<@")
	v = strings.TrimPrefix(v, "!")
	v = strings.TrimSuffix(v, ">")
	if v == "" {
		return ""
	}
	for _, r := range v {
		if r < '0' || r > '9' {
			return ""
		}
	}
	return v
}

// SetUserLanguage stores the user's preferred notification language
// (an i18n language code; "" = system default). The admin row may set
// its own language like any other user.
func (s *Store) SetUserLanguage(userID int64, lang string) error {
	var one int
	err := s.db.QueryRow(`SELECT 1 FROM users WHERE id = ?`, userID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrUserNotFound
	}
	if err != nil {
		return fmt.Errorf("inspect user %d: %w", userID, err)
	}
	res, err := s.db.Exec(`UPDATE users SET lang = ?, updated_at_ms = ? WHERE id = ?`,
		strings.ToLower(strings.TrimSpace(lang)), s.now().UnixMilli(), userID)
	if err != nil {
		return fmt.Errorf("set user %d language: %w", userID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("set user %d language: %w", userID, err)
	} else if n == 0 {
		return storage.ErrUserNotFound
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
// gracefully. The admin row may set its own opt-outs like any other
// user.
func (s *Store) SetUserChannelOptOuts(userID int64, kinds []string) error {
	var one int
	err := s.db.QueryRow(`SELECT 1 FROM users WHERE id = ?`, userID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrUserNotFound
	}
	if err != nil {
		return fmt.Errorf("inspect user %d: %w", userID, err)
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

// APRSCallsignOwners returns every registered APRS callsign mapped to the
// username that registered it (uppercase, SSID kept as entered). The
// admin APRS message history uses it to label senders and addressees.
func (s *Store) APRSCallsignOwners() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT u.username, a.callsign FROM user_aprs a JOIN users u ON u.id = a.user_id`)
	if err != nil {
		return nil, fmt.Errorf("list aprs owners: %w", err)
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var username, callsign string
		if err := rows.Scan(&username, &callsign); err != nil {
			return nil, fmt.Errorf("scan aprs owner: %w", err)
		}
		out[strings.ToUpper(strings.TrimSpace(callsign))] = username
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
	// The new hash and the invalidation of every outstanding reset link
	// commit together: a changed password must make each token issued
	// before it worthless, or a stolen recovery link stays a working way
	// back into the account (reported P1).
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin password change for user %d: %w", userID, err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `
		UPDATE users SET password_salt = ?, password_hash = ?, updated_at_ms = ?,
			auth_version = auth_version + 1
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
	if err := purgePasswordResetsTx(ctx, tx, userID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit password change for user %d: %w", userID, err)
	}
	return nil
}

// purgePasswordResetsTx deletes every outstanding reset token of one user
// inside the caller's transaction. A password change makes each link
// issued before it worthless — leaving them behind keeps a stolen
// recovery link a working way back into the account.
func purgePasswordResetsTx(ctx context.Context, tx *sql.Tx, userID int64) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM password_resets WHERE user_id = ?`, userID); err != nil {
		return fmt.Errorf("purge reset tokens for user %d: %w", userID, err)
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

// ConsumePasswordReset marks a plaintext reset token used and returns the
// owning user ID. Unknown, expired and already-used tokens report
// storage.ErrPasswordResetInvalid.
func (s *Store) ConsumePasswordReset(token string) (int64, error) {
	return claimResetToken(context.Background(), s.db, token, s.now().UnixMilli())
}

// ResetPasswordWithToken spends one reset token and stores the new
// password in ONE transaction: the token is claimed atomically (only the
// caller that flips it from unused wins), the new hash is written and
// every outstanding reset link of that account is dropped, because a
// password change ends them. Either the account has the new password and
// the link is spent, or nothing happened at all.
//
// The two-step version (validate, then mark, then set the password) let
// parallel submissions of one link all pass the validation and overwrite
// each other's password (reported P2), and a failed password write burned
// a link that changed nothing.
func (s *Store) ResetPasswordWithToken(token, password string) (int64, error) {
	// The reset endpoint is public, so a dead link must not cost a
	// password hash: this check is a cheap filter only, the claim below
	// is what decides — and it decides for exactly one submission.
	if err := s.PeekPasswordReset(token); err != nil {
		return 0, err
	}
	// Hashing stays outside the transaction: it takes tens of
	// milliseconds and must not hold the write lock.
	salt, hash, err := s.passwordFields(password)
	if err != nil {
		return 0, err
	}
	now := s.now().UnixMilli()
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin password reset: %w", err)
	}
	defer tx.Rollback()

	userID, err := claimResetToken(ctx, tx, token, now)
	if err != nil {
		return 0, err
	}
	var isAdmin int
	err = tx.QueryRowContext(ctx, `SELECT is_admin FROM users WHERE id = ?`, userID).Scan(&isAdmin)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, storage.ErrUserNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("inspect user %d: %w", userID, err)
	}
	if isAdmin != 0 {
		return 0, storage.ErrUserProtected
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE users SET password_salt = ?, password_hash = ?, updated_at_ms = ?,
			auth_version = auth_version + 1
		WHERE id = ? AND is_admin = 0`,
		salt, hash, now, userID)
	if err != nil {
		return 0, fmt.Errorf("set user %d password: %w", userID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return 0, fmt.Errorf("set user %d password: %w", userID, err)
	} else if n == 0 {
		return 0, storage.ErrUserNotFound
	}
	if err := purgePasswordResetsTx(ctx, tx, userID); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit password reset for user %d: %w", userID, err)
	}
	return userID, nil
}

// claimResetToken flips one reset token from unused to used and returns
// its owner. The WHERE clause carries the whole decision — an unknown,
// expired or already-spent token matches no row — so the statement is
// atomic: the first caller wins and every later one gets
// storage.ErrPasswordResetInvalid, whatever the interleaving.
func claimResetToken(ctx context.Context, q queryRower, token string, now int64) (int64, error) {
	var userID int64
	err := q.QueryRowContext(ctx, `
		UPDATE password_resets SET used_at_ms = ?
		WHERE token_hash = ? AND used_at_ms = 0 AND expires_at_ms >= ?
		RETURNING user_id`, now, tokenHashHex(token), now).Scan(&userID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, storage.ErrPasswordResetInvalid
	}
	if err != nil {
		return 0, fmt.Errorf("consume reset token: %w", err)
	}
	return userID, nil
}

// queryRower is the read shape shared by *sql.DB and *sql.Tx, so one
// statement helper serves both the standalone and the transactional call.
type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
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

// attachMeshtasticIDs fills MeshtasticIDs on the given users with one grouped query
// and returns the updated slice (the input elements are copies).
func (s *Store) attachMeshtasticIDs(users []storage.User) ([]storage.User, error) {
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
	rows, err := s.db.Query(`SELECT user_id, node_id FROM user_meshtastic_ids WHERE user_id IN (`+ph+`) ORDER BY node_id COLLATE NOCASE ASC`, ids...)
	if err != nil {
		return nil, fmt.Errorf("list user meshtastic ids: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var userID int64
		var nodeID string
		if err := rows.Scan(&userID, &nodeID); err != nil {
			return nil, fmt.Errorf("scan user meshtastic id: %w", err)
		}
		if i, ok := byID[userID]; ok {
			users[i].MeshtasticIDs = append(users[i].MeshtasticIDs, nodeID)
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
	users, err = s.attachMeshtasticIDs(users)
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
	if err := sc.Scan(&u.ID, &u.Username, &u.Phone, &u.Email, &u.Discord, &admin, &u.Role, &u.Lang, &ca, &ua, &u.AuthVersion); err != nil {
		return storage.User{}, err
	}
	u.IsAdmin = admin != 0
	u.CreatedAt = time.UnixMilli(ca).UTC()
	u.UpdatedAt = time.UnixMilli(ua).UTC()
	return u, nil
}
