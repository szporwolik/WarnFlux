// Package storage defines the persistence contract for hazard events and
// the durable change journal. The only implementation today is
// internal/storage/sqlite.
package storage

import (
	"errors"
	"time"
)

// User errors surfaced to the web layer.
var (
	// ErrUserNotFound is returned when no user matches the given ID.
	ErrUserNotFound = errors.New("user not found")
	// ErrUserProtected is returned when an operation targets the admin
	// user, which is read-only and can never be deleted or edited.
	ErrUserProtected = errors.New("admin user is read-only")
	// ErrUsernameTaken is returned when a username is already used by
	// another user (case-insensitive).
	ErrUsernameTaken = errors.New("username already exists")
	// ErrBadCredentials is returned by Authenticate when the username
	// does not exist or the password does not match.
	ErrBadCredentials = errors.New("bad credentials")
)

// Group errors surfaced to the web layer.
var (
	// ErrGroupNotFound is returned when no group matches the given ID.
	ErrGroupNotFound = errors.New("group not found")
	// ErrGroupNameTaken is returned when a group name is already used by
	// another group (case-insensitive).
	ErrGroupNameTaken = errors.New("group name already exists")
	// ErrInvalidSeverity is returned when a routing severity threshold is
	// not a canonical severity value.
	ErrInvalidSeverity = errors.New("invalid severity")
	// ErrPasswordResetInvalid is returned when a reset token is unknown,
	// expired or already used.
	ErrPasswordResetInvalid = errors.New("invalid or expired password reset token")
)

// User is one alert-recipient record. The admin user (the web auth
// account) is seeded from configuration and is read-only.
//
// Role is the access tier: "" for a plain recipient, "member" for a
// self-service account (edit own data only), "emcom" for an operator
// that may also compose communications, and "admin" for the full panel.
// operator that may sign in and use the compose module. Only users with
// a non-empty role (and a password set) can sign in; the admin tier
// always uses the configured auth account.
type User struct {
	ID       int64
	Username string
	Phone    string
	Email    string
	Discord  string
	IsAdmin  bool
	Role     string
	// APRSCallsigns are the ham radio callsigns (with optional -SSID)
	// registered for this user, normalized uppercase, sorted. The routing
	// engine hands them to APRS-capable actions so notifications reach
	// the right operators.
	APRSCallsigns []string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Group is one notification recipient group. Members is the number of
// users assigned to it (populated by ListGroups/ListAllGroups/GetGroup;
// zero when not requested).
type Group struct {
	ID        int64
	Name      string
	Members   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ChannelAssignment is one cell of a group's routing matrix: a configured
// action or output instance ID, the hazard event source (input plugin) it
// applies to, and the minimum severity that fires it ("unknown" =
// deliver everything). An empty Source matches every source: it is the
// fallback cell used when no source-specific cell for the action matches.
type ChannelAssignment struct {
	Source      string
	ID          string
	MinSeverity string
}

// GroupRouting is the full notification routing matrix of one group:
// every assigned cell reads "events from Source at severity ≥ threshold
// fire action ID". Output plugins are intentionally absent: they already
// receive every journal change by default, so a per-group output
// assignment would be redundant.
type GroupRouting struct {
	GroupID int64
	Name    string
	Actions []ChannelAssignment
}

// GroupStore persists notification groups and the many-to-many user
// membership.
type GroupStore interface {
	// ListGroups returns the groups on the given 1-based page plus the
	// total count, each with its member count. Pages beyond the last
	// valid one are clamped.
	ListGroups(page, perPage int) ([]Group, int, error)
	// ListAllGroups returns every group with its member count, ordered
	// by name.
	ListAllGroups() ([]Group, error)
	// GetGroup returns one group with its member count.
	GetGroup(id int64) (Group, error)
	// CreateGroup inserts a new group. A duplicate name reports
	// storage.ErrGroupNameTaken.
	CreateGroup(name string) (Group, error)
	// UpdateGroup renames a group. A duplicate name reports
	// storage.ErrGroupNameTaken.
	UpdateGroup(id int64, name string) (Group, error)
	// DeleteGroup removes a group and its membership rows.
	DeleteGroup(id int64) error
	// GroupIDsForUser returns the IDs of the groups the user belongs to.
	GroupIDsForUser(userID int64) ([]int64, error)
	// SetUserGroups replaces the user's group membership with groupIDs.
	SetUserGroups(userID int64, groupIDs []int64) error
	// GroupRouting returns the full routing matrix of one group. A missing
	// group reports storage.ErrGroupNotFound.
	GroupRouting(groupID int64) (GroupRouting, error)
	// SetGroupRouting replaces the group's routing matrix: each assigned
	// cell carries its own source and minimum severity. An invalid
	// severity reports storage.ErrInvalidSeverity; a missing group
	// reports storage.ErrGroupNotFound.
	SetGroupRouting(groupID int64, actions []ChannelAssignment) error
	// ListGroupRoutings returns the routing of every group (the rule
	// engine's authoritative source), ordered by group name.
	ListGroupRoutings() ([]GroupRouting, error)
	// GroupRecipientEmails returns the distinct non-empty email addresses
	// of the group's members, sorted. The rule engine hands them to
	// contact actions (e.g. smtp Bcc).
	GroupRecipientEmails(groupID int64) ([]string, error)
	// GroupRecipientAPRS returns the distinct non-empty APRS callsigns
	// registered for the group's members, sorted. The rule engine hands
	// them to APRS-capable actions.
	GroupRecipientAPRS(groupID int64) ([]string, error)
	// GroupRecipientDiscord returns the distinct non-empty Discord
	// handles of the group's members, sorted. The rule engine hands them
	// to Discord-capable actions.
	GroupRecipientDiscord(groupID int64) ([]string, error)
}

// DirectoryStore combines the user and group administration stores; the
// web UI needs both from one backend.
type DirectoryStore interface {
	UserStore
	GroupStore
}

// UserStore persists alert recipients. Page numbering is 1-based; a page
// beyond the last valid page is clamped by ListUsers.
//
// Role is "", "member" or "emcom". An empty password means the user cannot sign in
// (plain recipient); on UpdateUser an empty password keeps the current
// one.
type UserStore interface {
	// EnsureAdminUser makes the read-only admin row exist and keeps its
	// stored password in sync with the configured auth account (the
	// YAML/secret password is authoritative). Idempotent: a row whose
	// hash already verifies the given password is left untouched.
	EnsureAdminUser(username, password string) error
	// ListUsers returns the users on the given 1-based page plus the
	// total count. The admin row is always first.
	ListUsers(page, perPage int) ([]User, int, error)
	// GetUser returns one user by ID.
	GetUser(id int64) (User, error)
	// GetUserByUsername returns one user by username (case
	// insensitive); unknown names report ErrUserNotFound.
	GetUserByUsername(username string) (User, error)
	// CreateUser inserts a new regular user.
	CreateUser(username, phone, email, discord, role, password string) (User, error)
	// UpdateUser replaces the contact fields, role and (optionally) the
	// password of a regular user.
	UpdateUser(id int64, username, phone, email, discord, role, password string) (User, error)
	// DeleteUser removes a regular user.
	DeleteUser(id int64) error
	// SetUserAPRS replaces the user's registered APRS callsigns (with
	// optional -SSID). The store normalizes (uppercase) and de-duplicates
	// them; a missing or protected user reports the usual errors.
	SetUserAPRS(userID int64, callsigns []string) error
	// AllAPRSCallsigns returns the distinct BASE callsigns (SSID
	// stripped, uppercase) registered for any user, sorted. The APRS
	// message-routing bridge uses it as the sender allow-list.
	AllAPRSCallsigns() ([]string, error)
	// UserChannelOptOuts returns the delivery channels this user has
	// disabled, keyed by channel kind (see internal/notify). An empty
	// set means every channel is enabled — the default.
	UserChannelOptOuts(userID int64) (map[string]bool, error)
	// SetUserChannelOptOuts replaces the user's delivery-channel
	// opt-outs: listed kinds are disabled, every other channel stays on.
	SetUserChannelOptOuts(userID int64, kinds []string) error
	// SetUserPassword replaces a regular user's password. The admin row
	// reports ErrUserProtected (its password lives in configuration).
	SetUserPassword(userID int64, password string) error
	// CreatePasswordReset issues a one-time reset token for the user and
	// returns its plaintext form (the store keeps only a hash). Expired
	// tokens are pruned opportunistically.
	CreatePasswordReset(userID int64) (string, error)
	// ConsumePasswordReset validates a plaintext token (constant-time
	// hash lookup), marks it used and returns the user ID. Expired,
	// unknown or already-used tokens report ErrPasswordResetInvalid.
	ConsumePasswordReset(token string) (int64, error)
	// PeekPasswordReset validates a token WITHOUT consuming it (the
	// reset page checks the link before rendering the form).
	PeekPasswordReset(token string) error
	// Authenticate verifies a directory user's credentials and returns
	// the user. Unknown usernames, users without a password and wrong
	// passwords all report storage.ErrBadCredentials.
	Authenticate(username, password string) (User, error)
}
