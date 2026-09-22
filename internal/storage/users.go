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
)

// User is one alert-recipient record. The admin user (the web auth
// account) is seeded from configuration and is read-only.
type User struct {
	ID        int64
	Username  string
	Phone     string
	Email     string
	Discord   string
	IsAdmin   bool
	CreatedAt time.Time
	UpdatedAt time.Time
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
// action or output instance ID together with the minimum severity that
// fires it ("unknown" = deliver everything).
type ChannelAssignment struct {
	ID          string
	MinSeverity string
}

// GroupRouting is the full notification routing matrix of one group:
// every assigned action and output carries its own severity threshold.
type GroupRouting struct {
	GroupID int64
	Name    string
	Actions []ChannelAssignment
	Outputs []ChannelAssignment
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
	// action/output carries its own minimum severity. An invalid severity
	// reports storage.ErrInvalidSeverity; a missing group reports
	// storage.ErrGroupNotFound.
	SetGroupRouting(groupID int64, actions, outputs []ChannelAssignment) error
	// ListGroupRoutings returns the routing of every group (the rule
	// engine's authoritative source), ordered by group name.
	ListGroupRoutings() ([]GroupRouting, error)
	// GroupRecipientEmails returns the distinct non-empty email addresses
	// of the group's members, sorted. The rule engine hands them to
	// contact actions (e.g. smtp Bcc).
	GroupRecipientEmails(groupID int64) ([]string, error)
}

// DirectoryStore combines the user and group administration stores; the
// web UI needs both from one backend.
type DirectoryStore interface {
	UserStore
	GroupStore
}

// UserStore persists alert recipients. Page numbering is 1-based; a page
// beyond the last valid page is clamped by ListUsers.
type UserStore interface {
	// EnsureAdminUser makes the read-only admin row exist. It is
	// idempotent and never changes an existing row.
	EnsureAdminUser(username string) error
	// ListUsers returns the users on the given 1-based page plus the
	// total count. The admin row is always first.
	ListUsers(page, perPage int) ([]User, int, error)
	// GetUser returns one user by ID.
	GetUser(id int64) (User, error)
	// CreateUser inserts a new regular user.
	CreateUser(username, phone, email, discord string) (User, error)
	// UpdateUser replaces the contact fields of a regular user.
	UpdateUser(id int64, username, phone, email, discord string) (User, error)
	// DeleteUser removes a regular user.
	DeleteUser(id int64) error
}
