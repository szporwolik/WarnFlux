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
