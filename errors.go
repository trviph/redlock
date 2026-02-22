package redlock

import "errors"

// Sentinel errors returned by lock operations.
var (
	// ErrLockAlreadyHeld is returned when a requested lock is currently owned by another client.
	ErrLockAlreadyHeld = errors.New("lock already held")

	// ErrLockNotHeld is returned when attempting to extend or release an unowned lock.
	ErrLockNotHeld = errors.New("lock not held")

	// ErrMaxRetryExceeded is returned when a lock acquisition fails after exhausting all retry attempts.
	ErrMaxRetryExceeded = errors.New("max retry exceeded")

	// ErrValidityExpired is returned when a distributed lock is acquired, but the validity duration
	// is entirely consumed by clock drift or acquisition latency across instances.
	ErrValidityExpired = errors.New("lock validity expired")
)
