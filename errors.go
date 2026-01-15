package redlock

import "errors"

// Sentinel errors returned by lock operations.
var (
	// ErrLockAlreadyHeld is returned by TryAcquire when the lock is held by another client.
	ErrLockAlreadyHeld = errors.New("lock already held")

	// ErrLockNotHeld is returned by TryExtend when the lock doesn't exist or fencing token doesn't match.
	ErrLockNotHeld = errors.New("lock not held")

	// ErrMaxRetryExceeded is returned when the maximum retry attempts have been exhausted.
	ErrMaxRetryExceeded = errors.New("max retry exceeded")

	// ErrValidityExpired is returned when a lock is acquired but the validity time
	// has expired due to clock drift or slow acquisition across instances.
	ErrValidityExpired = errors.New("lock validity expired")
)
