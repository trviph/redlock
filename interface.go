package redlock

import (
	"context"
	"time"
)

type WaitInfo struct {
	DoneAt time.Time
	Err    error
}

// Waiter defines the interface for wait in-between retries.
type Waiter interface {
	// Notify returns a channel that will receive after the appropriate retry delay.
	// `times` is the number of times Wait is called (0 for the first call).
	// The method should return a channel that will receive a value or be closed after the wait duration.
	// If the retry limit is exceeded or retries should stop, it returns an error.
	Wait(ctx context.Context, times int) <-chan WaitInfo
}

// Locker defines the common interface implemented by both Lock and DistributedLock.
// This allows code to be written against the interface and work with either implementation.
type Locker interface {
	// Acquire acquires a lock with a randomly generated fencing token.
	// It retries according to the lock's configuration until success, context cancellation,
	// or max retries exceeded.
	// Returns the fencing token on success, which must be passed to Release.
	Acquire(ctx context.Context, key string, ttl time.Duration) (fencing string, err error)

	// TryAcquire attempts to acquire a lock exactly once without retrying.
	// Returns the fencing token on success, or ErrLockAlreadyHeld if the lock is held.
	TryAcquire(ctx context.Context, key string, ttl time.Duration) (fencing string, err error)

	// AcquireWithFencing acquires a lock with a provided fencing token.
	// It retries according to the lock's configuration until success, context cancellation,
	// or max retries exceeded.
	AcquireWithFencing(ctx context.Context, key, fencing string, ttl time.Duration) error

	// TryAcquireWithFencing attempts to acquire a lock exactly once with a provided fencing token.
	// Returns nil on success, or ErrLockAlreadyHeld if the lock is held.
	TryAcquireWithFencing(ctx context.Context, key, fencing string, ttl time.Duration) error

	// AcquireOrExtend acquires a lock or extends it if the fencing token matches.
	// It retries according to the lock's configuration until success, context cancellation,
	// or max retries exceeded.
	AcquireOrExtend(ctx context.Context, key, fencing string, ttl time.Duration) error

	// Extend extends the TTL of an existing lock if the fencing token matches.
	// Unlike AcquireOrExtend, this will not attempt to acquire if the lock doesn't exist.
	// It retries according to the lock's configuration.
	Extend(ctx context.Context, key, fencing string, ttl time.Duration) error

	// TryExtend attempts to extend the TTL exactly once without retrying.
	// Returns nil on success, ErrLockNotHeld if the lock doesn't exist or fencing doesn't match.
	TryExtend(ctx context.Context, key, fencing string, ttl time.Duration) error

	// Release releases a lock if the fencing token matches.
	// This operation is atomic and will only release if the caller owns the lock.
	Release(ctx context.Context, key, fencing string) error
}

// Compile-time interface satisfaction checks.
var (
	_ Locker = (*Lock)(nil)
	_ Locker = (*DistributedLock)(nil)
	_ Waiter = (*JitterWait)(nil)
)
