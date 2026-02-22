package redlock

import (
	"context"
	"time"
)

type WaitInfo struct {
	DoneAt time.Time
	Err    error
}

// Waiter controls the retry behavior and backoff strategies for lock acquisitions.
// Implementations dictate the delay duration between retry attempts and the termination condition.
type Waiter interface {
	// Wait returns a channel that receives a WaitInfo struct after the appropriate retry delay.
	//
	// The ctx parameter monitors for cancellation. If cancelled, Wait returns immediately
	// with a WaitInfo containing the context error.
	// The times parameter represents the current attempt number (0-indexed). 0 indicates
	// the initial attempt and should generally return immediately without delay.
	//
	// The returned channel should be buffered (e.g., make(chan WaitInfo, 1)) to guarantee
	// the sender does not block indefinitely if the receiver stops listening.
	Wait(ctx context.Context, times int) <-chan WaitInfo
}

// Locker defines the common interface implemented by both [Lock] and [DistributedLock].
type Locker interface {
	// Acquire generates a random fencing token, then attempts to acquire the lock.
	// It retries according to the underlying Waiter configuration until success, context cancellation,
	// or the retry limit is reached.
	Acquire(ctx context.Context, key string, ttl time.Duration) (fencing string, err error)

	// TryAcquire attempts to acquire the lock exactly once without retrying.
	// It returns ErrLockAlreadyHeld if the lock is currently held by another client.
	TryAcquire(ctx context.Context, key string, ttl time.Duration) (fencing string, err error)

	// AcquireWithFencing attempts to acquire the lock using the provided fencing token.
	// It retries according to the underlying Waiter configuration.
	AcquireWithFencing(ctx context.Context, key, fencing string, ttl time.Duration) error

	// TryAcquireWithFencing attempts to acquire the lock exactly once using the provided fencing token.
	TryAcquireWithFencing(ctx context.Context, key, fencing string, ttl time.Duration) error

	// AcquireOrExtend extends the lock if the provided fencing token matches the current owner.
	// If the lock does not exist, it behaves identically to AcquireWithFencing.
	AcquireOrExtend(ctx context.Context, key, fencing string, ttl time.Duration) error

	// Extend prolongs the TTL of an existing lock if the fencing token matches.
	// Unlike AcquireOrExtend, this will not acquire the lock if it is currently unowned.
	Extend(ctx context.Context, key, fencing string, ttl time.Duration) error

	// TryExtend attempts to extend the TTL exactly once without retrying.
	// It returns ErrLockNotHeld if the lock does not exist or the fencing token does not match.
	TryExtend(ctx context.Context, key, fencing string, ttl time.Duration) error

	// Release atomically deletes the lock if the caller's fencing token matches the current owner.
	// For DistributedLock, returning an error implies the release failed on at least one instance,
	// which may require manual inspection via the joined error.
	Release(ctx context.Context, key, fencing string) error
}

// Compile-time interface satisfaction checks.
var (
	_ Locker = (*Lock)(nil)
	_ Locker = (*DistributedLock)(nil)
	_ Waiter = (*JitterWait)(nil)
	_ Waiter = (*ExponentialWait)(nil)
)
