package redlock

import (
	"context"
	"time"
)

type WaitInfo struct {
	DoneAt time.Time
	Err    error
}

// Waiter defines the interface for controlling retry behavior and backoff strategies.
// Implementations of this interface determine how long to wait between retry attempts
// and when to stop retrying.
type Waiter interface {
	// Wait returns a channel that will receive a WaitInfo struct after the appropriate retry delay.
	//
	// Parameters:
	//   - ctx: The context to monitor for cancellation. If the context is cancelled while waiting,
	//          Wait must return immediately with a WaitInfo containing the context error.
	//   - times: The current attempt number (0-indexed). 0 indicates the initial attempt.
	//
	// Returns:
	//   A receive-only channel of WaitInfo.
	//   - If the retry limit is exceeded or the operation should stop, the channel will receive
	//     a WaitInfo with an Err (e.g., ErrMaxRetryExceeded).
	//   - If the wait completes successfully, the channel will receive a WaitInfo with DoneAt set
	//     to the current time.
	//   - The channel should be buffered or the sender should not block if the receiver stops listening
	//     (though typically the caller waits on the channel).
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
