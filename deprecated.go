package redlock

import (
	"context"
	"time"
)

// Deprecated: Use [WithErrorCallbacks] instead.
// This function was renamed to clarify that it only handles error callbacks.
// For successful lock extensions, use [WithExtensionCallbacks].
//
// WithCallbacks overrides the context for error callbacks and appends new callbacks.
func WithCallbacks(cbCtx context.Context, callbacks ...WatchDogCallback) WatchDogOption {
	return WithErrorCallbacks(cbCtx, callbacks...)
}

// Deprecated: Use [WithWaiter] instead.
// The Waiter interface provides a more robust retry configuration.
//
// WithJitterDuration sets the maximum jitter duration for retry backoff.
// When a lock acquisition fails (due to the lock being held or network errors),
// the retry delay will include a random jitter between 0 and this duration.
// Default is 300 milliseconds.
func WithJitterDuration(jitter time.Duration) LockOption {
	return func(dl *Lock) {
		if rp, ok := dl.waiter.(*JitterWait); ok {
			rp.maxJitterDuration = jitter
		}
	}
}

// Deprecated: Use [WithWaiter] instead.
// The Waiter interface provides a more robust retry configuration.
//
// WithMaxRetry sets the maximum number of retry attempts for lock acquisition.
// Set to a negative value (default) to retry indefinitely until context cancellation.
// Set to 0 to disable retries (equivalent to TryAcquire behavior).
func WithMaxRetry(maxRetry int) LockOption {
	return func(dl *Lock) {
		if rp, ok := dl.waiter.(*JitterWait); ok {
			rp.maxIteration = maxRetry
		}
	}
}

// Deprecated: Use [WithWaiter] instead.
// The Waiter interface provides a more robust retry configuration.
//
// WithMinRetryDelay sets the minimum delay between retry attempts.
// The actual delay will be this value plus a random jitter (see [WithJitterDuration]).
// Default is 0 (only jitter delay).
func WithMinRetryDelay(minDelay time.Duration) LockOption {
	return func(dl *Lock) {
		if rp, ok := dl.waiter.(*JitterWait); ok {
			rp.minDelay = minDelay
		}
	}
}

// Deprecated: Use [WithDistWaiter] instead.
// The Waiter interface provides a more robust retry configuration.
//
// WithDistMaxRetry sets the maximum number of retries for Acquire/AcquireWithFencing/Extend.
// Set to -1 for infinite retries (default).
// These methods retry by calling the corresponding Try* method until success,
// context cancellation, or max retries exceeded.
func WithDistMaxRetry(maxRetry int) DistributedLockOption {
	return func(dl *DistributedLock) {
		if rp, ok := dl.waiter.(*JitterWait); ok {
			rp.maxIteration = maxRetry
		}
	}
}

// Deprecated: Use [WithDistWaiter] instead.
// The Waiter interface provides a more robust retry configuration.
//
// WithDistMaxJitterDuration sets the maximum jitter duration for retry delays.
// The actual jitter is a random value between 0 and this duration.
// Default is 300ms.
func WithDistMaxJitterDuration(maxJitter time.Duration) DistributedLockOption {
	return func(dl *DistributedLock) {
		if rp, ok := dl.waiter.(*JitterWait); ok {
			rp.maxJitterDuration = maxJitter
		}
	}
}

// Deprecated: Use [WithDistWaiter] instead.
// The Waiter interface provides a more robust retry configuration.
//
// WithDistMinRetryDelay sets the minimum delay between retries.
// The actual delay is minRetryDelay + random jitter.
// Default is 0.
func WithDistMinRetryDelay(minDelay time.Duration) DistributedLockOption {
	return func(dl *DistributedLock) {
		if rp, ok := dl.waiter.(*JitterWait); ok {
			rp.minDelay = minDelay
		}
	}
}
