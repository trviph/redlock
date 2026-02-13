package redlock

import "time"

// LockOption configures a Lock instance.
type LockOption func(*Lock)

// Deprecated: Use [WithWaiter] instead.
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

// WithWaiter sets the [Waiter] for the lock.
// Default is [JitterWait] with default configuration [DefaultJitterWait].
func WithWaiter(waiter Waiter) LockOption {
	return func(dl *Lock) {
		dl.waiter = waiter
	}
}
