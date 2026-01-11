package redlock

import "time"

type LockOption func(*Lock)

// Set a max jitter duration for the lock.
// If the lock failed to acquire (due to network error, or the lock already taken)
// the lock will be retried with a random jitter duration up to the max jitter duration.
// Default is 300 milliseconds.
func WithJitterDuration(jitter time.Duration) LockOption {
	return func(dl *Lock) {
		dl.maxJitterDuration = jitter
	}
}

// Set a max retry count for the lock.
// If the lock failed to acquire (due to network error, or the lock already taken)
// the lock will be retried up to the max retry count.
// Set to negative value to retry forever, this is the default behavior.
func WithMaxRetry(maxRetry int) LockOption {
	return func(dl *Lock) {
		dl.maxRetry = maxRetry
	}
}

// Set a min retry delay for the lock.
// This is the minimum time to wait before retrying to acquire the lock.
// Default is 0.
func WithMinRetryDelay(minDelay time.Duration) LockOption {
	return func(dl *Lock) {
		dl.minRetryDelay = minDelay
	}
}
