package redlock

import "time"

// Set a max jitter duration for the lock.
// If the lock failed to acquire (due to network error, or the lock already taken)
// the lock will be retried with a random jitter duration up to the max jitter duration.
// Default is 300 milliseconds.
func SetJitterDuration(jitter time.Duration) func(*DistributedLock) {
	return func(dl *DistributedLock) {
		dl.maxJitterDuration = jitter
	}
}

// Set a max retry count for the lock.
// If the lock failed to acquire (due to network error, or the lock already taken)
// the lock will be retried up to the max retry count.
// Set to negative value to retry forever, this is the default behavior.
func SetMaxRetry(maxRetry int) func(*DistributedLock) {
	return func(dl *DistributedLock) {
		dl.maxRetry = maxRetry
	}
}
