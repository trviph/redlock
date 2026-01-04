package redlock

import "time"

type configurable interface {
	setJitterDuration(jitter time.Duration)
	setMinRetryDelay(minDelay time.Duration)
	setMaxRetry(maxRetry int)
}

// Set a max jitter duration for the lock.
// If the lock failed to acquire (due to network error, or the lock already taken)
// the lock will be retried with a random jitter duration up to the max jitter duration.
// Default is 300 milliseconds.
func SetJitterDuration(jitter time.Duration) func(configurable) {
	return func(dl configurable) {
		dl.setJitterDuration(jitter)
	}
}

// Set a max retry count for the lock.
// If the lock failed to acquire (due to network error, or the lock already taken)
// the lock will be retried up to the max retry count.
// Set to negative value to retry forever, this is the default behavior.
func SetMaxRetry(maxRetry int) func(configurable) {
	return func(dl configurable) {
		dl.setMaxRetry(maxRetry)
	}
}

// Set a min retry delay for the lock.
// This is the minimum time to wait before retrying to acquire the lock.
// Default is 0.
func SetMinRetryDelay(minDelay time.Duration) func(configurable) {
	return func(dl configurable) {
		dl.setMinRetryDelay(minDelay)
	}
}
