package redlock

import "time"

type DistributedLockOption func(*DistributedLock)

// WithClockDriftFactor sets the clock drift factor for the distributed lock.
// The default value is 0.01 (1%), as recommended by the Redlock paper.
// This factor is used to account for clock drift between Redis instances
// when calculating lock validity time.
func WithClockDriftFactor(factor float64) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.clockDriftFactor = factor
	}
}

// WithClockDriftBuffer sets a fixed buffer duration to subtract from lock validity.
// This accounts for network round-trip variance and other timing uncertainties.
// The default value is 2ms. Combined with the clock drift factor, the validity
// is calculated as: TTL - elapsed - (TTL * driftFactor) - driftBuffer.
func WithClockDriftBuffer(buffer time.Duration) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.clockDriftBuffer = buffer
	}
}

// WithReleaseTimeout sets the timeout for cleanup release operations.
// When a lock acquisition fails, partially acquired locks are released
// using this timeout. Default is 5 seconds.
func WithReleaseTimeout(timeout time.Duration) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.releaseTimeout = timeout
	}
}

// WithDistMaxRetry sets the maximum number of retries for Acquire/AcquireWithFencing/Extend.
// Set to -1 for infinite retries (default).
// These methods retry by calling the corresponding Try* method until success,
// context cancellation, or max retries exceeded.
func WithDistMaxRetry(maxRetry int) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.rc.maxRetry = maxRetry
	}
}

// WithDistMaxJitterDuration sets the maximum jitter duration for retry delays.
// The actual jitter is a random value between 0 and this duration.
// Default is 300ms.
func WithDistMaxJitterDuration(maxJitter time.Duration) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.rc.maxJitterDuration = maxJitter
	}
}

// WithDistMinRetryDelay sets the minimum delay between retries.
// The actual delay is minRetryDelay + random jitter.
// Default is 0.
func WithDistMinRetryDelay(minDelay time.Duration) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.rc.minRetryDelay = minDelay
	}
}
