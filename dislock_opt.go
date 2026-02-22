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

// WithDistWaiter sets the [Waiter] for the distributed lock.
// Default is [JitterWait] with default configuration [DefaultJitterWait].
func WithDistWaiter(waiter Waiter) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.waiter = waiter
	}
}
