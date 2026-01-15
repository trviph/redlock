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

// WithReleaseTimeout sets the timeout for cleanup release operations.
// When a lock acquisition fails, partially acquired locks are released
// using this timeout. Default is 5 seconds.
func WithReleaseTimeout(timeout time.Duration) DistributedLockOption {
	return func(dl *DistributedLock) {
		dl.releaseTimeout = timeout
	}
}
