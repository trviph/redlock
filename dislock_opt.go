package redlock

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
