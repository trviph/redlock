package redlock

import "time"

// JitterWaitOption defines a function that configures a JitterWait instance.
type JitterWaitOption func(*JitterWait)

// WithJitterMaxIteration sets the maximum number of retry attempts.
// A value of -1 indicates infinite retries.
func WithJitterMaxIteration(maxIteration int) JitterWaitOption {
	return func(jw *JitterWait) {
		jw.maxIteration = maxIteration
	}
}

// WithJitterMinDelay sets the minimum duration to wait before retrying.
func WithJitterMinDelay(minDelay time.Duration) JitterWaitOption {
	return func(jw *JitterWait) {
		jw.minDelay = minDelay
	}
}

// WithMaxJitterDuration sets the maximum random duration added to the delay.
func WithMaxJitterDuration(maxJitterDuration time.Duration) JitterWaitOption {
	return func(jw *JitterWait) {
		jw.maxJitterDuration = maxJitterDuration
	}
}

// ExponentialWaitOption defines a function that configures an ExponentialWait instance.
type ExponentialWaitOption func(*ExponentialWait)

// WithExpMaxIteration sets the maximum number of retry attempts.
func WithExpMaxIteration(maxIteration int) ExponentialWaitOption {
	return func(ew *ExponentialWait) {
		ew.maxIteration = maxIteration
	}
}

// WithExpMinDelay sets the initial delay duration.
func WithExpMinDelay(minDelay time.Duration) ExponentialWaitOption {
	return func(ew *ExponentialWait) {
		ew.minDelay = minDelay
	}
}

// WithExpMaxDelay sets the maximum delay cap.
func WithExpMaxDelay(maxDelay time.Duration) ExponentialWaitOption {
	return func(ew *ExponentialWait) {
		ew.maxDelay = maxDelay
	}
}

// WithExpFactor sets the multiplier for each retry.
func WithExpFactor(factor float64) ExponentialWaitOption {
	return func(ew *ExponentialWait) {
		ew.factor = factor
	}
}
