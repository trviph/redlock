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
