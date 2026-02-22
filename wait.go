package redlock

import (
	"context"
	"crypto/rand"
	"math"
	"math/big"
	"time"
)

// JitterWait implements a retry strategy using a constant base delay plus random jitter.
// The actual delay for each retry is calculated as:
//
//	Delay = MinDelay + random(0, MaxJitterDuration)
//
// This strategy helps prevent thundering herd problems by spreading out concurrent
// retry attempts across multiple clients.
type JitterWait struct {
	// MaxIteration specifies the maximum number of retry attempts.
	// A value of -1 indicates infinite retries.
	// If times > MaxIteration, Wait returns ErrMaxRetryExceeded.
	maxIteration int

	// MinDelay is the minimum duration to wait before retrying.
	minDelay time.Duration

	// MaxJitterDuration is the maximum random duration added to MinDelay.
	// This helps prevent thundering herd problems by spreading out retry attempts.
	maxJitterDuration time.Duration
}

// NewJitterWait creates a new JitterWait with the given options.
// It initializes the waiter with DefaultJitterWait() and then applies
// the provided options.
func NewJitterWait(opts ...JitterWaitOption) *JitterWait {
	jw := DefaultJitterWait()
	for _, opt := range opts {
		opt(jw)
	}
	return jw
}

// DefaultJitterWait returns the default retry configuration:
// infinite retries (MaxIteration: -1), 0 MinDelay, and 300ms MaxJitterDuration.
func DefaultJitterWait() *JitterWait {
	return &JitterWait{
		maxIteration:      -1,
		minDelay:          0,
		maxJitterDuration: 300 * time.Millisecond,
	}
}

// NextDelay calculates the duration for the upcoming retry attempt.
// The retries parameter indicates the number of attempts made so far (e.g., 1 for the first retry).
func (jr *JitterWait) NextDelay(retries int) time.Duration {
	var jitter time.Duration
	if jr.maxJitterDuration > 0 {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(jr.maxJitterDuration)))
		if err != nil {
			// If random generation fails, we default to the max jitter duration
			// This ensures we still wait some time and don't break the loop.
			jitter = jr.maxJitterDuration
		} else {
			jitter = time.Duration(n.Int64())
		}
	}
	return jr.minDelay + jitter
}

// Wait implements the [Waiter] interface using a jittered delay strategy.
//
// Behavior:
//   - Initial attempt (times == 0): Returns immediately.
//   - Retry limit exceeded (MaxIteration >= 0 and times > MaxIteration): Returns ErrMaxRetryExceeded.
//   - Otherwise: Waits for MinDelay + random(0, MaxJitterDuration).
//
// If the context is cancelled while waiting, it returns immediately with the context error.
func (jr *JitterWait) Wait(ctx context.Context, times int) <-chan WaitInfo {
	waitChan := make(chan WaitInfo, 1)
	defer close(waitChan)

	// IF retries == 0: It's the first attempt, so we should run immediately.
	if times == 0 {
		waitChan <- WaitInfo{DoneAt: time.Now()}
		return waitChan
	}

	// IF retries > maxRetry: We have exceeded the max retry limit, so we should return
	// ErrMaxRetryExceeded to let the caller handle the error.
	if jr.maxIteration >= 0 && times > jr.maxIteration {
		waitChan <- WaitInfo{Err: ErrMaxRetryExceeded}
		return waitChan
	}

	// times (1-based attempt count) is passed to NextDelay.
	delay := jr.NextDelay(times)

	select {
	case <-ctx.Done():
		waitChan <- WaitInfo{Err: ctx.Err()}
		return waitChan
	case at := <-time.After(delay):
		waitChan <- WaitInfo{DoneAt: at}
		return waitChan
	}
}

// ExponentialWait implements a retry strategy using exponential backoff.
// The delay increases exponentially with each subsequent attempt:
//
//	Delay = MinDelay * (Factor ^ (Attempts - 1))
//
// This strategy is ideal for environments where prolonged outages require
// aggressively expanding wait times to reduce load on the Redis cluster.
type ExponentialWait struct {
	// minDelay is the initial delay duration.
	minDelay time.Duration

	// maxDelay is the maximum delay cap. If 0, no maximum is applied.
	maxDelay time.Duration

	// factor is the multiplier for each retry. Should be >= 1.0.
	factor float64

	// maxIteration is the maximum number of retry attempts.
	// -1 indicates infinite retries.
	maxIteration int
}

// NewExponentialWait creates a new ExponentialWait with the given options.
func NewExponentialWait(opts ...ExponentialWaitOption) *ExponentialWait {
	ew := DefaultExponentialWait()
	for _, opt := range opts {
		opt(ew)
	}
	return ew
}

// DefaultExponentialWait returns the default configuration:
// infinite retries, 100ms minDelay, 1min maxDelay, factor 2.0.
func DefaultExponentialWait() *ExponentialWait {
	return &ExponentialWait{
		minDelay:     100 * time.Millisecond,
		maxDelay:     1 * time.Minute,
		factor:       2.0,
		maxIteration: -1,
	}
}

// NextDelay calculates the exponential duration for the upcoming retry attempt.
// The retries parameter indicates the number of attempts made so far.
func (ew *ExponentialWait) NextDelay(retries int) time.Duration {
	// If minDelay is 0, exponential backoff doesn't make sense.
	if ew.minDelay <= 0 {
		return 0
	}

	// retries=1 corresponds to the first retry (exponent 0)
	exponent := max(retries-1, 0)

	delayFloat := float64(ew.minDelay) * math.Pow(ew.factor, float64(exponent))
	delay := time.Duration(delayFloat)

	if ew.maxDelay > 0 && delay > ew.maxDelay {
		delay = ew.maxDelay
	}

	return delay
}

// Wait implements the [Waiter] interface using an exponential backoff strategy.
func (ew *ExponentialWait) Wait(ctx context.Context, times int) <-chan WaitInfo {
	waitChan := make(chan WaitInfo, 1)
	defer close(waitChan)

	// If times == 0, it is the initial attempt, so we return immediately.
	if times == 0 {
		waitChan <- WaitInfo{DoneAt: time.Now()}
		return waitChan
	}

	if ew.maxIteration >= 0 && times > ew.maxIteration {
		waitChan <- WaitInfo{Err: ErrMaxRetryExceeded}
		return waitChan
	}

	delay := ew.NextDelay(times)

	select {
	case <-ctx.Done():
		waitChan <- WaitInfo{Err: ctx.Err()}
		return waitChan
	case at := <-time.After(delay):
		waitChan <- WaitInfo{DoneAt: at}
		return waitChan
	}
}
