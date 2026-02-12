package redlock

import (
	"context"
	"crypto/rand"
	"math/big"
	"time"
)

// JitterWait implements a retry strategy with constant delay plus random jitter.
// It allows defining a maximum number of retries, a minimum base delay, and a maximum jitter duration.
// The actual delay for each retry is calculated as: MinDelay + random(0, MaxJitterDuration).
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

// Wait implements the Waiter interface.
// It calculates the delay based on JitterWait configuration and waits for that duration.
//
// Behavior:
//   - If times == 0 (first attempt): Returns immediately.
//   - If MaxIteration >= 0 and times > MaxIteration: Returns ErrMaxRetryExceeded.
//   - Otherwise: Waits for MinDelay + random(0, MaxJitterDuration).
//
// This method respects context cancellation. If ctx is cancelled during the wait,
// it returns immediately with the context error.
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

	select {
	case <-ctx.Done():
		waitChan <- WaitInfo{Err: ctx.Err()}
		return waitChan
	case at := <-time.After(jr.minDelay + jitter):
		waitChan <- WaitInfo{DoneAt: at}
		return waitChan
	}
}
