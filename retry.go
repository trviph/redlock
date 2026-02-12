package redlock

import (
	"context"
	"crypto/rand"
	"math/big"
	"time"
)

// JitterWait holds the configuration for retry behavior.
type JitterWait struct {
	MaxIteration      int
	MinDelay          time.Duration
	MaxJitterDuration time.Duration
}

// DefaultJitterWait returns the default retry configuration:
// infinite retries with 300ms max jitter.
func DefaultJitterWait() *JitterWait {
	return &JitterWait{
		MaxIteration:      -1,
		MinDelay:          0,
		MaxJitterDuration: 300 * time.Millisecond,
	}
}

// Wait returns a channel that will receive after the appropriate retry delay.
// If retries == 0, it returns immediately (first attempt).
// If retries > maxRetry (and maxRetry >= 0), it returns ErrMaxRetryExceeded.
// Otherwise, it waits for minRetryDelay + random jitter (up to maxJitterDuration).
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
	if jr.MaxIteration >= 0 && times > jr.MaxIteration {
		waitChan <- WaitInfo{Err: ErrMaxRetryExceeded}
		return waitChan
	}

	var jitter time.Duration
	if jr.MaxJitterDuration > 0 {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(jr.MaxJitterDuration)))
		if err != nil {
			// If random generation fails, we default to the max jitter duration
			// This ensures we still wait some time and don't break the loop.
			jitter = jr.MaxJitterDuration
		} else {
			jitter = time.Duration(n.Int64())
		}
	}

	select {
	case <-ctx.Done():
		waitChan <- WaitInfo{Err: ctx.Err()}
		return waitChan
	case at := <-time.After(jr.MinDelay + jitter):
		waitChan <- WaitInfo{DoneAt: at}
		return waitChan
	}
}
