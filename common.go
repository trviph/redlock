package redlock

import (
	"crypto/rand"
	"math/big"
	"time"

	"github.com/google/uuid"
)

// closedChan is used to immediately unblock select statements
// for first attempts or when max retries are exceeded.
var closedChan = make(chan time.Time)

// retryConfig holds the configuration for retry behavior.
type retryConfig struct {
	maxRetry          int
	minRetryDelay     time.Duration
	maxJitterDuration time.Duration
}

// defaultRetryConfig returns the default retry configuration:
// infinite retries with 300ms max jitter.
func defaultRetryConfig() retryConfig {
	return retryConfig{
		maxRetry:          -1,
		minRetryDelay:     0,
		maxJitterDuration: 300 * time.Millisecond,
	}
}

// wait returns a channel that will receive after the appropriate retry delay.
// If retries == 0, it returns immediately (first attempt).
// If retries > maxRetry (and maxRetry >= 0), it returns immediately to let the caller handle the error.
// Otherwise, it waits for minRetryDelay + random jitter (up to maxJitterDuration).
func (rc retryConfig) wait(retries int) <-chan time.Time {
	// IF retries == 0: It's the first attempt, so we should run immediately.
	// IF retries > maxRetry: We have exceeded the max retry limit, so we should return immediately
	// to let the loop handle the error.
	// In both cases, we return a closed channel to unblock the select statement immediately.
	if retries == 0 || (rc.maxRetry >= 0 && retries > rc.maxRetry) {
		return closedChan
	}

	var jitter time.Duration
	if rc.maxJitterDuration > 0 {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(rc.maxJitterDuration)))
		if err != nil {
			// If random generation fails, we default to the max jitter duration
			// This ensures we still wait some time and don't break the loop.
			jitter = rc.maxJitterDuration
		} else {
			jitter = time.Duration(n.Int64())
		}
	}
	return time.After(rc.minRetryDelay + jitter)
}

// newFencingToken generates a new UUID fencing token.
func newFencingToken() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}
