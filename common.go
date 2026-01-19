package redlock

import (
	"context"
	"crypto/rand"
	"math/big"
	"time"

	"github.com/google/uuid"
)

// Watch starts a watchdog goroutine that periodically extends the lock's TTL.
// It is designed to be used for long-running operations where the duration is unknown.
// The watchdog stops when the provided context is canceled.
//
// The watchdog attempts to extend the lock at intervals of TTL/2.
//
// WARNING(trviph): Do not use context.Background() without a cancel mechanism (e.g. WithCancel),
// otherwise the watchdog will never terminate.
//
// BUG(trviph): The Watch function relies on TryExtend. When using a DistributedLock, it suffers
// from the partial extension issue on quorum failure. If DistributedLock.TryExtend fails to
// achieve quorum, the successfully extended instances remain locked until TTL expires.
//
// This bug will probably never be fixed. Do not use Watch with DistributedLock if you are not
// comfortable with this uncertainty.
func Watch(ctx context.Context, locker Locker, key, fencing string, ttl time.Duration) {
	WatchWithInterval(ctx, locker, key, fencing, ttl, ttl/2)
}

// WatchWithInterval starts a watchdog goroutine that periodically extends the lock's TTL.
// It allows customizing the interval between extension attempts.
// The watchdog stops when the provided context is canceled.
//
// WARNING(trviph): Do not use context.Background() without a cancel mechanism (e.g. WithCancel),
// otherwise the watchdog will never terminate.
//
// BUG(trviph): The WatchWithInterval function relies on TryExtend. When using a DistributedLock,
// it suffers from the partial extension issue on quorum failure. If DistributedLock.TryExtend fails to
// achieve quorum, the successfully extended instances remain locked until TTL expires.
//
// This bug will probably never be fixed. Do not use WatchWithInterval with DistributedLock if you are not
// comfortable with this uncertainty.
func WatchWithInterval(ctx context.Context, locker Locker, key, fencing string, ttl, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = locker.TryExtend(ctx, key, fencing, ttl)
			}
		}
	}()
}

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
