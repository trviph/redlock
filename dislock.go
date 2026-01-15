package redlock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// DistributedLock implements the Redlock algorithm for distributed locking
// across multiple independent Redis instances. It provides stronger guarantees
// than a single-instance lock by requiring a quorum (N/2 + 1) of instances
// to agree on lock ownership.
type DistributedLock struct {
	locks            []*Lock
	clockDriftFactor float64
}

// NewDistributedLock creates a new DistributedLock with the given Lock instances.
// Each Lock should be connected to an independent Redis instance.
// For optimal fault tolerance, use an odd number of instances (e.g., 3, 5, or 7).
func NewDistributedLock(locks []*Lock, opts ...DistributedLockOption) *DistributedLock {
	dl := &DistributedLock{
		locks:            locks,
		clockDriftFactor: 0.01, // 1% clock drift factor, as recommended by the Redlock paper
	}
	for _, opt := range opts {
		opt(dl)
	}
	return dl
}

// Acquire attempts to acquire the lock across all Redis instances concurrently.
// It generates a unique fencing token and requires a quorum (N/2 + 1) of instances
// to successfully acquire the lock. If quorum is not reached, all acquired locks
// are automatically released.
//
// Returns the fencing token on success, which must be passed to Release.
// Returns an error if quorum cannot be achieved or context is cancelled.
func (dl *DistributedLock) Acquire(ctx context.Context, key string, ttl time.Duration) (fencing string, err error) {
	fencing = uuid.NewString()

	var win atomic.Int32
	var wg sync.WaitGroup

	defer func() {
		if err != nil {
			if releaseErr := dl.Release(ctx, key, fencing); releaseErr != nil {
				err = errors.Join(err, fmt.Errorf("cleanup release also failed: %w", releaseErr))
			}
		}
	}()

	errChan := make(chan error, len(dl.locks))
	for _, lock := range dl.locks {
		wg.Add(1)
		go func(lock *Lock) {
			defer wg.Done()
			if err := lock.AcquireWithFencing(ctx, key, fencing, ttl); err != nil {
				errChan <- err
				return
			}
			win.Add(1)
		}(lock)
	}
	wg.Wait()
	close(errChan)
	if win.Load() >= int32(len(dl.locks)/2+1) {
		return fencing, nil
	}

	var finalError error
	errCount := 0
	for e := range errChan {
		errCount++
		finalError = e
	}
	return "", fmt.Errorf("acquire failed on %d of %d instance(s), with one error as %w", errCount, len(dl.locks), finalError)
}

// Release releases the lock from all Redis instances.
// Returns an error if at least one release failed, aggregating the error count.
func (dl *DistributedLock) Release(ctx context.Context, key string, fencing string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(dl.locks))

	for _, lock := range dl.locks {
		wg.Add(1)
		go func(lock *Lock) {
			defer wg.Done()
			if err := lock.Release(ctx, key, fencing); err != nil {
				errChan <- err
			}
		}(lock)
	}
	wg.Wait()
	close(errChan)

	var finalError error
	errCount := 0
	for e := range errChan {
		errCount++
		finalError = e
	}
	if finalError != nil {
		return fmt.Errorf("release failed on %d of %d instance(s), with one error as %w", errCount, len(dl.locks), finalError)
	}
	return nil
}
