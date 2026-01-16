package redlock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// DistributedLock implements the Redlock algorithm for distributed locking
// across multiple independent Redis instances. It provides stronger guarantees
// than a single-instance lock by requiring a quorum (N/2 + 1) of instances
// to agree on lock ownership.
//
// BUG(trviph): The Extend method does not release partially-extended locks on
// quorum failure. Unlike Acquire/TryAcquire/AcquireOrExtend, if Extend fails to
// achieve quorum, the successfully extended instances remain locked until TTL expires.
type DistributedLock struct {
	locks            []*Lock
	clockDriftFactor float64
	releaseTimeout   time.Duration
	rc               retryConfig
}

// NewDistributedLock creates a new DistributedLock with the given Lock instances.
// Each Lock should be connected to an independent Redis instance.
// For optimal fault tolerance, use an odd number of instances (e.g., 3, 5, or 7).
// By default, the lock retries indefinitely with 300ms max jitter.
func NewDistributedLock(locks []*Lock, opts ...DistributedLockOption) *DistributedLock {
	dl := &DistributedLock{
		locks:            locks,
		clockDriftFactor: 0.01, // 1% clock drift factor, as recommended by the Redlock paper
		releaseTimeout:   5 * time.Second,
		rc:               defaultRetryConfig(),
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
// The method also validates that the lock is still valid after acquisition by
// checking that the elapsed time plus clock drift allowance is less than the TTL.
//
// Internally, this method retries by calling [DistributedLock.TryAcquire] until
// success, context cancellation, or max retries exceeded.
//
// Returns the fencing token on success, which must be passed to Release.
// Returns an error if quorum cannot be achieved, clock drift check fails, or context is cancelled.
func (dl *DistributedLock) Acquire(ctx context.Context, key string, ttl time.Duration) (fencing string, err error) {
	fencing, err = newFencingToken()
	if err != nil {
		return "", fmt.Errorf("failed to generate fencing token: %w", err)
	}
	err = dl.AcquireWithFencing(ctx, key, fencing, ttl)
	return fencing, err
}

// AcquireWithFencing attempts to acquire the lock with a provided fencing token
// across all Redis instances concurrently. It requires a quorum (N/2 + 1) of instances
// to successfully acquire the lock. If quorum is not reached, all acquired locks
// are automatically released.
//
// The method also validates that the lock is still valid after acquisition by
// checking that the elapsed time plus clock drift allowance is less than the TTL.
//
// Internally, this method retries by calling [DistributedLock.TryAcquireWithFencing]
// until success, context cancellation, or max retries exceeded.
//
// Returns an error if quorum cannot be achieved, clock drift check fails, or context is cancelled.
func (dl *DistributedLock) AcquireWithFencing(ctx context.Context, key, fencing string, ttl time.Duration) error {
	retries := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-dl.rc.wait(retries):
			if retries > dl.rc.maxRetry && dl.rc.maxRetry >= 0 {
				return ErrMaxRetryExceeded
			}
			err := dl.TryAcquireWithFencing(ctx, key, fencing, ttl)
			if err == nil {
				return nil
			}
			// Only retry on ErrLockAlreadyHeld; other errors should be returned immediately
			if !errors.Is(err, ErrLockAlreadyHeld) {
				return err
			}
			retries++
		}
	}
}

// TryAcquire attempts to acquire the lock exactly once across all Redis instances
// without retrying. It requires a quorum (N/2 + 1) of instances to successfully
// acquire the lock. If quorum is not reached, all acquired locks are automatically released.
//
// Returns the fencing token on success, or ErrLockAlreadyHeld if quorum cannot be achieved.
func (dl *DistributedLock) TryAcquire(ctx context.Context, key string, ttl time.Duration) (fencing string, err error) {
	fencing, err = newFencingToken()
	if err != nil {
		return "", fmt.Errorf("failed to generate fencing token: %w", err)
	}
	err = dl.TryAcquireWithFencing(ctx, key, fencing, ttl)
	if err != nil {
		return "", err
	}
	return fencing, nil
}

// TryAcquireWithFencing attempts to acquire the lock exactly once with a provided fencing token
// across all Redis instances without retrying. It requires a quorum (N/2 + 1) of instances
// to successfully acquire the lock. If quorum is not reached, all acquired locks are automatically released.
//
// Returns nil on success, or ErrLockAlreadyHeld if quorum cannot be achieved.
func (dl *DistributedLock) TryAcquireWithFencing(ctx context.Context, key, fencing string, ttl time.Duration) (err error) {
	startTime := time.Now()

	var win atomic.Int32
	var wg sync.WaitGroup

	defer func() {
		if err != nil {
			// Use a fresh context for cleanup to avoid issues if original context is cancelled
			releaseCtx, cancel := context.WithTimeout(context.Background(), dl.releaseTimeout)
			defer cancel()
			if releaseErr := dl.Release(releaseCtx, key, fencing); releaseErr != nil {
				err = errors.Join(err, fmt.Errorf("cleanup release also failed: %w", releaseErr))
			}
		}
	}()

	errChan := make(chan error, len(dl.locks))
	for _, lock := range dl.locks {
		wg.Go(func() {
			if err := lock.TryAcquireWithFencing(ctx, key, fencing, ttl); err != nil {
				errChan <- err
				return
			}
			win.Add(1)
		})
	}
	wg.Wait()
	close(errChan)

	if win.Load() >= int32(len(dl.locks)/2+1) {
		// Clock drift check: ensure the lock is still valid
		elapsed := time.Since(startTime)
		drift := time.Duration(float64(ttl) * dl.clockDriftFactor)
		validity := ttl - elapsed - drift
		if validity <= 0 {
			return fmt.Errorf("lock acquired but validity expired (elapsed %v, drift %v, ttl %v): %w", elapsed, drift, ttl, ErrValidityExpired)
		}
		return nil
	}

	return ErrLockAlreadyHeld
}

// Release releases the lock from all Redis instances.
// Returns an error if at least one release failed, aggregating the error count.
func (dl *DistributedLock) Release(ctx context.Context, key string, fencing string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(dl.locks))

	for _, lock := range dl.locks {
		wg.Go(func() {
			if err := lock.Release(ctx, key, fencing); err != nil {
				errChan <- err
			}
		})
	}
	wg.Wait()
	close(errChan)

	errs := make([]error, 0, len(dl.locks))
	for e := range errChan {
		errs = append(errs, e)
	}
	if len(errs) > 0 {
		return fmt.Errorf("release failed on %d of %d instance(s): %w", len(errs), len(dl.locks), errors.Join(errs...))
	}
	return nil
}

// Extend extends the TTL of an existing lock across all Redis instances concurrently.
// Requires a quorum (N/2 + 1) of instances to successfully extend the lock.
// Also validates that the lock is still valid after extension by checking clock drift.
//
// Internally, this method retries by calling [DistributedLock.TryExtend] until
// success, context cancellation, or max retries exceeded.
//
// Returns an error if quorum cannot be achieved, clock drift check fails, or context is cancelled.
func (dl *DistributedLock) Extend(ctx context.Context, key string, fencing string, ttl time.Duration) error {
	retries := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-dl.rc.wait(retries):
			if retries > dl.rc.maxRetry && dl.rc.maxRetry >= 0 {
				return ErrMaxRetryExceeded
			}
			err := dl.TryExtend(ctx, key, fencing, ttl)
			if err == nil {
				return nil
			}
			// Only retry on ErrLockNotHeld; other errors should be returned immediately
			if !errors.Is(err, ErrLockNotHeld) {
				return err
			}
			retries++
		}
	}
}

// TryExtend attempts to extend the TTL of an existing lock exactly once across all Redis instances.
// Requires a quorum (N/2 + 1) of instances to successfully extend the lock.
// Returns nil on success, ErrLockNotHeld if quorum cannot be achieved.
func (dl *DistributedLock) TryExtend(ctx context.Context, key string, fencing string, ttl time.Duration) error {
	startTime := time.Now()
	var win atomic.Int32
	var wg sync.WaitGroup

	errChan := make(chan error, len(dl.locks))
	for _, lock := range dl.locks {
		wg.Go(func() {
			if err := lock.TryExtend(ctx, key, fencing, ttl); err != nil {
				errChan <- err
				return
			}
			win.Add(1)
		})
	}
	wg.Wait()
	close(errChan)

	if win.Load() >= int32(len(dl.locks)/2+1) {
		// Clock drift check: ensure the lock is still valid
		elapsed := time.Since(startTime)
		drift := time.Duration(float64(ttl) * dl.clockDriftFactor)
		validity := ttl - elapsed - drift
		if validity <= 0 {
			return fmt.Errorf("lock extended but validity expired (elapsed %v, drift %v, ttl %v): %w", elapsed, drift, ttl, ErrValidityExpired)
		}
		return nil
	}

	return ErrLockNotHeld
}

// AcquireOrExtend acquires a new lock or extends an existing one if the fencing token matches.
// It attempts the operation across all Redis instances concurrently and requires a quorum
// (N/2 + 1) of instances to succeed.
// Also validates that the lock is still valid after the operation by checking clock drift.
// Returns an error if quorum cannot be achieved, clock drift check fails, or context is cancelled.
func (dl *DistributedLock) AcquireOrExtend(ctx context.Context, key string, fencing string, ttl time.Duration) (err error) {
	startTime := time.Now()
	var win atomic.Int32
	var wg sync.WaitGroup

	defer func() {
		if err != nil {
			// Use a fresh context for cleanup to avoid issues if original context is cancelled
			releaseCtx, cancel := context.WithTimeout(context.Background(), dl.releaseTimeout)
			defer cancel()
			if releaseErr := dl.Release(releaseCtx, key, fencing); releaseErr != nil {
				err = errors.Join(err, fmt.Errorf("cleanup release also failed: %w", releaseErr))
			}
		}
	}()

	errChan := make(chan error, len(dl.locks))
	for _, lock := range dl.locks {
		wg.Go(func() {
			if err := lock.AcquireOrExtend(ctx, key, fencing, ttl); err != nil {
				errChan <- err
				return
			}
			win.Add(1)
		})
	}
	wg.Wait()
	close(errChan)

	if win.Load() >= int32(len(dl.locks)/2+1) {
		// Clock drift check: ensure the lock is still valid
		elapsed := time.Since(startTime)
		drift := time.Duration(float64(ttl) * dl.clockDriftFactor)
		validity := ttl - elapsed - drift
		if validity <= 0 {
			return fmt.Errorf("lock acquired/extended but validity expired (elapsed %v, drift %v, ttl %v): %w", elapsed, drift, ttl, ErrValidityExpired)
		}
		return nil
	}

	errs := make([]error, 0, len(dl.locks))
	for e := range errChan {
		errs = append(errs, e)
	}
	return fmt.Errorf("acquire or extend failed on %d of %d instance(s): %w", len(errs), len(dl.locks), errors.Join(errs...))
}
