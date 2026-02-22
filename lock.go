package redlock

import (
	"context"
	"fmt"
	"time"

	redis "github.com/redis/go-redis/v9"
)

// redisClient defines the Redis operations required by Lock.
// Both *redis.Client and *redis.ClusterClient satisfy this interface.
type redisClient interface {
	redis.Scripter
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
}

// Lock provides a distributed lock backed by a single Redis instance.
// It supports automatic retries with configurable backoff (via [Waiter]), atomic operations
// using Lua scripts, and internally generated fencing tokens to guarantee safe lock ownership.
type Lock struct {
	rcli   redisClient
	waiter Waiter
}

// NewLock creates a new Lock backed by the given Redis client.
// By default, the lock retries indefinitely with 300ms max jitter.
func NewLock(rcli redisClient, opts ...LockOption) *Lock {
	dl := &Lock{
		rcli:   rcli,
		waiter: DefaultJitterWait(),
	}
	for _, opt := range opts {
		opt(dl)
	}
	return dl
}

// Acquire generates a UUID fencing token and attempts to acquire the lock.
// It returns the generated fencing token on success, or an error if the context is
// cancelled, the retry limit is exceeded, or token generation fails.
func (dl *Lock) Acquire(ctx context.Context, key string, ttl time.Duration) (fencing string, err error) {
	fencing, err = newFencingToken()
	if err != nil {
		return "", fmt.Errorf("failed to generate fencing token: %w", err)
	}
	err = dl.AcquireWithFencing(ctx, key, fencing, ttl)
	return fencing, err
}

// TryAcquire generates a UUID fencing token and attempts to acquire the lock exactly once.
// It returns ErrLockAlreadyHeld if the resource is currently locked by another client.
func (dl *Lock) TryAcquire(ctx context.Context, key string, ttl time.Duration) (fencing string, err error) {
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

// TryAcquireWithFencing attempts to acquire the lock exactly once using the provided fencing token.
// It returns ErrLockAlreadyHeld if the resource is currently locked.
func (dl *Lock) TryAcquireWithFencing(ctx context.Context, key, fencing string, ttl time.Duration) error {
	cmd := dl.rcli.SetNX(ctx, key, fencing, ttl)
	if cmd.Err() != nil {
		return cmd.Err()
	}
	if !cmd.Val() {
		return ErrLockAlreadyHeld
	}
	return nil
}

// AcquireOrExtend prolongs the lock TTL if the current owner matches the provided fencing token.
// If the lock does not exist, it behaves identically to AcquireWithFencing and attempts to claim it.
func (dl *Lock) AcquireOrExtend(ctx context.Context, key, fencing string, ttl time.Duration) error {
	retries := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case waitInfo := <-dl.waiter.Wait(ctx, retries):
			if waitInfo.Err != nil {
				return waitInfo.Err
			}

			cmd := runScript(ctx, dl.rcli, scriptAcquireOrExtend, shaAcquireOrExtend, []string{key}, fencing, ttl.Milliseconds())
			if cmd.Err() != nil {
				return cmd.Err()
			}
			val, _ := cmd.Int64()
			if val > 0 {
				return nil
			}
			retries++
		}
	}
}

// Extend prolongs the TTL of an existing lock if the fencing token matches the current owner.
// Unlike AcquireOrExtend, it does not attempt to claim an unowned lock.
// It returns ErrLockNotHeld if the lock does not exist or the token is incorrect.
func (dl *Lock) Extend(ctx context.Context, key, fencing string, ttl time.Duration) error {
	retries := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case waitInfo := <-dl.waiter.Wait(ctx, retries):
			if waitInfo.Err != nil {
				return waitInfo.Err
			}

			err := dl.TryExtend(ctx, key, fencing, ttl)
			if err == nil {
				return nil
			}
			retries++
		}
	}
}

// TryExtend attempts to extend the TTL of an existing lock exactly once.
// It returns ErrLockNotHeld if the lock does not exist or the fencing token does not match.
func (dl *Lock) TryExtend(ctx context.Context, key, fencing string, ttl time.Duration) error {
	cmd := runScript(ctx, dl.rcli, scriptExtend, shaExtend, []string{key}, fencing, ttl.Milliseconds())
	if cmd.Err() != nil {
		return cmd.Err()
	}
	val, _ := cmd.Int64()
	if val > 0 {
		return nil
	}
	return ErrLockNotHeld
}

// AcquireWithFencing attempts to acquire the lock using the provided fencing token.
// It retries according to the underlying Waiter configuration.
func (dl *Lock) AcquireWithFencing(ctx context.Context, key, fencing string, ttl time.Duration) error {
	retries := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case waitInfo := <-dl.waiter.Wait(ctx, retries):
			if waitInfo.Err != nil {
				return waitInfo.Err
			}

			cmd := dl.rcli.SetNX(ctx, key, fencing, ttl)
			if cmd.Err() != nil {
				return cmd.Err()
			}
			if cmd.Val() {
				return nil
			}
			retries++
		}
	}
}

// Release atomically deletes the lock using a Lua script if the fencing token matches.
// An error is returned only if the underlying script execution fails entirely.
func (dl *Lock) Release(ctx context.Context, key, fencing string) error {
	return runScript(ctx, dl.rcli, scriptRelease, shaRelease, []string{key}, fencing).Err()
}
