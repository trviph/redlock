package redlock

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
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
// It supports automatic retry with configurable backoff, atomic operations
// via Lua scripts, and fencing tokens for safe lock ownership.
type Lock struct {
	rcli              redisClient
	maxJitterDuration time.Duration
	minRetryDelay     time.Duration
	maxRetry          int
}

// NewLock creates a new Lock backed by the given Redis client.
// By default, the lock retries indefinitely with 300ms max jitter.
func NewLock(rcli redisClient, opts ...LockOption) *Lock {
	dl := &Lock{
		rcli:              rcli,
		maxRetry:          -1,
		maxJitterDuration: 300 * time.Millisecond,
		minRetryDelay:     0,
	}
	for _, opt := range opts {
		opt(dl)
	}
	return dl
}

// Acquire acquires a lock with a random uuid fencing value.
// It returns the fencing token on success, or an error on failure.
func (dl *Lock) Acquire(ctx context.Context, key string, ttl time.Duration) (fencing string, err error) {
	fencing, err = newFencingToken()
	if err != nil {
		return "", fmt.Errorf("failed to generate fencing token: %w", err)
	}
	err = dl.AcquireWithFencing(ctx, key, fencing, ttl)
	return fencing, err
}

// TryAcquire attempts to acquire a lock exactly once without retrying.
// It returns the fencing token on success, or an error on failure.
// If the lock is already held, it returns ErrLockAlreadyHeld.
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

// TryAcquireWithFencing attempts to acquire a lock exactly once with a provided fencing token.
// Returns nil on success, or ErrLockAlreadyHeld if the lock is held.
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

// AcquireOrExtend acquires a lock, if the fencing value matches, extends the lock.
// If the lock does not exist, it attempts to acquire it.
// It returns nil on success, or an error on failure.
func (dl *Lock) AcquireOrExtend(ctx context.Context, key, fencing string, ttl time.Duration) error {
	retries := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-dl.waitRetry(retries):
			if retries > dl.maxRetry && dl.maxRetry >= 0 {
				return ErrMaxRetryExceeded
			}

			script := `
				if redis.call("get", KEYS[1]) == ARGV[1] then
					return redis.call("pexpire", KEYS[1], ARGV[2])
				elseif redis.call("set", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
					return 1
				else
					return 0
				end
			`
			cmd := dl.rcli.Eval(ctx, script, []string{key}, fencing, ttl.Milliseconds())
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

// Extend extends the TTL of an existing lock if the fencing token matches.
// Unlike AcquireOrExtend, this will not attempt to acquire if the lock doesn't exist.
// Returns nil on success, or an error if the lock doesn't exist, fencing doesn't match,
// or max retries exceeded.
func (dl *Lock) Extend(ctx context.Context, key, fencing string, ttl time.Duration) error {
	retries := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-dl.waitRetry(retries):
			if retries > dl.maxRetry && dl.maxRetry >= 0 {
				return ErrMaxRetryExceeded
			}

			err := dl.tryExtend(ctx, key, fencing, ttl)
			if err == nil {
				return nil
			}
			if err != ErrLockNotHeld {
				return err
			}
			retries++
		}
	}
}

// TryExtend attempts to extend the TTL of an existing lock exactly once without retrying.
// Returns nil on success, ErrLockNotHeld if the lock doesn't exist or fencing doesn't match.
func (dl *Lock) TryExtend(ctx context.Context, key, fencing string, ttl time.Duration) error {
	return dl.tryExtend(ctx, key, fencing, ttl)
}

func (dl *Lock) tryExtend(ctx context.Context, key, fencing string, ttl time.Duration) error {
	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`
	cmd := dl.rcli.Eval(ctx, script, []string{key}, fencing, ttl.Milliseconds())
	if cmd.Err() != nil {
		return cmd.Err()
	}
	val, _ := cmd.Int64()
	if val > 0 {
		return nil
	}
	return ErrLockNotHeld
}

// AcquireWithFencing acquires a lock with a fencing value.
// It returns nil on success, or an error on failure.
func (dl *Lock) AcquireWithFencing(ctx context.Context, key, fencing string, ttl time.Duration) error {
	retries := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-dl.waitRetry(retries):
			if retries > dl.maxRetry && dl.maxRetry >= 0 {
				return ErrMaxRetryExceeded
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

// Release releases a lock with a fencing value.
func (dl *Lock) Release(ctx context.Context, key string, fencing string) error {
	script := `
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
	`
	return dl.rcli.Eval(ctx, script, []string{key}, fencing).Err()
}

func (dl *Lock) waitRetry(retries int) <-chan time.Time {
	// IF retries == 0: It's the first attempt, so we should run immediately.
	// IF retries > maxRetry: We have exceeded the max retry limit, so we should return immediately
	// to let the loop handle the error.
	// In both cases, we return a closed channel to unblock the select statement immediately.
	if retries == 0 || (dl.maxRetry >= 0 && retries > dl.maxRetry) {
		return closedChan
	}

	var jitter time.Duration
	if dl.maxJitterDuration > 0 {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(dl.maxJitterDuration)))
		if err != nil {
			// If random generation fails, we default to the max jitter duration
			// This ensures we still wait some time and don't break the loop.
			jitter = dl.maxJitterDuration
		} else {
			jitter = time.Duration(n.Int64())
		}
	}
	return time.After(dl.minRetryDelay + jitter)
}
