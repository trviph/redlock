// Package redlock provides a Redis-backed distributed lock implementation.
// It is designed to work with a single Redis instance or cluster and is not a full implementation
// of the multi-master Redlock algorithm.
package redlock

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

// Redlock need these functionality from a Redis client.
type redisClient interface {
	redis.Scripter
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
}

type Lock struct {
	rcli              redisClient
	maxJitterDuration time.Duration
	minRetryDelay     time.Duration
	maxRetry          int
}

var ErrLockAlreadyHeld = errors.New("lock already held")

func NewLock(rcli redisClient, opts ...func(configurable)) *Lock {
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
	defer func() {
		// This recover block is to handle panic from uuid.NewString()
		// A failure to generate a uuid is not expected to happen,
		// but if it does, we want to handle it gracefully.
		if r := recover(); r != nil {
			var typeOk bool
			if err, typeOk = r.(error); !typeOk {
				err = fmt.Errorf("panic: %v", r)
			}
		}
	}()

	fencing = uuid.NewString()
	err = dl.AcquireWithFencing(ctx, key, fencing, ttl)
	return fencing, err
}

// TryAcquire attempts to acquire a lock exactly once without retrying.
// It returns the fencing token on success, or an error on failure.
// If the lock is already held, it returns ErrLockAlreadyHeld.
func (dl *Lock) TryAcquire(ctx context.Context, key string, ttl time.Duration) (fencing string, err error) {
	defer func() {
		if r := recover(); r != nil {
			var typeOk bool
			if err, typeOk = r.(error); !typeOk {
				err = fmt.Errorf("panic: %v", r)
			}
		}
	}()

	fencing = uuid.NewString()
	cmd := dl.rcli.SetNX(ctx, key, fencing, ttl)
	if cmd.Err() != nil {
		return "", cmd.Err()
	}
	if !cmd.Val() {
		return "", ErrLockAlreadyHeld
	}
	return fencing, nil
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
				return errors.New("max retry exceeded")
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
				return errors.New("max retry exceeded")
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

var closedChan = make(chan time.Time)

func init() {
	close(closedChan)
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

func (dl *Lock) setJitterDuration(jitter time.Duration) {
	dl.maxJitterDuration = jitter
}

func (dl *Lock) setMinRetryDelay(minDelay time.Duration) {
	dl.minRetryDelay = minDelay
}

func (dl *Lock) setMaxRetry(maxRetry int) {
	dl.maxRetry = maxRetry
}
