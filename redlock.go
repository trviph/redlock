// Package redlock provides a Redis-backed distributed lock implementation.
// It is designed to work with a single Redis instance or cluster and is not a full implementation
// of the multi-master Redlock algorithm.
package redlock

import (
	"context"
	"errors"
	"math/rand"
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

func NewLock(rcli redisClient, opts ...func(*Lock)) *Lock {
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
func (dl *Lock) Acquire(ctx context.Context, key string, ttl time.Duration) (cmd *redis.BoolCmd, fencing string, err error) {
	defer func() {
		// This recover block is to handle panic from uuid.NewString()
		// A failure to generate a uuid is not expected to happen,
		// but if it does, we want to handle it gracefully.
		if r := recover(); r != nil {
			cmd = nil
			err = r.(error)
		}
	}()

	fencing = uuid.NewString()
	cmd, err = dl.AcquireWithFencing(ctx, key, fencing, ttl)
	return cmd, fencing, err
}

// AcquireOrExtend acquires a lock, if the fencing value matches, extends the lock.
// If the lock does not exist, it attempts to acquire it.
func (dl *Lock) AcquireOrExtend(ctx context.Context, key, fencing string, ttl time.Duration) (*redis.Cmd, error) {
	retries := 0
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			if retries > dl.maxRetry && dl.maxRetry >= 0 {
				return nil, errors.New("max retry exceeded")
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
				return nil, cmd.Err()
			}
			val, _ := cmd.Int64()
			if val > 0 {
				return cmd, nil
			}
			dl.waitRetry()
			retries++
		}
	}

}

// AcquireWithFencing acquires a lock with a fencing value.
func (dl *Lock) AcquireWithFencing(ctx context.Context, key, fencing string, ttl time.Duration) (*redis.BoolCmd, error) {
	retries := 0
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			if retries > dl.maxRetry && dl.maxRetry >= 0 {
				return nil, errors.New("max retry exceeded")
			}
			cmd := dl.rcli.SetNX(ctx, key, fencing, ttl)
			if cmd.Err() != nil {
				return nil, cmd.Err()
			}
			if cmd.Val() {
				return cmd, nil
			}
			dl.waitRetry()
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

func (dl *Lock) waitRetry() {
	time.Sleep(dl.minRetryDelay + time.Duration(rand.Int63n(int64(dl.maxJitterDuration))))
}
