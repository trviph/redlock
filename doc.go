// Package redlock provides Redis-backed distributed lock implementations.
//
// This package offers two complementary lock types for different deployment scenarios:
//
// # Lock (Single Instance)
//
// [Lock] provides a distributed lock backed by a single Redis instance. It supports:
//   - Automatic retry with configurable jitter and backoff
//   - Atomic acquisition and release using Lua scripts
//   - Fencing tokens (UUIDs) to prevent unsafe lock hand-off
//   - Lock extension for long-running operations
//
// Example usage:
//
//	lock := redlock.NewLock(redisClient,
//	    redlock.WithMaxRetry(5),
//	    redlock.WithJitterDuration(200*time.Millisecond),
//	)
//	fencing, err := lock.Acquire(ctx, "my-resource", 30*time.Second)
//	if err != nil {
//	    // handle error
//	}
//	defer lock.Release(ctx, "my-resource", fencing)
//
// # DistributedLock (Multi-Instance Redlock)
//
// [DistributedLock] implements the Redlock algorithm for environments requiring
// higher availability. It acquires locks across multiple independent Redis instances
// and uses quorum-based consensus (N/2 + 1) to determine lock ownership.
//
// Example usage:
//
//	locks := []*redlock.Lock{
//	    redlock.NewLock(redis1),
//	    redlock.NewLock(redis2),
//	    redlock.NewLock(redis3),
//	}
//	dl := redlock.NewDistributedLock(locks,
//	    redlock.WithClockDriftFactor(0.01),
//	)
//	fencing, err := dl.Acquire(ctx, "my-resource", 30*time.Second)
//	if err != nil {
//	    // handle error
//	}
//	defer dl.Release(ctx, "my-resource", fencing)
//
// # Fencing Tokens
//
// Both lock types return a fencing token (UUID string) upon successful acquisition.
// This token must be passed to [Lock.Release] or [DistributedLock.Release] to ensure
// only the lock owner can release it. Fencing tokens can also be used by downstream
// systems to detect stale lock holders.
//
// # Configuration
//
// Both lock types use functional options for configuration:
//   - [Lock]: [WithMaxRetry], [WithJitterDuration], [WithMinRetryDelay]
//   - [DistributedLock]: [WithClockDriftFactor]
//
// # Sentinel Errors
//
// The package exports sentinel errors for reliable error checking with [errors.Is]:
//   - [ErrLockAlreadyHeld]: returned by TryAcquire when the lock is held by another client
//   - [ErrMaxRetryExceeded]: returned when retry attempts are exhausted
//   - [ErrValidityExpired]: returned when clock drift causes lock validity to expire
//
// # References
//
//   - Redis Redlock Algorithm: https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/
//   - Martin Kleppmann's Analysis: https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
package redlock
