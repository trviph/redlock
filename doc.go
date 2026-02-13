// Package redlock provides Redis-backed distributed lock implementations.
//
// This package offers two complementary lock types for different deployment scenarios:
//
// # Lock (Single Instance)
//
// [Lock] provides a distributed lock backed by a single Redis instance. It supports:
//   - Automatic retry with configurable jitter and backoff via [Waiter] interface
//   - Atomic acquisition and release using Lua scripts
//   - Fencing tokens (UUIDs) to prevent unsafe lock hand-off
//   - Lock extension for long-running operations
//   - Watchdog pattern support via [Watch] and [WatchWithInterval]
//
// Example usage:
//
//	lock := redlock.NewLock(redisClient,
//	    redlock.WithWaiter(redlock.NewJitterWait(
//	        redlock.WithJitterMaxIteration(5),
//	        redlock.WithMaxJitterDuration(200*time.Millisecond),
//	    )),
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
//	    redlock.WithClockDriftBuffer(2*time.Millisecond),
//	    redlock.WithDistWaiter(redlock.NewJitterWait(
//	        redlock.WithJitterMaxIteration(-1),
//	        redlock.WithMaxJitterDuration(300*time.Millisecond),
//	    )),
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
//   - [Lock]: [WithWaiter] (and [JitterWaitOption]s for [NewJitterWait])
//   - [DistributedLock]: [WithClockDriftFactor], [WithClockDriftBuffer], [WithReleaseTimeout],
//     [WithDistWaiter]
//
// Note: Older configuration options (e.g., [WithMaxRetry], [WithJitterDuration]) are deprecated
// but remain available for backward compatibility when using the default [JitterWait].
//
// # Watchdog Pattern
//
// [Watch] and [WatchWithInterval] provide a background goroutine to automatically extend
// the lock duration.
//
// Warning: The watchdog will **not** stop automatically if the lock is lost or fails
// to extend. It will continue attempting to extend the lock indefinitely until the
// provided context is canceled.
//
// This design handles cases where the watchdog is started before the lock is successfully
// acquired (e.g., in a background retry loop) and ensures resilience against transient
// network failures. The user is responsible for managing the watchdog's lifecycle via
// the context.
//
// # WatchDog with Callbacks
//
// [WatchDog] allows monitoring multiple locks with custom error handling via callbacks.
//
//	// Define a callback to handle errors
//	errHandler := func(ctx context.Context, item *redlock.WatchItem, err error) {
//	    if err == context.Canceled {
//	        // Context cancellation is always the last error received
//	        log.Printf("WatchDog stopped for key %s\n", item.Key)
//	    } else {
//	        log.Printf("WatchDog error for key %s: %v\n", item.Key, err)
//	    }
//	}
//
//	// Start WatchDog with the callback
//	wd := redlock.NewWatchDog(locker,
//	    redlock.WithCallbacks(cbCtx, errHandler),
//	    // Watch item with specific interval (pass 0 for default ttl/2)
//	    redlock.WithItem("resource-1", "token-1", 10*time.Second, 2*time.Second),
//	)
//	go wd.Run(ctx)
//
// # Sentinel Errors
//
// The package exports sentinel errors for reliable error checking with [errors.Is]:
//   - [ErrLockAlreadyHeld]: returned by TryAcquire when the lock is held by another client
//   - [ErrLockNotHeld]: returned by TryExtend when the lock doesn't exist or fencing token doesn't match
//   - [ErrMaxRetryExceeded]: returned when retry attempts are exhausted
//   - [ErrValidityExpired]: returned when clock drift causes lock validity to expire
//
// # References
//
//   - Redis Redlock Algorithm: https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/
//   - Martin Kleppmann's Analysis: https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
package redlock
