// Package redlock provides Redis-backed distributed lock implementations.
//
// This package offers two complementary lock types for different deployment scenarios
// and emphasizes strict safety through fencing tokens and consensus.
//
// For in-depth usage examples, idiom references, and integration guides,
// see the package README.md.
//
// # Core Components
//
//   - [Lock]: A distributed lock backed by a single Redis instance. It supports automatic
//     retries, configurable backoff (via [Waiter]), and atomic operations using Lua scripts.
//   - [DistributedLock]: An implementation of the Redlock algorithm for environments
//     requiring high availability. It acquires locks across multiple independent Redis
//     instances and uses quorum-based consensus (N/2 + 1) to determine ownership.
//   - [Waiter]: An interface for controlling retry behavior and backoff strategies
//     during lock acquisition. Built-in implementations include [JitterWait] and [ExponentialWait].
//
// # Trade-offs
//
//   - Performance vs Safety: [Lock] provides faster acquisition via a single network hop
//     but sacrifices availability if the Redis node fails. [DistributedLock] guarantees
//     safety and availability during node failures at the cost of higher latency.
//   - Auto-renewal constraints: Background watchdogs ([Watch], [WatchWithInterval], [WatchDog])
//     rely exclusively on context cancellation for termination. They will NOT halt automatically if
//     the underlying lock is lost or TTL extension fails.
//
// # Fencing Tokens
//
// Fencing tokens are UUID strings generated upon successful lock acquisition.
// These tokens must be passed to release or extend operations to prevent race conditions
// and unsafe lock hand-offs. They can also be used by downstream systems to detect stale locks.
//
// # Configuration
//
// Both lock types use functional options for configuration:
//   - [Lock]: [WithWaiter] (configuring [JitterWaitOption] or [ExponentialWaitOption]).
//   - [DistributedLock]: [WithClockDriftFactor], [WithClockDriftBuffer], [WithReleaseTimeout],
//     and [WithDistWaiter].
//
// # Sentinel Errors
//
// The package exports sentinel errors for reliable error checking with [errors.Is]:
//   - [ErrLockAlreadyHeld]: The requested lock is currently owned by another client.
//   - [ErrLockNotHeld]: Attempting to extend or release an unowned lock.
//   - [ErrMaxRetryExceeded]: Lock acquisition failed after exhausting all retry attempts.
//   - [ErrValidityExpired]: The distributed lock was acquired, but its validity duration
//     was completely consumed by clock drift or acquisition latency.
//
// # References
//
//   - Redis Redlock Algorithm: https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/
//   - Martin Kleppmann's Analysis: https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
package redlock
