# Redlock

[![Go Test](https://github.com/trviph/redlock/actions/workflows/test.yml/badge.svg)](https://github.com/trviph/redlock/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/trviph/redlock)](https://goreportcard.com/report/github.com/trviph/redlock)
[![Go Reference](https://pkg.go.dev/badge/github.com/trviph/redlock.svg)](https://pkg.go.dev/github.com/trviph/redlock)
[![codecov](https://codecov.io/gh/trviph/redlock/graph/badge.svg?token=CODECOV_TOKEN)](https://codecov.io/gh/trviph/redlock)

A distributed lock implementation in Go backed by Redis, supporting both single-instance locks and quorum-based multi-instance locks via the [Redlock algorithm](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/).

## Table of Contents

- [Redlock](#redlock)
  - [Table of Contents](#table-of-contents)
  - [Architecture \& Trade-offs](#architecture--trade-offs)
    - [Core Components](#core-components)
  - [Known Quirks \& Limitations](#known-quirks--limitations)
  - [Installation](#installation)
  - [Usage](#usage)
    - [Single Instance](#single-instance)
      - [Key Methods](#key-methods)
    - [Multi-Instance (Redlock Algorithm)](#multi-instance-redlock-algorithm)
    - [Watchdog Pattern (Auto-Renewal)](#watchdog-pattern-auto-renewal)
    - [Custom Retry Strategies](#custom-retry-strategies)
  - [Error Handling](#error-handling)
    - [Unwrapping Joined Errors](#unwrapping-joined-errors)
  - [Testing](#testing)
  - [License](#license)

## Architecture & Trade-offs

### Core Components

- **`Lock` (Single Instance)**
  - **Trade-off:** High performance (single network hop) vs. Lower availability (fails if the single Redis node goes down).
  - **Best for:** Non-critical background jobs where occasional failure isn't catastrophic.
- **`DistributedLock` (Multi-Instance)**
  - **Trade-off:** High availability and safety (survives `N/2` node failures) vs. Lower performance (multiple network hops).
  - **Best for:** Critical distributed coordination where safety and consensus are paramount.
- **`Waiter` (Retry Strategies)**
  - Controls backoff behavior (`JitterWait` vs `ExponentialWait`) to prevent thundering herd scenarios across clients trying to claim the same resource.
- **Fencing Tokens**
  - UUIDs generated upon lock acquisition. These are essential for pairing a lock owner with lock release/extension logic natively within the package.
  - *Note:* They are random UUIDs, not monotonically increasing counters, and cannot be used for external shielding (e.g., preventing split-brain writes in database storage).

---

## Known Quirks & Limitations

- **Partial Extensions on Quorum Failure:** When using `DistributedLock`, the `Extend` and `TryExtend` methods (and by extension the `Watch`, `WatchWithInterval`, and `WatchDog` utilities) suffer from a partial extension issue. If extending the lock fails to achieve quorum across the independent Redis instances, the successfully extended instances are **not** automatically rolled back. They will remain locked until their TTL naturally expires.

---

## Installation

```bash
go get github.com/trviph/redlock
```

## Usage

### Single Instance

```go
import (
    "context"
    "time"
    "github.com/redis/go-redis/v9"
    "github.com/trviph/redlock"
)

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
waiter := redlock.NewJitterWait(
    redlock.WithJitterMaxIteration(-1),                  // Default: -1 (infinite)
    redlock.WithJitterMinDelay(0),                       // Default: 0
    redlock.WithMaxJitterDuration(300*time.Millisecond), // Default: 300ms
)

lock := redlock.NewLock(rdb, redlock.WithWaiter(waiter))

// Alternatively, use Exponential Backoff:
// expWaiter := redlock.NewExponentialWait(
//     redlock.WithExpMinDelay(100*time.Millisecond), // Start wait time
//     redlock.WithExpMaxDelay(10*time.Second),       // Max wait time cap
//     redlock.WithExpFactor(2.0),                    // Multiplier
//     redlock.WithExpMaxIteration(10),               // Max retry attempts
// )
// lock := redlock.NewLock(rdb, redlock.WithWaiter(expWaiter))

ctx := context.Background()
key := "my-resource"
ttl := 10 * time.Second

// Acquire lock (retries until success, context cancellation, or max retries)
fencing, err := lock.Acquire(ctx, key, ttl)
if err != nil {
    panic(err)
}
defer lock.Release(ctx, key, fencing)

// Do work...
```

#### Key Methods

| Method             | Description                                                    |
| ------------------ | -------------------------------------------------------------- |
| `Acquire`          | Acquires lock with retry, returns fencing token                |
| `TryAcquire`       | Single attempt, no retry; returns `ErrLockAlreadyHeld` if held |
| `Extend`           | Extends TTL with retry if fencing token matches                |
| `TryExtend`        | Single extend attempt; returns `ErrLockNotHeld` on failure     |
| `AcquireOrExtend`  | Extends if held, otherwise acquires (with retry)               |
| `Release`          | Atomically releases lock if fencing token matches              |
| `ReleaseWithCount` | Releases lock and returns `ReleaseStatus` with detailed stats  |

> [!NOTE]
> If you require strict monotonic fencing tokens for external shielding, you can generate them yourself (e.g., using a separate counter) and pass them to the `AcquireWithFencing` or `TryAcquireWithFencing` methods. However, if strong consistency is a strict requirement, it is recommended to consider systems designed for it, such as **etcd** or **Zookeeper**, instead of Redis.

---

### Multi-Instance (Redlock Algorithm)

`DistributedLock` implements the Redlock algorithm for higher availability. It requires a quorum (N/2 + 1) to succeed.

```go
redis1 := redis.NewClient(&redis.Options{Addr: "redis1:6379"})
redis2 := redis.NewClient(&redis.Options{Addr: "redis2:6379"})
redis3 := redis.NewClient(&redis.Options{Addr: "redis3:6379"})

locks := []*redlock.Lock{
    redlock.NewLock(redis1),
    redlock.NewLock(redis2),
    redlock.NewLock(redis3),
}

waiter := redlock.NewJitterWait(
    redlock.WithJitterMaxIteration(-1),                  // Default: -1 (infinite)
    redlock.WithJitterMinDelay(0),                       // Default: 0
    redlock.WithMaxJitterDuration(300*time.Millisecond), // Default: 300ms
)

dl := redlock.NewDistributedLock(locks,
    redlock.WithClockDriftFactor(0.01),               // Default: 1%
    redlock.WithClockDriftBuffer(2*time.Millisecond), // Default: 2ms
    redlock.WithReleaseTimeout(5*time.Second),        // Default: 5s
    redlock.WithDistWaiter(waiter),
)

fencing, err := dl.Acquire(ctx, "my-resource", 30*time.Second)
if err != nil {
    panic(err)
}
defer dl.Release(ctx, "my-resource", fencing)
```

The API mirrors `Lock` for consistency (`Acquire`, `TryAcquire`, `Extend`, `TryExtend`, `AcquireOrExtend`, `Release`). It also provides `ReleaseWithCount` for detailed release statistics.

> [!TIP]
> Use an odd number of instances (3, 5, 7) for optimal fault tolerance.

---

### Watchdog Pattern (Auto-Renewal)

For long-running operations where duration is unknown, use a watchdog goroutine to periodically extend the lock. This pattern works with both `Lock` and `DistributedLock`:

```go
fencing, _ := lock.Acquire(ctx, key, 10*time.Second)

watchCtx, watchCancel := context.WithCancel(ctx)
defer watchCancel()

redlock.Watch(watchCtx, lock, key, fencing, 10*time.Second)

// Do long-running work...

watchCancel() // Stop the watchdog explicitly
lock.Release(ctx, key, fencing)
```

You can customize the extension interval using `WatchWithInterval` or utilize the full `WatchDog` struct for advanced callback handling, such as **early cancellation**:

```go
watchCtx, watchCancel := context.WithCancel(ctx)
defer watchCancel()

// Define a callback to handle errors and trigger early cancellation if the lock is lost
errHandler := func(ctx context.Context, item *redlock.WatchItem, err error) {
    if err == context.Canceled {
        log.Printf("WatchDog stopped for key %s", item.Key)
        return
    }
    
    log.Printf("WatchDog error: %v", err)

    // Stop the watchdog early if the lock no longer exists (e.g. expired)
    if errors.Is(err, redlock.ErrLockNotHeld) {
        log.Println("Lock lost! Triggering early cancellation...")
        watchCancel()
    }
}

wd := redlock.NewWatchDog(locker,
    redlock.WithErrorCallbacks(context.Background(), errHandler),
    redlock.WithItem("resource-1", "token-1", 10*time.Second, 2*time.Second),
)
go wd.Run(watchCtx)
```

> [!WARNING]
> The isolated background watchdog logic will **not** stop automatically if the lock is lost or fails to extend. It will continue attempting to extend the lock indefinitely until the provided `context` is canceled. This intentional design prevents premature termination during transient network failures.

---

### Custom Retry Strategies

Implement your own retry logic by satisfying the `Waiter` interface:

```go
type Waiter interface {
    Wait(ctx context.Context, times int) <-chan WaitInfo
}
```

**Implementation Nuances:**
- **0-indexed `times`**: The `times` argument starts at `0`. Your implementation **must** return immediately when `times == 0`.
- **Buffered Channel**: Use a buffered channel (e.g., `make(chan WaitInfo, 1)`) to avoid goroutine leaks if the caller stops listening.
- **Context Handling**: Respect `ctx.Done()` and return `WaitInfo{Err: ctx.Err()}` immediately if cancelled.

---

## Error Handling

The package provides sentinel errors for reliable error checking:

| Error                 | Description                                                                    |
| --------------------- | ------------------------------------------------------------------------------ |
| `ErrLockAlreadyHeld`  | Lock is held by another client                                                 |
| `ErrLockNotHeld`      | Attempting to extend or release an unowned lock                                |
| `ErrMaxRetryExceeded` | Maximum retry attempts exhausted                                               |
| `ErrValidityExpired`  | Lock acquired but validity expired due to clock drift (`DistributedLock` only) |

### Unwrapping Joined Errors

`DistributedLock` operations may join errors from multiple instances using `errors.Join()`. You can unwrap these for granular inspection:

```go
if unwrapper, ok := err.(interface{ Unwrap() []error }); ok {
    for _, e := range unwrapper.Unwrap() {
        log.Printf("Instance error: %v", e)
    }
}
```

> [!CAUTION]
> **Release Error Handling**: `Release` for `DistributedLock` returns an error if **any** single Redis instance fails to release the lock. This ensures you are aware of potential cleanup issues, even if the release was successful on the majority of nodes (quorum). Use `ReleaseWithCount` if you need detailed success rates.

---

## Testing

This project uses Docker Compose for integration testing:

```bash
# Start Redis instances
docker compose up -d

# Run tests
go test -v ./...

# Cleanup
docker compose down
```

## License

MIT
