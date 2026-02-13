# Redlock

[![Go Test](https://github.com/trviph/redlock/actions/workflows/test.yml/badge.svg)](https://github.com/trviph/redlock/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/trviph/redlock)](https://goreportcard.com/report/github.com/trviph/redlock)
[![Go Reference](https://pkg.go.dev/badge/github.com/trviph/redlock.svg)](https://pkg.go.dev/github.com/trviph/redlock)
[![codecov](https://codecov.io/gh/trviph/redlock/graph/badge.svg?token=CODECOV_TOKEN)](https://codecov.io/gh/trviph/redlock)

A distributed lock implementation in Go backed by Redis, supporting both single-instance locks and quorum-based multi-instance locks via the [Redlock algorithm](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/).

## Table of Contents

- [Redlock](#redlock)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Usage](#usage)
    - [Single Instance](#single-instance)
      - [Key Methods](#key-methods)
    - [Multi-Instance (Redlock Algorithm)](#multi-instance-redlock-algorithm)
    - [Watchdog Pattern (Auto-Renewal)](#watchdog-pattern-auto-renewal)
    - [Custom Retry Strategies](#custom-retry-strategies)
      - [Example: Fixed Delay Waiter](#example-fixed-delay-waiter)
      - [Implementation Nuances](#implementation-nuances)
  - [Error Handling](#error-handling)
    - [Unwrapping Joined Errors](#unwrapping-joined-errors)
  - [Testing](#testing)
  - [License](#license)

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
lock := redlock.NewLock(rdb,
    redlock.WithWaiter(redlock.NewJitterWait(
        redlock.WithJitterMaxIteration(-1),               // Default: -1 (infinite)
        redlock.WithJitterMinDelay(0),                    // Default: 0
        redlock.WithMaxJitterDuration(300*time.Millisecond), // Default: 300ms
    )),
)

// Alternatively, use Exponential Backoff:
// lock := redlock.NewLock(rdb,
//     redlock.WithWaiter(redlock.NewExponentialWait(
//         redlock.WithExpMinDelay(100*time.Millisecond), // Start wait time
//         redlock.WithExpMaxDelay(10*time.Second),      // Max wait time cap
//         redlock.WithExpFactor(2.0),                    // Multiplier
//         redlock.WithExpMaxIteration(10),               // Max retry attempts
//     )),
// )

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

| Method            | Description                                                    |
| ----------------- | -------------------------------------------------------------- |
| `Acquire`         | Acquires lock with retry, returns fencing token                |
| `TryAcquire`      | Single attempt, no retry; returns `ErrLockAlreadyHeld` if held |
| `Extend`          | Extends TTL with retry if fencing token matches                |
| `TryExtend`       | Single extend attempt; returns `ErrLockNotHeld` on failure     |
| `AcquireOrExtend` | Extends if held, otherwise acquires (with retry)               |
| `Release`         | Atomically releases lock if fencing token matches              |

> [!NOTE]
> The `fencing` token returned by `Acquire` is a random UUID used solely to identify the lock owner and prevent race conditions when extending or releasing the lock. It is **not** a monotonically increasing number and cannot be used for external shielding (e.g., preventing split-brain writes in storage systems) as described in [Martin Kleppmann's critique](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html).
>
> If you require strict monotonic fencing tokens for external shielding, you can generate them yourself (e.g., using a separate counter) and pass them to the `AcquireWithFencing` or `TryAcquireWithFencing` methods. However, if strong consistency is a strict requirement, it is recommended to consider systems designed for it, such as **etcd** or **Zookeeper**, instead of Redis.

---

### Multi-Instance (Redlock Algorithm)

`DistributedLock` implements the Redlock algorithm for higher availability. It acquires locks across multiple independent Redis instances and requires a quorum (N/2 + 1) to succeed.

```go
redis1 := redis.NewClient(&redis.Options{Addr: "redis1:6379"})
redis2 := redis.NewClient(&redis.Options{Addr: "redis2:6379"})
redis3 := redis.NewClient(&redis.Options{Addr: "redis3:6379"})

locks := []*redlock.Lock{
    redlock.NewLock(redis1),
    redlock.NewLock(redis2),
    redlock.NewLock(redis3),
}

dl := redlock.NewDistributedLock(locks,
    redlock.WithClockDriftFactor(0.01),                // Default: 1%
    redlock.WithClockDriftBuffer(2*time.Millisecond),  // Default: 2ms
    redlock.WithReleaseTimeout(5*time.Second),       // Default: 5s
    redlock.WithDistWaiter(redlock.NewJitterWait(
        redlock.WithJitterMaxIteration(-1),               // Default: -1 (infinite)
        redlock.WithJitterMinDelay(0),                    // Default: 0
        redlock.WithMaxJitterDuration(300*time.Millisecond), // Default: 300ms
    )),
    // Or use ExponentialWait:
    // redlock.WithDistWaiter(redlock.NewExponentialWait(...)),
)

fencing, err := dl.Acquire(ctx, "my-resource", 30*time.Second)
if err != nil {
    panic(err)
}
defer dl.Release(ctx, "my-resource", fencing)
```

The API mirrors `Lock` for consistency (`Acquire`, `TryAcquire`, `Extend`, `TryExtend`, `AcquireOrExtend`, `Release`).

> [!NOTE]
> Older configuration options (e.g., `WithMaxRetry`, `WithJitterDuration`) are deprecated
> but remain available for backward compatibility when using the default `JitterWait`.

> **Note:** Use an odd number of instances (3, 5, 7) for optimal fault tolerance.

---

### Watchdog Pattern (Auto-Renewal)

For long-running operations where duration is unknown, use a watchdog goroutine to periodically extend the lock. This pattern works with both `Lock` and `DistributedLock`:

```go
fencing, _ := lock.Acquire(ctx, key, 10*time.Second)

watchdogCtx, stop := context.WithCancel(ctx)
defer stop()

go func() {
    ticker := time.NewTicker(5 * time.Second) // Extend at ~half TTL
    defer ticker.Stop()
    for {
        select {
        case <-watchdogCtx.Done():
            return
        case <-ticker.C:
            lock.TryExtend(watchdogCtx, key, fencing, 10*time.Second)
        }
    }
}()

// Do long-running work...
lock.Release(ctx, key, fencing)
```

Alternatively, you can use the built-in `Watch` helper which simplifies this pattern:

```go
fencing, err := lock.Acquire(ctx, key, ttl)
if err != nil {
    // Handle error
}

watchCtx, watchCancel := context.WithCancel(ctx)
defer watchCancel()

redlock.Watch(watchCtx, lock, key, fencing, ttl)

// Do long-running work...

watchCancel() // Stop the watchdog
lock.Release(ctx, key, fencing)
```

You can also customize the extension interval using `WatchWithInterval`:

```go
// Check every 1 second instead of default ttl/2
redlock.WatchWithInterval(watchCtx, lock, key, fencing, ttl, 1*time.Second)
```

For more control on handling errors (logging, early stopping), use `WatchDog`:

```go
// Define a callback to handle errors
errHandler := func(ctx context.Context, item *redlock.WatchItem, err error) {
    if err == context.Canceled {
        // Context cancellation is always the last error received
        log.Printf("WatchDog stopped for key %s", item.Key)
        return
    }
    log.Printf("WatchDog error for key %s: %v", item.Key, err)
}

// Start WatchDog with the callback
wd := redlock.NewWatchDog(locker,
    redlock.WithCallbacks(cbCtx, errHandler),
    // Watch item with specific interval (pass 0 for default ttl/2)
    redlock.WithItem("resource-1", "token-1", 10*time.Second, 2*time.Second),
)
go wd.Run(ctx)
```

> [!WARNING]
> The watchdog goroutine (`Watch` or `WatchWithInterval`) will **not** stop automatically if the lock is lost or fails to extend. It will continue attempting to extend the lock indefinitely until the provided `context` is canceled.
>
> **Design Rationale:** This behavior is intentional to handle cases where the watchdog is started before the lock is successfully acquired (e.g., during a retry loop) or to survive transient network failures. It avoids prematurely killing the watchdog due to temporary errors.
>
> Always ensure you cancel the context when the operation is finished or if you detect that the lock has been lost.

### Custom Retry Strategies

You can implement your own retry logic by satisfying the `Waiter` interface:

```go
type Waiter interface {
    Wait(ctx context.Context, times int) <-chan WaitInfo
}
```

#### Example: Fixed Delay Waiter

```go
type FixedWait struct {
    delay time.Duration
    max   int
}

func (f *FixedWait) Wait(ctx context.Context, times int) <-chan redlock.WaitInfo {
    ch := make(chan redlock.WaitInfo, 1) // Buffered to prevent leaks
    defer close(ch)

    // 1. Handle initial attempt (times=0) immediately
    if times == 0 {
        ch <- redlock.WaitInfo{DoneAt: time.Now()}
        return ch
    }

    // 2. check max retries
    if f.max >= 0 && times > f.max {
        ch <- redlock.WaitInfo{Err: redlock.ErrMaxRetryExceeded}
        return ch
    }

    // 3. Wait for delay or context cancellation
    select {
    case <-ctx.Done():
        ch <- redlock.WaitInfo{Err: ctx.Err()}
    case t := <-time.After(f.delay):
        ch <- redlock.WaitInfo{DoneAt: t}
    }
    
    return ch
}
```

#### Implementation Nuances

- **0-indexed `times`**: The `times` argument starts at `0` for the initial acquisition attempt. Your implementation **must** return immediately (no delay) when `times == 0`.
- **Buffered Channel**: Always use a buffered channel (`make(chan WaitInfo, 1)`). This ensures that if the caller cancels or stops waiting, your goroutine (if you spawned one) or the channel send doesn't block forever.
- **Context Handling**: You must respect `ctx.Done()` and return `WaitInfo{Err: ctx.Err()}` immediately if cancelled.
- **Error Propagation**: Return `ErrMaxRetryExceeded` when your retry limit is reached.


## Error Handling

The package provides sentinel errors for reliable error checking:

| Error                 | Description                                                                    |
| --------------------- | ------------------------------------------------------------------------------ |
| `ErrLockAlreadyHeld`  | Lock is held by another client (from `TryAcquire`)                             |
| `ErrLockNotHeld`      | Lock doesn't exist or fencing token mismatch (from `TryExtend`)                |
| `ErrMaxRetryExceeded` | Maximum retry attempts exhausted                                               |
| `ErrValidityExpired`  | Lock acquired but validity expired due to clock drift (`DistributedLock` only) |

```go
fencing, err := lock.Acquire(ctx, key, ttl)
if err != nil {
    switch {
    case errors.Is(err, redlock.ErrLockAlreadyHeld):
        log.Println("Resource busy")
    case errors.Is(err, redlock.ErrMaxRetryExceeded):
        log.Println("Max retries reached")
    case errors.Is(err, redlock.ErrValidityExpired):
        log.Println("Lock validity expired")
    case errors.Is(err, redlock.ErrLockNotHeld):
        log.Println("Cannot extend: lock not held")
    case errors.Is(err, context.DeadlineExceeded):
        log.Println("Timeout")
    default:
        log.Printf("Error: %v", err)
    }
}
```

### Unwrapping Joined Errors

`DistributedLock` operations may join errors from multiple instances using `errors.Join()`:

```go
if unwrapper, ok := err.(interface{ Unwrap() []error }); ok {
    for _, e := range unwrapper.Unwrap() {
        log.Printf("Instance error: %v", e)
    }
}
```

> [!CAUTION]
> **Release Error Handling**: The `Release` method for `DistributedLock` will return an error if **any** single Redis instance fails to release the lock, even if the release was successful on the majority of nodes (quorum).
>
> This ensures you are aware of potential cleanup issues. It does **not** necessarily mean the lock is still valid or held. You should inspect the error (using `errors.Join` unwrapping as shown above) to decide how to proceed (e.g., ignore if it was a minor network blip on one node).

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
