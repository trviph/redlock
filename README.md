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
    redlock.WithMaxRetry(-1),                         // Default: -1 (infinite)
    redlock.WithMinRetryDelay(0),                     // Default: 0
    redlock.WithJitterDuration(300*time.Millisecond), // Default: 300ms
)

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
    redlock.WithDistMaxRetry(-1),                    // Default: -1 (infinite)
    redlock.WithDistMinRetryDelay(0),                // Default: 0
    redlock.WithDistMaxJitterDuration(300*time.Millisecond), // Default: 300ms
)

fencing, err := dl.Acquire(ctx, "my-resource", 30*time.Second)
if err != nil {
    panic(err)
}
defer dl.Release(ctx, "my-resource", fencing)
```

The API mirrors `Lock` for consistency (`Acquire`, `TryAcquire`, `Extend`, `TryExtend`, `AcquireOrExtend`, `Release`).

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
