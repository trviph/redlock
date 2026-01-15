# Redlock

[![Go Test](https://github.com/trviph/redlock/actions/workflows/test.yml/badge.svg)](https://github.com/trviph/redlock/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/trviph/redlock)](https://goreportcard.com/report/github.com/trviph/redlock)
[![Go Reference](https://pkg.go.dev/badge/github.com/trviph/redlock.svg)](https://pkg.go.dev/github.com/trviph/redlock)


Redlock is a distributed lock service implementation in Go backed by Redis. It provides both single-instance and multi-instance (Redlock algorithm) distributed locks.

## Installation

```bash
go get github.com/trviph/redlock
```

## Usage

### Initialization

```go
import (
    "context"
    "time"
    "github.com/redis/go-redis/v9"
    "github.com/trviph/redlock"
)

// Initialize Redis client
rdb := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})

// Initialize Redlock
dl := redlock.NewLock(rdb)
```

### Configuration

You can configure the lock behavior using functional options:

```go
// Set custom retry limit (default is -1, which means infinite retry)
lock := redlock.NewLock(rdb, redlock.WithMaxRetry(3))

// Set custom jitter duration (default is 300ms)
lock := redlock.NewLock(rdb, redlock.WithJitterDuration(500*time.Millisecond))
```

### Acquire a Lock

```go
ctx := context.Background()
key := "my-resource-lock"
ttl := 10 * time.Second

// err will be nil if lock is acquired
fencing, err := dl.Acquire(ctx, key, ttl)
if err != nil {
    // Handle error (failed to acquire)
    panic(err)
}

// Lock acquired
defer dl.Release(ctx, key, fencing)

// Do work...
```

### Try to Acquire a Lock (No Retry)

If you want to attempt to acquire the lock exactly once without any retries (fail fast), use `TryAcquire`.

```go
fencing, err := lock.TryAcquire(ctx, key, ttl)
if err != nil {
    if errors.Is(err, redlock.ErrLockAlreadyHeld) {
        // Lock is already taken
    } else {
        // Error (e.g. Redis connection)
    }
}
```

### Acquire or Extend a Lock

If your work takes longer than expected, or if you want to ensure you hold the lock even if it might have expired (but no one else took it), you can use `AcquireOrExtend`.

```go
// Extends the lock by another 10 seconds, or re-acquires it if missing
err := dl.AcquireOrExtend(ctx, key, fencing, 10*time.Second)
if err != nil {
    // Handle error
}
```

### Release a Lock

```go
err := lock.Release(ctx, key, fencing)
```

## DistributedLock (Multi-Instance Redlock)

For higher availability, use `DistributedLock` which implements the [Redlock algorithm](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/) across multiple independent Redis instances.

### Setup

```go
// Connect to multiple independent Redis instances
redis1 := redis.NewClient(&redis.Options{Addr: "redis1:6379"})
redis2 := redis.NewClient(&redis.Options{Addr: "redis2:6379"})
redis3 := redis.NewClient(&redis.Options{Addr: "redis3:6379"})

// Create individual locks for each instance
locks := []*redlock.Lock{
    redlock.NewLock(redis1),
    redlock.NewLock(redis2),
    redlock.NewLock(redis3),
}

// Create distributed lock with quorum-based consensus
dl := redlock.NewDistributedLock(locks,
    redlock.WithClockDriftFactor(0.01), // 1% clock drift (default)
)
```

### Usage

The API mirrors `Lock` for consistency:

```go
// Acquire with retry
fencing, err := dl.Acquire(ctx, "my-resource", 30*time.Second)
if err != nil {
    // Failed to achieve quorum
}
defer dl.Release(ctx, "my-resource", fencing)

// Try acquire (no retry, fail fast)
fencing, err := dl.TryAcquire(ctx, "my-resource", 30*time.Second)
if errors.Is(err, redlock.ErrLockAlreadyHeld) {
    // Could not achieve quorum
}

// Extend or re-acquire
err := dl.AcquireOrExtend(ctx, "my-resource", fencing, 30*time.Second)
```

## Sentinel Errors

The package exports sentinel errors for reliable error checking:

```go
import "errors"

// Check if lock is already held
if errors.Is(err, redlock.ErrLockAlreadyHeld) { ... }

// Check if max retries exceeded
if errors.Is(err, redlock.ErrMaxRetryExceeded) { ... }

// Check if lock validity expired (clock drift)
if errors.Is(err, redlock.ErrValidityExpired) { ... }
```

## Testing

This project uses Docker Compose to spin up a Redis instance for integration testing.

1.  **Start Redis:**
    ```bash
    docker compose up -d
    ```

2.  **Run Tests:**
    ```bash
    go test -v ./...
    ```

3.  **Cleanup:**
    ```bash
    docker compose down
    ```

## License

MIT
