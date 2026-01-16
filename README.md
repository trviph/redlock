# Redlock

[![Go Test](https://github.com/trviph/redlock/actions/workflows/test.yml/badge.svg)](https://github.com/trviph/redlock/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/trviph/redlock)](https://goreportcard.com/report/github.com/trviph/redlock)
[![Go Reference](https://pkg.go.dev/badge/github.com/trviph/redlock.svg)](https://pkg.go.dev/github.com/trviph/redlock)


Redlock is a distributed lock service implementation in Go backed by Redis. It provides both single-instance and multi-instance (Redlock algorithm) distributed locks.

## Table of Contents

- [Redlock](#redlock)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Usage](#usage)
    - [Initialization](#initialization)
    - [Configuration](#configuration)
    - [Acquire a Lock](#acquire-a-lock)
    - [Try to Acquire a Lock (No Retry)](#try-to-acquire-a-lock-no-retry)
    - [Acquire or Extend a Lock](#acquire-or-extend-a-lock)
    - [Extend Lock TTL](#extend-lock-ttl)
    - [Watchdog Pattern (Auto-Renewal)](#watchdog-pattern-auto-renewal)
    - [Error Handling](#error-handling)
      - [Unwrapping Joined Errors](#unwrapping-joined-errors)
    - [Release a Lock](#release-a-lock)
  - [DistributedLock (Multi-Instance Redlock)](#distributedlock-multi-instance-redlock)
    - [Setup](#setup)
    - [Usage](#usage-1)
  - [Sentinel Errors](#sentinel-errors)
  - [Testing](#testing)
  - [License](#license)

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

### Extend Lock TTL

If you need to extend a lock you already hold without the risk of re-acquiring it (safer for long-running operations):

```go
// Extend with retry until success or context cancellation
err := lock.Extend(ctx, key, fencing, 30*time.Second)
if err != nil {
    // Lock was lost or fencing token doesn't match
}
```

For fail-fast behavior (no retries):

```go
err := lock.TryExtend(ctx, key, fencing, 30*time.Second)
if errors.Is(err, redlock.ErrLockNotHeld) {
    // Lock doesn't exist or fencing token doesn't match
}
```

### Watchdog Pattern (Auto-Renewal)

For long-running operations where you don't know the duration upfront, use a watchdog goroutine to periodically extend the lock:

```go
func doWorkWithWatchdog(ctx context.Context, lock *redlock.Lock, key string) error {
    fencing, err := lock.Acquire(ctx, key, 10*time.Second)
    if err != nil {
        return err
    }

    // Create a context that cancels when work is done
    watchdogCtx, stopWatchdog := context.WithCancel(ctx)
    defer stopWatchdog()

    // Start watchdog goroutine
    watchdogErr := make(chan error, 1)
    go func() {
        ticker := time.NewTicker(5 * time.Second) // Extend at half the TTL
        defer ticker.Stop()
        for {
            select {
            case <-watchdogCtx.Done():
                return
            case <-ticker.C:
                if err := lock.TryExtend(watchdogCtx, key, fencing, 10*time.Second); err != nil {
                    watchdogErr <- err
                    return
                }
            }
        }
    }()

    // Do the actual work
    if err := doWork(ctx); err != nil {
        return err
    }

    // Check if watchdog encountered an error
    select {
    case err := <-watchdogErr:
        return fmt.Errorf("lock lost during work: %w", err)
    default:
    }

    return lock.Release(ctx, key, fencing)
}
```

> **Tip:** Extend at roughly half the TTL interval to provide a safety margin.

### Error Handling

The package provides sentinel errors for reliable error checking:

```go
fencing, err := lock.Acquire(ctx, key, ttl)
if err != nil {
    switch {
    case errors.Is(err, redlock.ErrLockAlreadyHeld):
        // Lock is held by another client (only from TryAcquire)
        log.Println("Resource is busy, try again later")
        
    case errors.Is(err, redlock.ErrMaxRetryExceeded):
        // Retry limit reached without acquiring the lock
        log.Println("Could not acquire lock after max retries")
        
    case errors.Is(err, redlock.ErrValidityExpired):
        // Lock acquired but validity expired due to clock drift (DistributedLock)
        log.Println("Lock validity expired, operation may not be safe")
        
    case errors.Is(err, redlock.ErrLockNotHeld):
        // Extend/TryExtend: lock doesn't exist or fencing doesn't match
        log.Println("Cannot extend: lock not held")
        
    case errors.Is(err, context.DeadlineExceeded):
        // Context timeout
        log.Println("Operation timed out")
        
    default:
        // Redis connection error or other failure
        log.Printf("Unexpected error: %v", err)
    }
    return err
}
```

#### Unwrapping Joined Errors

When `DistributedLock` operations fail on multiple instances, errors are joined using `errors.Join()`. To inspect individual errors:

```go
// Unwrap to get individual errors from a joined error
if unwrapper, ok := err.(interface{ Unwrap() []error }); ok {
    for _, e := range unwrapper.Unwrap() {
        log.Printf("Instance error: %v", e)
    }
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
