# Redlock

[![Go Test](https://github.com/trviph/redlock/actions/workflows/test.yml/badge.svg)](https://github.com/trviph/redlock/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/trviph/redlock)](https://goreportcard.com/report/github.com/trviph/redlock)
[![Go Reference](https://pkg.go.dev/badge/github.com/trviph/redlock.svg)](https://pkg.go.dev/github.com/trviph/redlock)


Redlock is a distributed lock service implementation in Go backed by Redis. It provides a distributed lock for your application.

> **Note**: This package implements the standard single-instance Redis distributed lock pattern. It does **not** implement the multi-master [Redlock algorithm](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/). If you need the fault tolerance of the full Redlock algorithm, you might want to look for other libraries.

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
dl := redlock.NewLock(rdb, redlock.SetMaxRetry(3))

// Set custom jitter duration (default is 300ms)
dl := redlock.NewLock(rdb, redlock.SetJitterDuration(500*time.Millisecond))
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
err := dl.Release(ctx, key, fencing)
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
