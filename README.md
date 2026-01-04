# Redlock

[![Go Test](https://github.com/trviph/redlock/actions/workflows/test.yml/badge.svg)](https://github.com/trviph/redlock/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/trviph/redlock)](https://goreportcard.com/report/github.com/trviph/redlock)
[![Go Reference](https://pkg.go.dev/badge/github.com/trviph/redlock.svg)](https://pkg.go.dev/github.com/trviph/redlock)


Redlock is a distributed lock service implementation in Go. It provides a distributed lock for your application following the [pattern](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/) published by Redis.

**NOTE:** This is a demonstration for my blog post [Distributed Lock with Redis](https://vinhphuoc.dev/en/posts/redlock). While it is working as intended, please understand that it is not actively maintained.

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
dl := redlock.NewClient(rdb)
```

### Acquire a Lock

```go
ctx := context.Background()
key := "my-resource-lock"
ttl := 10 * time.Second

// cmd.Val() will be true if lock is acquired
cmd, fencing, err := dl.Acquire(ctx, key, ttl)
if err != nil {
    panic(err)
}

if cmd.Val() {
    // Lock acquired
    defer dl.Release(ctx, key, fencing)
    
    // Do work...
}
```

### Acquire or Extend a Lock

If your work takes longer than expected, or if you want to ensure you hold the lock even if it might have expired (but no one else took it), you can use `AcquireOrExtend`.

```go
// Extends the lock by another 10 seconds, or re-acquires it if missing
cmd, err := dl.AcquireOrExtend(ctx, key, fencing, 10*time.Second)
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
