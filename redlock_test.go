package redlock_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/trviph/redlock"
)

func setupRedis(t *testing.T) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis is not available: %v", err)
	}
	return rdb
}

func TestRedlock_Acquire(t *testing.T) {
	rdb := setupRedis(t)
	defer rdb.Close()
	ctx := context.Background()

	dl := redlock.NewLock(rdb)
	key := "test-lock-" + uuid.NewString()

	// 1. Acquire successfully
	cmd, fencing, err := dl.Acquire(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if !cmd.Val() {
		t.Errorf("Expected cmd.Val() to be true")
	}
	if fencing == "" {
		t.Errorf("Expected fencing to be non-empty")
	}

	// 2. Fail to acquire same key (should fail immediately or retry? Acquire retries logic)
	// Acquire loops until success or context cancel.
	// If we want to test that it doesn't acquire IMMEDIATELY, we'd need to mock or inspect.
	// But since it's blocking, calling Acquire again with same key will BLOCK until timeout or existing lock expires.
	// Wait, Acquire logic:
	// loop { SetNX; if success return; sleep }
	// So if I call Acquire again, it will BLOCK.
	// I should test this blocking behavior or use a timeout.

	ctxTimeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	cmd2, _, err2 := dl.Acquire(ctxTimeout, key, 10*time.Second)
	if err2 == nil {
		t.Errorf("Expected acquire to timeout/fail when key locked, got success")
	}
	// We expect err2 to be context deadline exceeded usually, or check if cmd2 is nil/false.
	if cmd2 != nil && cmd2.Val() {
		t.Errorf("Should not have acquired lock")
	}

	// Cleanup
	rdb.Del(ctx, key)
}

func TestRedlock_AcquireOrExtend(t *testing.T) {
	rdb := setupRedis(t)
	defer rdb.Close()
	ctx := context.Background()

	dl := redlock.NewLock(rdb)
	key := "test-extend-" + uuid.NewString()

	// 1. Acquire
	_, fencing, err := dl.Acquire(ctx, key, 1*time.Second) // Short TTL
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	// 2. Extend successfully
	cmd, err := dl.AcquireOrExtend(ctx, key, fencing, 10*time.Second)
	if err != nil {
		t.Fatalf("AcquireOrExtend failed: %v", err)
	}
	if cmd == nil {
		t.Fatal("Expected cmd to be not nil")
	}
	val, _ := cmd.Int64()
	if val <= 0 {
		t.Errorf("Expected positive result from AcquireOrExtend, got %v", val)
	}

	// Verify TTL checking
	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("Failed to get TTL: %v", err)
	}
	if ttl <= 5*time.Second {
		t.Errorf("Expected TTL > 5s, got %v", ttl)
	}

	// 3. Re-acquire successfully (simulate expiration/deletion)
	rdb.Del(ctx, key)
	cmd3, err3 := dl.AcquireOrExtend(ctx, key, fencing, 10*time.Second)
	if err3 != nil {
		t.Fatalf("AcquireOrExtend (re-acquire) failed: %v", err3)
	}
	val3, _ := cmd3.Int64()
	if val3 <= 0 {
		t.Errorf("Expected positive result from re-acquire, got %v", val3)
	}
	// Verify it exists again
	if rdb.Exists(ctx, key).Val() != 1 {
		t.Errorf("Expected key to exist after re-acquire")
	}

	// 4. Fail to extend with wrong fencing
	wrongFencing := "wrong-" + uuid.NewString()

	// AcquireOrExtend also loops. Use timeout.
	ctxTimeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	_, err2 := dl.AcquireOrExtend(ctxTimeout, key, wrongFencing, 10*time.Second)
	if err2 == nil {
		// Should timeout
		// But strictly speaking Redlock returns nil, ctx.Err() on context done
		// So err2 should be non-nil.
		t.Errorf("Expected error/timeout on wrong fencing")
	}

	// Cleanup
	rdb.Del(ctx, key)
}

func TestRedlock_Release(t *testing.T) {
	rdb := setupRedis(t)
	defer rdb.Close()
	ctx := context.Background()

	dl := redlock.NewLock(rdb)
	key := "test-release-" + uuid.NewString()

	// 1. Acquire
	_, fencing, err := dl.Acquire(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	// Verify exists
	if rdb.Exists(ctx, key).Val() != 1 {
		t.Errorf("Expected key to exist")
	}

	// 2. Release successfully
	err = dl.Release(ctx, key, fencing)
	if err != nil {
		t.Errorf("Release failed: %v", err)
	}

	// Verify gone
	if rdb.Exists(ctx, key).Val() != 0 {
		t.Errorf("Expected key to be deleted")
	}
}

func TestRedlock_MinRetryDelay(t *testing.T) {
	rdb := setupRedis(t)
	defer rdb.Close()
	ctx := context.Background()

	// 1. Acquire with client A
	dlA := redlock.NewLock(rdb)
	key := "test-min-wait-" + uuid.NewString()

	_, _, err := dlA.Acquire(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("Client A failed to acquire: %v", err)
	}
	defer rdb.Del(ctx, key)

	// 2. Try with client B with min wait
	minWait := 200 * time.Millisecond
	dlB := redlock.NewLock(rdb, redlock.SetMinRetryDelay(minWait), redlock.SetMaxRetry(1))

	start := time.Now()
	// This is expected to fail.
	// We set MaxRetry to 1, which means:
	// 1. Initial attempt -> fails -> wait (minWait + jitter)
	// 2. Retry #1 -> fails -> wait (minWait + jitter)
	// 3. Retry #2 (which is > maxRetry 1) -> aborts
	// Total wait time should be at least 2 * minWait.

	_, _, errB := dlB.Acquire(ctx, key, 10*time.Second)
	elapsed := time.Since(start)

	if errB == nil {
		t.Fatal("Client B should have failed to acquire")
	}

	expectedDuration := 2 * minWait
	if elapsed < expectedDuration {
		t.Errorf("Expected duration >= %v, got %v", expectedDuration, elapsed)
	}
}
