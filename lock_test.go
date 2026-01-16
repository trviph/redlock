package redlock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/trviph/redlock"
)

func setupRedis(t *testing.T, port string) *redis.Client {
	if port == "" {
		port = "6379"
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:" + port,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis on port %s is not available: %v", port, err)
	}
	t.Cleanup(func() {
		rdb.Close()
	})
	return rdb
}

func TestLock_Acquire(t *testing.T) {
	rdb := setupRedis(t, "6379")
	ctx := context.Background()
	dl := redlock.NewLock(rdb)
	key := "test-lock-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	t.Run("Success", func(t *testing.T) {
		fencing, err := dl.Acquire(ctx, key, 10*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
		if fencing == "" {
			t.Errorf("Expected fencing to be non-empty")
		}
	})

	t.Run("FailWhenAlreadyHeld", func(t *testing.T) {
		ctxTimeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()

		_, err := dl.Acquire(ctxTimeout, key, 10*time.Second)
		if err == nil {
			t.Errorf("Expected acquire to timeout/fail when key locked, got success")
		}
	})
}

func TestLock_TryAcquire(t *testing.T) {
	rdb := setupRedis(t, "6379")
	ctx := context.Background()
	dl := redlock.NewLock(rdb)
	key := "test-try-acquire-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	t.Run("Success", func(t *testing.T) {
		fencing, err := dl.TryAcquire(ctx, key, 10*time.Second)
		if err != nil {
			t.Fatalf("TryAcquire failed: %v", err)
		}
		if fencing == "" {
			t.Error("Expected fencing token")
		}
	})

	t.Run("FailsImmediatelyWhenHeld", func(t *testing.T) {
		start := time.Now()
		_, err := dl.TryAcquire(ctx, key, 10*time.Second)
		elapsed := time.Since(start)

		if err == nil || !errors.Is(err, redlock.ErrLockAlreadyHeld) {
			t.Errorf("Expected ErrLockAlreadyHeld, got %v", err)
		}

		if elapsed > 100*time.Millisecond {
			t.Errorf("TryAcquire took too long: %v", elapsed)
		}
	})
}

func TestLock_AcquireOrExtend(t *testing.T) {
	rdb := setupRedis(t, "6379")
	ctx := context.Background()
	dl := redlock.NewLock(rdb)
	key := "test-extend-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	var fencing string

	t.Run("AcquireFirst", func(t *testing.T) {
		var err error
		fencing, err = dl.Acquire(ctx, key, 1*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
	})

	t.Run("ExtendSuccessfully", func(t *testing.T) {
		err := dl.AcquireOrExtend(ctx, key, fencing, 10*time.Second)
		if err != nil {
			t.Fatalf("AcquireOrExtend failed: %v", err)
		}

		ttl, err := rdb.TTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("Failed to get TTL: %v", err)
		}
		if ttl <= 5*time.Second {
			t.Errorf("Expected TTL > 5s, got %v", ttl)
		}
	})

	t.Run("ReAcquireAfterDeletion", func(t *testing.T) {
		rdb.Del(ctx, key)
		err := dl.AcquireOrExtend(ctx, key, fencing, 10*time.Second)
		if err != nil {
			t.Fatalf("AcquireOrExtend (re-acquire) failed: %v", err)
		}
		if rdb.Exists(ctx, key).Val() != 1 {
			t.Errorf("Expected key to exist after re-acquire")
		}
	})

	t.Run("FailWithWrongFencing", func(t *testing.T) {
		wrongFencing := "wrong-" + uuid.NewString()
		ctxTimeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()

		err := dl.AcquireOrExtend(ctxTimeout, key, wrongFencing, 10*time.Second)
		if err == nil {
			t.Errorf("Expected error/timeout on wrong fencing")
		}
	})
}

func TestLock_Extend(t *testing.T) {
	rdb := setupRedis(t, "6379")
	ctx := context.Background()
	dl := redlock.NewLock(rdb)
	key := "test-extend-only-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	t.Run("ExtendExistingLock", func(t *testing.T) {
		fencing, err := dl.Acquire(ctx, key, 2*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		err = dl.Extend(ctx, key, fencing, 10*time.Second)
		if err != nil {
			t.Fatalf("Extend failed: %v", err)
		}

		ttl, err := rdb.TTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("Failed to get TTL: %v", err)
		}
		if ttl <= 8*time.Second {
			t.Errorf("Expected TTL > 8s after extend, got %v", ttl)
		}
	})
}

func TestLock_TryExtend(t *testing.T) {
	rdb := setupRedis(t, "6379")
	ctx := context.Background()
	dl := redlock.NewLock(rdb)
	key := "test-tryextend-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	t.Run("FailsWhenLockNotExists", func(t *testing.T) {
		err := dl.TryExtend(ctx, key, "any-fencing", 10*time.Second)
		if !errors.Is(err, redlock.ErrLockNotHeld) {
			t.Errorf("Expected ErrLockNotHeld, got %v", err)
		}
	})

	t.Run("FailsWithWrongFencing", func(t *testing.T) {
		_, err := dl.Acquire(ctx, key, 10*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		err = dl.TryExtend(ctx, key, "wrong-fencing", 10*time.Second)
		if !errors.Is(err, redlock.ErrLockNotHeld) {
			t.Errorf("Expected ErrLockNotHeld, got %v", err)
		}
	})

	t.Run("SucceedsWithCorrectFencing", func(t *testing.T) {
		key2 := "test-tryextend-2-" + uuid.NewString()
		t.Cleanup(func() { rdb.Del(ctx, key2) })

		fencing, err := dl.Acquire(ctx, key2, 2*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		err = dl.TryExtend(ctx, key2, fencing, 10*time.Second)
		if err != nil {
			t.Errorf("TryExtend should succeed, got %v", err)
		}
	})
}

func TestLock_Release(t *testing.T) {
	rdb := setupRedis(t, "6379")
	ctx := context.Background()
	dl := redlock.NewLock(rdb)
	key := "test-release-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	t.Run("Success", func(t *testing.T) {
		fencing, err := dl.Acquire(ctx, key, 10*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		if rdb.Exists(ctx, key).Val() != 1 {
			t.Errorf("Expected key to exist")
		}

		err = dl.Release(ctx, key, fencing)
		if err != nil {
			t.Errorf("Release failed: %v", err)
		}

		if rdb.Exists(ctx, key).Val() != 0 {
			t.Errorf("Expected key to be deleted")
		}
	})

	t.Run("WrongFencingDoesNotDelete", func(t *testing.T) {
		key2 := "test-release-wrong-" + uuid.NewString()
		t.Cleanup(func() { rdb.Del(ctx, key2) })

		fencing, err := dl.Acquire(ctx, key2, 10*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		wrongFencing := "wrong-" + uuid.NewString()
		_ = dl.Release(ctx, key2, wrongFencing)

		if rdb.Exists(ctx, key2).Val() != 1 {
			t.Errorf("Expected key to still exist after release with wrong fencing")
		}

		err = dl.Release(ctx, key2, fencing)
		if err != nil {
			t.Errorf("Release with correct fencing failed: %v", err)
		}
	})
}

func TestLock_MaxRetryExceeded(t *testing.T) {
	rdb := setupRedis(t, "6379")
	ctx := context.Background()

	dlA := redlock.NewLock(rdb)
	key := "test-maxretry-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	_, err := dlA.Acquire(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("Client A failed to acquire: %v", err)
	}

	dlB := redlock.NewLock(rdb,
		redlock.WithMaxRetry(2),
		redlock.WithMinRetryDelay(50*time.Millisecond),
	)

	_, err = dlB.Acquire(ctx, key, 10*time.Second)
	if !errors.Is(err, redlock.ErrMaxRetryExceeded) {
		t.Errorf("Expected ErrMaxRetryExceeded, got %v", err)
	}
}

func TestLock_MinRetryDelay(t *testing.T) {
	rdb := setupRedis(t, "6379")
	ctx := context.Background()

	dlA := redlock.NewLock(rdb)
	key := "test-min-wait-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	_, err := dlA.Acquire(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("Client A failed to acquire: %v", err)
	}

	minWait := 200 * time.Millisecond
	dlB := redlock.NewLock(rdb, redlock.WithMinRetryDelay(minWait), redlock.WithMaxRetry(1))

	start := time.Now()
	_, errB := dlB.Acquire(ctx, key, 10*time.Second)
	elapsed := time.Since(start)

	if errB == nil {
		t.Fatal("Client B should have failed to acquire")
	}

	if elapsed < minWait {
		t.Errorf("Expected duration >= %v, got %v", minWait, elapsed)
	}
}
