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
		port = testRedisPort1
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
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()
	dl := redlock.NewLock(rdb)
	key := "test-lock-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	t.Run("Success", func(t *testing.T) {
		// Run
		fencing, err := dl.Acquire(ctx, key, testTTLLong)

		// Assert
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
		if fencing == "" {
			t.Errorf("Expected fencing to be non-empty")
		}
	})

	t.Run("FailWhenAlreadyHeld", func(t *testing.T) {
		// Setup - key already held from previous subtest
		ctxTimeout, cancel := context.WithTimeout(ctx, testTimeoutShort)
		defer cancel()

		// Run
		_, err := dl.Acquire(ctxTimeout, key, testTTLLong)

		// Assert
		if err == nil {
			t.Errorf("Expected acquire to timeout/fail when key locked, got success")
		}
	})
}

func TestLock_TryAcquire(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()
	dl := redlock.NewLock(rdb)
	key := "test-try-acquire-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	t.Run("Success", func(t *testing.T) {
		// Run
		fencing, err := dl.TryAcquire(ctx, key, testTTLLong)

		// Assert
		if err != nil {
			t.Fatalf("TryAcquire failed: %v", err)
		}
		if fencing == "" {
			t.Error("Expected fencing token")
		}
	})

	t.Run("FailsImmediatelyWhenHeld", func(t *testing.T) {
		// Setup - key already held from previous subtest
		start := time.Now()

		// Run
		_, err := dl.TryAcquire(ctx, key, testTTLLong)
		elapsed := time.Since(start)

		// Assert
		if err == nil || !errors.Is(err, redlock.ErrLockAlreadyHeld) {
			t.Errorf("Expected ErrLockAlreadyHeld, got %v", err)
		}
		if elapsed > testTimingMargin {
			t.Errorf("TryAcquire should be immediate, took: %v (expected < %v)", elapsed, testTimingMargin)
		}
	})
}

func TestLock_AcquireOrExtend(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()
	dl := redlock.NewLock(rdb)
	key := "test-extend-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	var fencing string
	initialTTL := 1 * time.Second
	extendedTTL := testTTLLong

	t.Run("AcquireFirst", func(t *testing.T) {
		// Run
		var err error
		fencing, err = dl.Acquire(ctx, key, initialTTL)

		// Assert
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
	})

	t.Run("ExtendSuccessfully", func(t *testing.T) {
		// Run
		err := dl.AcquireOrExtend(ctx, key, fencing, extendedTTL)

		// Assert
		if err != nil {
			t.Fatalf("AcquireOrExtend failed: %v", err)
		}

		ttl, err := rdb.TTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("Failed to get TTL: %v", err)
		}
		// TTL should be > half of extendedTTL (accounts for time elapsed during test)
		expectedMinTTL := extendedTTL / 2
		if ttl <= expectedMinTTL {
			t.Errorf("Expected TTL > %v (extendedTTL/2), got %v", expectedMinTTL, ttl)
		}
	})

	t.Run("ReAcquireAfterDeletion", func(t *testing.T) {
		// Setup
		rdb.Del(ctx, key)

		// Run
		err := dl.AcquireOrExtend(ctx, key, fencing, testTTLLong)

		// Assert
		if err != nil {
			t.Fatalf("AcquireOrExtend (re-acquire) failed: %v", err)
		}
		if rdb.Exists(ctx, key).Val() != 1 {
			t.Errorf("Expected key to exist after re-acquire")
		}
	})

	t.Run("FailWithWrongFencing", func(t *testing.T) {
		// Setup
		wrongFencing := "wrong-" + uuid.NewString()
		ctxTimeout, cancel := context.WithTimeout(ctx, testTimeoutShort)
		defer cancel()

		// Run
		err := dl.AcquireOrExtend(ctxTimeout, key, wrongFencing, testTTLLong)

		// Assert
		if err == nil {
			t.Errorf("Expected error/timeout on wrong fencing")
		}
	})
}

func TestLock_Extend(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()
	dl := redlock.NewLock(rdb)
	key := "test-extend-only-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	t.Run("ExtendExistingLock", func(t *testing.T) {
		// Setup
		initialTTL := testTTLMedium
		extendedTTL := testTTLLong
		fencing, err := dl.Acquire(ctx, key, initialTTL)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		// Run
		err = dl.Extend(ctx, key, fencing, extendedTTL)

		// Assert
		if err != nil {
			t.Fatalf("Extend failed: %v", err)
		}

		ttl, err := rdb.TTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("Failed to get TTL: %v", err)
		}
		// TTL should be > 80% of extendedTTL (accounts for time elapsed)
		expectedMinTTL := time.Duration(float64(extendedTTL) * 0.8)
		if ttl <= expectedMinTTL {
			t.Errorf("Expected TTL > %v (80%% of extendedTTL=%v), got %v", expectedMinTTL, extendedTTL, ttl)
		}
	})
}

func TestLock_TryExtend(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()
	dl := redlock.NewLock(rdb)
	key := "test-tryextend-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	t.Run("FailsWhenLockNotExists", func(t *testing.T) {
		// Run
		err := dl.TryExtend(ctx, key, "any-fencing", testTTLLong)

		// Assert
		if !errors.Is(err, redlock.ErrLockNotHeld) {
			t.Errorf("Expected ErrLockNotHeld, got %v", err)
		}
	})

	t.Run("FailsWithWrongFencing", func(t *testing.T) {
		// Setup
		_, err := dl.Acquire(ctx, key, testTTLLong)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		// Run
		err = dl.TryExtend(ctx, key, "wrong-fencing", testTTLLong)

		// Assert
		if !errors.Is(err, redlock.ErrLockNotHeld) {
			t.Errorf("Expected ErrLockNotHeld, got %v", err)
		}
	})

	t.Run("SucceedsWithCorrectFencing", func(t *testing.T) {
		// Setup
		key2 := "test-tryextend-2-" + uuid.NewString()
		t.Cleanup(func() { rdb.Del(ctx, key2) })

		fencing, err := dl.Acquire(ctx, key2, testTTLMedium)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		// Run
		err = dl.TryExtend(ctx, key2, fencing, testTTLLong)

		// Assert
		if err != nil {
			t.Errorf("TryExtend should succeed, got %v", err)
		}
	})
}

func TestLock_Release(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()
	dl := redlock.NewLock(rdb)
	key := "test-release-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	t.Run("Success", func(t *testing.T) {
		// Setup
		fencing, err := dl.Acquire(ctx, key, testTTLLong)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
		expectedExistsBeforeRelease := int64(1)
		expectedExistsAfterRelease := int64(0)

		// Assert setup precondition
		if rdb.Exists(ctx, key).Val() != expectedExistsBeforeRelease {
			t.Errorf("Expected key to exist (count=%d)", expectedExistsBeforeRelease)
		}

		// Run
		err = dl.Release(ctx, key, fencing)

		// Assert
		if err != nil {
			t.Errorf("Release failed: %v", err)
		}
		if rdb.Exists(ctx, key).Val() != expectedExistsAfterRelease {
			t.Errorf("Expected key to be deleted (count=%d)", expectedExistsAfterRelease)
		}
	})

	t.Run("WrongFencingDoesNotDelete", func(t *testing.T) {
		// Setup
		key2 := "test-release-wrong-" + uuid.NewString()
		t.Cleanup(func() { rdb.Del(ctx, key2) })

		fencing, err := dl.Acquire(ctx, key2, testTTLLong)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		wrongFencing := "wrong-" + uuid.NewString()
		expectedExistsAfterWrongRelease := int64(1)

		// Run - attempt release with wrong fencing
		_ = dl.Release(ctx, key2, wrongFencing)

		// Assert - key should still exist
		if rdb.Exists(ctx, key2).Val() != expectedExistsAfterWrongRelease {
			t.Errorf("Expected key to still exist (count=%d) after release with wrong fencing", expectedExistsAfterWrongRelease)
		}

		// Run - release with correct fencing
		err = dl.Release(ctx, key2, fencing)

		// Assert
		if err != nil {
			t.Errorf("Release with correct fencing failed: %v", err)
		}
	})
}

func TestLock_MaxRetryExceeded(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()

	dlA := redlock.NewLock(rdb)
	key := "test-maxretry-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	_, err := dlA.Acquire(ctx, key, testTTLLong)
	if err != nil {
		t.Fatalf("Client A failed to acquire: %v", err)
	}

	dlB := redlock.NewLock(rdb,
		redlock.WithMaxRetry(testMaxRetryMedium),
		redlock.WithMinRetryDelay(testMinRetryDelay),
	)

	// Run
	_, err = dlB.Acquire(ctx, key, testTTLLong)

	// Assert
	if !errors.Is(err, redlock.ErrMaxRetryExceeded) {
		t.Errorf("Expected ErrMaxRetryExceeded, got %v", err)
	}
}

func TestLock_MinRetryDelay(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()

	dlA := redlock.NewLock(rdb)
	key := "test-min-wait-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	_, err := dlA.Acquire(ctx, key, testTTLLong)
	if err != nil {
		t.Fatalf("Client A failed to acquire: %v", err)
	}

	minWait := 200 * time.Millisecond
	dlB := redlock.NewLock(rdb,
		redlock.WithMinRetryDelay(minWait),
		redlock.WithMaxRetry(testMaxRetrySmall),
	)

	// Run
	start := time.Now()
	_, errB := dlB.Acquire(ctx, key, testTTLLong)
	elapsed := time.Since(start)

	// Assert
	if errB == nil {
		t.Fatal("Client B should have failed to acquire")
	}

	expectedMinDuration := minWait * time.Duration(testMaxRetrySmall)
	if elapsed < expectedMinDuration {
		t.Errorf("Expected duration >= %v (minWait=%v * retries=%d), got %v",
			expectedMinDuration, minWait, testMaxRetrySmall, elapsed)
	}
}
