package redlock_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/trviph/redlock"
)

func TestDistributedLock_Acquire(t *testing.T) {
	// Setup
	rdb1 := setupRedis(t, testRedisPort1)
	rdb2 := setupRedis(t, testRedisPort2)
	rdb3 := setupRedis(t, testRedisPort3)

	locks := []*redlock.Lock{
		redlock.NewLock(rdb1),
		redlock.NewLock(rdb2),
		redlock.NewLock(rdb3),
	}
	numLocks := len(locks)
	expectedQuorum := quorum(numLocks)

	dl := redlock.NewDistributedLock(locks)
	ctx := context.Background()
	key := "dist-lock-" + uuid.NewString()
	t.Cleanup(func() {
		rdb1.Del(ctx, key)
		rdb2.Del(ctx, key)
		rdb3.Del(ctx, key)
	})

	var fencing string

	t.Run("Success", func(t *testing.T) {
		// Run
		var err error
		fencing, err = dl.Acquire(ctx, key, testTTLLong)

		// Assert
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
		if fencing == "" {
			t.Error("Expected valid fencing token")
		}
	})

	t.Run("QuorumAchieved", func(t *testing.T) {
		// Run
		count := int64(0)
		if rdb1.Exists(ctx, key).Val() == 1 {
			count++
		}
		if rdb2.Exists(ctx, key).Val() == 1 {
			count++
		}
		if rdb3.Exists(ctx, key).Val() == 1 {
			count++
		}

		// Assert
		if count < int64(expectedQuorum) {
			t.Errorf("Expected lock to be held by at least quorum(%d)=%d, got %d",
				numLocks, expectedQuorum, count)
		}
	})

	t.Run("ReleaseSuccess", func(t *testing.T) {
		// Run
		err := dl.Release(ctx, key, fencing)

		// Assert
		if err != nil {
			t.Errorf("Release failed: %v", err)
		}

		expectedKeysAfterRelease := int64(0)
		if rdb1.Exists(ctx, key).Val() != 0 || rdb2.Exists(ctx, key).Val() != 0 || rdb3.Exists(ctx, key).Val() != 0 {
			t.Errorf("Expected %d keys after release, but some still exist", expectedKeysAfterRelease)
		}
	})
}

func TestDistributedLock_TryAcquire(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	locks := []*redlock.Lock{redlock.NewLock(rdb)}
	dl := redlock.NewDistributedLock(locks)
	ctx := context.Background()
	key := "dist-try-" + uuid.NewString()
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

	t.Run("FailsWhenHeld", func(t *testing.T) {
		// Run
		_, err := dl.TryAcquire(ctx, key, testTTLLong)

		// Assert
		if !errors.Is(err, redlock.ErrLockAlreadyHeld) {
			t.Errorf("Expected ErrLockAlreadyHeld, got %v", err)
		}
	})
}

func TestDistributedLock_Extend(t *testing.T) {
	// Setup
	rdb1 := setupRedis(t, testRedisPort1)
	rdb2 := setupRedis(t, testRedisPort2)
	rdb3 := setupRedis(t, testRedisPort3)

	locks := []*redlock.Lock{
		redlock.NewLock(rdb1),
		redlock.NewLock(rdb2),
		redlock.NewLock(rdb3),
	}
	numLocks := len(locks)
	expectedQuorum := quorum(numLocks)

	dl := redlock.NewDistributedLock(locks)
	ctx := context.Background()
	key := "dist-extend-" + uuid.NewString()
	t.Cleanup(func() {
		rdb1.Del(ctx, key)
		rdb2.Del(ctx, key)
		rdb3.Del(ctx, key)
	})

	t.Run("Success", func(t *testing.T) {
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

		// Verify TTL was extended on at least quorum
		minExpectedTTL := extendedTTL / 2
		var extendedCount int
		ttl1, _ := rdb1.TTL(ctx, key).Result()
		ttl2, _ := rdb2.TTL(ctx, key).Result()
		ttl3, _ := rdb3.TTL(ctx, key).Result()
		if ttl1 > minExpectedTTL {
			extendedCount++
		}
		if ttl2 > minExpectedTTL {
			extendedCount++
		}
		if ttl3 > minExpectedTTL {
			extendedCount++
		}
		if extendedCount < expectedQuorum {
			t.Errorf("Expected at least quorum(%d)=%d with extended TTL (> %v), got %d (ttls: %v, %v, %v)",
				numLocks, expectedQuorum, minExpectedTTL, extendedCount, ttl1, ttl2, ttl3)
		}
	})

	t.Run("MaxRetryExceeded", func(t *testing.T) {
		// Setup
		key2 := "dist-extend-maxretry-" + uuid.NewString()
		t.Cleanup(func() {
			rdb1.Del(ctx, key2)
			rdb2.Del(ctx, key2)
			rdb3.Del(ctx, key2)
		})

		fencing, err := dl.Acquire(ctx, key2, testTTLLong)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		// Create a new distributed lock with limited retries
		dlLimited := redlock.NewDistributedLock(locks,
			redlock.WithDistMaxRetry(testMaxRetryMedium),
			redlock.WithDistMinRetryDelay(testMinRetryDelay),
		)

		// Run - try to extend with wrong fencing
		wrongFencing := "wrong-" + uuid.NewString()
		err = dlLimited.Extend(ctx, key2, wrongFencing, testTTLLong)

		// Assert
		if !errors.Is(err, redlock.ErrMaxRetryExceeded) {
			t.Errorf("Expected ErrMaxRetryExceeded, got %v", err)
		}

		// Cleanup
		_ = dl.Release(ctx, key2, fencing)
	})
}

func TestDistributedLock_TryExtend(t *testing.T) {
	// Setup
	rdb1 := setupRedis(t, testRedisPort1)
	rdb2 := setupRedis(t, testRedisPort2)
	rdb3 := setupRedis(t, testRedisPort3)

	locks := []*redlock.Lock{
		redlock.NewLock(rdb1),
		redlock.NewLock(rdb2),
		redlock.NewLock(rdb3),
	}

	dl := redlock.NewDistributedLock(locks)
	ctx := context.Background()
	key := "dist-tryextend-" + uuid.NewString()
	t.Cleanup(func() {
		rdb1.Del(ctx, key)
		rdb2.Del(ctx, key)
		rdb3.Del(ctx, key)
	})

	t.Run("FailsWhenNotHeld", func(t *testing.T) {
		// Run
		err := dl.TryExtend(ctx, key, "any-fencing", testTTLLong)

		// Assert
		if !errors.Is(err, redlock.ErrLockNotHeld) {
			t.Errorf("Expected ErrLockNotHeld, got %v", err)
		}
	})

	t.Run("FailsWithWrongFencing", func(t *testing.T) {
		// Setup
		fencing, err := dl.Acquire(ctx, key, testTTLLong)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		// Run
		wrongFencing := "wrong-" + fencing
		err = dl.TryExtend(ctx, key, wrongFencing, testTTLLong)

		// Assert
		if !errors.Is(err, redlock.ErrLockNotHeld) {
			t.Errorf("Expected ErrLockNotHeld, got %v", err)
		}
	})

	t.Run("SucceedsWithCorrectFencing", func(t *testing.T) {
		// Setup
		key2 := "dist-tryextend-2-" + uuid.NewString()
		t.Cleanup(func() {
			rdb1.Del(ctx, key2)
			rdb2.Del(ctx, key2)
			rdb3.Del(ctx, key2)
		})

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

func TestDistributedLock_AcquireOrExtend(t *testing.T) {
	// Setup
	rdb1 := setupRedis(t, testRedisPort1)
	rdb2 := setupRedis(t, testRedisPort2)
	rdb3 := setupRedis(t, testRedisPort3)

	locks := []*redlock.Lock{
		redlock.NewLock(rdb1),
		redlock.NewLock(rdb2),
		redlock.NewLock(rdb3),
	}

	dl := redlock.NewDistributedLock(locks)
	ctx := context.Background()
	key := "dist-acquireorextend-" + uuid.NewString()
	t.Cleanup(func() {
		rdb1.Del(ctx, key)
		rdb2.Del(ctx, key)
		rdb3.Del(ctx, key)
	})

	fencing := uuid.NewString()

	t.Run("FreshAcquisition", func(t *testing.T) {
		// Run
		err := dl.AcquireOrExtend(ctx, key, fencing, testTTLMedium)

		// Assert
		if err != nil {
			t.Fatalf("AcquireOrExtend failed: %v", err)
		}
	})

	t.Run("ExtendWithSameFencing", func(t *testing.T) {
		// Run
		err := dl.AcquireOrExtend(ctx, key, fencing, testTTLLong)

		// Assert
		if err != nil {
			t.Fatalf("AcquireOrExtend (extend) failed: %v", err)
		}
	})

	t.Run("FailWithDifferentFencing", func(t *testing.T) {
		// Setup
		wrongFencing := "wrong-" + uuid.NewString()
		ctxTimeout, cancel := context.WithTimeout(ctx, testTimeoutMedium)
		defer cancel()

		// Run
		err := dl.AcquireOrExtend(ctxTimeout, key, wrongFencing, testTTLLong)

		// Assert
		if err == nil {
			t.Error("Expected error with wrong fencing")
		}
	})
}

func TestDistributedLock_MaxRetryExceeded(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()
	key := "dist-maxretry-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	// Holder acquires the lock
	holderLock := redlock.NewLock(rdb)
	locks := []*redlock.Lock{holderLock}
	dlHolder := redlock.NewDistributedLock(locks)

	_, err := dlHolder.Acquire(ctx, key, testTTLLong)
	if err != nil {
		t.Fatalf("Holder failed to acquire: %v", err)
	}

	// Contender with max retry
	contenderLock := redlock.NewLock(rdb)
	dlContender := redlock.NewDistributedLock(
		[]*redlock.Lock{contenderLock},
		redlock.WithDistMaxRetry(testMaxRetryMedium),
		redlock.WithDistMinRetryDelay(testMinRetryDelay),
	)

	// Run
	_, err = dlContender.Acquire(ctx, key, testTTLLong)

	// Assert
	if !errors.Is(err, redlock.ErrMaxRetryExceeded) {
		t.Errorf("Expected ErrMaxRetryExceeded, got %v", err)
	}
}

func TestDistributedLock_Release(t *testing.T) {
	// Setup
	rdb1 := setupRedis(t, testRedisPort1)
	rdb2 := setupRedis(t, testRedisPort2)
	rdb3 := setupRedis(t, testRedisPort3)

	locks := []*redlock.Lock{
		redlock.NewLock(rdb1),
		redlock.NewLock(rdb2),
		redlock.NewLock(rdb3),
	}
	numLocks := len(locks)
	expectedQuorum := quorum(numLocks)

	dl := redlock.NewDistributedLock(locks)
	ctx := context.Background()
	key := "dist-release-" + uuid.NewString()
	t.Cleanup(func() {
		rdb1.Del(ctx, key)
		rdb2.Del(ctx, key)
		rdb3.Del(ctx, key)
	})

	t.Run("Success", func(t *testing.T) {
		// Setup
		fencing, err := dl.Acquire(ctx, key, testTTLLong)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		// Run
		err = dl.Release(ctx, key, fencing)

		// Assert
		if err != nil {
			t.Errorf("Release failed: %v", err)
		}

		// Verify all instances released
		expectedKeysAfterRelease := int64(0)
		count := rdb1.Exists(ctx, key).Val() + rdb2.Exists(ctx, key).Val() + rdb3.Exists(ctx, key).Val()
		if count != expectedKeysAfterRelease {
			t.Errorf("Expected %d keys after release, got %d", expectedKeysAfterRelease, count)
		}
	})

	t.Run("WrongFencingDoesNotRelease", func(t *testing.T) {
		// Setup
		key2 := "dist-release-wrong-" + uuid.NewString()
		t.Cleanup(func() {
			rdb1.Del(ctx, key2)
			rdb2.Del(ctx, key2)
			rdb3.Del(ctx, key2)
		})

		fencing, err := dl.Acquire(ctx, key2, testTTLLong)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		wrongFencing := "wrong-" + uuid.NewString()

		// Run - attempt release with wrong fencing
		_ = dl.Release(ctx, key2, wrongFencing)

		// Assert - Lock should still be held on quorum
		count := rdb1.Exists(ctx, key2).Val() + rdb2.Exists(ctx, key2).Val() + rdb3.Exists(ctx, key2).Val()
		if count < int64(expectedQuorum) {
			t.Errorf("Expected at least quorum(%d)=%d to still hold lock, got %d",
				numLocks, expectedQuorum, count)
		}

		// Run - Correct release should work
		err = dl.Release(ctx, key2, fencing)

		// Assert
		if err != nil {
			t.Errorf("Release with correct fencing failed: %v", err)
		}
	})
}
