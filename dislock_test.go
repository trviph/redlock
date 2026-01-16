package redlock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/trviph/redlock"
)

func TestDistributedLock_Acquire(t *testing.T) {
	rdb1 := setupRedis(t, "6379")
	rdb2 := setupRedis(t, "6380")
	rdb3 := setupRedis(t, "6381")

	locks := []*redlock.Lock{
		redlock.NewLock(rdb1),
		redlock.NewLock(rdb2),
		redlock.NewLock(rdb3),
	}

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
		var err error
		fencing, err = dl.Acquire(ctx, key, 10*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}
		if fencing == "" {
			t.Error("Expected valid fencing token")
		}
	})

	t.Run("QuorumAchieved", func(t *testing.T) {
		count := 0
		if rdb1.Exists(ctx, key).Val() == 1 {
			count++
		}
		if rdb2.Exists(ctx, key).Val() == 1 {
			count++
		}
		if rdb3.Exists(ctx, key).Val() == 1 {
			count++
		}
		if count < 2 {
			t.Errorf("Expected lock to be held by at least quorum (2), got %d", count)
		}
	})

	t.Run("ReleaseSuccess", func(t *testing.T) {
		err := dl.Release(ctx, key, fencing)
		if err != nil {
			t.Errorf("Release failed: %v", err)
		}

		if rdb1.Exists(ctx, key).Val() != 0 || rdb2.Exists(ctx, key).Val() != 0 || rdb3.Exists(ctx, key).Val() != 0 {
			t.Error("Lock key should be gone after release")
		}
	})
}

func TestDistributedLock_TryAcquire(t *testing.T) {
	rdb := setupRedis(t, "6379")
	locks := []*redlock.Lock{redlock.NewLock(rdb)}
	dl := redlock.NewDistributedLock(locks)
	ctx := context.Background()
	key := "dist-try-" + uuid.NewString()
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

	t.Run("FailsWhenHeld", func(t *testing.T) {
		_, err := dl.TryAcquire(ctx, key, 10*time.Second)
		if !errors.Is(err, redlock.ErrLockAlreadyHeld) {
			t.Errorf("Expected ErrLockAlreadyHeld, got %v", err)
		}
	})
}

func TestDistributedLock_Extend(t *testing.T) {
	rdb1 := setupRedis(t, "6379")
	rdb2 := setupRedis(t, "6380")
	rdb3 := setupRedis(t, "6381")

	locks := []*redlock.Lock{
		redlock.NewLock(rdb1),
		redlock.NewLock(rdb2),
		redlock.NewLock(rdb3),
	}

	dl := redlock.NewDistributedLock(locks)
	ctx := context.Background()
	key := "dist-extend-" + uuid.NewString()
	t.Cleanup(func() {
		rdb1.Del(ctx, key)
		rdb2.Del(ctx, key)
		rdb3.Del(ctx, key)
	})

	t.Run("Success", func(t *testing.T) {
		fencing, err := dl.Acquire(ctx, key, 2*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		err = dl.Extend(ctx, key, fencing, 10*time.Second)
		if err != nil {
			t.Fatalf("Extend failed: %v", err)
		}

		// Verify TTL was extended on at least quorum
		var extendedCount int
		for _, rdb := range []*redlock.Lock{} {
			_ = rdb // placeholder to avoid unused
		}
		ttl1, _ := rdb1.TTL(ctx, key).Result()
		ttl2, _ := rdb2.TTL(ctx, key).Result()
		ttl3, _ := rdb3.TTL(ctx, key).Result()
		if ttl1 > 5*time.Second {
			extendedCount++
		}
		if ttl2 > 5*time.Second {
			extendedCount++
		}
		if ttl3 > 5*time.Second {
			extendedCount++
		}
		if extendedCount < 2 {
			t.Errorf("Expected at least quorum (2) with extended TTL, got %d", extendedCount)
		}
	})
}

func TestDistributedLock_TryExtend(t *testing.T) {
	rdb1 := setupRedis(t, "6379")
	rdb2 := setupRedis(t, "6380")
	rdb3 := setupRedis(t, "6381")

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
		err := dl.TryExtend(ctx, key, "any-fencing", 10*time.Second)
		if !errors.Is(err, redlock.ErrLockNotHeld) {
			t.Errorf("Expected ErrLockNotHeld, got %v", err)
		}
	})

	t.Run("FailsWithWrongFencing", func(t *testing.T) {
		fencing, err := dl.Acquire(ctx, key, 10*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		err = dl.TryExtend(ctx, key, "wrong-"+fencing, 10*time.Second)
		if !errors.Is(err, redlock.ErrLockNotHeld) {
			t.Errorf("Expected ErrLockNotHeld, got %v", err)
		}
	})

	t.Run("SucceedsWithCorrectFencing", func(t *testing.T) {
		key2 := "dist-tryextend-2-" + uuid.NewString()
		t.Cleanup(func() {
			rdb1.Del(ctx, key2)
			rdb2.Del(ctx, key2)
			rdb3.Del(ctx, key2)
		})

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

func TestDistributedLock_AcquireOrExtend(t *testing.T) {
	rdb1 := setupRedis(t, "6379")
	rdb2 := setupRedis(t, "6380")
	rdb3 := setupRedis(t, "6381")

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
		err := dl.AcquireOrExtend(ctx, key, fencing, 2*time.Second)
		if err != nil {
			t.Fatalf("AcquireOrExtend failed: %v", err)
		}
	})

	t.Run("ExtendWithSameFencing", func(t *testing.T) {
		err := dl.AcquireOrExtend(ctx, key, fencing, 10*time.Second)
		if err != nil {
			t.Fatalf("AcquireOrExtend (extend) failed: %v", err)
		}
	})

	t.Run("FailWithDifferentFencing", func(t *testing.T) {
		wrongFencing := "wrong-" + uuid.NewString()
		ctxTimeout, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		err := dl.AcquireOrExtend(ctxTimeout, key, wrongFencing, 10*time.Second)
		if err == nil {
			t.Error("Expected error with wrong fencing")
		}
	})
}

func TestDistributedLock_MaxRetryExceeded(t *testing.T) {
	rdb := setupRedis(t, "6379")
	ctx := context.Background()
	key := "dist-maxretry-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	// Holder acquires the lock
	holderLock := redlock.NewLock(rdb)
	locks := []*redlock.Lock{holderLock}
	dlHolder := redlock.NewDistributedLock(locks)

	_, err := dlHolder.Acquire(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("Holder failed to acquire: %v", err)
	}

	// Contender with max retry
	contenderLock := redlock.NewLock(rdb)
	dlContender := redlock.NewDistributedLock(
		[]*redlock.Lock{contenderLock},
		redlock.WithDistMaxRetry(2),
		redlock.WithDistMinRetryDelay(50*time.Millisecond),
	)

	_, err = dlContender.Acquire(ctx, key, 10*time.Second)
	if !errors.Is(err, redlock.ErrMaxRetryExceeded) {
		t.Errorf("Expected ErrMaxRetryExceeded, got %v", err)
	}
}

func TestDistributedLock_Release(t *testing.T) {
	rdb1 := setupRedis(t, "6379")
	rdb2 := setupRedis(t, "6380")
	rdb3 := setupRedis(t, "6381")

	locks := []*redlock.Lock{
		redlock.NewLock(rdb1),
		redlock.NewLock(rdb2),
		redlock.NewLock(rdb3),
	}

	dl := redlock.NewDistributedLock(locks)
	ctx := context.Background()
	key := "dist-release-" + uuid.NewString()
	t.Cleanup(func() {
		rdb1.Del(ctx, key)
		rdb2.Del(ctx, key)
		rdb3.Del(ctx, key)
	})

	t.Run("Success", func(t *testing.T) {
		fencing, err := dl.Acquire(ctx, key, 10*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		err = dl.Release(ctx, key, fencing)
		if err != nil {
			t.Errorf("Release failed: %v", err)
		}

		// Verify all instances released
		count := rdb1.Exists(ctx, key).Val() + rdb2.Exists(ctx, key).Val() + rdb3.Exists(ctx, key).Val()
		if count != 0 {
			t.Errorf("Expected 0 keys after release, got %d", count)
		}
	})

	t.Run("WrongFencingDoesNotRelease", func(t *testing.T) {
		key2 := "dist-release-wrong-" + uuid.NewString()
		t.Cleanup(func() {
			rdb1.Del(ctx, key2)
			rdb2.Del(ctx, key2)
			rdb3.Del(ctx, key2)
		})

		fencing, err := dl.Acquire(ctx, key2, 10*time.Second)
		if err != nil {
			t.Fatalf("Acquire failed: %v", err)
		}

		wrongFencing := "wrong-" + uuid.NewString()
		_ = dl.Release(ctx, key2, wrongFencing)

		// Lock should still be held on quorum
		count := rdb1.Exists(ctx, key2).Val() + rdb2.Exists(ctx, key2).Val() + rdb3.Exists(ctx, key2).Val()
		if count < 2 {
			t.Errorf("Expected at least quorum (2) to still hold lock, got %d", count)
		}

		// Correct release should work
		err = dl.Release(ctx, key2, fencing)
		if err != nil {
			t.Errorf("Release with correct fencing failed: %v", err)
		}
	})
}
