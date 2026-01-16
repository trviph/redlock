package redlock_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/trviph/redlock"
)

func TestWatch(t *testing.T) {
	// Setup
	rdb1 := setupRedis(t, testRedisPort1)
	rdb2 := setupRedis(t, testRedisPort2)
	rdb3 := setupRedis(t, testRedisPort3)

	runScenario := func(t *testing.T, locker redlock.Locker, key string) {
		ctx := context.Background()
		ttl := testTTLShort

		// Setup: Acquire lock
		fencing, err := locker.Acquire(ctx, key, ttl)
		if err != nil {
			t.Fatalf("Failed to acquire lock: %v", err)
		}

		// Setup: Start watchdog
		watchCtx, watchCancel := context.WithCancel(ctx)
		defer watchCancel()
		redlock.Watch(watchCtx, locker, key, fencing, ttl)

		// Run: Concurrent contention
		var wg sync.WaitGroup
		for range testConcurrentContention {
			wg.Go(func() {
				if _, tryErr := locker.TryAcquire(ctx, key, ttl); tryErr == nil {
					t.Errorf("Concurrent TryAcquire succeeded but should have failed")
				}
			})
		}
		wg.Wait()

		// Run: Wait beyond TTL (watchdog should keep it alive)
		waitTime := ttl + ttl/2
		time.Sleep(waitTime)

		// Assert: Lock still held due to watchdog
		_, err = locker.TryAcquire(ctx, key, ttl)
		if err == nil {
			t.Errorf("Acquire succeeded after %v but watchdog should have held it", waitTime)
		} else if !errors.Is(err, redlock.ErrLockAlreadyHeld) {
			t.Errorf("Expected ErrLockAlreadyHeld, got %v", err)
		}

		// Run: Cancel watchdog and wait for TTL expiry
		watchCancel()
		time.Sleep(ttl + testTimingMargin)

		// Assert: Lock released after watchdog stopped
		newFencing, err := locker.Acquire(ctx, key, ttl)
		if err != nil {
			t.Fatalf("Failed to acquire lock after watchdog stopped: %v", err)
		}
		_ = locker.Release(ctx, key, newFencing)
	}

	t.Run("SingleInstanceLock", func(t *testing.T) {
		locker := redlock.NewLock(rdb1)
		key := "watch-test-single-" + uuid.NewString()
		t.Cleanup(func() { rdb1.Del(context.Background(), key) })

		runScenario(t, locker, key)
	})

	t.Run("DistributedLock", func(t *testing.T) {
		locks := []*redlock.Lock{
			redlock.NewLock(rdb1),
			redlock.NewLock(rdb2),
			redlock.NewLock(rdb3),
		}
		locker := redlock.NewDistributedLock(locks)
		key := "watch-test-dist-" + uuid.NewString()
		t.Cleanup(func() {
			rdb1.Del(context.Background(), key)
			rdb2.Del(context.Background(), key)
			rdb3.Del(context.Background(), key)
		})

		runScenario(t, locker, key)
	})
}
