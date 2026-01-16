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

func TestWatch_Integration(t *testing.T) {
	// Setup Redis clients using the same helper as other tests
	rdb1 := setupRedis(t, "6379")
	rdb2 := setupRedis(t, "6380")
	rdb3 := setupRedis(t, "6381")

	// Helper to run the test scenario on a generic Locker
	runTestScenario := func(t *testing.T, locker redlock.Locker, key string) {
		ctx := context.Background()
		ttl := 200 * time.Millisecond // Short TTL for testing

		// 1. Acquire lock
		fencing, err := locker.Acquire(ctx, key, ttl)
		if err != nil {
			t.Fatalf("Failed to acquire lock: %v", err)
		}

		// 2. Start Watch
		watchCtx, watchCancel := context.WithCancel(ctx)
		defer watchCancel() // Ensure cleanup
		redlock.Watch(watchCtx, locker, key, fencing, ttl)

		// 3. Try to acquire lock with contention
		var wg sync.WaitGroup
		for range 50 {
			wg.Go(func() {
				// TryAcquire should fail immediately if locked
				_, err := locker.TryAcquire(ctx, key, ttl)
				if err == nil {
					t.Errorf("Concurrent TryAcquire succeeded but should have failed")
				}
			})
		}
		wg.Wait()

		// 4. Wait for > TTL
		// The lock would expire at ttl. We wait 1.5 * ttl.
		// Watchdog should have extended it at ~0.5 * ttl and ~1.0 * ttl.
		time.Sleep(ttl + ttl/2)

		// 5. Try acquire again - should still fail because watchdog kept it alive
		_, err = locker.TryAcquire(ctx, key, ttl)
		if err == nil {
			t.Errorf("Acquire succeeded after TTL but watchdog should have held it")
		} else if !errors.Is(err, redlock.ErrLockAlreadyHeld) {
			t.Errorf("Expected ErrLockAlreadyHeld, got %v", err)
		}

		// 6. Cancel watch context
		watchCancel()

		// 7. Wait for TTL again (so the last extension expires)
		// Since we just cancelled, the last extension (at most recent half-ttl mark) need to run out.
		// We wait for full TTL + small buffer to be sure.
		time.Sleep(ttl + 50*time.Millisecond)

		// 8. Acquire again - should work now
		newFencing, err := locker.Acquire(ctx, key, ttl)
		if err != nil {
			t.Fatalf("Failed to acquire lock after watchdog stopped and TTL expired: %v", err)
		}

		// Cleanup
		_ = locker.Release(ctx, key, newFencing)
	}

	t.Run("SingleInstanceLock", func(t *testing.T) {
		locker := redlock.NewLock(rdb1)
		key := "watch-test-single-" + uuid.NewString()
		t.Cleanup(func() { rdb1.Del(context.Background(), key) })

		runTestScenario(t, locker, key)
	})

	t.Run("DistributedLock", func(t *testing.T) {
		locks := []*redlock.Lock{
			redlock.NewLock(rdb1),
			redlock.NewLock(rdb2),
			redlock.NewLock(rdb3),
		}

		// Note: DistributedLock minimum validity might be affected by clock drift etc,
		// but with 200ms TTL it should be fine for local docker test.
		locker := redlock.NewDistributedLock(locks)
		key := "watch-test-dist-" + uuid.NewString()
		t.Cleanup(func() {
			rdb1.Del(context.Background(), key)
			rdb2.Del(context.Background(), key)
			rdb3.Del(context.Background(), key)
		})

		runTestScenario(t, locker, key)
	})
}
