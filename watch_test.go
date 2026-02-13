package redlock_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/trviph/redlock"
)

func TestWatchDog(t *testing.T) {
	rdb1 := setupRedis(t, testRedisPort1)
	locker := redlock.NewLock(rdb1)

	t.Run("BasicExtension", func(t *testing.T) {
		key := "wd-basic-" + uuid.NewString()
		ttl := testTTLShort
		fencing, err := locker.Acquire(context.Background(), key, ttl)
		if err != nil {
			t.Fatalf("Failed to acquire lock: %v", err)
		}
		defer func() { _ = locker.Release(context.Background(), key, fencing) }()

		// Use a wait group to wait for at least one extension
		var extended atomic.Bool
		extensionCh := make(chan struct{}, 1)

		// Setup WatchDog with a callback via option
		wd := redlock.NewWatchDog(locker,
			redlock.WithItem(key, fencing, ttl, ttl/4),
			redlock.WithExtensionCallbacks(context.Background(), func(ctx context.Context, item *redlock.WatchItem, _ error) {
				select {
				case extensionCh <- struct{}{}:
				default:
				}
			}),
		)

		ctx := t.Context()
		go wd.Run(ctx)

		// Wait for extension signal instead of sleep
		select {
		case <-extensionCh:
			// Success
		case <-time.After(ttl + ttl/2):
			t.Fatal("Timeout waiting for extension")
		}

		// Check if lock is still held
		err = locker.TryAcquireWithFencing(context.Background(), key, "other", ttl)
		if !errors.Is(err, redlock.ErrLockAlreadyHeld) {
			t.Errorf("Lock should still be held, got: %v", err)
		}
		extended.Store(true)
	})

	t.Run("CallbackOnError", func(t *testing.T) {
		key := "wd-error-" + uuid.NewString()
		// Fencing mismatch will cause extension failure
		fencing := "wrong-token"
		ttl := testTTLShort

		var wg sync.WaitGroup
		wg.Add(1)

		errHandler := func(ctx context.Context, item *redlock.WatchItem, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			if item.Key != key {
				t.Errorf("Callback received wrong key: got %s, want %s", item.Key, key)
			}
			if !errors.Is(err, redlock.ErrLockNotHeld) {
				t.Errorf("Expected ErrLockNotHeld, got %v", err)
			}
			wg.Done()
		}

		wd := redlock.NewWatchDog(locker,
			redlock.WithCallbacks(context.Background(), errHandler),
			redlock.WithItem(key, fencing, ttl, time.Millisecond*100),
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go wd.Run(ctx)

		// Wait for callback
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for error callback")
		}
	})

	t.Run("MultipleItems", func(t *testing.T) {
		key1 := "wd-multi-1-" + uuid.NewString()
		key2 := "wd-multi-2-" + uuid.NewString()
		ttl := testTTLShort

		f1, _ := locker.Acquire(context.Background(), key1, ttl)
		f2, _ := locker.Acquire(context.Background(), key2, ttl)
		defer func() {
			_ = locker.Release(context.Background(), key1, f1)
		}()
		defer func() {
			_ = locker.Release(context.Background(), key2, f2)
		}()

		wd := redlock.NewWatchDog(locker,
			redlock.WithItem(key1, f1, ttl, ttl/4),
			redlock.WithItem(key2, f2, ttl, ttl/4),
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go wd.Run(ctx)

		time.Sleep(ttl + ttl/2)

		// Both should still be held
		if err := locker.TryAcquireWithFencing(context.Background(), key1, "x", ttl); !errors.Is(err, redlock.ErrLockAlreadyHeld) {
			t.Errorf("Key1 lost: %v", err)
		}
		if err := locker.TryAcquireWithFencing(context.Background(), key2, "x", ttl); !errors.Is(err, redlock.ErrLockAlreadyHeld) {
			t.Errorf("Key2 lost: %v", err)
		}
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		key := "wd-cancel-" + uuid.NewString()

		var wg sync.WaitGroup
		wg.Add(1)

		errHandler := func(ctx context.Context, item *redlock.WatchItem, err error) {
			if err == context.Canceled {
				wg.Done()
			}
		}

		wd := redlock.NewWatchDog(locker,
			redlock.WithCallbacks(context.Background(), errHandler),
			redlock.WithItem(key, "token", time.Second, time.Millisecond*100),
		)

		ctx, cancel := context.WithCancel(context.Background())
		go wd.Run(ctx)

		// Let it run briefly
		time.Sleep(time.Millisecond * 50)
		cancel()

		// Wait for cancellation callback
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for cancellation callback")
		}
	})
}

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
