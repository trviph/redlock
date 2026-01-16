package redlock_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/trviph/redlock"
)

// TestLock_ConcurrentAcquire tests that concurrent Acquire calls
// result in exactly one winner.
func TestLock_ConcurrentAcquire(t *testing.T) {
	rdb := setupRedis(t, "6379")
	ctx := context.Background()
	key := "test-concurrent-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	const numClients = 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	fencingTokens := make([]string, 0)

	// Use a short timeout so losers fail quickly
	ctxTimeout, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	for range numClients {
		wg.Go(func() {
			dl := redlock.NewLock(rdb)
			fencing, err := dl.Acquire(ctxTimeout, key, 10*time.Second)
			if err == nil {
				mu.Lock()
				winners++
				fencingTokens = append(fencingTokens, fencing)
				mu.Unlock()
			}
		})
	}

	wg.Wait()

	if winners != 1 {
		t.Errorf("Expected exactly 1 winner, got %d", winners)
	}

	if len(fencingTokens) != 1 {
		t.Errorf("Expected 1 fencing token, got %d", len(fencingTokens))
	}
}

// TestDistributedLock_ConcurrentAcquire tests that concurrent Acquire calls
// across multiple Redis instances result in exactly one winner.
func TestDistributedLock_ConcurrentAcquire(t *testing.T) {
	rdb1 := setupRedis(t, "6379")
	rdb2 := setupRedis(t, "6380")
	rdb3 := setupRedis(t, "6381")

	ctx := context.Background()
	key := "dist-concurrent-" + uuid.NewString()
	t.Cleanup(func() {
		rdb1.Del(ctx, key)
		rdb2.Del(ctx, key)
		rdb3.Del(ctx, key)
	})

	const numClients = 5
	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	fencingTokens := make([]string, 0)

	// Use a short timeout so losers fail quickly
	ctxTimeout, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	for range numClients {
		wg.Go(func() {
			locks := []*redlock.Lock{
				redlock.NewLock(rdb1),
				redlock.NewLock(rdb2),
				redlock.NewLock(rdb3),
			}
			dl := redlock.NewDistributedLock(locks)
			fencing, err := dl.Acquire(ctxTimeout, key, 10*time.Second)
			if err == nil {
				mu.Lock()
				winners++
				fencingTokens = append(fencingTokens, fencing)
				mu.Unlock()
			}
		})
	}

	wg.Wait()

	if winners != 1 {
		t.Errorf("Expected exactly 1 winner, got %d", winners)
	}

	if len(fencingTokens) != 1 {
		t.Errorf("Expected 1 fencing token, got %d", len(fencingTokens))
	}
}
