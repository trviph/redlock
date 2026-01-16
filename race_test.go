package redlock_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/trviph/redlock"
)

func TestLock_ConcurrentAcquire(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()
	key := "test-concurrent-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	fencingTokens := make([]string, 0, testConcurrentClientsMed)

	ctxTimeout, cancel := context.WithTimeout(ctx, testTimeoutMedium)
	defer cancel()

	// Run
	for range testConcurrentClientsMed {
		wg.Go(func() {
			dl := redlock.NewLock(rdb)
			fencing, err := dl.Acquire(ctxTimeout, key, testTTLLong)
			if err == nil {
				mu.Lock()
				winners++
				fencingTokens = append(fencingTokens, fencing)
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	// Assert
	if winners != testExpectedWinners {
		t.Errorf("Expected exactly %d winner(s) from %d clients, got %d",
			testExpectedWinners, testConcurrentClientsMed, winners)
	}
	if len(fencingTokens) != testExpectedWinners {
		t.Errorf("Expected %d fencing token(s), got %d",
			testExpectedWinners, len(fencingTokens))
	}
}

func TestDistributedLock_ConcurrentAcquire(t *testing.T) {
	// Setup
	rdb1 := setupRedis(t, testRedisPort1)
	rdb2 := setupRedis(t, testRedisPort2)
	rdb3 := setupRedis(t, testRedisPort3)

	ctx := context.Background()
	key := "dist-concurrent-" + uuid.NewString()
	t.Cleanup(func() {
		rdb1.Del(ctx, key)
		rdb2.Del(ctx, key)
		rdb3.Del(ctx, key)
	})

	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	fencingTokens := make([]string, 0, testConcurrentClientsSmall)

	ctxTimeout, cancel := context.WithTimeout(ctx, testTimeoutMedium)
	defer cancel()

	// Run
	for range testConcurrentClientsSmall {
		wg.Go(func() {
			locks := []*redlock.Lock{
				redlock.NewLock(rdb1),
				redlock.NewLock(rdb2),
				redlock.NewLock(rdb3),
			}
			dl := redlock.NewDistributedLock(locks)
			fencing, err := dl.Acquire(ctxTimeout, key, testTTLLong)
			if err == nil {
				mu.Lock()
				winners++
				fencingTokens = append(fencingTokens, fencing)
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	// Assert
	if winners != testExpectedWinners {
		t.Errorf("Expected exactly %d winner(s) from %d clients, got %d",
			testExpectedWinners, testConcurrentClientsSmall, winners)
	}
	if len(fencingTokens) != testExpectedWinners {
		t.Errorf("Expected %d fencing token(s), got %d",
			testExpectedWinners, len(fencingTokens))
	}
}
