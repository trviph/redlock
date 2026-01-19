package redlock_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/trviph/redlock"
)

func TestWithClockDriftFactor(t *testing.T) {
	// Setup
	rdb1 := setupRedis(t, testRedisPort1)
	rdb2 := setupRedis(t, testRedisPort2)
	rdb3 := setupRedis(t, testRedisPort3)
	ctx := context.Background()

	locks := []*redlock.Lock{
		redlock.NewLock(rdb1),
		redlock.NewLock(rdb2),
		redlock.NewLock(rdb3),
	}
	key := "test-drift-opt-" + uuid.NewString()
	t.Cleanup(func() {
		rdb1.Del(ctx, key)
		rdb2.Del(ctx, key)
		rdb3.Del(ctx, key)
	})

	// Run
	dl := redlock.NewDistributedLock(locks, redlock.WithClockDriftFactor(0.05))
	fencing, err := dl.Acquire(ctx, key, testTTLLong)

	// Assert
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if fencing == "" {
		t.Error("Expected valid fencing token")
	}

	_ = dl.Release(ctx, key, fencing)
}

func TestWithClockDriftBuffer(t *testing.T) {
	// Setup
	rdb1 := setupRedis(t, testRedisPort1)
	rdb2 := setupRedis(t, testRedisPort2)
	rdb3 := setupRedis(t, testRedisPort3)
	ctx := context.Background()

	locks := []*redlock.Lock{
		redlock.NewLock(rdb1),
		redlock.NewLock(rdb2),
		redlock.NewLock(rdb3),
	}
	key := "test-drift-buffer-opt-" + uuid.NewString()
	t.Cleanup(func() {
		rdb1.Del(ctx, key)
		rdb2.Del(ctx, key)
		rdb3.Del(ctx, key)
	})

	// Run with a custom buffer of 10ms
	dl := redlock.NewDistributedLock(locks, redlock.WithClockDriftBuffer(10*time.Millisecond))
	fencing, err := dl.Acquire(ctx, key, testTTLLong)

	// Assert
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if fencing == "" {
		t.Error("Expected valid fencing token")
	}

	_ = dl.Release(ctx, key, fencing)
}

func TestWithReleaseTimeout(t *testing.T) {
	// Setup
	rdb1 := setupRedis(t, testRedisPort1)
	rdb2 := setupRedis(t, testRedisPort2)
	rdb3 := setupRedis(t, testRedisPort3)
	ctx := context.Background()

	locks := []*redlock.Lock{
		redlock.NewLock(rdb1),
		redlock.NewLock(rdb2),
		redlock.NewLock(rdb3),
	}
	key := "test-release-timeout-opt-" + uuid.NewString()
	t.Cleanup(func() {
		rdb1.Del(ctx, key)
		rdb2.Del(ctx, key)
		rdb3.Del(ctx, key)
	})

	// Run
	dl := redlock.NewDistributedLock(locks, redlock.WithReleaseTimeout(1*time.Second))
	fencing, err := dl.Acquire(ctx, key, testTTLLong)

	// Assert
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if err := dl.Release(ctx, key, fencing); err != nil {
		t.Errorf("Release failed: %v", err)
	}
}

func TestWithDistMaxJitterDuration(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()
	key := "test-dist-jitter-opt-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	holderLocks := []*redlock.Lock{redlock.NewLock(rdb)}
	holderDL := redlock.NewDistributedLock(holderLocks)
	if _, err := holderDL.Acquire(ctx, key, testTTLLong); err != nil {
		t.Fatalf("Holder failed to acquire: %v", err)
	}

	customJitter := 10 * time.Millisecond

	// Run
	contenderLocks := []*redlock.Lock{redlock.NewLock(rdb)}
	contenderDL := redlock.NewDistributedLock(contenderLocks,
		redlock.WithDistMaxJitterDuration(customJitter),
		redlock.WithDistMaxRetry(testMaxRetryMedium),
		redlock.WithDistMinRetryDelay(0),
	)

	start := time.Now()
	_, err := contenderDL.Acquire(ctx, key, testTTLLong)
	elapsed := time.Since(start)

	// Assert
	if err == nil {
		t.Fatal("Contender should have failed to acquire")
	}
	maxExpected := time.Duration(testMaxRetryMedium) * (customJitter + testTimingMargin)
	if elapsed > maxExpected {
		t.Errorf("Expected duration < %v, got %v", maxExpected, elapsed)
	}
}

func TestWithDistMinRetryDelay(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()
	key := "test-dist-mindelay-opt-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	holderLocks := []*redlock.Lock{redlock.NewLock(rdb)}
	holderDL := redlock.NewDistributedLock(holderLocks)
	if _, err := holderDL.Acquire(ctx, key, testTTLLong); err != nil {
		t.Fatalf("Holder failed to acquire: %v", err)
	}

	minDelay := 200 * time.Millisecond

	// Run
	contenderLocks := []*redlock.Lock{redlock.NewLock(rdb)}
	contenderDL := redlock.NewDistributedLock(contenderLocks,
		redlock.WithDistMinRetryDelay(minDelay),
		redlock.WithDistMaxRetry(testMaxRetrySmall),
		redlock.WithDistMaxJitterDuration(0),
	)

	start := time.Now()
	_, err := contenderDL.Acquire(ctx, key, testTTLLong)
	elapsed := time.Since(start)

	// Assert
	if err == nil {
		t.Fatal("Contender should have failed to acquire")
	}
	expectedMin := minDelay * time.Duration(testMaxRetrySmall)
	if elapsed < expectedMin {
		t.Errorf("Expected duration >= %v, got %v", expectedMin, elapsed)
	}
}

func TestWithDistMaxRetry(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()
	key := "test-dist-maxretry-opt-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	holderLocks := []*redlock.Lock{redlock.NewLock(rdb)}
	holderDL := redlock.NewDistributedLock(holderLocks)
	if _, err := holderDL.Acquire(ctx, key, testTTLLong); err != nil {
		t.Fatalf("Holder failed to acquire: %v", err)
	}

	// Run
	contenderLocks := []*redlock.Lock{redlock.NewLock(rdb)}
	contenderDL := redlock.NewDistributedLock(contenderLocks, redlock.WithDistMaxRetry(0))
	_, err := contenderDL.Acquire(ctx, key, testTTLLong)

	// Assert
	if err == nil {
		t.Error("Expected immediate failure with maxRetry=0")
	}
}
