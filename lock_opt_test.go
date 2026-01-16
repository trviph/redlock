package redlock_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/trviph/redlock"
)

func TestWithJitterDuration(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()
	key := "test-jitter-opt-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	holderLock := redlock.NewLock(rdb)
	if _, err := holderLock.Acquire(ctx, key, testTTLLong); err != nil {
		t.Fatalf("Holder failed to acquire: %v", err)
	}

	customJitter := 10 * time.Millisecond

	// Run
	contenderLock := redlock.NewLock(rdb,
		redlock.WithJitterDuration(customJitter),
		redlock.WithMaxRetry(testMaxRetryMedium),
		redlock.WithMinRetryDelay(0),
	)

	start := time.Now()
	_, err := contenderLock.Acquire(ctx, key, testTTLLong)
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

func TestWithMaxRetry(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()
	key := "test-maxretry-opt-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	holderLock := redlock.NewLock(rdb)
	if _, err := holderLock.Acquire(ctx, key, testTTLLong); err != nil {
		t.Fatalf("Holder failed to acquire: %v", err)
	}

	// Run
	contenderLock := redlock.NewLock(rdb, redlock.WithMaxRetry(0))
	_, err := contenderLock.Acquire(ctx, key, testTTLLong)

	// Assert
	if err == nil {
		t.Error("Expected immediate failure with maxRetry=0")
	}
}

func TestWithMinRetryDelay(t *testing.T) {
	// Setup
	rdb := setupRedis(t, testRedisPort1)
	ctx := context.Background()
	key := "test-mindelay-opt-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, key) })

	holderLock := redlock.NewLock(rdb)
	if _, err := holderLock.Acquire(ctx, key, testTTLLong); err != nil {
		t.Fatalf("Holder failed to acquire: %v", err)
	}

	minDelay := 200 * time.Millisecond

	// Run
	contenderLock := redlock.NewLock(rdb,
		redlock.WithMinRetryDelay(minDelay),
		redlock.WithMaxRetry(testMaxRetrySmall),
		redlock.WithJitterDuration(0),
	)

	start := time.Now()
	_, err := contenderLock.Acquire(ctx, key, testTTLLong)
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
