package redlock_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/trviph/redlock"
)

func TestDistributedLock_Acquire(t *testing.T) {
	rdb1 := setupRedis(t, "6379")
	defer rdb1.Close()

	rdb2 := setupRedis(t, "6380")
	defer rdb2.Close()

	rdb3 := setupRedis(t, "6381")
	defer rdb3.Close()

	locks := []*redlock.Lock{
		redlock.NewLock(rdb1),
		redlock.NewLock(rdb2),
		redlock.NewLock(rdb3),
	}

	dl := redlock.NewDistributedLock(locks)
	ctx := context.Background()
	key := "dist-lock-" + uuid.NewString()

	// 1. Acquire
	fencing, err := dl.Acquire(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if fencing == "" {
		t.Error("Expected valid fencing token")
	}

	// 2. Validate it is held on all (or quorum)
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

	// 3. Release
	err = dl.Release(ctx, key, fencing)
	if err != nil {
		t.Errorf("Release failed: %v", err)
	}

	if rdb1.Exists(ctx, key).Val() != 0 || rdb2.Exists(ctx, key).Val() != 0 || rdb3.Exists(ctx, key).Val() != 0 {
		t.Error("Lock key should be gone after release")
	}
}

func TestDistributedLock_TryAcquire(t *testing.T) {
	rdb := setupRedis(t, "6379")
	defer rdb.Close()
	locks := []*redlock.Lock{redlock.NewLock(rdb)}
	dl := redlock.NewDistributedLock(locks)
	ctx := context.Background()
	key := "dist-try-" + uuid.NewString()

	fencing, err := dl.TryAcquire(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("TryAcquire failed: %v", err)
	}

	err = dl.Release(ctx, key, fencing)
	if err != nil {
		t.Errorf("Release failed: %v", err)
	}
}
