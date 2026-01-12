package redlock

import (
	"context"
	"time"
)

type DistributedLock struct {
	locks []*Lock
}

func NewDistributedLock(locks []*Lock, opts ...DistributedLockOption) *DistributedLock {
	return &DistributedLock{
		locks: locks,
	}
}

func (dl *DistributedLock) Acquire(ctx context.Context, key string, ttl time.Duration) (fencing string, err error) {
	for _, lock := range dl.locks {
		fencing, err = lock.Acquire(ctx, key, ttl)
		if err == nil {
			return fencing, nil
		}
	}
	return "", err
}

func (dl *DistributedLock) Release(ctx context.Context, key string, fencing string) error {
	for _, lock := range dl.locks {
		if err := lock.Release(ctx, key, fencing); err != nil {
			return err
		}
	}
	return nil
}
