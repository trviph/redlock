package redlock

import (
	"context"
	"time"
)

// WatchDogCallback defines a function executed when an event occurs during the watchdog loop.
// In the event of context cancellation, the context error is passed as the final error.
type WatchDogCallback func(ctx context.Context, item *WatchItem, err error)

// WatchItem defines a specific lock instance monitored and renewed by the [WatchDog].
type WatchItem struct {
	Key           string
	Fencing       string
	TTL, Interval time.Duration
}

// WatchDog is responsible for automatically extending the TTL of locks.
//
// BUG(trviph): The WatchDog works similarly to [Watch]/[WatchWithInterval] and relies on TryExtend.
// When using a [DistributedLock], it suffers from the partial extension issue on quorum failure.
// If [DistributedLock.TryExtend] fails to achieve quorum, the successfully extended instances
// remain locked until TTL expires.
//
// This bug will probably never be fixed. Do not use WatchDog with DistributedLock if you are not
// comfortable with this uncertainty.
type WatchDog struct {
	lock     Locker
	errCBCtx context.Context
	extCBCtx context.Context
	errCBs   []WatchDogCallback
	onExtCBs []WatchDogCallback
	items    []*WatchItem
}

// NewWatchDog creates a new WatchDog instance with the corresponding lock provider and options.
// For simpler usage, where you don't care about error handling, see [Watch] and [WatchWithInterval].
func NewWatchDog(lock Locker, opts ...WatchDogOption) *WatchDog {
	w := &WatchDog{
		lock: lock,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Run starts the watchdog loop to continuously monitor and prolong locks.
// It executes indefinitely until the provided context is cancelled.
//
// WARNING(trviph): Do not pass context.Background() without a cancellation mechanism
// (e.g., context.WithCancel), otherwise the watchdog goroutine will leak and never terminate.
func (w *WatchDog) Run(ctx context.Context) {
	for _, item := range w.items {
		go func(item *WatchItem) {
			ticker := time.NewTicker(item.Interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					callCtx := w.errCBCtx
					if callCtx == nil {
						callCtx = ctx
					}
					for _, cb := range w.errCBs {
						go cb(callCtx, item, ctx.Err())
					}
					return
				case <-ticker.C:
					err := w.lock.TryExtend(ctx, item.Key, item.Fencing, item.TTL)
					if err != nil {
						callCtx := w.errCBCtx
						if callCtx == nil {
							callCtx = ctx
						}
						for _, cb := range w.errCBs {
							go cb(callCtx, item, err)
						}
					} else {
						callCtx := w.extCBCtx
						if callCtx == nil {
							callCtx = ctx
						}
						for _, cb := range w.onExtCBs {
							go cb(callCtx, item, nil)
						}
					}
				}
			}
		}(item)
	}
}

// Watch spawns a background goroutine to periodically prolong a lock's TTL.
// It is intended for operations of an unknown duration and extends the lock at
// an interval of half the TTL. The watchdog terminates only when the context is cancelled.
//
// Use [WatchDog] for advanced error handling, logging, or premature termination.
//
// WARNING(trviph): Do not pass context.Background() without a cancellation mechanism
// (e.g., context.WithCancel), otherwise the watchdog goroutine will leak and never terminate.
//
// BUG(trviph): The Watch function relies on TryExtend. When using a DistributedLock, it suffers
// from the partial extension issue on quorum failure. If DistributedLock.TryExtend fails to
// achieve quorum, the successfully extended instances remain locked until TTL expires.
//
// This bug will probably never be fixed. Do not use Watch with DistributedLock if you are not
// comfortable with this uncertainty.
func Watch(ctx context.Context, locker Locker, key, fencing string, ttl time.Duration) {
	WatchWithInterval(ctx, locker, key, fencing, ttl, ttl/2)
}

// WatchWithInterval spans a background goroutine to periodically prolong a lock's TTL
// using a custom interval between extension attempts.
// The watchdog terminates only when the context is cancelled.
//
// Use [WatchDog] for advanced error handling, logging, or premature termination.
//
// WARNING(trviph): Do not pass context.Background() without a cancellation mechanism
// (e.g., context.WithCancel), otherwise the watchdog goroutine will leak and never terminate.
//
// BUG(trviph): The WatchWithInterval function relies on TryExtend. When using a DistributedLock,
// it suffers from the partial extension issue on quorum failure. If DistributedLock.TryExtend fails to
// achieve quorum, the successfully extended instances remain locked until TTL expires.
//
// This bug will probably never be fixed. Do not use WatchWithInterval with DistributedLock if you are not
// comfortable with this uncertainty.
func WatchWithInterval(ctx context.Context, locker Locker, key, fencing string, ttl, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = locker.TryExtend(ctx, key, fencing, ttl)
			}
		}
	}()
}
