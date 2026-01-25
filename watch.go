package redlock

import (
	"context"
	"time"
)

// WatchDogCallback is a function that will be called when an error occurs during the watch loop.
// Note that if the context is canceled, the context error will be sent as the last error.
type WatchDogCallback func(ctx context.Context, item *WatchItem, err error)

// WatchItem represents a lock item that needs to be watched and renewed.
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
	lock      Locker
	cbCtx     context.Context
	callbacks []WatchDogCallback
	items     []*WatchItem
}

// NewWatchDog creates a new WatchDog instance with the corresponding lock provider and options.
// For simpler usage, where you don't care about error handling, see [Watch] and [WatchWithInterval].
func NewWatchDog(lock Locker, opts ...WatchDogOption) *WatchDog {
	w := &WatchDog{
		cbCtx: context.Background(),
		lock:  lock,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Run starts the watchdog loop to monitor and extend the locks.
// It runs until the context is canceled.
//
// WARNING(trviph): Do not use context.Background() without a cancel mechanism (e.g. WithCancel),
// otherwise the watchdog will never terminate.
func (w *WatchDog) Run(ctx context.Context) {
	for _, item := range w.items {
		go func(item *WatchItem) {
			ticker := time.NewTicker(item.Interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					for _, cb := range w.callbacks {
						go cb(w.cbCtx, item, ctx.Err())
					}
					return
				case <-ticker.C:
					err := w.lock.TryExtend(ctx, item.Key, item.Fencing, item.TTL)
					if err != nil {
						for _, cb := range w.callbacks {
							go cb(w.cbCtx, item, err)
						}
					}
				}
			}
		}(item)
	}
}

// Watch starts a watchdog goroutine that periodically extends the lock's TTL.
// It is designed to be used for long-running operations where the duration is unknown.
// The watchdog stops when the provided context is canceled.
//
// The watchdog attempts to extend the lock at intervals of TTL/2.
//
// For more control on handling errors (logging, early stopping), use [WatchDog].
//
// WARNING(trviph): Do not use context.Background() without a cancel mechanism (e.g. WithCancel),
// otherwise the watchdog will never terminate.
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

// WatchWithInterval starts a watchdog goroutine that periodically extends the lock's TTL.
// It allows customizing the interval between extension attempts.
// The watchdog stops when the provided context is canceled.
//
// For more control on handling errors (logging, early stopping), use [WatchDog].
//
// WARNING(trviph): Do not use context.Background() without a cancel mechanism (e.g. WithCancel),
// otherwise the watchdog will never terminate.
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
