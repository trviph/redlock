package redlock

import (
	"context"
	"time"
)

// WatchDogOption configures the WatchDog.
type WatchDogOption func(*WatchDog)

// WithErrorCallbacks overrides the context for error callbacks and appends new callbacks.
func WithErrorCallbacks(cbCtx context.Context, callbacks ...WatchDogCallback) WatchDogOption {
	return func(w *WatchDog) {
		w.errCBCtx = cbCtx
		w.errCBs = append(w.errCBs, callbacks...)
	}
}

// WithCallbacks overrides the context for error callbacks and appends new callbacks.
//
// Deprecated: Use [WithErrorCallbacks] instead.
// This function was renamed to clarify that it only handles error callbacks.
// For successful lock extensions, use [WithExtensionCallbacks].
func WithCallbacks(cbCtx context.Context, callbacks ...WatchDogCallback) WatchDogOption {
	return WithErrorCallbacks(cbCtx, callbacks...)
}

// WithExtensionCallbacks overrides the context for extension callbacks and appends new callbacks.
func WithExtensionCallbacks(cbCtx context.Context, callbacks ...WatchDogCallback) WatchDogOption {
	return func(w *WatchDog) {
		w.extCBCtx = cbCtx
		w.onExtCBs = append(w.onExtCBs, callbacks...)
	}
}

// WithItem appends a single lock item to be watched.
// If interval is less than or equal to 0, it defaults to ttl/2.
func WithItem(key, fencing string, ttl, interval time.Duration) WatchDogOption {
	return func(w *WatchDog) {
		if interval <= 0 {
			interval = ttl / 2
		}
		w.items = append(w.items, &WatchItem{
			Key:      key,
			Fencing:  fencing,
			TTL:      ttl,
			Interval: interval,
		})
	}
}

// WithItems appends multiple lock items to be watched.
// If an item's interval is less than or equal to 0, it defaults to ttl/2.
func WithItems(items ...*WatchItem) WatchDogOption {
	return func(w *WatchDog) {
		for _, item := range items {
			if item.Interval <= 0 {
				item.Interval = item.TTL / 2
			}
		}
		w.items = append(w.items, items...)
	}
}
