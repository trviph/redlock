package redlock

// LockOption configures a Lock instance.
type LockOption func(*Lock)

// WithWaiter sets the [Waiter] for the lock.
// Default is [JitterWait] with default configuration [DefaultJitterWait].
func WithWaiter(waiter Waiter) LockOption {
	return func(dl *Lock) {
		dl.waiter = waiter
	}
}
