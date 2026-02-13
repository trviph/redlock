package redlock

import (
	"context"
	"testing"
	"time"
)

func TestExponentialWait(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		ew := NewExponentialWait()
		if ew.factor != 2.0 {
			t.Errorf("expected default factor 2.0, got %f", ew.factor)
		}
		if ew.minDelay != 100*time.Millisecond {
			t.Errorf("expected default minDelay 100ms, got %v", ew.minDelay)
		}
	})

	t.Run("NextDelay", func(t *testing.T) {
		minDelay := 10 * time.Millisecond
		maxDelay := 100 * time.Millisecond
		factor := 2.0
		ew := NewExponentialWait(
			WithExpMinDelay(minDelay),
			WithExpMaxDelay(maxDelay),
			WithExpFactor(factor),
		)

		tests := []struct {
			retries int
			want    time.Duration
		}{
			{1, minDelay},     // 1st retry: 10ms * 2^0 = 10ms
			{2, minDelay * 2}, // 2nd retry: 10ms * 2^1 = 20ms
			{3, minDelay * 4}, // 3rd retry: 10ms * 2^2 = 40ms
			{4, minDelay * 8}, // 4th retry: 10ms * 2^3 = 80ms
			{5, maxDelay},     // 5th retry: 10ms * 2^4 = 160ms > 100ms
			{11, maxDelay},    // capped
		}

		for _, tt := range tests {
			got := ew.NextDelay(tt.retries)
			if got != tt.want {
				t.Errorf("NextDelay(%d) = %v, want %v", tt.retries, got, tt.want)
			}
		}
	})

	t.Run("Wait", func(t *testing.T) {
		ew := NewExponentialWait(WithExpMinDelay(1 * time.Millisecond))
		ctx := context.Background()

		// Attempt 0 (immediate)
		waitChan := ew.Wait(ctx, 0)
		select {
		case info := <-waitChan:
			if info.Err != nil {
				t.Errorf("unexpected error at attempt 0: %v", info.Err)
			}
		case <-time.After(10 * time.Millisecond):
			t.Fatal("timeout waiting for attempt 0")
		}

		// Attempt 1 (1ms delay)
		start := time.Now()
		<-ew.Wait(ctx, 1)
		elapsed := time.Since(start)
		if elapsed < 1*time.Millisecond {
			t.Errorf("expected at least 1ms delay, got %v", elapsed)
		}
	})

	t.Run("MaxRetry", func(t *testing.T) {
		ew := NewExponentialWait(WithExpMaxIteration(3), WithExpMinDelay(1*time.Millisecond))
		ctx := context.Background()

		// Should succeed for attempts 0, 1, 2, 3
		for i := 0; i <= 3; i++ {
			if info := <-ew.Wait(ctx, i); info.Err != nil {
				t.Errorf("expected attempt %d to succeed, got error: %v", i, info.Err)
			}
		}

		// Should fail for attempt 4
		if info := <-ew.Wait(ctx, 4); info.Err != ErrMaxRetryExceeded {
			t.Errorf("expected ErrMaxRetryExceeded at attempt 4, got %v", info.Err)
		}
	})
}

func TestJitterWait(t *testing.T) {
	t.Run("NextDelay", func(t *testing.T) {
		minDelay := 10 * time.Millisecond
		maxJitter := 20 * time.Millisecond
		jw := NewJitterWait(
			WithJitterMinDelay(minDelay),
			WithMaxJitterDuration(maxJitter),
		)

		for i := 1; i <= 100; i++ {
			delay := jw.NextDelay(i)
			if delay < minDelay {
				t.Errorf("NextDelay(%d) = %v, want >= %v", i, delay, minDelay)
			}
			if delay >= minDelay+maxJitter {
				t.Errorf("NextDelay(%d) = %v, want < %v", i, delay, minDelay+maxJitter)
			}
		}
	})

	t.Run("Wait", func(t *testing.T) {
		jw := NewJitterWait(WithJitterMinDelay(1 * time.Millisecond))
		ctx := context.Background()

		// Attempt 0 (immediate)
		waitChan := jw.Wait(ctx, 0)
		select {
		case info := <-waitChan:
			if info.Err != nil {
				t.Errorf("unexpected error at attempt 0: %v", info.Err)
			}
		case <-time.After(10 * time.Millisecond):
			t.Fatal("timeout waiting for attempt 0")
		}

		// Attempt 1 (at least minDelay)
		start := time.Now()
		<-jw.Wait(ctx, 1)
		elapsed := time.Since(start)
		if elapsed < 1*time.Millisecond {
			t.Errorf("expected at least 1ms delay, got %v", elapsed)
		}
	})
}
